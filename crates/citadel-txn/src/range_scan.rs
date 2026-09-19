//! Shared range traversal with one independent handle per visited leaf.

use citadel_buffer::cursor::{Cursor, PageLoader};
use citadel_core::{CancelToken, PageId, Result, ValueType};
use citadel_page::leaf_node::{self, OverflowRef};
use citadel_page::page::Page;

use crate::{overflow_io, ReadBudget};

/// The handle must remain valid while loading other pages through this view.
/// Read snapshots pin an Arc; writer scans can also borrow an owned page for
/// the complete read-only scan. Neither handle borrows the mutable loader.
pub(crate) trait LeafLoader: PageLoader {
    type Leaf: AsRef<Page>;

    fn load_leaf(&mut self, id: PageId) -> Result<Self::Leaf>;
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn scan_from<const CHECK_EACH_ROW: bool, P, F>(
    view: &mut P,
    root: PageId,
    start_key: &[u8],
    prefix: Option<&[u8]>,
    cancel: Option<&CancelToken>,
    budget: Option<&ReadBudget>,
    rows_scanned: &mut u64,
    mut f: F,
) -> Result<()>
where
    P: LeafLoader,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut cursor = Cursor::seek_lazy(view, root, start_key)?;
    while cursor.is_valid() {
        if !CHECK_EACH_ROW {
            if let Some(token) = cancel {
                token.check()?;
            }
        }
        let leaf = view.load_leaf(cursor.leaf_page_id())?;
        let page = leaf.as_ref();
        let cells = page.num_cells();
        for index in cursor.cell_index()..cells {
            let cell = leaf_node::read_cell(page, index);
            if CHECK_EACH_ROW {
                if let Some(token) = cancel {
                    token.check()?;
                }
            }
            if prefix.is_some_and(|prefix| !cell.key.starts_with(prefix)) {
                return Ok(());
            }
            *rows_scanned += 1;
            let keep_scanning = match cell.val_type {
                ValueType::Tombstone => true,
                ValueType::Inline => {
                    if let Some(budget) = budget {
                        budget.try_charge(cell.value.len())?;
                    }
                    f(cell.key, cell.value)?
                }
                ValueType::Overflow => {
                    let reference = OverflowRef::from_bytes(cell.value);
                    let value = overflow_io::read_chain_value_with_budget(
                        view, &reference, cancel, budget,
                    )?;
                    f(cell.key, &value)?
                }
            };
            if !keep_scanning {
                return Ok(());
            }
        }
        cursor.set_cell_index(cells);
        if !cursor.advance_to_next_leaf(view)? {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "range_scan_tests.rs"]
mod tests;
