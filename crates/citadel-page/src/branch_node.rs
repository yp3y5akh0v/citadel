//! Branch (interior) node operations for the B+ tree.
//!
//! Branch cell format: `[child: u32][key_len: u16][key_data: var]`
//! Total cell size: 6 + key_len
//!
//! A branch node with n cells has n+1 children:
//! - cell[i].child handles keys where key[i-1] <= k < key[i] (key[-1] = -∞)
//! - right_child handles keys where key[n-1] <= k

use crate::page::{checked_cell_offsets, validate_cell_layout, CellDecodeError, CellSpan, Page};
use citadel_core::types::{PageId, PageType};
use citadel_core::BODY_SIZE;

/// Size of fixed fields in a branch cell (child: 4 + key_len: 2).
const BRANCH_CELL_FIXED: usize = 6;

#[derive(Debug, Clone, Copy)]
pub struct BranchCell<'a> {
    pub child: PageId,
    pub key: &'a [u8],
}

pub fn read_cell(page: &Page, i: u16) -> BranchCell<'_> {
    let offset = page.cell_offset(i) as usize;
    let child = u32::from_le_bytes(page.data[offset..offset + 4].try_into().unwrap());
    let key_len =
        u16::from_le_bytes(page.data[offset + 4..offset + 6].try_into().unwrap()) as usize;
    let key = &page.data[offset + 6..offset + 6 + key_len];
    BranchCell {
        child: PageId(child),
        key,
    }
}

/// Decode and validate every branch cell without unchecked indexing. Beyond the
/// shared layout checks, this enforces the invariants a caller needs before
/// following child pointers: separators strictly ordered, and every child valid,
/// unique within the page, and not a self-reference.
pub fn read_cells_checked(page: &Page) -> Result<Vec<BranchCell<'_>>, CellDecodeError> {
    if page.page_type() != Some(PageType::Branch) {
        return Err(CellDecodeError::new(format!(
            "checked branch decode received page type {}",
            page.page_type_raw()
        )));
    }

    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for (index, offset) in offsets.enumerate() {
        let fixed_end = offset.checked_add(BRANCH_CELL_FIXED).ok_or_else(|| {
            CellDecodeError::new(format!("branch cell {index} header length overflows"))
        })?;
        if fixed_end > BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "branch cell {index} header ends at {fixed_end}, beyond page body {BODY_SIZE}"
            )));
        }
        let child = PageId(u32::from_le_bytes([
            page.data[offset],
            page.data[offset + 1],
            page.data[offset + 2],
            page.data[offset + 3],
        ]));
        let key_len = u16::from_le_bytes([page.data[offset + 4], page.data[offset + 5]]) as usize;
        let end = fixed_end.checked_add(key_len).ok_or_else(|| {
            CellDecodeError::new(format!("branch cell {index} key length overflows"))
        })?;
        if end > BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "branch cell {index} key ends at {end}, beyond page body {BODY_SIZE}"
            )));
        }
        spans.push(CellSpan {
            index,
            start: offset,
            end,
        });
        cells.push(BranchCell {
            child,
            key: &page.data[fixed_end..end],
        });
    }
    validate_cell_layout(page, &mut spans)?;

    if let Some((index, _)) = cells
        .windows(2)
        .enumerate()
        .find(|(_, pair)| pair[0].key >= pair[1].key)
    {
        return Err(CellDecodeError::new(format!(
            "branch separator keys {index} and {} are not strictly ordered",
            index + 1
        )));
    }

    let own_page = page.page_id();
    let mut children = Vec::with_capacity(cells.len() + 1);
    for (index, child) in cells
        .iter()
        .map(|cell| cell.child)
        .chain(std::iter::once(page.right_child()))
        .enumerate()
    {
        if !child.is_valid() {
            return Err(CellDecodeError::new(format!(
                "branch child {index} is invalid"
            )));
        }
        if child == own_page {
            return Err(CellDecodeError::new(format!(
                "branch child {index} points back to page {own_page}"
            )));
        }
        children.push(child);
    }
    children.sort_unstable();
    if let Some(pair) = children.windows(2).find(|pair| pair[0] == pair[1]) {
        let child = pair[0];
        return Err(CellDecodeError::new(format!(
            "branch child duplicates page {child}"
        )));
    }

    Ok(cells)
}

/// Get the total byte size of a branch cell on disk.
pub fn cell_size(key_len: usize) -> usize {
    BRANCH_CELL_FIXED + key_len
}

/// Build a branch cell into a byte buffer.
pub fn build_cell(child: PageId, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(BRANCH_CELL_FIXED + key.len());
    buf.extend_from_slice(&child.as_u32().to_le_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

pub fn get_cell_size(page: &Page, i: u16) -> usize {
    let offset = page.cell_offset(i) as usize;
    let key_len =
        u16::from_le_bytes(page.data[offset + 4..offset + 6].try_into().unwrap()) as usize;
    BRANCH_CELL_FIXED + key_len
}

/// Binary search returning the child index (0..=num_cells).
/// Index num_cells means right_child.
pub fn search_child_index(page: &Page, search_key: &[u8]) -> usize {
    let n = page.num_cells() as usize;
    // Binary search for the first key > search_key
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = read_cell(page, mid as u16);
        if search_key < cell.key {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Get the child PageId at a given child index (0..=num_cells).
pub fn get_child(page: &Page, child_idx: usize) -> PageId {
    let n = page.num_cells() as usize;
    if child_idx < n {
        read_cell(page, child_idx as u16).child
    } else {
        page.right_child()
    }
}

/// Insert a separator key and right child into the branch page.
///
/// After a child at `child_idx` splits, we need to insert the separator.
/// The left child stays at `child_idx`, and we insert (right_child, sep_key).
///
/// For child_idx < num_cells:
///   - old cell[child_idx] had (old_child, old_key)
///   - We set cell[child_idx] = (left_child, sep_key)
///   - We insert new cell (right_child, old_key) at child_idx + 1
///
/// For child_idx == num_cells (right_child split):
///   - We append cell (left_child, sep_key)
///   - We set right_child = right_child
///
/// Returns false if not enough space.
pub fn insert_separator(
    page: &mut Page,
    child_idx: usize,
    left_child: PageId,
    sep_key: &[u8],
    right_child: PageId,
) -> bool {
    let n = page.num_cells() as usize;

    if child_idx < n {
        // Read old key before modifying
        let old_cell = read_cell(page, child_idx as u16);
        let old_key = old_cell.key.to_vec();
        let old_cell_size = get_cell_size(page, child_idx as u16);

        // Build the new cell that replaces cell[child_idx]
        let new_cell = build_cell(left_child, sep_key);
        // Build the cell to insert at child_idx + 1
        let insert_cell = build_cell(right_child, &old_key);

        // Check if we have space for the size difference + new cell + pointer
        // We need: new_cell.len - old_cell_size + insert_cell.len + 2 (for new ptr)
        if page.available_space() + old_cell_size < new_cell.len() + insert_cell.len() + 2 {
            // Not enough space - caller must split this branch
            return false;
        }

        // Strategy: rebuild all cells to avoid fragmentation issues.
        // Collect all cells, modify, and rebuild.
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(n + 1);
        for i in 0..n {
            if i == child_idx {
                cells.push(new_cell.clone());
            } else {
                let c = read_cell(page, i as u16);
                cells.push(build_cell(c.child, c.key));
            }
        }
        cells.insert(child_idx + 1, insert_cell);

        let rc = page.right_child();
        let cell_refs: Vec<&[u8]> = cells.iter().map(|c| c.as_slice()).collect();
        page.rebuild_cells(&cell_refs);
        page.set_right_child(rc);
    } else {
        // right_child split
        let new_cell = build_cell(left_child, sep_key);
        if page.available_space() < new_cell.len() {
            return false;
        }
        page.write_cell(&new_cell);
        page.set_right_child(right_child);
    }

    true
}

/// Split a branch page. Returns (separator_key, right_page_cells, right_child).
///
/// The split point is at num_cells / 2.
/// - Left page keeps cells [0..split_point], right_child = promoted_cell.child
/// - Promoted: key = cells[split_point].key
/// - Right page gets cells [split_point+1..n], right_child = old right_child
pub fn split(page: &Page) -> (Vec<u8>, Vec<Vec<u8>>, PageId, PageId) {
    let n = page.num_cells() as usize;
    let split_point = n / 2;

    let promoted = read_cell(page, split_point as u16);
    let sep_key = promoted.key.to_vec();
    let promoted_child = promoted.child; // becomes left page's right_child

    let mut right_cells = Vec::with_capacity(n - split_point - 1);
    for i in (split_point + 1)..n {
        let c = read_cell(page, i as u16);
        right_cells.push(build_cell(c.child, c.key));
    }

    let old_right_child = page.right_child(); // becomes right page's right_child

    (sep_key, right_cells, promoted_child, old_right_child)
}

#[cfg(test)]
#[path = "branch_node_tests.rs"]
mod tests;
