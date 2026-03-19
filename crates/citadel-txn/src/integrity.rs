use std::collections::HashSet;

use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::Result;
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

use crate::catalog::TableDescriptor;
use crate::manager::TxnManager;

#[derive(Debug)]
pub struct IntegrityReport {
    pub pages_checked: u64,
    pub errors: Vec<IntegrityError>,
}

impl IntegrityReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug)]
pub enum IntegrityError {
    PageReadFailed {
        page: PageId,
        error: String,
    },
    KeyOrderViolation {
        page: PageId,
        index: usize,
    },
    DuplicatePageRef(PageId),
    EntryCountMismatch {
        expected: u64,
        actual: u64,
    },
    InvalidPageType {
        page: PageId,
        expected: &'static str,
    },
}

pub(crate) fn run_integrity_check(mgr: &TxnManager) -> Result<IntegrityReport> {
    let slot = mgr.current_slot();
    let mut visited = HashSet::new();
    let mut errors = Vec::new();
    let mut pages_checked: u64 = 0;

    // Walk the default tree
    let default_count = walk_tree(
        mgr,
        slot.tree_root,
        &mut visited,
        &mut errors,
        &mut pages_checked,
    );

    // Check default table entry count
    if default_count != slot.tree_entries {
        errors.push(IntegrityError::EntryCountMismatch {
            expected: slot.tree_entries,
            actual: default_count,
        });
    }

    // Walk catalog tree and named tables
    if slot.catalog_root.is_valid() {
        walk_catalog(
            mgr,
            slot.catalog_root,
            &mut visited,
            &mut errors,
            &mut pages_checked,
        );
    }

    // Walk pending-free chain
    if slot.pending_free_root.is_valid() {
        walk_chain(
            mgr,
            slot.pending_free_root,
            &mut visited,
            &mut errors,
            &mut pages_checked,
        );
    }

    Ok(IntegrityReport {
        pages_checked,
        errors,
    })
}

fn walk_tree(
    mgr: &TxnManager,
    root: PageId,
    visited: &mut HashSet<PageId>,
    errors: &mut Vec<IntegrityError>,
    pages_checked: &mut u64,
) -> u64 {
    let mut entry_count: u64 = 0;
    let mut stack = vec![root];

    while let Some(page_id) = stack.pop() {
        if !visited.insert(page_id) {
            errors.push(IntegrityError::DuplicatePageRef(page_id));
            continue;
        }

        let page = match mgr.read_page_from_disk(page_id) {
            Ok(p) => p,
            Err(e) => {
                errors.push(IntegrityError::PageReadFailed {
                    page: page_id,
                    error: e.to_string(),
                });
                continue;
            }
        };
        *pages_checked += 1;

        match page.page_type() {
            Some(PageType::Leaf) => {
                let n = page.num_cells();
                entry_count += n as u64;
                check_leaf_ordering(&page, page_id, errors);
            }
            Some(PageType::Branch) => {
                for i in 0..page.num_cells() as usize {
                    stack.push(branch_node::get_child(&page, i));
                }
                let right = page.right_child();
                if right.is_valid() {
                    stack.push(right);
                }
            }
            _ => {
                errors.push(IntegrityError::InvalidPageType {
                    page: page_id,
                    expected: "Leaf or Branch",
                });
            }
        }
    }

    entry_count
}

fn check_leaf_ordering(page: &Page, page_id: PageId, errors: &mut Vec<IntegrityError>) {
    let n = page.num_cells();
    for i in 1..n {
        let prev = leaf_node::read_cell(page, i - 1);
        let curr = leaf_node::read_cell(page, i);
        if prev.key >= curr.key {
            errors.push(IntegrityError::KeyOrderViolation {
                page: page_id,
                index: i as usize,
            });
        }
    }
}

fn walk_catalog(
    mgr: &TxnManager,
    catalog_root: PageId,
    visited: &mut HashSet<PageId>,
    errors: &mut Vec<IntegrityError>,
    pages_checked: &mut u64,
) {
    // First walk the catalog tree itself
    let table_roots = collect_named_table_roots(mgr, catalog_root, visited, errors, pages_checked);

    // Then walk each named table tree
    for root in table_roots {
        walk_tree(mgr, root, visited, errors, pages_checked);
    }
}

fn collect_named_table_roots(
    mgr: &TxnManager,
    catalog_root: PageId,
    visited: &mut HashSet<PageId>,
    errors: &mut Vec<IntegrityError>,
    pages_checked: &mut u64,
) -> Vec<PageId> {
    let mut roots = Vec::new();
    let mut stack = vec![catalog_root];

    while let Some(page_id) = stack.pop() {
        if !visited.insert(page_id) {
            errors.push(IntegrityError::DuplicatePageRef(page_id));
            continue;
        }

        let page = match mgr.read_page_from_disk(page_id) {
            Ok(p) => p,
            Err(e) => {
                errors.push(IntegrityError::PageReadFailed {
                    page: page_id,
                    error: e.to_string(),
                });
                continue;
            }
        };
        *pages_checked += 1;

        match page.page_type() {
            Some(PageType::Leaf) => {
                for i in 0..page.num_cells() {
                    let cell = leaf_node::read_cell(&page, i);
                    if cell.val_type != ValueType::Tombstone && cell.value.len() >= 4 {
                        let desc = TableDescriptor::deserialize(cell.value);
                        if desc.root_page.is_valid() {
                            roots.push(desc.root_page);
                        }
                    }
                }
            }
            Some(PageType::Branch) => {
                for i in 0..page.num_cells() as usize {
                    stack.push(branch_node::get_child(&page, i));
                }
                let right = page.right_child();
                if right.is_valid() {
                    stack.push(right);
                }
            }
            _ => {
                errors.push(IntegrityError::InvalidPageType {
                    page: page_id,
                    expected: "Leaf or Branch (catalog)",
                });
            }
        }
    }

    roots
}

fn walk_chain(
    mgr: &TxnManager,
    root: PageId,
    visited: &mut HashSet<PageId>,
    errors: &mut Vec<IntegrityError>,
    pages_checked: &mut u64,
) {
    let mut current = root;
    while current.is_valid() {
        if !visited.insert(current) {
            errors.push(IntegrityError::DuplicatePageRef(current));
            break;
        }

        let page = match mgr.read_page_from_disk(current) {
            Ok(p) => p,
            Err(e) => {
                errors.push(IntegrityError::PageReadFailed {
                    page: current,
                    error: e.to_string(),
                });
                break;
            }
        };
        *pages_checked += 1;

        current = page.right_child();
    }
}
