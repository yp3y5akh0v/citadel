use super::*;

#[test]
fn page_size_consistency() {
    assert_eq!(PAGE_SIZE, IV_SIZE + BODY_SIZE + MAC_SIZE);
}

#[test]
fn usable_size_consistency() {
    assert_eq!(USABLE_SIZE, BODY_SIZE - PAGE_HEADER_SIZE);
    assert_eq!(USABLE_SIZE, 8096);
}

#[test]
fn two_cells_per_page_invariant() {
    let max_branch_cell = 4 + 2 + MAX_KEY_SIZE;
    let max_leaf_cell = 2 + 4 + MAX_KEY_SIZE + 1 + MAX_INLINE_VALUE_SIZE;
    let max_cell = max_branch_cell.max(max_leaf_cell);
    assert!(2 * max_cell <= USABLE_SIZE, "2 cells must fit in one page");
}

#[test]
fn file_header_fits() {
    let needed = COMMIT_SLOT_OFFSET + 2 * COMMIT_SLOT_SIZE;
    assert!(
        needed <= FILE_HEADER_SIZE,
        "commit slots must fit in header"
    );
}

#[test]
fn pending_free_entries_per_page() {
    assert_eq!(PENDING_FREE_ENTRIES_PER_PAGE, 674);
}
