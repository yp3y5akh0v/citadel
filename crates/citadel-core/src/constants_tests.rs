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
    const CELL_POINTER_SIZE: usize = 2;
    let max_branch_cell = 4 + 2 + MAX_KEY_SIZE;
    let max_leaf_cell = 2 + 4 + MAX_KEY_SIZE + 1 + MAX_INLINE_VALUE_SIZE;
    let max_cell = max_branch_cell.max(max_leaf_cell);
    assert!(
        2 * (max_cell + CELL_POINTER_SIZE) <= USABLE_SIZE,
        "2 cells and their pointers must fit in one page"
    );
}

#[test]
fn file_header_fields_do_not_overlap() {
    assert_eq!(
        COMMIT_SLOT_OFFSET,
        FILE_ID_OFFSET + core::mem::size_of::<u64>()
    );
    assert_eq!(COMMIT_SLOT_OFFSET + 2 * COMMIT_SLOT_SIZE, FILE_HEADER_SIZE);
}

#[test]
fn pending_free_entries_per_page() {
    assert_eq!(
        PENDING_FREE_ENTRIES_PER_PAGE,
        (USABLE_SIZE - core::mem::size_of::<u32>()) / PENDING_FREE_ENTRY_SIZE
    );
    assert_eq!(PENDING_FREE_ENTRIES_PER_PAGE, 674);
}

#[test]
fn persistent_magic_values_are_frozen() {
    assert_eq!(MAGIC, 0xC17A_D3E1);
    assert_eq!(KEY_FILE_MAGIC, 0x4B45_5953);
    assert_eq!(REGION_STORE_MAGIC, 0x5247_4E53);
    assert_eq!(ATOM_STORE_MAGIC, 0x4154_4D53);
    assert_eq!(KEY_BACKUP_MAGIC, 0x4B45_5942);
    assert_eq!(AUDIT_LOG_MAGIC, 0x4155_4454);
    assert_eq!(AUDIT_ENTRY_MAGIC, 0x454E_5452);
    assert_eq!(SLOT_MARKER_V1, 0xC17A);
}

#[test]
fn persistent_format_versions_are_frozen() {
    assert_eq!(FORMAT_VERSION, 1);
    assert_eq!(KEY_FILE_VERSION, 1);
    assert_eq!(REGION_STORE_VERSION, 1);
    assert_eq!(ATOM_STORE_VERSION, 1);
    assert_eq!(KEY_BACKUP_VERSION, 1);
    assert_eq!(AUDIT_LOG_VERSION_LEGACY, 1);
    assert_eq!(AUDIT_LOG_VERSION, 2);
    assert_eq!(SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1, 1);
}
