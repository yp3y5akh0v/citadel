use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_core::MERKLE_HASH_SIZE;
use citadel_txn::catalog::TableDescriptor;

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"merkle-test")
        .argon2_profile(Argon2Profile::Iot)
}

const ZERO_HASH: [u8; MERKLE_HASH_SIZE] = [0u8; MERKLE_HASH_SIZE];

/// Walk the catalog B+ tree to find a named table's root page.
fn find_table_root(
    mgr: &citadel_txn::manager::TxnManager,
    table_name: &[u8],
) -> Option<citadel_core::types::PageId> {
    let slot = mgr.current_slot();
    if !slot.catalog_root.is_valid() {
        return None;
    }
    // Walk catalog leaf pages to find the table descriptor
    find_in_btree(mgr, slot.catalog_root, table_name)
}

fn find_in_btree(
    mgr: &citadel_txn::manager::TxnManager,
    root: citadel_core::types::PageId,
    key: &[u8],
) -> Option<citadel_core::types::PageId> {
    use citadel_core::types::PageType;
    let page = mgr.read_page_from_disk(root).ok()?;
    match page.page_type()? {
        PageType::Leaf => {
            for i in 0..page.num_cells() {
                let cell = citadel_page::leaf_node::read_cell(&page, i);
                if cell.key == key && cell.value.len() >= 20 {
                    let desc = TableDescriptor::deserialize(cell.value);
                    return Some(desc.root_page);
                }
            }
            None
        }
        PageType::Branch => {
            for i in 0..page.num_cells() {
                let cell = citadel_page::branch_node::read_cell(&page, i);
                if key < cell.key {
                    return find_in_btree(mgr, cell.child, key);
                }
            }
            let right = page.right_child();
            if right.is_valid() {
                find_in_btree(mgr, right, key)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn read_merkle_hash(
    mgr: &citadel_txn::manager::TxnManager,
    page_id: citadel_core::types::PageId,
) -> [u8; MERKLE_HASH_SIZE] {
    mgr.read_page_from_disk(page_id).unwrap().merkle_hash()
}

#[test]
fn named_table_has_nonzero_merkle_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"users").unwrap();
    wtx.table_insert(b"users", b"key1", b"val1").unwrap();
    wtx.commit().unwrap();

    let root = find_table_root(db.manager(), b"users")
        .expect("table should exist in catalog");
    let hash = read_merkle_hash(db.manager(), root);
    assert_ne!(hash, ZERO_HASH, "named table root must have non-zero Merkle hash");
}

#[test]
fn named_table_merkle_changes_on_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.table_insert(b"data", b"k1", b"v1").unwrap();
    wtx.commit().unwrap();

    let root1 = find_table_root(db.manager(), b"data").unwrap();
    let hash1 = read_merkle_hash(db.manager(), root1);

    let mut wtx = db.begin_write().unwrap();
    wtx.table_insert(b"data", b"k2", b"v2").unwrap();
    wtx.commit().unwrap();

    let root2 = find_table_root(db.manager(), b"data").unwrap();
    let hash2 = read_merkle_hash(db.manager(), root2);

    assert_ne!(hash1, hash2, "Merkle hash must change when table data changes");
}

#[test]
fn named_table_merkle_stable_when_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"stable").unwrap();
    wtx.table_insert(b"stable", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let root1 = find_table_root(db.manager(), b"stable").unwrap();
    let hash1 = read_merkle_hash(db.manager(), root1);

    // Commit that only touches the default tree, not "stable"
    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"default_key", b"default_val").unwrap();
    wtx.commit().unwrap();

    // Table root may have been CoW'd if catalog changed, but hash should be same
    let root2 = find_table_root(db.manager(), b"stable").unwrap();
    let hash2 = read_merkle_hash(db.manager(), root2);

    assert_eq!(hash1, hash2, "Merkle hash must not change when table is untouched");
}

#[test]
fn two_tables_different_merkle_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"alpha").unwrap();
    wtx.create_table(b"beta").unwrap();
    wtx.table_insert(b"alpha", b"k", b"alpha_value").unwrap();
    wtx.table_insert(b"beta", b"k", b"beta_value").unwrap();
    wtx.commit().unwrap();

    let root_a = find_table_root(db.manager(), b"alpha").unwrap();
    let root_b = find_table_root(db.manager(), b"beta").unwrap();
    let hash_a = read_merkle_hash(db.manager(), root_a);
    let hash_b = read_merkle_hash(db.manager(), root_b);

    assert_ne!(hash_a, ZERO_HASH);
    assert_ne!(hash_b, ZERO_HASH);
    assert_ne!(hash_a, hash_b, "tables with different data must have different hashes");
}

#[test]
fn named_table_merkle_does_not_affect_default_tree() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"dk", b"dv").unwrap();
    wtx.commit().unwrap();
    let default_hash = db.stats().merkle_root;

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"tbl").unwrap();
    wtx.table_insert(b"tbl", b"tk", b"tv").unwrap();
    wtx.commit().unwrap();

    assert_eq!(db.stats().merkle_root, default_hash,
        "named table operations must not change default tree merkle root");
}

#[test]
fn catalog_tree_has_nonzero_merkle_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    wtx.create_table(b"cat_test").unwrap();
    wtx.table_insert(b"cat_test", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let catalog_root = db.manager().current_slot().catalog_root;
    assert!(catalog_root.is_valid(), "catalog root must be valid after creating a table");
    let catalog_hash = read_merkle_hash(db.manager(), catalog_root);
    assert_ne!(catalog_hash, ZERO_HASH, "catalog tree root must have non-zero Merkle hash");
}
