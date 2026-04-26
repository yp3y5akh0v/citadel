use super::*;

#[test]
fn diff_entry_equality() {
    let a = DiffEntry {
        key: b"key1".to_vec(),
        value: b"val1".to_vec(),
        val_type: 0,
    };
    let b = DiffEntry {
        key: b"key1".to_vec(),
        value: b"val1".to_vec(),
        val_type: 0,
    };
    let c = DiffEntry {
        key: b"key1".to_vec(),
        value: b"val2".to_vec(),
        val_type: 0,
    };
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn diff_result_empty() {
    let r = DiffResult {
        entries: vec![],
        pages_compared: 0,
        subtrees_skipped: 0,
    };
    assert!(r.is_empty());
    assert_eq!(r.len(), 0);
}

#[test]
fn diff_result_non_empty() {
    let r = DiffResult {
        entries: vec![DiffEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            val_type: 0,
        }],
        pages_compared: 1,
        subtrees_skipped: 0,
    };
    assert!(!r.is_empty());
    assert_eq!(r.len(), 1);
}

#[test]
fn page_digest_leaf_has_no_children() {
    let d = PageDigest {
        page_id: PageId(0),
        page_type: PageType::Leaf,
        merkle_hash: [0u8; MERKLE_HASH_SIZE],
        children: vec![],
    };
    assert!(d.children.is_empty());
}

#[test]
fn page_digest_branch_has_children() {
    let d = PageDigest {
        page_id: PageId(0),
        page_type: PageType::Branch,
        merkle_hash: [1u8; MERKLE_HASH_SIZE],
        children: vec![PageId(1), PageId(2), PageId(3)],
    };
    assert_eq!(d.children.len(), 3);
}
