//! Inline Merkle hashing for sync diff detection.
//!
//! Each page stores a BLAKE3 hash (28 bytes) in its header at [36..64].
//! - Leaf pages: hash of all cell contents (key-value entries in sorted order)
//! - Branch pages: hash of all children's Merkle hashes concatenated
//!
//! The root page's hash serves as a database fingerprint — if two snapshots
//! have the same root hash, they contain identical data.

use std::collections::HashMap;

use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{Result, MERKLE_HASH_SIZE};
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

pub fn compute_tree_merkle(
    pages: &mut HashMap<PageId, Page>,
    root: PageId,
    txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    compute_page_merkle(pages, root, txn_id, read_clean_hash)
}

fn compute_page_merkle(
    pages: &mut HashMap<PageId, Page>,
    page_id: PageId,
    txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    // Page not in write set — it's clean, just read its hash
    let page = match pages.get(&page_id) {
        Some(page) => page,
        None => return read_clean_hash(page_id),
    };

    // Clean page in HashMap — hash already valid in header
    if page.txn_id() != txn_id {
        return Ok(page.merkle_hash());
    }

    // Dirty page — compute fresh hash
    let page_type = page.page_type();
    let hash = match page_type {
        Some(PageType::Leaf) => compute_leaf_hash(page),
        Some(PageType::Branch) => {
            // Collect child page IDs first (avoid borrow conflict)
            let num_cells = page.num_cells();
            let mut children: Vec<PageId> = Vec::with_capacity(num_cells as usize + 1);
            for i in 0..num_cells as usize {
                children.push(branch_node::get_child(page, i));
            }
            let right = page.right_child();
            if right.is_valid() {
                children.push(right);
            }

            // Recursively compute children's hashes
            let mut hasher = blake3::Hasher::new();
            for child_id in children {
                let child_hash = compute_page_merkle(pages, child_id, txn_id, read_clean_hash)?;
                hasher.update(&child_hash);
            }
            truncate_hash(&hasher.finalize())
        }
        _ => [0u8; MERKLE_HASH_SIZE],
    };

    // Store hash in page header
    let page = pages.get_mut(&page_id).unwrap();
    page.set_merkle_hash(&hash);

    Ok(hash)
}

/// Compute the Merkle hash for a leaf page from its cell contents.
///
/// Hash input: for each cell in key order:
///   key_len (u16 LE) || key || val_type (u8) || val_len (u32 LE) || value
fn compute_leaf_hash(page: &Page) -> [u8; MERKLE_HASH_SIZE] {
    let mut hasher = blake3::Hasher::new();
    let num_cells = page.num_cells();

    for i in 0..num_cells {
        let cell = leaf_node::read_cell(page, i);
        hasher.update(&(cell.key.len() as u16).to_le_bytes());
        hasher.update(cell.key);
        hasher.update(&[cell.val_type as u8]);
        hasher.update(&(cell.value.len() as u32).to_le_bytes());
        hasher.update(cell.value);
    }

    truncate_hash(&hasher.finalize())
}

/// Truncate a 32-byte BLAKE3 hash to MERKLE_HASH_SIZE (28 bytes).
fn truncate_hash(hash: &blake3::Hash) -> [u8; MERKLE_HASH_SIZE] {
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_core::types::{PageType, ValueType};
    use citadel_page::leaf_node;

    fn make_leaf(page_id: PageId, txn_id: TxnId, entries: &[(&[u8], &[u8])]) -> Page {
        let mut page = Page::new(page_id, PageType::Leaf, txn_id);
        for (key, value) in entries {
            let cell = leaf_node::build_cell(key, ValueType::Inline, value);
            page.insert_cell_at(page.num_cells(), &cell);
        }
        page
    }

    fn make_branch(
        page_id: PageId,
        txn_id: TxnId,
        children: &[(PageId, &[u8])],
        right_child: PageId,
    ) -> Page {
        let mut page = Page::new(page_id, PageType::Branch, txn_id);
        for (child, sep_key) in children {
            let cell = citadel_page::branch_node::build_cell(*child, sep_key);
            page.insert_cell_at(page.num_cells(), &cell);
        }
        page.set_right_child(right_child);
        page
    }

    #[test]
    fn leaf_hash_deterministic() {
        let txn = TxnId(1);
        let leaf = make_leaf(PageId(0), txn, &[(b"key1", b"val1"), (b"key2", b"val2")]);
        let h1 = compute_leaf_hash(&leaf);

        let leaf2 = make_leaf(PageId(0), txn, &[(b"key1", b"val1"), (b"key2", b"val2")]);
        let h2 = compute_leaf_hash(&leaf2);

        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; MERKLE_HASH_SIZE]);
    }

    #[test]
    fn leaf_hash_changes_with_data() {
        let txn = TxnId(1);
        let leaf1 = make_leaf(PageId(0), txn, &[(b"key1", b"val1")]);
        let leaf2 = make_leaf(PageId(0), txn, &[(b"key1", b"val2")]);
        let leaf3 = make_leaf(PageId(0), txn, &[(b"key2", b"val1")]);

        let h1 = compute_leaf_hash(&leaf1);
        let h2 = compute_leaf_hash(&leaf2);
        let h3 = compute_leaf_hash(&leaf3);

        assert_ne!(h1, h2);
        assert_ne!(h1, h3);
        assert_ne!(h2, h3);
    }

    #[test]
    fn empty_leaf_hash() {
        let leaf = make_leaf(PageId(0), TxnId(1), &[]);
        let h = compute_leaf_hash(&leaf);
        assert_ne!(h, [0u8; MERKLE_HASH_SIZE]);
    }

    #[test]
    fn single_leaf_tree() {
        let txn = TxnId(1);
        let leaf = make_leaf(PageId(0), txn, &[(b"a", b"1")]);

        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), leaf);

        let root_hash = compute_tree_merkle(&mut pages, PageId(0), txn, &|_| {
            panic!("should not read disk")
        })
        .unwrap();

        assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);

        // Hash should be stored in page header
        assert_eq!(pages[&PageId(0)].merkle_hash(), root_hash);
    }

    #[test]
    fn branch_hash_from_children() {
        let txn = TxnId(1);
        let left = make_leaf(PageId(1), txn, &[(b"a", b"1")]);
        let right = make_leaf(PageId(2), txn, &[(b"c", b"3")]);
        let branch = make_branch(PageId(0), txn, &[(PageId(1), b"b")], PageId(2));

        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), branch);
        pages.insert(PageId(1), left);
        pages.insert(PageId(2), right);

        let root_hash = compute_tree_merkle(&mut pages, PageId(0), txn, &|_| {
            panic!("should not read disk")
        })
        .unwrap();

        assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);

        // Verify all pages got their hashes set
        assert_ne!(pages[&PageId(0)].merkle_hash(), [0u8; MERKLE_HASH_SIZE]);
        assert_ne!(pages[&PageId(1)].merkle_hash(), [0u8; MERKLE_HASH_SIZE]);
        assert_ne!(pages[&PageId(2)].merkle_hash(), [0u8; MERKLE_HASH_SIZE]);
    }

    #[test]
    fn branch_hash_changes_when_child_changes() {
        let txn = TxnId(1);

        // Tree 1: left has "a"="1"
        let left1 = make_leaf(PageId(1), txn, &[(b"a", b"1")]);
        let right1 = make_leaf(PageId(2), txn, &[(b"c", b"3")]);
        let branch1 = make_branch(PageId(0), txn, &[(PageId(1), b"b")], PageId(2));

        let mut pages1: HashMap<PageId, Page> = HashMap::new();
        pages1.insert(PageId(0), branch1);
        pages1.insert(PageId(1), left1);
        pages1.insert(PageId(2), right1);

        let h1 = compute_tree_merkle(&mut pages1, PageId(0), txn, &|_| panic!("no disk")).unwrap();

        // Tree 2: left has "a"="2" (different value)
        let left2 = make_leaf(PageId(1), txn, &[(b"a", b"2")]);
        let right2 = make_leaf(PageId(2), txn, &[(b"c", b"3")]);
        let branch2 = make_branch(PageId(0), txn, &[(PageId(1), b"b")], PageId(2));

        let mut pages2: HashMap<PageId, Page> = HashMap::new();
        pages2.insert(PageId(0), branch2);
        pages2.insert(PageId(1), left2);
        pages2.insert(PageId(2), right2);

        let h2 = compute_tree_merkle(&mut pages2, PageId(0), txn, &|_| panic!("no disk")).unwrap();

        assert_ne!(h1, h2);
    }

    #[test]
    fn clean_page_uses_existing_hash() {
        let dirty_txn = TxnId(5);
        let clean_txn = TxnId(3); // older txn = clean

        // Create a clean leaf with a pre-set hash
        let mut clean_leaf = make_leaf(PageId(2), clean_txn, &[(b"x", b"y")]);
        let expected_hash = [0xAB; MERKLE_HASH_SIZE];
        clean_leaf.set_merkle_hash(&expected_hash);

        // Create dirty left leaf
        let dirty_leaf = make_leaf(PageId(1), dirty_txn, &[(b"a", b"1")]);

        // Branch references both
        let branch = make_branch(PageId(0), dirty_txn, &[(PageId(1), b"m")], PageId(2));

        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), branch);
        pages.insert(PageId(1), dirty_leaf);
        pages.insert(PageId(2), clean_leaf);

        let root_hash = compute_tree_merkle(&mut pages, PageId(0), dirty_txn, &|_| {
            panic!("should not read disk")
        })
        .unwrap();

        assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);

        // Clean leaf should still have its original hash (not recomputed)
        assert_eq!(pages[&PageId(2)].merkle_hash(), expected_hash);
    }

    #[test]
    fn reads_clean_hash_from_pool() {
        let dirty_txn = TxnId(5);
        let clean_txn = TxnId(3);

        // Clean leaf NOT in HashMap — hash fetched via callback
        let mut clean_leaf = make_leaf(PageId(2), clean_txn, &[(b"x", b"y")]);
        let precomputed_hash = compute_leaf_hash(&clean_leaf);
        clean_leaf.set_merkle_hash(&precomputed_hash);

        // Dirty leaf
        let dirty_leaf = make_leaf(PageId(1), dirty_txn, &[(b"a", b"1")]);
        let branch = make_branch(PageId(0), dirty_txn, &[(PageId(1), b"m")], PageId(2));

        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), branch);
        pages.insert(PageId(1), dirty_leaf);
        // PageId(2) NOT in pages — read_clean_hash will be called

        let root_hash = compute_tree_merkle(&mut pages, PageId(0), dirty_txn, &|page_id| {
            assert_eq!(page_id, PageId(2));
            Ok(precomputed_hash)
        })
        .unwrap();

        assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);

        // Clean page should NOT be loaded into the HashMap (hash-only fetch)
        assert!(!pages.contains_key(&PageId(2)));
    }

    #[test]
    fn merkle_hash_page_header_roundtrip() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        assert_eq!(page.merkle_hash(), [0u8; MERKLE_HASH_SIZE]);

        let hash = [0x42u8; MERKLE_HASH_SIZE];
        page.set_merkle_hash(&hash);
        assert_eq!(page.merkle_hash(), hash);
    }

    #[test]
    fn hash_covered_by_page_checksum() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        let hash = [0x42u8; MERKLE_HASH_SIZE];
        page.set_merkle_hash(&hash);
        page.update_checksum();
        assert!(page.verify_checksum());

        // Tamper with the hash
        let mut bad_hash = hash;
        bad_hash[0] ^= 0xFF;
        page.set_merkle_hash(&bad_hash);
        assert!(!page.verify_checksum());
    }

    #[test]
    fn three_level_tree() {
        let txn = TxnId(1);

        // Level 0 (leaves)
        let l0 = make_leaf(PageId(3), txn, &[(b"a", b"1")]);
        let l1 = make_leaf(PageId(4), txn, &[(b"c", b"3")]);
        let l2 = make_leaf(PageId(5), txn, &[(b"e", b"5")]);
        let l3 = make_leaf(PageId(6), txn, &[(b"g", b"7")]);

        // Level 1 (branches)
        let b0 = make_branch(PageId(1), txn, &[(PageId(3), b"b")], PageId(4));
        let b1 = make_branch(PageId(2), txn, &[(PageId(5), b"f")], PageId(6));

        // Level 2 (root)
        let root = make_branch(PageId(0), txn, &[(PageId(1), b"d")], PageId(2));

        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), root);
        pages.insert(PageId(1), b0);
        pages.insert(PageId(2), b1);
        pages.insert(PageId(3), l0);
        pages.insert(PageId(4), l1);
        pages.insert(PageId(5), l2);
        pages.insert(PageId(6), l3);

        let root_hash = compute_tree_merkle(&mut pages, PageId(0), txn, &|_| {
            panic!("no disk reads expected")
        })
        .unwrap();

        assert_ne!(root_hash, [0u8; MERKLE_HASH_SIZE]);

        // All 7 pages should have non-zero hashes
        for i in 0..7 {
            assert_ne!(
                pages[&PageId(i)].merkle_hash(),
                [0u8; MERKLE_HASH_SIZE],
                "page {i} should have a hash"
            );
        }
    }

    #[test]
    fn identical_trees_same_hash() {
        let txn = TxnId(1);
        let entries: &[(&[u8], &[u8])] =
            &[(b"alpha", b"one"), (b"beta", b"two"), (b"gamma", b"three")];

        let leaf1 = make_leaf(PageId(0), txn, entries);
        let mut pages1: HashMap<PageId, Page> = HashMap::new();
        pages1.insert(PageId(0), leaf1);
        let h1 = compute_tree_merkle(&mut pages1, PageId(0), txn, &|_| panic!()).unwrap();

        let leaf2 = make_leaf(PageId(0), txn, entries);
        let mut pages2: HashMap<PageId, Page> = HashMap::new();
        pages2.insert(PageId(0), leaf2);
        let h2 = compute_tree_merkle(&mut pages2, PageId(0), txn, &|_| panic!()).unwrap();

        assert_eq!(h1, h2);
    }

    #[test]
    fn leaf_with_tombstone() {
        let txn = TxnId(1);
        let mut page = Page::new(PageId(0), PageType::Leaf, txn);

        let cell1 = leaf_node::build_cell(b"alive", ValueType::Inline, b"data");
        page.insert_cell_at(0, &cell1);

        let cell2 = leaf_node::build_cell(b"dead", ValueType::Tombstone, &[]);
        page.insert_cell_at(1, &cell2);

        let h1 = compute_leaf_hash(&page);

        // Different tombstone — different hash
        let mut page2 = Page::new(PageId(0), PageType::Leaf, txn);
        let cell3 = leaf_node::build_cell(b"alive", ValueType::Inline, b"data");
        page2.insert_cell_at(0, &cell3);

        let h2 = compute_leaf_hash(&page2);

        assert_ne!(h1, h2);
    }

    #[test]
    fn truncation_is_28_bytes() {
        let h = blake3::hash(b"test");
        let truncated = truncate_hash(&h);
        assert_eq!(truncated.len(), 28);
        assert_eq!(&truncated[..], &h.as_bytes()[..28]);
    }

    #[test]
    fn large_leaf_hash() {
        let txn = TxnId(1);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100u32)
            .map(|i| {
                (
                    format!("key-{i:05}").into_bytes(),
                    format!("val-{i:05}").into_bytes(),
                )
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let leaf = make_leaf(PageId(0), txn, &entry_refs);
        let h = compute_leaf_hash(&leaf);
        assert_ne!(h, [0u8; MERKLE_HASH_SIZE]);
    }

    #[test]
    fn recomputing_gives_same_hash() {
        let txn = TxnId(1);
        let leaf = make_leaf(PageId(0), txn, &[(b"k", b"v")]);
        let mut pages: HashMap<PageId, Page> = HashMap::new();
        pages.insert(PageId(0), leaf);

        let h1 = compute_tree_merkle(&mut pages, PageId(0), txn, &|_| panic!()).unwrap();

        // Reset hash and recompute
        pages
            .get_mut(&PageId(0))
            .unwrap()
            .set_merkle_hash(&[0u8; MERKLE_HASH_SIZE]);
        // Page is still "dirty" (txn_id matches), so it will be recomputed
        let h2 = compute_tree_merkle(&mut pages, PageId(0), txn, &|_| panic!()).unwrap();

        assert_eq!(h1, h2);
    }
}
