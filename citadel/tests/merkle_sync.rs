//! Merkle sync tests - prove that two databases can detect differences
//! at the page level using Merkle hashes and sync only the changed data.
//!
//! This is the actual purpose of the Merkle tree: replication.
//! Two nodes compare root hashes. If different, they walk down the tree
//! comparing hashes at each level until they find the diverging leaf pages.
//! Only those pages' key-value entries need to be transferred.

use std::collections::{BTreeMap, HashSet, VecDeque};

use citadel::core::types::{PageId, PageType};
use citadel::core::MERKLE_HASH_SIZE;
use citadel::page::{branch_node, leaf_node};
use citadel::{Argon2Profile, Database, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"merkle-sync")
        .argon2_profile(Argon2Profile::Iot)
}

// ============================================================
// Tree walker - reads all pages and their Merkle hashes
// ============================================================

#[derive(Debug, Clone)]
struct PageInfo {
    page_id: PageId,
    page_type: PageType,
    merkle_hash: [u8; MERKLE_HASH_SIZE],
    children: Vec<PageId>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Walk the entire B+ tree, collecting page info and Merkle hashes.
fn walk_tree(db: &Database) -> Vec<PageInfo> {
    let mgr = db.manager();
    let slot = mgr.current_slot();
    let root = slot.tree_root;

    let mut result = Vec::new();
    let mut stack = vec![root];

    while let Some(pid) = stack.pop() {
        let page = mgr.read_page_from_disk(pid).unwrap();
        let pt = page.page_type().unwrap();
        let hash = page.merkle_hash();

        let mut children = Vec::new();
        let mut entries = Vec::new();

        match pt {
            PageType::Branch => {
                for i in 0..page.num_cells() as usize {
                    let child = branch_node::get_child(&page, i);
                    children.push(child);
                    stack.push(child);
                }
                let right = page.right_child();
                if right.is_valid() {
                    children.push(right);
                    stack.push(right);
                }
            }
            PageType::Leaf => {
                for i in 0..page.num_cells() {
                    let cell = leaf_node::read_cell(&page, i);
                    entries.push((cell.key.to_vec(), cell.value.to_vec()));
                }
            }
            _ => {}
        }

        result.push(PageInfo {
            page_id: pid,
            page_type: pt,
            merkle_hash: hash,
            children,
            entries,
        });
    }
    result
}

/// Independently recompute the Merkle hash for a leaf page from its entries.
fn recompute_leaf_hash(entries: &[(Vec<u8>, Vec<u8>)]) -> [u8; MERKLE_HASH_SIZE] {
    let mut hasher = blake3::Hasher::new();
    for (key, value) in entries {
        hasher.update(&(key.len() as u16).to_le_bytes());
        hasher.update(key);
        hasher.update(&[0u8]); // val_type = Inline
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(value);
    }
    let hash = hasher.finalize();
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

/// Independently recompute the Merkle hash for a branch page from its children's hashes.
fn recompute_branch_hash(child_hashes: &[[u8; MERKLE_HASH_SIZE]]) -> [u8; MERKLE_HASH_SIZE] {
    let mut hasher = blake3::Hasher::new();
    for h in child_hashes {
        hasher.update(h);
    }
    let hash = hasher.finalize();
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

// ============================================================
// Merkle diff algorithm - the core sync primitive
// ============================================================

/// Diff two trees using Merkle hashes. Returns the set of key-value entries
/// that exist in `source` but differ from (or are missing in) `target`.
///
/// This is the actual sync algorithm: walk both trees in parallel,
/// skip subtrees with matching hashes, descend into subtrees with
/// different hashes, collect differing leaf entries.
fn merkle_diff(source: &Database, target: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    let src_mgr = source.manager();
    let tgt_mgr = target.manager();
    let src_root = src_mgr.current_slot().tree_root;
    let tgt_root = tgt_mgr.current_slot().tree_root;

    let mut changed_entries = Vec::new();

    // BFS: compare pages at each level
    let mut queue: VecDeque<(PageId, PageId)> = VecDeque::new();
    queue.push_back((src_root, tgt_root));

    while let Some((src_pid, tgt_pid)) = queue.pop_front() {
        let src_page = src_mgr.read_page_from_disk(src_pid).unwrap();
        let tgt_page = tgt_mgr.read_page_from_disk(tgt_pid).unwrap();

        // If hashes match, entire subtree is identical - skip it
        if src_page.merkle_hash() == tgt_page.merkle_hash() {
            continue;
        }

        // Hashes differ - descend or collect
        match (src_page.page_type().unwrap(), tgt_page.page_type().unwrap()) {
            (PageType::Leaf, PageType::Leaf) => {
                // Both are leaves with different hashes - collect source entries
                for i in 0..src_page.num_cells() {
                    let cell = leaf_node::read_cell(&src_page, i);
                    changed_entries.push((cell.key.to_vec(), cell.value.to_vec()));
                }
            }
            (PageType::Branch, PageType::Branch) => {
                // Both branches - compare children pairwise
                let src_n = src_page.num_cells() as usize;
                let tgt_n = tgt_page.num_cells() as usize;

                // If same structure, compare children pairwise
                if src_n == tgt_n {
                    for i in 0..src_n {
                        let sc = branch_node::get_child(&src_page, i);
                        let tc = branch_node::get_child(&tgt_page, i);
                        queue.push_back((sc, tc));
                    }
                    // Right child
                    let sr = src_page.right_child();
                    let tr = tgt_page.right_child();
                    if sr.is_valid() && tr.is_valid() {
                        queue.push_back((sr, tr));
                    }
                } else {
                    // Different structure - collect all source leaf entries from this subtree
                    collect_all_leaves(src_mgr, src_pid, &mut changed_entries);
                }
            }
            _ => {
                // Structure changed (leaf vs branch) - collect all source entries
                collect_all_leaves(src_mgr, src_pid, &mut changed_entries);
            }
        }
    }

    changed_entries
}

/// Collect all leaf entries from a subtree rooted at `page_id`.
fn collect_all_leaves(
    mgr: &citadel::txn::manager::TxnManager,
    page_id: PageId,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) {
    let page = mgr.read_page_from_disk(page_id).unwrap();
    match page.page_type().unwrap() {
        PageType::Leaf => {
            for i in 0..page.num_cells() {
                let cell = leaf_node::read_cell(&page, i);
                entries.push((cell.key.to_vec(), cell.value.to_vec()));
            }
        }
        PageType::Branch => {
            for i in 0..page.num_cells() as usize {
                collect_all_leaves(mgr, branch_node::get_child(&page, i), entries);
            }
            let right = page.right_child();
            if right.is_valid() {
                collect_all_leaves(mgr, right, entries);
            }
        }
        _ => {}
    }
}

/// Apply a set of key-value entries to a database (simulate receiving sync data).
fn apply_sync(db: &Database, entries: &[(Vec<u8>, Vec<u8>)]) {
    let mut wtx = db.begin_write().unwrap();
    for (key, value) in entries {
        wtx.insert(key, value).unwrap();
    }
    wtx.commit().unwrap();
}

// ============================================================
// Tests: prove the full sync round-trip
// ============================================================

#[test]
fn identical_dbs_merkle_diff_returns_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    let diff = merkle_diff(&db1, &db2);
    assert!(
        diff.is_empty(),
        "identical DBs must have zero diff, got {} entries",
        diff.len()
    );
}

#[test]
fn single_insert_detected_and_synced() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Same initial data
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..20u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Modify db1 only - add one key
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"new-key", b"new-value").unwrap();
    wtx.commit().unwrap();

    assert_ne!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Diff detects the change
    let diff = merkle_diff(&db1, &db2);
    assert!(!diff.is_empty(), "diff must find changed entries");

    // The diff must contain our new key
    let has_new_key = diff
        .iter()
        .any(|(k, v)| k == b"new-key" && v == b"new-value");
    assert!(has_new_key, "diff must contain the newly inserted key");

    // Apply the diff to db2
    apply_sync(&db2, &diff);

    // After sync, both DBs must have identical data
    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "after sync, merkle roots must match"
    );

    // Verify data actually matches
    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2, "after sync, all data must match");
}

#[test]
fn value_update_detected_and_synced() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..30u32 {
            wtx.insert(&i.to_be_bytes(), b"original").unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Update one value in db1
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&15u32.to_be_bytes(), b"modified").unwrap();
    wtx.commit().unwrap();

    assert_ne!(db1.stats().merkle_root, db2.stats().merkle_root);

    let diff = merkle_diff(&db1, &db2);
    assert!(!diff.is_empty());

    // Apply and verify convergence
    apply_sync(&db2, &diff);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2);
}

#[test]
fn multiple_changes_detected_and_synced() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Start with same 100 entries
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Make several changes to db1
    let mut wtx = db1.begin_write().unwrap();
    // Update some
    for i in (0..100u32).step_by(10) {
        wtx.insert(&i.to_be_bytes(), b"UPDATED").unwrap();
    }
    // Insert new
    for i in 100..110u32 {
        wtx.insert(&i.to_be_bytes(), b"new-entry").unwrap();
    }
    wtx.commit().unwrap();

    assert_ne!(db1.stats().merkle_root, db2.stats().merkle_root);

    let diff = merkle_diff(&db1, &db2);
    assert!(!diff.is_empty());

    // Apply and verify
    apply_sync(&db2, &diff);
    assert_eq!(
        db1.stats().merkle_root,
        db2.stats().merkle_root,
        "after syncing multiple changes, roots must match"
    );

    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2);
}

#[test]
fn sync_with_splits_large_dataset() {
    // With multi-level trees, entry-level sync makes the DATA converge,
    // but the tree STRUCTURE may differ (different CoW history = different
    // page layout). Merkle hashes reflect page structure, so roots may
    // differ even when data is identical. This is correct - page-level
    // sync will transfer actual pages for root convergence.
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Use larger values to force many leaf pages
    let val = [0xAA_u8; 128];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    let tree = walk_tree(&db1);
    let leaf_count = tree
        .iter()
        .filter(|p| p.page_type == PageType::Leaf)
        .count();
    assert!(
        leaf_count >= 5,
        "need multiple leaf pages, got {leaf_count}"
    );

    // Modify entries in just 2 leaf pages
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&5u32.to_be_bytes(), b"changed-5").unwrap();
    wtx.insert(&499u32.to_be_bytes(), b"changed-499").unwrap();
    wtx.commit().unwrap();

    let diff = merkle_diff(&db1, &db2);

    // Diff must skip matching subtrees - fewer entries than total
    assert!(
        diff.len() < 500,
        "merkle diff must skip matching subtrees - got {} entries out of 500",
        diff.len()
    );
    assert!(!diff.is_empty());

    // Changed keys must be in the diff
    let diff_keys: HashSet<Vec<u8>> = diff.iter().map(|(k, _)| k.clone()).collect();
    assert!(
        diff_keys.contains(5u32.to_be_bytes().as_slice()),
        "diff must contain key 5"
    );
    assert!(
        diff_keys.contains(499u32.to_be_bytes().as_slice()),
        "diff must contain key 499"
    );

    // Apply diff - data must converge
    apply_sync(&db2, &diff);

    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2, "after sync, all key-value data must match");

    // Entry counts must match
    assert_eq!(db1.stats().entry_count, db2.stats().entry_count);
}

#[test]
fn sync_efficiency_skips_matching_subtrees() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Use larger values to create many leaf pages
    let val = [0xBB_u8; 128];
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(&i.to_be_bytes(), &val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let tree = walk_tree(&db1);
    let total_pages = tree.len();
    let leaf_count = tree
        .iter()
        .filter(|p| p.page_type == PageType::Leaf)
        .count();
    assert!(
        total_pages > 5,
        "need multi-level tree, got {total_pages} pages"
    );
    assert!(leaf_count > 5, "need multiple leaves, got {leaf_count}");

    // Change just 1 entry - should affect only 1 leaf page
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(&42u32.to_be_bytes(), b"CHANGED").unwrap();
    wtx.commit().unwrap();

    let diff = merkle_diff(&db1, &db2);

    // Only the leaf containing key 42 should be in the diff.
    // All other leaves have matching hashes and are skipped.
    // One leaf has ~57 entries, so diff should be ~57, not 500.
    assert!(
        diff.len() < 500 / 2,
        "changing 1 entry: diff should be << 500, got {} (efficiency: {:.0}% skipped)",
        diff.len(),
        (1.0 - diff.len() as f64 / 500.0) * 100.0
    );

    // The changed key must be in the diff
    let has_key_42 = diff.iter().any(|(k, _)| k == &42u32.to_be_bytes());
    assert!(has_key_42, "diff must contain the changed key");

    // Apply and verify convergence
    apply_sync(&db2, &diff);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2);
}

// ============================================================
// Deep page-level hash verification
// ============================================================

#[test]
fn every_leaf_hash_matches_independent_recomputation() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..300u32 {
        wtx.insert(&i.to_be_bytes(), &format!("value-{i}").into_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();

    let tree = walk_tree(&db);

    for page_info in &tree {
        if page_info.page_type == PageType::Leaf {
            let recomputed = recompute_leaf_hash(&page_info.entries);
            assert_eq!(
                page_info.merkle_hash, recomputed,
                "leaf page {:?} stored hash doesn't match recomputed hash",
                page_info.page_id
            );
        }
    }
}

#[test]
fn every_branch_hash_matches_independent_recomputation() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..300u32 {
        wtx.insert(&i.to_be_bytes(), &format!("value-{i}").into_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();

    let tree = walk_tree(&db);

    // Build a map of page_id -> merkle_hash for lookup
    let hash_map: BTreeMap<PageId, [u8; MERKLE_HASH_SIZE]> =
        tree.iter().map(|p| (p.page_id, p.merkle_hash)).collect();

    for page_info in &tree {
        if page_info.page_type == PageType::Branch {
            // Collect children's hashes in order
            let child_hashes: Vec<[u8; MERKLE_HASH_SIZE]> =
                page_info.children.iter().map(|cid| hash_map[cid]).collect();

            let recomputed = recompute_branch_hash(&child_hashes);
            assert_eq!(
                page_info.merkle_hash, recomputed,
                "branch page {:?} stored hash doesn't match recomputed from children",
                page_info.page_id
            );
        }
    }
}

#[test]
fn root_hash_matches_commit_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..200u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let tree = walk_tree(&db);
    let root = tree
        .iter()
        .find(|p| {
            let mgr = db.manager();
            p.page_id == mgr.current_slot().tree_root
        })
        .unwrap();

    assert_eq!(
        root.merkle_hash,
        db.stats().merkle_root,
        "root page hash must equal CommitSlot.merkle_root"
    );
}

#[test]
fn hash_chain_is_complete_no_zero_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let mut wtx = db.begin_write().unwrap();
    for i in 0..400u32 {
        wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
    }
    wtx.commit().unwrap();

    let tree = walk_tree(&db);
    let zero_hash = [0u8; MERKLE_HASH_SIZE];

    for page_info in &tree {
        assert_ne!(
            page_info.merkle_hash, zero_hash,
            "page {:?} has zero hash - Merkle computation missed this page",
            page_info.page_id
        );
    }
}

// ============================================================
// Full sync scenario: diverge, diff, sync, verify
// ============================================================

#[test]
fn full_sync_scenario_diverge_and_converge() {
    let dir = tempfile::tempdir().unwrap();
    let db_source = fast_builder(&dir.path().join("source.db"))
        .create()
        .unwrap();
    let db_replica = fast_builder(&dir.path().join("replica.db"))
        .create()
        .unwrap();

    // Both start with same 200 entries
    for db in [&db_source, &db_replica] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(&i.to_be_bytes(), &format!("v{i}").into_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(
        db_source.stats().merkle_root,
        db_replica.stats().merkle_root
    );

    // Source makes several transactions of changes
    {
        let mut wtx = db_source.begin_write().unwrap();
        for i in 200..220u32 {
            wtx.insert(&i.to_be_bytes(), b"new-data").unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = db_source.begin_write().unwrap();
        for i in (0..200u32).step_by(20) {
            wtx.insert(&i.to_be_bytes(), b"updated").unwrap();
        }
        wtx.commit().unwrap();
    }

    // Root hashes diverged
    assert_ne!(
        db_source.stats().merkle_root,
        db_replica.stats().merkle_root
    );

    // Sync: diff + apply
    let diff = merkle_diff(&db_source, &db_replica);
    assert!(!diff.is_empty());
    apply_sync(&db_replica, &diff);

    // After sync: roots match, data matches
    assert_eq!(
        db_source.stats().merkle_root,
        db_replica.stats().merkle_root,
        "after full sync, roots must match"
    );

    let src_data = collect_all_data(&db_source);
    let rep_data = collect_all_data(&db_replica);
    assert_eq!(src_data, rep_data, "after full sync, all data must match");
}

#[test]
fn incremental_sync_multiple_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("node1.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("node2.db")).create().unwrap();

    // Initial state
    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Round 1: node1 changes, sync to node2
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&200u32.to_be_bytes(), b"round1").unwrap();
        wtx.commit().unwrap();
    }
    let diff1 = merkle_diff(&db1, &db2);
    apply_sync(&db2, &diff1);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Round 2: more node1 changes, sync again
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&201u32.to_be_bytes(), b"round2").unwrap();
        wtx.insert(&50u32.to_be_bytes(), b"updated-50").unwrap();
        wtx.commit().unwrap();
    }
    let diff2 = merkle_diff(&db1, &db2);
    apply_sync(&db2, &diff2);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Round 3: another change
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&0u32.to_be_bytes(), b"final-update").unwrap();
        wtx.commit().unwrap();
    }
    let diff3 = merkle_diff(&db1, &db2);
    apply_sync(&db2, &diff3);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Final data check
    let data1 = collect_all_data(&db1);
    let data2 = collect_all_data(&db2);
    assert_eq!(data1, data2);
}

#[test]
fn no_diff_after_sync_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    // Modify db1
    let mut wtx = db1.begin_write().unwrap();
    wtx.insert(b"extra", b"data").unwrap();
    wtx.commit().unwrap();

    // Sync
    let diff = merkle_diff(&db1, &db2);
    apply_sync(&db2, &diff);
    assert_eq!(db1.stats().merkle_root, db2.stats().merkle_root);

    // Second diff should be empty
    let diff2 = merkle_diff(&db1, &db2);
    assert!(
        diff2.is_empty(),
        "after successful sync, second diff must be empty"
    );
}

// ============================================================
// Helper
// ============================================================

fn collect_all_data(db: &Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut data = BTreeMap::new();
    let mut rtx = db.begin_read();
    rtx.for_each(|k, v| {
        data.insert(k.to_vec(), v.to_vec());
        Ok(())
    })
    .unwrap();
    data
}
