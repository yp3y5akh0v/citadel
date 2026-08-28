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

struct SingleLeafReader {
    hash: MerkleHash,
    entry: DiffEntry,
}

impl TreeReader for SingleLeafReader {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(0), self.hash))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(PageDigest {
            page_id,
            page_type: PageType::Leaf,
            merkle_hash: self.hash,
            children: vec![],
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        Ok(vec![self.entry.clone()])
    }
}

/// Regression: SyncMode::Off commits zero page hashes instead of recomputing
/// them; two matching all-zero hashes mean "unknown", not "identical", so the
/// diff must traverse and surface the divergent entries.
#[test]
fn zero_hashes_never_prune_subtrees() {
    let entry = |val: &[u8]| DiffEntry {
        key: b"k".to_vec(),
        value: val.to_vec(),
        val_type: 0,
    };
    let source = SingleLeafReader {
        hash: [0u8; MERKLE_HASH_SIZE],
        entry: entry(b"v2"),
    };
    let target = SingleLeafReader {
        hash: [0u8; MERKLE_HASH_SIZE],
        entry: entry(b"v1"),
    };

    let result = merkle_diff(&source, &target).unwrap();
    assert_eq!(result.subtrees_skipped, 0);
    assert_eq!(result.entries, vec![entry(b"v2")]);
}

#[test]
fn hostile_tree_reader_cannot_emit_an_unknown_value_type() {
    let source = SingleLeafReader {
        hash: [1u8; MERKLE_HASH_SIZE],
        entry: DiffEntry {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            val_type: u8::MAX,
        },
    };
    let target = SingleLeafReader {
        hash: [2u8; MERKLE_HASH_SIZE],
        entry: DiffEntry {
            key: b"other".to_vec(),
            value: b"value".to_vec(),
            val_type: citadel_core::types::ValueType::Inline as u8,
        },
    };

    assert!(matches!(
        merkle_diff(&source, &target),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

#[test]
fn custom_reader_entry_lengths_cannot_exceed_wire_limits() {
    assert!(matches!(
        validate_diff_entry_fields(
            MAX_KEY_SIZE + 1,
            0,
            citadel_core::types::ValueType::Inline as u8
        ),
        Err(citadel_core::Error::KeyTooLarge { max, .. }) if max == MAX_KEY_SIZE
    ));
    assert!(matches!(
        validate_diff_entry_fields(
            1,
            MAX_SYNC_VALUE_SIZE + 1,
            citadel_core::types::ValueType::Inline as u8
        ),
        Err(citadel_core::Error::Sync(message))
            if message.contains("streaming sync is required")
    ));
    assert!(matches!(
        validate_diff_entry_fields(1, 1, citadel_core::types::ValueType::Tombstone as u8),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

struct CyclicRemoteReader;

impl TreeReader for CyclicRemoteReader {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(7), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(PageDigest {
            page_id,
            page_type: PageType::Branch,
            merkle_hash: UNKNOWN_HASH,
            children: vec![page_id],
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        panic!("a cyclic branch must never be treated as a leaf")
    }
}

#[test]
fn hostile_subtree_cycle_is_rejected_without_recursing() {
    let target = SingleLeafReader {
        hash: UNKNOWN_HASH,
        entry: DiffEntry {
            key: b"target".to_vec(),
            value: b"value".to_vec(),
            val_type: 0,
        },
    };

    assert!(matches!(
        merkle_diff(&CyclicRemoteReader, &target),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

struct SharedChildReader;

impl TreeReader for SharedChildReader {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(9), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(if page_id == PageId(9) {
            PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(10), PageId(10)],
            }
        } else {
            PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: UNKNOWN_HASH,
                children: Vec::new(),
            }
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        Ok(Vec::new())
    }
}

#[test]
fn duplicate_child_reference_is_rejected() {
    assert!(matches!(
        SharedChildReader.subtree_entries(PageId(9)),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

struct OversizedAcyclicBranch;

impl TreeReader for OversizedAcyclicBranch {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(1), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(PageDigest {
            page_id,
            page_type: PageType::Branch,
            merkle_hash: UNKNOWN_HASH,
            children: (0..=MAX_BRANCH_CHILDREN)
                .map(|index| PageId(index as u32 + 2))
                .collect(),
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        panic!("an impossible branch fanout must be rejected before traversal")
    }
}

#[test]
fn acyclic_but_physically_impossible_branch_is_bounded() {
    assert!(matches!(
        OversizedAcyclicBranch.subtree_entries(PageId(1)),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

struct CrossBoundarySource;

impl TreeReader for CrossBoundarySource {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(1), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(match page_id {
            PageId(1) => PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(2), PageId(3)],
            },
            PageId(2) => PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: [7u8; MERKLE_HASH_SIZE],
                children: Vec::new(),
            },
            PageId(3) => PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(2)],
            },
            _ => panic!("unexpected page"),
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        Ok(Vec::new())
    }
}

struct CrossBoundaryTarget;

impl TreeReader for CrossBoundaryTarget {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(10), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(match page_id {
            PageId(10) => PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(20), PageId(30)],
            },
            PageId(20) => PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: [7u8; MERKLE_HASH_SIZE],
                children: Vec::new(),
            },
            PageId(30) => PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: UNKNOWN_HASH,
                children: Vec::new(),
            },
            _ => panic!("unexpected page"),
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        Ok(Vec::new())
    }
}

#[test]
fn subtree_collection_cannot_revisit_a_page_seen_by_paired_walk() {
    assert!(matches!(
        merkle_diff(&CrossBoundarySource, &CrossBoundaryTarget),
        Err(citadel_core::Error::DatabaseCorrupted)
    ));
}

struct MultiLeafBudgetSource;

impl TreeReader for MultiLeafBudgetSource {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(1), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(if page_id == PageId(1) {
            PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(2), PageId(3)],
            }
        } else {
            PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: UNKNOWN_HASH,
                children: Vec::new(),
            }
        })
    }

    fn leaf_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>> {
        Ok(vec![DiffEntry {
            key: page_id.as_u32().to_be_bytes().to_vec(),
            value: vec![0; MAX_DIFF_BYTES / 2],
            val_type: citadel_core::types::ValueType::Inline as u8,
        }])
    }
}

struct MultiLeafBudgetTarget;

impl TreeReader for MultiLeafBudgetTarget {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((PageId(10), UNKNOWN_HASH))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        Ok(if page_id == PageId(10) {
            PageDigest {
                page_id,
                page_type: PageType::Branch,
                merkle_hash: UNKNOWN_HASH,
                children: vec![PageId(20), PageId(30)],
            }
        } else {
            PageDigest {
                page_id,
                page_type: PageType::Leaf,
                merkle_hash: [page_id.as_u32() as u8; MERKLE_HASH_SIZE],
                children: Vec::new(),
            }
        })
    }

    fn leaf_entries(&self, _page_id: PageId) -> Result<Vec<DiffEntry>> {
        panic!("target entries are not needed for a source-to-target diff")
    }
}

#[test]
fn aggregate_budget_stops_multiple_large_leaf_responses() {
    assert!(matches!(
        merkle_diff(&MultiLeafBudgetSource, &MultiLeafBudgetTarget),
        Err(citadel_core::Error::Sync(message)) if message.contains("sync payload limit")
    ));
}
