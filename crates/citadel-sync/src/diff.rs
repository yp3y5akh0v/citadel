use std::collections::{HashSet, VecDeque};

use citadel_core::constants::{MAX_KEY_SIZE, USABLE_SIZE};
use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{Result, MERKLE_HASH_SIZE};

use crate::protocol::MAX_SYNC_VALUE_SIZE;

/// 28-byte BLAKE3 Merkle hash.
pub type MerkleHash = [u8; MERKLE_HASH_SIZE];

/// SyncMode::Off commits zero Merkle hashes instead of recomputing them, so an
/// all-zero hash means "unknown" and must never prune a subtree as identical.
pub(crate) const UNKNOWN_HASH: MerkleHash = [0u8; MERKLE_HASH_SIZE];
const MAX_TREE_PAGES: usize = 1_000_000;
const MAX_DIFF_ENTRIES: usize = 1_000_000;
const MAX_DIFF_BYTES: usize = 64 * 1024 * 1024 - 5 - 18;
// A branch cell needs at least a six-byte header plus its two-byte slot.
// The right child accounts for the final pointer.
pub(crate) const MAX_BRANCH_CHILDREN: usize = USABLE_SIZE / 8 + 1;

/// Digest of a single page - hash, type, and children.
#[derive(Debug, Clone)]
pub struct PageDigest {
    pub page_id: PageId,
    pub page_type: PageType,
    pub merkle_hash: MerkleHash,
    /// Child page IDs for branch pages. Empty for leaves.
    pub children: Vec<PageId>,
}

/// A key-value entry from a leaf page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub val_type: u8,
}

/// Result of a Merkle diff operation.
#[derive(Debug, Clone)]
pub struct DiffResult {
    /// Entries from source that differ from target.
    pub entries: Vec<DiffEntry>,
    /// Number of pages whose hashes were compared.
    pub pages_compared: u64,
    /// Number of subtrees skipped because hashes matched.
    pub subtrees_skipped: u64,
}

impl DiffResult {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Abstraction for reading tree structure during diff.
///
/// For local databases, `LocalTreeReader` retains a registered read snapshot
/// while reading immutable pages from `TxnManager`.
/// For remote databases, the transport implements this via messages.
pub trait TreeReader {
    /// Root page ID and its Merkle hash.
    fn root_info(&self) -> Result<(PageId, MerkleHash)>;

    /// Read a page digest (hash + type + children).
    fn page_digest(&self, page_id: PageId) -> Result<PageDigest>;

    /// Read all leaf entries from a leaf page.
    fn leaf_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>>;

    /// Collect all leaf entries from a subtree.
    fn subtree_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>> {
        let mut visited = HashSet::new();
        collect_subtree_entries(self, page_id, None, &mut visited)
    }
}

fn collect_subtree_entries<R: TreeReader + ?Sized>(
    reader: &R,
    page_id: PageId,
    known_root: Option<PageDigest>,
    visited: &mut HashSet<PageId>,
) -> Result<Vec<DiffEntry>> {
    let mut entries = Vec::new();
    let mut entry_bytes = 0usize;
    let mut stack = vec![(page_id, known_root)];

    while let Some((current, known_digest)) = stack.pop() {
        let digest = if let Some(digest) = known_digest {
            if !visited.contains(&current) {
                return Err(citadel_core::Error::DatabaseCorrupted);
            }
            digest
        } else {
            if visited.len() >= MAX_TREE_PAGES {
                return Err(citadel_core::Error::DatabaseCorrupted);
            }
            if !visited.insert(current) {
                return Err(citadel_core::Error::DatabaseCorrupted);
            }
            reader.page_digest(current)?
        };
        validate_digest(current, &digest)?;
        match digest.page_type {
            PageType::Leaf => {
                extend_entries(
                    &mut entries,
                    reader.leaf_entries(current)?,
                    &mut entry_bytes,
                )?;
            }
            PageType::Branch => {
                let discovered = visited
                    .len()
                    .checked_add(stack.len())
                    .and_then(|count| count.checked_add(digest.children.len()))
                    .ok_or(citadel_core::Error::DatabaseCorrupted)?;
                if discovered > MAX_TREE_PAGES {
                    return Err(citadel_core::Error::DatabaseCorrupted);
                }
                stack.extend(digest.children.into_iter().rev().map(|child| (child, None)));
            }
            _ => unreachable!("validate_digest accepts only tree pages"),
        }
    }

    Ok(entries)
}

fn validate_digest(requested: PageId, digest: &PageDigest) -> Result<()> {
    if !requested.is_valid() || digest.page_id != requested {
        return Err(citadel_core::Error::DatabaseCorrupted);
    }
    match digest.page_type {
        PageType::Leaf if digest.children.is_empty() => Ok(()),
        PageType::Branch if !digest.children.is_empty() => {
            if digest.children.len() > MAX_BRANCH_CHILDREN {
                return Err(citadel_core::Error::DatabaseCorrupted);
            }
            let mut children = HashSet::with_capacity(digest.children.len());
            for child in &digest.children {
                if !child.is_valid() || *child == requested || !children.insert(*child) {
                    return Err(citadel_core::Error::DatabaseCorrupted);
                }
            }
            Ok(())
        }
        PageType::Leaf | PageType::Branch => Err(citadel_core::Error::DatabaseCorrupted),
        _ => Err(citadel_core::Error::InvalidPageType(
            digest.page_type as u16,
            requested,
        )),
    }
}

fn extend_entries(
    target: &mut Vec<DiffEntry>,
    added: Vec<DiffEntry>,
    byte_count: &mut usize,
) -> Result<()> {
    let total = target
        .len()
        .checked_add(added.len())
        .ok_or(citadel_core::Error::DatabaseCorrupted)?;
    if total > MAX_DIFF_ENTRIES {
        return Err(citadel_core::Error::DatabaseCorrupted);
    }
    for entry in &added {
        validate_diff_entry_fields(entry.key.len(), entry.value.len(), entry.val_type)?;
    }
    let added_bytes = added.iter().try_fold(0usize, |size, entry| {
        // Reserve the CRDT metadata overhead even for a non-CRDT caller: diff
        // is mode-agnostic, so its result must remain serializable either way.
        size.checked_add(7 + crate::crdt::CRDT_META_SIZE)
            .and_then(|size| size.checked_add(entry.key.len()))
            .and_then(|size| size.checked_add(entry.value.len()))
            .ok_or(citadel_core::Error::DatabaseCorrupted)
    })?;
    *byte_count = byte_count
        .checked_add(added_bytes)
        .ok_or(citadel_core::Error::DatabaseCorrupted)?;
    if *byte_count > MAX_DIFF_BYTES {
        return Err(citadel_core::Error::Sync(format!(
            "diff is {} bytes, sync payload limit is {MAX_DIFF_BYTES}",
            *byte_count
        )));
    }
    target.extend(added);
    Ok(())
}

fn validate_diff_entry_fields(key_len: usize, value_len: usize, val_type: u8) -> Result<()> {
    let Some(value_type) = ValueType::from_u8(val_type) else {
        return Err(citadel_core::Error::DatabaseCorrupted);
    };
    if value_type == ValueType::Tombstone && value_len != 0 {
        return Err(citadel_core::Error::DatabaseCorrupted);
    }
    if key_len > MAX_KEY_SIZE {
        return Err(citadel_core::Error::KeyTooLarge {
            size: key_len,
            max: MAX_KEY_SIZE,
        });
    }
    if value_len > MAX_SYNC_VALUE_SIZE {
        return Err(citadel_core::Error::Sync(format!(
            "value is {value_len} bytes, sync limit is {MAX_SYNC_VALUE_SIZE}; streaming sync is required"
        )));
    }
    Ok(())
}

/// Compute the Merkle diff between two trees.
///
/// Entries in `source` that differ from or are missing in `target`.
/// Walks both trees in parallel using BFS, skipping entire subtrees when
/// Merkle hashes match.
pub fn merkle_diff(source: &dyn TreeReader, target: &dyn TreeReader) -> Result<DiffResult> {
    let (src_root, src_root_hash) = source.root_info()?;
    let (tgt_root, tgt_root_hash) = target.root_info()?;

    let mut result = DiffResult {
        entries: Vec::new(),
        pages_compared: 0,
        subtrees_skipped: 0,
    };

    // Roots match - databases are identical (unknown hashes always traverse)
    if src_root_hash == tgt_root_hash && src_root_hash != UNKNOWN_HASH {
        return Ok(result);
    }

    let mut queue: VecDeque<(PageId, PageId)> = VecDeque::new();
    queue.push_back((src_root, tgt_root));
    let mut visited_source = HashSet::new();
    let mut visited_target = HashSet::new();
    let mut entry_bytes = 0usize;

    while let Some((src_pid, tgt_pid)) = queue.pop_front() {
        if !visited_source.insert(src_pid) || !visited_target.insert(tgt_pid) {
            return Err(citadel_core::Error::DatabaseCorrupted);
        }
        let src_digest = source.page_digest(src_pid)?;
        let tgt_digest = target.page_digest(tgt_pid)?;
        validate_digest(src_pid, &src_digest)?;
        validate_digest(tgt_pid, &tgt_digest)?;
        result.pages_compared += 1;

        if src_digest.merkle_hash == tgt_digest.merkle_hash
            && src_digest.merkle_hash != UNKNOWN_HASH
        {
            result.subtrees_skipped += 1;
            continue;
        }

        match (src_digest.page_type, tgt_digest.page_type) {
            (PageType::Leaf, PageType::Leaf) => {
                extend_entries(
                    &mut result.entries,
                    source.leaf_entries(src_pid)?,
                    &mut entry_bytes,
                )?;
            }
            (PageType::Branch, PageType::Branch)
                if src_digest.children.len() == tgt_digest.children.len() =>
            {
                let discovered = visited_source
                    .len()
                    .checked_add(queue.len())
                    .and_then(|count| count.checked_add(src_digest.children.len()))
                    .ok_or(citadel_core::Error::DatabaseCorrupted)?;
                if discovered > MAX_TREE_PAGES {
                    return Err(citadel_core::Error::DatabaseCorrupted);
                }
                for (sc, tc) in src_digest.children.iter().zip(&tgt_digest.children) {
                    queue.push_back((*sc, *tc));
                }
            }
            _ => {
                let subtree = collect_subtree_entries(
                    source,
                    src_pid,
                    Some(src_digest.clone()),
                    &mut visited_source,
                )?;
                extend_entries(&mut result.entries, subtree, &mut entry_bytes)?;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
#[path = "diff_tests.rs"]
mod tests;
