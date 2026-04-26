use std::collections::VecDeque;

use citadel_core::types::{PageId, PageType};
use citadel_core::{Result, MERKLE_HASH_SIZE};

/// 28-byte BLAKE3 Merkle hash.
pub type MerkleHash = [u8; MERKLE_HASH_SIZE];

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
/// For local databases, `LocalTreeReader` reads from `TxnManager` directly.
/// For remote databases, the transport layer implements this via message exchange.
pub trait TreeReader {
    /// Root page ID and its Merkle hash.
    fn root_info(&self) -> Result<(PageId, MerkleHash)>;

    /// Read a page digest (hash + type + children).
    fn page_digest(&self, page_id: PageId) -> Result<PageDigest>;

    /// Read all leaf entries from a leaf page.
    fn leaf_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>>;

    /// Collect all leaf entries from a subtree.
    fn subtree_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>> {
        let digest = self.page_digest(page_id)?;
        match digest.page_type {
            PageType::Leaf => self.leaf_entries(page_id),
            PageType::Branch => {
                let mut entries = Vec::new();
                for child in &digest.children {
                    entries.extend(self.subtree_entries(*child)?);
                }
                Ok(entries)
            }
            _ => Ok(Vec::new()),
        }
    }
}

/// Compute the Merkle diff between two trees.
///
/// Returns entries from `source` that are different from or missing in `target`.
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

    // Roots match - databases are identical
    if src_root_hash == tgt_root_hash {
        return Ok(result);
    }

    let mut queue: VecDeque<(PageId, PageId)> = VecDeque::new();
    queue.push_back((src_root, tgt_root));

    while let Some((src_pid, tgt_pid)) = queue.pop_front() {
        let src_digest = source.page_digest(src_pid)?;
        let tgt_digest = target.page_digest(tgt_pid)?;
        result.pages_compared += 1;

        if src_digest.merkle_hash == tgt_digest.merkle_hash {
            result.subtrees_skipped += 1;
            continue;
        }

        match (src_digest.page_type, tgt_digest.page_type) {
            (PageType::Leaf, PageType::Leaf) => {
                result.entries.extend(source.leaf_entries(src_pid)?);
            }
            (PageType::Branch, PageType::Branch)
                if src_digest.children.len() == tgt_digest.children.len() =>
            {
                for (sc, tc) in src_digest.children.iter().zip(&tgt_digest.children) {
                    queue.push_back((*sc, *tc));
                }
            }
            _ => {
                result.entries.extend(source.subtree_entries(src_pid)?);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
#[path = "diff_tests.rs"]
mod tests;
