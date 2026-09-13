//! Inline Merkle hashing for sync diff detection.
//!
//! Each page stores a BLAKE3 hash (28 bytes) in its header at [36..64].
//! - Leaf pages: hash of logical key-value entries in sorted order; overflow
//!   references contribute payload digests, never physical page IDs
//! - Branch pages: hash of all children's Merkle hashes concatenated
//!
//! The root page's hash serves as a database fingerprint - if two snapshots
//! have the same root hash, they contain identical data.

#[cfg(test)]
use rustc_hash::FxHashMap;

use citadel_buffer::cursor::MutablePageMap;
use citadel_core::types::{PageId, PageType, TxnId, ValueType};
use citadel_core::{Error, Result, MERKLE_HASH_SIZE};
#[cfg(test)]
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

// Frozen scheme identifier, not the crate version: changing it changes
// overflow payload digests and every enclosing tree hash.
const OVERFLOW_PAYLOAD_CONTEXT: &str = "CitadelDB overflow payload Merkle digest v1";

/// Incremental, domain-separated digest of one logical overflow value.
/// Physical page IDs and next-page links are excluded.
pub(crate) struct OverflowPayloadDigest(blake3::Hasher);

impl OverflowPayloadDigest {
    pub(crate) fn new(total_len: u32) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key(OVERFLOW_PAYLOAD_CONTEXT);
        hasher.update(&total_len.to_le_bytes());
        Self(hasher)
    }

    pub(crate) fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    pub(crate) fn finalize(self) -> [u8; MERKLE_HASH_SIZE] {
        truncate_hash(&self.0.finalize())
    }
}

#[cfg(test)]
pub(crate) fn overflow_payload_hash(value: &[u8]) -> [u8; MERKLE_HASH_SIZE] {
    debug_assert!(u32::try_from(value.len()).is_ok());
    let mut digest = OverflowPayloadDigest::new(value.len() as u32);
    digest.update(value);
    digest.finalize()
}

/// Hash leaf cells by their logical values. Overflow storage locations are
/// replaced by the authenticated digest stored on the chain's head page.
pub(crate) fn hash_logical_leaf_cells<'a, I, F>(
    cells: I,
    mut overflow_digest: F,
) -> Result<[u8; MERKLE_HASH_SIZE]>
where
    I: IntoIterator<Item = (&'a [u8], ValueType, &'a [u8])>,
    F: FnMut(&leaf_node::OverflowRef) -> Result<[u8; MERKLE_HASH_SIZE]>,
{
    let mut hasher = blake3::Hasher::new();
    for (key, value_type, value) in cells {
        let key_len = u16::try_from(key.len()).map_err(|_| Error::DatabaseCorrupted)?;
        hasher.update(&key_len.to_le_bytes());
        hasher.update(key);
        hasher.update(&[value_type as u8]);

        if value_type == ValueType::Overflow {
            if value.len() != 8 {
                return Err(Error::CorruptOverflowChain(format!(
                    "overflow reference has {} bytes instead of 8",
                    value.len()
                )));
            }
            let reference = leaf_node::OverflowRef {
                first_page: PageId(u32::from_le_bytes([value[0], value[1], value[2], value[3]])),
                total_len: u32::from_le_bytes([value[4], value[5], value[6], value[7]]),
            };
            hasher.update(&reference.total_len.to_le_bytes());
            let digest = overflow_digest(&reference)?;
            if digest == [0u8; MERKLE_HASH_SIZE] {
                return Err(Error::CorruptOverflowChain(format!(
                    "overflow head {} has no payload digest",
                    reference.first_page
                )));
            }
            hasher.update(&digest);
        } else {
            let value_len = u32::try_from(value.len()).map_err(|_| Error::DatabaseCorrupted)?;
            hasher.update(&value_len.to_le_bytes());
            hasher.update(value);
        }
    }
    Ok(truncate_hash(&hasher.finalize()))
}

pub fn compute_tree_merkle(
    pages: &mut impl MutablePageMap,
    root: PageId,
    base_txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    compute_page_merkle(pages, root, base_txn_id, read_clean_hash)
}

fn compute_page_merkle(
    pages: &mut impl MutablePageMap,
    page_id: PageId,
    base_txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    let page = match pages.get_page(&page_id) {
        Some(page) => page,
        None => return read_clean_hash(page_id),
    };

    // Savepoints bump txn_id mid-txn, so single-value equality won't work here.
    if page.txn_id() < base_txn_id {
        return Ok(page.merkle_hash());
    }

    let page_type = page.page_type();
    let hash = match page_type {
        Some(PageType::Leaf) => hash_logical_leaf_cells(
            (0..page.num_cells()).map(|index| {
                let cell = leaf_node::read_cell(page, index);
                (cell.key, cell.val_type, cell.value)
            }),
            |reference| match pages.get_page(&reference.first_page) {
                Some(head) => Ok(head.merkle_hash()),
                None => read_clean_hash(reference.first_page),
            },
        )?,
        Some(PageType::Branch) => {
            // Collect IDs before recursing — pages map borrow would conflict.
            let num_cells = page.num_cells();
            let mut children: Vec<PageId> = Vec::with_capacity(num_cells as usize + 1);
            for i in 0..num_cells as usize {
                children.push(branch_node::get_child(page, i));
            }
            let right = page.right_child();
            if right.is_valid() {
                children.push(right);
            }

            let mut hasher = blake3::Hasher::new();
            let mut complete = true;
            for child_id in children {
                let child_hash =
                    compute_page_merkle(pages, child_id, base_txn_id, read_clean_hash)?;
                // Zero means UNKNOWN, not the hash of a child. Hashing that
                // sentinel into a nonzero parent would let two partially
                // known, divergent trees compare equal and be pruned by sync.
                if child_hash == [0u8; MERKLE_HASH_SIZE] {
                    complete = false;
                } else {
                    hasher.update(&child_hash);
                }
            }
            if complete {
                truncate_hash(&hasher.finalize())
            } else {
                [0u8; MERKLE_HASH_SIZE]
            }
        }
        _ => [0u8; MERKLE_HASH_SIZE],
    };

    let page = pages.get_page_mut(&page_id).unwrap();
    page.set_merkle_hash(&hash);

    Ok(hash)
}

#[cfg(test)]
fn compute_leaf_hash(page: &Page) -> [u8; MERKLE_HASH_SIZE] {
    hash_logical_leaf_cells(
        (0..page.num_cells()).map(|index| {
            let cell = leaf_node::read_cell(page, index);
            (cell.key, cell.val_type, cell.value)
        }),
        |_| Err(Error::DatabaseCorrupted),
    )
    .expect("test leaf contains no overflow references")
}

/// Truncate a 32-byte BLAKE3 hash to MERKLE_HASH_SIZE (28 bytes).
pub(crate) fn truncate_hash(hash: &blake3::Hash) -> [u8; MERKLE_HASH_SIZE] {
    let mut out = [0u8; MERKLE_HASH_SIZE];
    out.copy_from_slice(&hash.as_bytes()[..MERKLE_HASH_SIZE]);
    out
}

#[cfg(test)]
#[path = "merkle_tests.rs"]
mod tests;
