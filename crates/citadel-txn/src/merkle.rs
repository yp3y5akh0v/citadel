//! Inline Merkle hashing for sync diff detection.
//!
//! Each page stores a BLAKE3 hash (28 bytes) in its header at [36..64].
//! - Leaf pages: hash of all cell contents (key-value entries in sorted order)
//! - Branch pages: hash of all children's Merkle hashes concatenated
//!
//! The root page's hash serves as a database fingerprint - if two snapshots
//! have the same root hash, they contain identical data.

use rustc_hash::FxHashMap;

use citadel_core::types::{PageId, PageType, TxnId};
use citadel_core::{Result, MERKLE_HASH_SIZE};
use citadel_page::page::Page;
use citadel_page::{branch_node, leaf_node};

pub fn compute_tree_merkle(
    pages: &mut FxHashMap<PageId, Page>,
    root: PageId,
    base_txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    compute_page_merkle(pages, root, base_txn_id, read_clean_hash)
}

fn compute_page_merkle(
    pages: &mut FxHashMap<PageId, Page>,
    page_id: PageId,
    base_txn_id: TxnId,
    read_clean_hash: &dyn Fn(PageId) -> Result<[u8; MERKLE_HASH_SIZE]>,
) -> Result<[u8; MERKLE_HASH_SIZE]> {
    let page = match pages.get(&page_id) {
        Some(page) => page,
        None => return read_clean_hash(page_id),
    };

    // Savepoints bump txn_id mid-txn, so single-value equality won't work here.
    if page.txn_id() < base_txn_id {
        return Ok(page.merkle_hash());
    }

    let page_type = page.page_type();
    let hash = match page_type {
        Some(PageType::Leaf) => compute_leaf_hash(page),
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
            for child_id in children {
                let child_hash =
                    compute_page_merkle(pages, child_id, base_txn_id, read_clean_hash)?;
                hasher.update(&child_hash);
            }
            truncate_hash(&hasher.finalize())
        }
        _ => [0u8; MERKLE_HASH_SIZE],
    };

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
#[path = "merkle_tests.rs"]
mod tests;
