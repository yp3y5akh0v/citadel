use citadel_core::types::{PageId, PageType};
use citadel_core::{Result, MERKLE_HASH_SIZE};
use citadel_page::{branch_node, leaf_node};
use citadel_txn::manager::TxnManager;

use crate::diff::{DiffEntry, MerkleHash, PageDigest, TreeReader};

/// `TreeReader` implementation for a local database.
///
/// Reads pages directly from the `TxnManager`, decrypting and
/// verifying HMAC on each read.
pub struct LocalTreeReader<'a> {
    manager: &'a TxnManager,
}

impl<'a> LocalTreeReader<'a> {
    pub fn new(manager: &'a TxnManager) -> Self {
        Self { manager }
    }
}

impl<'a> TreeReader for LocalTreeReader<'a> {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        let slot = self.manager.current_slot();
        Ok((slot.tree_root, slot.merkle_root))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        let page = self.manager.read_page_from_disk(page_id)?;
        let page_type = page
            .page_type()
            .ok_or_else(|| {
                citadel_core::Error::InvalidPageType(page.page_type_raw(), page_id)
            })?;
        let merkle_hash: [u8; MERKLE_HASH_SIZE] = page.merkle_hash();
        let mut children = Vec::new();

        if page_type == PageType::Branch {
            for i in 0..page.num_cells() as usize {
                children.push(branch_node::get_child(&page, i));
            }
            let right = page.right_child();
            if right.is_valid() {
                children.push(right);
            }
        }

        Ok(PageDigest {
            page_id,
            page_type,
            merkle_hash,
            children,
        })
    }

    fn leaf_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>> {
        let page = self.manager.read_page_from_disk(page_id)?;
        let mut entries = Vec::with_capacity(page.num_cells() as usize);
        for i in 0..page.num_cells() {
            let cell = leaf_node::read_cell(&page, i);
            entries.push(DiffEntry {
                key: cell.key.to_vec(),
                value: cell.value.to_vec(),
                val_type: cell.val_type as u8,
            });
        }
        Ok(entries)
    }
}
