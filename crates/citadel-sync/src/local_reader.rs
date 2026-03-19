use citadel_core::types::{PageId, PageType};
use citadel_core::{Result, MERKLE_HASH_SIZE};
use citadel_page::{branch_node, leaf_node};
use citadel_txn::manager::TxnManager;

use crate::diff::{DiffEntry, MerkleHash, PageDigest, TreeReader};

/// `TreeReader` implementation for a local database.
pub struct LocalTreeReader<'a> {
    manager: &'a TxnManager,
    root_page: PageId,
    root_hash: MerkleHash,
}

impl<'a> LocalTreeReader<'a> {
    /// Create a reader for the default (main) tree.
    pub fn new(manager: &'a TxnManager) -> Self {
        let slot = manager.current_slot();
        Self {
            manager,
            root_page: slot.tree_root,
            root_hash: slot.merkle_root,
        }
    }

    /// Create a reader for a named table's tree.
    pub fn for_table(manager: &'a TxnManager, root_page: PageId) -> Result<Self> {
        let root_hash = if root_page.is_valid() {
            manager.read_page_from_disk(root_page)?.merkle_hash()
        } else {
            [0u8; MERKLE_HASH_SIZE]
        };
        Ok(Self {
            manager,
            root_page,
            root_hash,
        })
    }
}

impl<'a> TreeReader for LocalTreeReader<'a> {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((self.root_page, self.root_hash))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        let page = self.manager.read_page_from_disk(page_id)?;
        let page_type = page
            .page_type()
            .ok_or_else(|| citadel_core::Error::InvalidPageType(page.page_type_raw(), page_id))?;
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
