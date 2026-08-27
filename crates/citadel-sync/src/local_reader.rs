use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{Error, Result, MERKLE_HASH_SIZE};
use citadel_page::{branch_node, leaf_node, leaf_node::OverflowRef};
use citadel_txn::manager::TxnManager;
use citadel_txn::read_txn::ReadTxn;

use crate::diff::{DiffEntry, MerkleHash, PageDigest, TreeReader};

const MAX_SYNC_PAYLOAD_SIZE: usize = 64 * 1024 * 1024 - 5;

/// `TreeReader` implementation for a local database.
pub struct LocalTreeReader<'a> {
    snapshot: Arc<LocalSnapshot<'a>>,
    root_page: PageId,
    root_hash: MerkleHash,
    trust_merkle: bool,
    authorization: Mutex<ReaderAuthorization>,
}

struct LocalSnapshot<'a> {
    transaction: ReadTxn<'a>,
}

struct ReaderAuthorization {
    allowed: HashSet<PageId>,
    proven_leaves: HashSet<PageId>,
}

impl ReaderAuthorization {
    fn new(root_page: PageId) -> Self {
        Self {
            allowed: HashSet::from([root_page]),
            proven_leaves: HashSet::new(),
        }
    }
}

pub(crate) type NamedTreeReaders<'a> = Vec<(Vec<u8>, LocalTreeReader<'a>)>;

impl<'a> LocalTreeReader<'a> {
    /// Create a reader for the default (main) tree.
    pub fn new(manager: &'a TxnManager) -> Self {
        let transaction = manager.begin_read();
        let root_page = transaction.root();
        let root_hash = transaction.root_hash();
        let trust_merkle = transaction.has_logical_merkle_hashes();
        Self {
            snapshot: Arc::new(LocalSnapshot { transaction }),
            root_page,
            root_hash,
            trust_merkle,
            authorization: Mutex::new(ReaderAuthorization::new(root_page)),
        }
    }

    /// Create a reader for a named table's tree.
    pub fn for_table(manager: &'a TxnManager, table_name: &[u8]) -> Result<Self> {
        let transaction = manager.begin_read();
        let root_page = transaction.table_root_page(table_name)?.ok_or_else(|| {
            Error::TableNotFound(String::from_utf8_lossy(table_name).into_owned())
        })?;
        let trust_merkle = transaction.has_logical_merkle_hashes();
        let root_hash = if trust_merkle && root_page.is_valid() {
            transaction.read_reachable_page(root_page)?.merkle_hash()
        } else {
            [0u8; MERKLE_HASH_SIZE]
        };
        Ok(Self {
            snapshot: Arc::new(LocalSnapshot { transaction }),
            root_page,
            root_hash,
            trust_merkle,
            authorization: Mutex::new(ReaderAuthorization::new(root_page)),
        })
    }

    /// Capture all named trees from one catalog generation and keep that
    /// generation pinned until every returned reader is dropped.
    pub(crate) fn for_all_tables(manager: &'a TxnManager) -> Result<NamedTreeReaders<'a>> {
        let transaction = manager.begin_read();
        let tables = transaction.list_tables()?;
        let trust_merkle = transaction.has_logical_merkle_hashes();
        let snapshot = Arc::new(LocalSnapshot { transaction });
        let mut readers = Vec::with_capacity(tables.len());
        for (name, descriptor) in tables {
            let root_hash = if trust_merkle && descriptor.root_page.is_valid() {
                snapshot
                    .transaction
                    .read_reachable_page(descriptor.root_page)?
                    .merkle_hash()
            } else {
                [0u8; MERKLE_HASH_SIZE]
            };
            readers.push((
                name,
                Self {
                    snapshot: Arc::clone(&snapshot),
                    root_page: descriptor.root_page,
                    root_hash,
                    trust_merkle,
                    authorization: Mutex::new(ReaderAuthorization::new(descriptor.root_page)),
                },
            ));
        }
        Ok(readers)
    }
}

impl TreeReader for LocalTreeReader<'_> {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((self.root_page, self.root_hash))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        if !self
            .authorization
            .lock()
            .map_err(|_| Error::DatabaseCorrupted)?
            .allowed
            .contains(&page_id)
        {
            return Err(Error::DatabaseCorrupted);
        }

        let page = self.snapshot.transaction.read_reachable_page(page_id)?;
        let page_type = page
            .page_type()
            .ok_or_else(|| Error::InvalidPageType(page.page_type_raw(), page_id))?;
        let merkle_hash = if self.trust_merkle {
            page.merkle_hash()
        } else {
            [0u8; MERKLE_HASH_SIZE]
        };
        let mut children = Vec::new();

        match page_type {
            PageType::Branch => {
                for index in 0..page.num_cells() as usize {
                    children.push(branch_node::get_child(&page, index));
                }
                let right = page.right_child();
                if !right.is_valid() {
                    return Err(Error::DatabaseCorrupted);
                }
                children.push(right);
                self.authorization
                    .lock()
                    .map_err(|_| Error::DatabaseCorrupted)?
                    .allowed
                    .extend(children.iter().copied());
            }
            PageType::Leaf => {
                self.authorization
                    .lock()
                    .map_err(|_| Error::DatabaseCorrupted)?
                    .proven_leaves
                    .insert(page_id);
            }
            _ => return Err(Error::InvalidPageType(page.page_type_raw(), page_id)),
        }

        Ok(PageDigest {
            page_id,
            page_type,
            merkle_hash,
            children,
        })
    }

    fn leaf_entries(&self, page_id: PageId) -> Result<Vec<DiffEntry>> {
        if page_id != self.root_page
            && !self
                .authorization
                .lock()
                .map_err(|_| Error::DatabaseCorrupted)?
                .proven_leaves
                .contains(&page_id)
        {
            return Err(Error::DatabaseCorrupted);
        }

        let page = self.snapshot.transaction.read_reachable_page(page_id)?;
        if page.page_type() != Some(PageType::Leaf) {
            return Err(Error::InvalidPageType(page.page_type_raw(), page_id));
        }

        let mut payload_len = 4usize;
        for index in 0..page.num_cells() {
            let cell = leaf_node::read_cell(&page, index);
            let value_len = match cell.val_type {
                ValueType::Overflow => {
                    if cell.value.len() != 8 {
                        return Err(Error::DatabaseCorrupted);
                    }
                    OverflowRef::from_bytes(cell.value).total_len as usize
                }
                ValueType::Inline | ValueType::Tombstone => cell.value.len(),
            };
            payload_len = payload_len
                .checked_add(7)
                .and_then(|len| len.checked_add(cell.key.len()))
                .and_then(|len| len.checked_add(value_len))
                .ok_or_else(|| Error::Sync("sync response size overflow".into()))?;
            if payload_len > MAX_SYNC_PAYLOAD_SIZE {
                return Err(Error::Sync(format!(
                    "leaf response is {payload_len} bytes, sync payload limit is {MAX_SYNC_PAYLOAD_SIZE}; streaming sync is required"
                )));
            }
        }

        let mut entries = Vec::with_capacity(page.num_cells() as usize);
        for index in 0..page.num_cells() {
            let cell = leaf_node::read_cell(&page, index);
            let value = match cell.val_type {
                ValueType::Overflow => {
                    let reference = OverflowRef::from_bytes(cell.value);
                    self.snapshot
                        .transaction
                        .read_reachable_overflow_value(&reference)?
                }
                ValueType::Inline | ValueType::Tombstone => cell.value.to_vec(),
            };
            entries.push(DiffEntry {
                key: cell.key.to_vec(),
                value,
                val_type: cell.val_type as u8,
            });
        }
        Ok(entries)
    }
}
