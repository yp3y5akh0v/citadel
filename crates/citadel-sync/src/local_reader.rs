use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{Result, MERKLE_HASH_SIZE};
use citadel_page::{branch_node, leaf_node, leaf_node::OverflowRef};
use citadel_txn::manager::TxnManager;
use citadel_txn::read_txn::ReadTxn;

use crate::diff::{DiffEntry, MerkleHash, PageDigest, TreeReader};
use crate::protocol::{MAX_SYNC_PAYLOAD_SIZE, MAX_SYNC_VALUE_SIZE};

/// `TreeReader` implementation for a local database.
pub struct LocalTreeReader<'a> {
    snapshot: Arc<LocalSnapshot<'a>>,
    root_page: PageId,
    root_hash: MerkleHash,
    trust_merkle: bool,
    authorization: Mutex<ReaderAuthorization>,
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

/// One registered reader horizon may back every named-table reader advertised
/// by a multi-table sync session.
struct LocalSnapshot<'a> {
    transaction: ReadTxn<'a>,
}

pub(crate) type NamedTreeReaders<'a> = Vec<(Vec<u8>, LocalTreeReader<'a>)>;

impl<'a> LocalTreeReader<'a> {
    /// Create a reader for the default (main) tree.
    pub fn new(manager: &'a TxnManager) -> Self {
        let snapshot = manager.begin_read();
        let root_page = snapshot.root();
        let root_hash = snapshot.root_hash();
        let trust_merkle = snapshot.has_logical_merkle_hashes();
        Self {
            snapshot: Arc::new(LocalSnapshot {
                transaction: snapshot,
            }),
            root_page,
            root_hash,
            trust_merkle,
            authorization: Mutex::new(ReaderAuthorization::new(root_page)),
        }
    }

    /// Create a reader for a named table's tree.
    ///
    /// The name and root are resolved through the same registered snapshot
    /// that serves all later page reads.
    pub fn for_table(manager: &'a TxnManager, table_name: &[u8]) -> Result<Self> {
        let snapshot = manager.begin_read();
        let root_page = snapshot.table_root_page(table_name)?.ok_or_else(|| {
            citadel_core::Error::TableNotFound(String::from_utf8_lossy(table_name).into_owned())
        })?;
        let trust_merkle = snapshot.has_logical_merkle_hashes();
        let root_hash = if trust_merkle {
            snapshot.read_reachable_page(root_page)?.merkle_hash()
        } else {
            [0u8; MERKLE_HASH_SIZE]
        };
        Ok(Self {
            snapshot: Arc::new(LocalSnapshot {
                transaction: snapshot,
            }),
            root_page,
            root_hash,
            trust_merkle,
            authorization: Mutex::new(ReaderAuthorization::new(root_page)),
        })
    }

    pub(crate) fn commit_generation(&self) -> u64 {
        self.snapshot.transaction.commit_generation()
    }

    /// Capture all named tables through one catalog snapshot and one reader
    /// horizon. Each returned reader advertises and serves the same generation.
    pub(crate) fn for_all_tables(manager: &'a TxnManager) -> Result<(u64, NamedTreeReaders<'a>)> {
        let transaction = manager.begin_read();
        let commit_generation = transaction.commit_generation();
        let tables = transaction.list_tables()?;
        let trust_merkle = transaction.has_logical_merkle_hashes();
        let snapshot = Arc::new(LocalSnapshot { transaction });
        let mut readers = Vec::with_capacity(tables.len());
        for (name, descriptor) in tables {
            let root_hash = if trust_merkle {
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
        Ok((commit_generation, readers))
    }
}

impl<'a> TreeReader for LocalTreeReader<'a> {
    fn root_info(&self) -> Result<(PageId, MerkleHash)> {
        Ok((self.root_page, self.root_hash))
    }

    fn page_digest(&self, page_id: PageId) -> Result<PageDigest> {
        if !self
            .authorization
            .lock()
            .map_err(|_| citadel_core::Error::DatabaseCorrupted)?
            .allowed
            .contains(&page_id)
        {
            return Err(citadel_core::Error::DatabaseCorrupted);
        }
        let page = self.snapshot.transaction.read_reachable_page(page_id)?;
        let page_type = page
            .page_type()
            .ok_or_else(|| citadel_core::Error::InvalidPageType(page.page_type_raw(), page_id))?;
        let merkle_hash = if self.trust_merkle {
            page.merkle_hash()
        } else {
            [0u8; MERKLE_HASH_SIZE]
        };
        let mut children = Vec::new();

        match page_type {
            PageType::Branch => {
                let cells = branch_node::read_cells_checked(&page)
                    .map_err(|_| citadel_core::Error::DatabaseCorrupted)?;
                children.extend(cells.into_iter().map(|cell| cell.child));
                children.push(page.right_child());
                self.authorization
                    .lock()
                    .map_err(|_| citadel_core::Error::DatabaseCorrupted)?
                    .allowed
                    .extend(children.iter().copied());
            }
            PageType::Leaf => {
                leaf_node::read_cells_checked(&page)
                    .map_err(|_| citadel_core::Error::DatabaseCorrupted)?;
                self.authorization
                    .lock()
                    .map_err(|_| citadel_core::Error::DatabaseCorrupted)?
                    .proven_leaves
                    .insert(page_id);
            }
            _ => {
                return Err(citadel_core::Error::InvalidPageType(
                    page.page_type_raw(),
                    page_id,
                ));
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
        if page_id != self.root_page
            && !self
                .authorization
                .lock()
                .map_err(|_| citadel_core::Error::DatabaseCorrupted)?
                .proven_leaves
                .contains(&page_id)
        {
            return Err(citadel_core::Error::DatabaseCorrupted);
        }
        let page = self.snapshot.transaction.read_reachable_page(page_id)?;
        if page.page_type() != Some(PageType::Leaf) {
            return Err(citadel_core::Error::InvalidPageType(
                page.page_type_raw(),
                page_id,
            ));
        }

        let cells = leaf_node::read_cells_checked(&page)
            .map_err(|_| citadel_core::Error::DatabaseCorrupted)?;

        // Prove the response fits before following any overflow chain, rather
        // than allocating a large value only to find the leaf unrepresentable.
        let mut payload_len = 4usize; // entry count
        for cell in &cells {
            let value_len = match cell.val_type {
                ValueType::Overflow => OverflowRef::from_bytes(cell.value).total_len as usize,
                ValueType::Inline | ValueType::Tombstone => cell.value.len(),
            };
            if value_len > MAX_SYNC_VALUE_SIZE {
                return Err(citadel_core::Error::Sync(format!(
                    "value is {value_len} bytes, sync limit is {MAX_SYNC_VALUE_SIZE}; streaming sync is required"
                )));
            }
            payload_len = payload_len
                .checked_add(7)
                .and_then(|len| len.checked_add(cell.key.len()))
                .and_then(|len| len.checked_add(value_len))
                .ok_or_else(|| citadel_core::Error::Sync("sync response size overflow".into()))?;
            if payload_len > MAX_SYNC_PAYLOAD_SIZE {
                return Err(citadel_core::Error::Sync(format!(
                    "leaf response is {payload_len} bytes, sync payload limit is {MAX_SYNC_PAYLOAD_SIZE}"
                )));
            }
        }

        let mut entries = Vec::with_capacity(cells.len());
        for cell in cells {
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
