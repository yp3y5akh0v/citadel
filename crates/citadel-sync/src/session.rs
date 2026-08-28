use std::collections::{BTreeSet, HashMap, HashSet};

use citadel_txn::manager::TxnManager;

use crate::apply::{apply_patch_if_generation, apply_patch_to_table_if_generation, ApplyResult};
use crate::diff::{merkle_diff, MerkleHash, TreeReader, UNKNOWN_HASH};
use crate::local_reader::LocalTreeReader;
use crate::node_id::NodeId;
use crate::patch::SyncPatch;
use crate::protocol::{SyncMessage, TableInfo};
use crate::transport::{msg_name, RemoteTreeReader, SyncError, SyncTransport};

use citadel_core::types::PageId;
use citadel_core::MAX_KEY_SIZE;

/// Sync direction for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Push local changes to the remote peer.
    Push,
    /// Pull remote changes to the local database.
    Pull,
    /// Push then pull (full bidirectional sync).
    Bidirectional,
}

/// Configuration for a sync session.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub node_id: NodeId,
    pub direction: SyncDirection,
    pub crdt_aware: bool,
}

/// Outcome of a sync session.
#[derive(Debug, Clone)]
pub struct SyncOutcome {
    /// Result of the push phase (if Push or Bidirectional).
    pub pushed: Option<ApplyResult>,
    /// Result of the pull phase (if Pull or Bidirectional).
    pub pulled: Option<ApplyResult>,
    /// True if both databases were already identical.
    pub already_in_sync: bool,
}

/// Orchestrates a sync session between two databases.
///
/// The initiator drives the protocol: sends Hello, computes diffs,
/// builds patches, and coordinates push/pull phases.
/// The responder answers requests and applies patches.
pub struct SyncSession {
    config: SyncConfig,
}

impl SyncSession {
    pub fn new(config: SyncConfig) -> Self {
        Self { config }
    }

    /// Run as the initiator (client) side of a sync session.
    pub fn sync_as_initiator(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
    ) -> std::result::Result<SyncOutcome, SyncError> {
        let local_reader = LocalTreeReader::new(manager);
        let (local_root, local_hash) = local_reader.root_info().map_err(SyncError::Database)?;

        transport.send(&SyncMessage::Hello {
            node_id: self.config.node_id,
            root_page: local_root,
            root_hash: local_hash,
            crdt_aware: self.config.crdt_aware,
        })?;

        let (remote_root, remote_hash, in_sync) = match transport.recv()? {
            SyncMessage::HelloAck {
                root_page,
                root_hash,
                in_sync,
                crdt_aware,
                ..
            } => {
                if crdt_aware != self.config.crdt_aware {
                    return Err(SyncError::Handshake(
                        "peer uses a different CRDT mode".into(),
                    ));
                }
                (root_page, root_hash, in_sync)
            }
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => {
                return Err(SyncError::UnexpectedMessage {
                    expected: "HelloAck".into(),
                    actual: msg_name(&other).into(),
                })
            }
        };

        let hashes_in_sync = local_hash == remote_hash && local_hash != UNKNOWN_HASH;
        if in_sync != hashes_in_sync {
            return Err(SyncError::Handshake(
                "peer sent an in_sync flag inconsistent with the advertised root hash".into(),
            ));
        }

        if in_sync {
            transport.send(&SyncMessage::Done)?;
            return Ok(SyncOutcome {
                pushed: None,
                pulled: None,
                already_in_sync: true,
            });
        }

        let mut outcome = SyncOutcome {
            pushed: None,
            pulled: None,
            already_in_sync: false,
        };

        // Push phase: diff(local -> remote), send patch to remote
        if self.config.direction == SyncDirection::Push
            || self.config.direction == SyncDirection::Bidirectional
        {
            let result = self.initiator_push(&local_reader, transport, remote_root, remote_hash)?;
            outcome.pushed = Some(result);
        }

        // Pull phase: diff(remote -> local), apply patch locally
        if self.config.direction == SyncDirection::Pull
            || self.config.direction == SyncDirection::Bidirectional
        {
            // For bidirectional after push, get updated remote state
            let (pull_root, pull_hash) = if self.config.direction == SyncDirection::Bidirectional {
                transport.send(&SyncMessage::PullRequest)?;
                match transport.recv()? {
                    SyncMessage::PullResponse {
                        root_page,
                        root_hash,
                    } => (root_page, root_hash),
                    SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
                    other => {
                        return Err(SyncError::UnexpectedMessage {
                            expected: "PullResponse".into(),
                            actual: msg_name(&other).into(),
                        })
                    }
                }
            } else {
                (remote_root, remote_hash)
            };

            let result =
                self.initiator_pull(manager, &local_reader, transport, pull_root, pull_hash)?;
            outcome.pulled = Some(result);
        }

        transport.send(&SyncMessage::Done)?;
        Ok(outcome)
    }

    /// Run as the responder (server) side of a sync session.
    pub fn sync_as_responder(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
    ) -> std::result::Result<SyncOutcome, SyncError> {
        let mut local_reader = LocalTreeReader::new(manager);
        let mut expected_generation = local_reader.commit_generation();
        let (local_root, local_hash) = notify_database_result(local_reader.root_info(), transport)?;

        let remote_hash = match transport.recv()? {
            SyncMessage::Hello {
                root_hash,
                crdt_aware,
                ..
            } => {
                if crdt_aware != self.config.crdt_aware {
                    transport.send(&SyncMessage::Error {
                        message: "peers use different CRDT modes".into(),
                    })?;
                    return Err(SyncError::Handshake(
                        "peer uses a different CRDT mode".into(),
                    ));
                }
                root_hash
            }
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => {
                return Err(SyncError::UnexpectedMessage {
                    expected: "Hello".into(),
                    actual: msg_name(&other).into(),
                })
            }
        };

        // Two Off-mode endpoints both present UNKNOWN_HASH while divergent.
        let in_sync = local_hash == remote_hash && local_hash != UNKNOWN_HASH;

        transport.send(&SyncMessage::HelloAck {
            node_id: self.config.node_id,
            root_page: local_root,
            root_hash: local_hash,
            in_sync,
            crdt_aware: self.config.crdt_aware,
        })?;

        if in_sync {
            match transport.recv()? {
                SyncMessage::Done => {}
                other => {
                    transport.send(&SyncMessage::Error {
                        message: "expected Done after an in-sync handshake".into(),
                    })?;
                    return Err(SyncError::UnexpectedMessage {
                        expected: "Done".into(),
                        actual: msg_name(&other).into(),
                    });
                }
            }
            return Ok(SyncOutcome {
                pushed: None,
                pulled: None,
                already_in_sync: true,
            });
        }

        let mut outcome = SyncOutcome {
            pushed: None,
            pulled: None,
            already_in_sync: false,
        };

        'messages: loop {
            let msg = transport.recv()?;
            match msg {
                SyncMessage::DigestRequest { page_ids } => {
                    let Some(page_id) = single_page_request(&page_ids) else {
                        transport.send(&SyncMessage::Error {
                            message: "digest request must contain exactly one page".into(),
                        })?;
                        continue;
                    };
                    match local_reader.page_digest(page_id) {
                        Ok(digest) => transport.send(&SyncMessage::DigestResponse {
                            digests: vec![digest],
                        })?,
                        Err(error) => {
                            transport.send(&SyncMessage::Error {
                                message: error.to_string(),
                            })?;
                            continue 'messages;
                        }
                    }
                }
                SyncMessage::EntriesRequest { page_ids } => {
                    let Some(page_id) = single_page_request(&page_ids) else {
                        transport.send(&SyncMessage::Error {
                            message: "entries request must contain exactly one page".into(),
                        })?;
                        continue;
                    };
                    match local_reader.leaf_entries(page_id) {
                        Ok(entries) => transport.send(&SyncMessage::EntriesResponse { entries })?,
                        Err(error) => {
                            transport.send(&SyncMessage::Error {
                                message: error.to_string(),
                            })?;
                            continue 'messages;
                        }
                    }
                }
                SyncMessage::PatchData { data } => {
                    let patch = notify_patch_result(SyncPatch::deserialize(&data), transport)?;
                    if patch.crdt_aware != self.config.crdt_aware {
                        transport.send(&SyncMessage::Error {
                            message: "patch CRDT mode differs from the negotiated session".into(),
                        })?;
                        return Err(SyncError::Handshake(
                            "patch uses a different CRDT mode".into(),
                        ));
                    }
                    let Some((result, generation)) = notify_database_result(
                        apply_patch_if_generation(manager, &patch, expected_generation),
                        transport,
                    )?
                    else {
                        let error = "sync target changed after its root was advertised";
                        transport.send(&SyncMessage::Error {
                            message: error.into(),
                        })?;
                        return Err(SyncError::Database(citadel_core::Error::Sync(error.into())));
                    };
                    expected_generation = generation;
                    outcome.pushed = Some(result.clone());
                    transport.send(&SyncMessage::PatchAck { result })?;
                }
                SyncMessage::PullRequest => {
                    // A preceding push may have committed, so advertise one fresh
                    // snapshot and retain that reader for the whole pull phase.
                    local_reader = LocalTreeReader::new(manager);
                    expected_generation = local_reader.commit_generation();
                    let (root_page, root_hash) =
                        notify_database_result(local_reader.root_info(), transport)?;
                    transport.send(&SyncMessage::PullResponse {
                        root_page,
                        root_hash,
                    })?;
                }
                SyncMessage::Done => {
                    break;
                }
                SyncMessage::Error { message } => {
                    return Err(SyncError::Remote(message));
                }
                _ => {
                    transport.send(&SyncMessage::Error {
                        message: "unexpected message".into(),
                    })?;
                }
            }
        }

        Ok(outcome)
    }

    /// Push: diff(local -> remote) via merkle_diff, send patch.
    fn initiator_push(
        &self,
        local_reader: &LocalTreeReader<'_>,
        transport: &dyn SyncTransport,
        remote_root: PageId,
        remote_hash: MerkleHash,
    ) -> std::result::Result<ApplyResult, SyncError> {
        let remote_reader = RemoteTreeReader::new(transport, remote_root, remote_hash);

        // source = local, target = remote
        let diff = notify_database_result(merkle_diff(local_reader, &remote_reader), transport)?;

        if diff.is_empty() {
            return Ok(ApplyResult::empty());
        }

        let patch = SyncPatch::from_diff_owned(self.config.node_id, diff, self.config.crdt_aware);
        send_patch_and_wait(&patch, transport)
    }

    /// Run multi-table sync as the initiator.
    ///
    /// Each table commits independently, so an error can leave earlier tables
    /// durable: treat it as a potentially partial sync.
    pub fn sync_tables_as_initiator(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
    ) -> std::result::Result<Vec<(Vec<u8>, ApplyResult)>, SyncError> {
        transport.send(&SyncMessage::TableListRequest {
            crdt_aware: self.config.crdt_aware,
        })?;

        let remote_tables = match transport.recv()? {
            SyncMessage::TableListResponse { tables } => tables,
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => {
                return Err(SyncError::UnexpectedMessage {
                    expected: "TableListResponse".into(),
                    actual: msg_name(&other).into(),
                })
            }
        };
        if let Err(message) = validate_table_infos(&remote_tables) {
            transport.send(&SyncMessage::Error {
                message: message.clone(),
            })?;
            return Err(SyncError::Database(citadel_core::Error::Sync(message)));
        }

        let (_, local_tables) =
            notify_database_result(LocalTreeReader::for_all_tables(manager), transport)?;
        if let Some((name, _)) = local_tables
            .iter()
            .find(|(name, _)| is_reserved_table(name))
        {
            let message = format!(
                "all-table sync does not support reserved table '{}'",
                String::from_utf8_lossy(name)
            );
            transport.send(&SyncMessage::Error {
                message: message.clone(),
            })?;
            return Err(SyncError::Database(citadel_core::Error::Sync(message)));
        }

        let local_tables: HashMap<_, _> = local_tables.into_iter().collect();
        let remote_tables: HashMap<_, _> = remote_tables
            .into_iter()
            .map(|info| (info.name.clone(), info))
            .collect();
        let all_names: BTreeSet<_> = local_tables
            .keys()
            .chain(remote_tables.keys())
            .cloned()
            .collect();

        let mut results = Vec::new();

        for table_name in all_names {
            let remote_info = remote_tables.get(&table_name);
            let local_reader = local_tables.get(&table_name);
            let (local_root, local_hash) = match local_reader {
                Some(reader) => notify_database_result(reader.root_info(), transport)?,
                None => (PageId::INVALID, UNKNOWN_HASH),
            };

            let remote_root = remote_info.map(|t| t.root_page).unwrap_or(PageId::INVALID);
            let remote_hash = remote_info
                .map(|t| t.root_hash)
                .unwrap_or([0u8; citadel_core::MERKLE_HASH_SIZE]);

            if local_hash == remote_hash
                && local_hash != UNKNOWN_HASH
                && local_root.is_valid()
                && remote_root.is_valid()
            {
                continue;
            }

            transport.send(&SyncMessage::TableSyncBegin {
                table_name: table_name.clone(),
                root_page: local_root,
                root_hash: local_hash,
            })?;

            if let (Some(local_reader), true) = (local_reader, remote_root.is_valid()) {
                let remote_reader = RemoteTreeReader::new(transport, remote_root, remote_hash);
                let diff =
                    notify_database_result(merkle_diff(local_reader, &remote_reader), transport)?;

                if !diff.is_empty() {
                    let patch = SyncPatch::from_diff_owned(
                        self.config.node_id,
                        diff,
                        self.config.crdt_aware,
                    );
                    let result = send_patch_and_wait(&patch, transport)?;
                    results.push((table_name.clone(), result));
                }
            } else if let Some(local_reader) = local_reader {
                let entries =
                    notify_database_result(local_reader.subtree_entries(local_root), transport)?;
                let diff = crate::diff::DiffResult {
                    entries,
                    pages_compared: 0,
                    subtrees_skipped: 0,
                };
                let patch =
                    SyncPatch::from_diff_owned(self.config.node_id, diff, self.config.crdt_aware);
                let result = send_patch_and_wait(&patch, transport)?;
                results.push((table_name.clone(), result));
            }

            transport.send(&SyncMessage::TableSyncEnd {
                table_name: table_name.clone(),
            })?;
        }

        transport.send(&SyncMessage::Done)?;
        Ok(results)
    }

    /// Handle multi-table sync as the responder.
    ///
    /// Each table commits independently, so an error can leave earlier tables
    /// durable: treat it as a potentially partial sync.
    pub fn handle_table_sync_as_responder(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
    ) -> std::result::Result<Vec<(Vec<u8>, ApplyResult)>, SyncError> {
        match transport.recv()? {
            SyncMessage::TableListRequest { crdt_aware } => {
                if crdt_aware != self.config.crdt_aware {
                    transport.send(&SyncMessage::Error {
                        message: "peers use different CRDT modes".into(),
                    })?;
                    return Err(SyncError::Handshake(
                        "peer uses a different CRDT mode".into(),
                    ));
                }
            }
            SyncMessage::Done => return Ok(Vec::new()),
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => {
                return Err(SyncError::UnexpectedMessage {
                    expected: "TableListRequest".into(),
                    actual: msg_name(&other).into(),
                })
            }
        }

        let (mut expected_generation, local_tables) =
            notify_database_result(LocalTreeReader::for_all_tables(manager), transport)?;
        if let Some((name, _)) = local_tables
            .iter()
            .find(|(name, _)| is_reserved_table(name))
        {
            let message = format!(
                "all-table sync does not support reserved table '{}'",
                String::from_utf8_lossy(name)
            );
            transport.send(&SyncMessage::Error {
                message: message.clone(),
            })?;
            return Err(SyncError::Database(citadel_core::Error::Sync(message)));
        }
        let mut table_readers = HashMap::with_capacity(local_tables.len());
        let mut table_infos = Vec::new();
        for (name, reader) in local_tables {
            let (root_page, root_hash) = notify_database_result(reader.root_info(), transport)?;
            table_infos.push(TableInfo {
                name: name.clone(),
                root_page,
                root_hash,
            });
            table_readers.insert(name, reader);
        }
        table_infos.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if let Err(message) = validate_table_infos(&table_infos) {
            transport.send(&SyncMessage::Error {
                message: message.clone(),
            })?;
            return Err(SyncError::Database(citadel_core::Error::Sync(message)));
        }
        transport.send(&SyncMessage::TableListResponse {
            tables: table_infos,
        })?;

        let mut results = Vec::new();
        let mut current_table: Option<Vec<u8>> = None;

        'messages: loop {
            let msg = transport.recv()?;
            match msg {
                SyncMessage::TableSyncBegin {
                    table_name,
                    root_page,
                    ..
                } => {
                    if current_table.is_some() {
                        transport.send(&SyncMessage::Error {
                            message: "nested table sync scope".into(),
                        })?;
                        continue;
                    }
                    if table_name.is_empty() || table_name.len() > MAX_KEY_SIZE {
                        transport.send(&SyncMessage::Error {
                            message: format!("invalid table sync name length {}", table_name.len()),
                        })?;
                        continue;
                    }
                    if is_reserved_table(&table_name) {
                        transport.send(&SyncMessage::Error {
                            message: "reserved tables cannot be synchronized directly".into(),
                        })?;
                        continue;
                    }
                    let locally_advertised = table_readers.contains_key(table_name.as_slice());
                    if !locally_advertised && !root_page.is_valid() {
                        transport.send(&SyncMessage::Error {
                            message: "table was advertised by neither peer".into(),
                        })?;
                        continue;
                    }
                    current_table = Some(table_name);
                }
                SyncMessage::TableSyncEnd { table_name } => {
                    if current_table.as_deref() != Some(table_name.as_slice()) {
                        transport.send(&SyncMessage::Error {
                            message: "table sync end does not match the active table".into(),
                        })?;
                        continue;
                    }
                    current_table = None;
                }
                SyncMessage::DigestRequest { page_ids } => {
                    let Some(tname) = current_table.as_ref() else {
                        transport.send(&SyncMessage::Error {
                            message: "digest request outside a table sync".into(),
                        })?;
                        continue;
                    };
                    let Some(reader) = table_readers.get(tname.as_slice()) else {
                        transport.send(&SyncMessage::Error {
                            message: format!(
                                "table '{}' was not advertised",
                                String::from_utf8_lossy(tname)
                            ),
                        })?;
                        continue;
                    };

                    let Some(page_id) = single_page_request(&page_ids) else {
                        transport.send(&SyncMessage::Error {
                            message: "digest request must contain exactly one page".into(),
                        })?;
                        continue;
                    };
                    match reader.page_digest(page_id) {
                        Ok(digest) => transport.send(&SyncMessage::DigestResponse {
                            digests: vec![digest],
                        })?,
                        Err(error) => {
                            transport.send(&SyncMessage::Error {
                                message: error.to_string(),
                            })?;
                            continue 'messages;
                        }
                    }
                }
                SyncMessage::EntriesRequest { page_ids } => {
                    let Some(tname) = current_table.as_ref() else {
                        transport.send(&SyncMessage::Error {
                            message: "entries request outside a table sync".into(),
                        })?;
                        continue;
                    };
                    let Some(reader) = table_readers.get(tname.as_slice()) else {
                        transport.send(&SyncMessage::Error {
                            message: format!(
                                "table '{}' was not advertised",
                                String::from_utf8_lossy(tname)
                            ),
                        })?;
                        continue;
                    };

                    let Some(page_id) = single_page_request(&page_ids) else {
                        transport.send(&SyncMessage::Error {
                            message: "entries request must contain exactly one page".into(),
                        })?;
                        continue;
                    };
                    match reader.leaf_entries(page_id) {
                        Ok(entries) => transport.send(&SyncMessage::EntriesResponse { entries })?,
                        Err(error) => {
                            transport.send(&SyncMessage::Error {
                                message: error.to_string(),
                            })?;
                            continue 'messages;
                        }
                    }
                }
                SyncMessage::PatchData { data } => {
                    let Some(table_name) = current_table.as_ref() else {
                        transport.send(&SyncMessage::Error {
                            message: "patch outside a table sync".into(),
                        })?;
                        continue;
                    };
                    let patch = notify_patch_result(SyncPatch::deserialize(&data), transport)?;
                    if patch.crdt_aware != self.config.crdt_aware {
                        transport.send(&SyncMessage::Error {
                            message: "patch CRDT mode differs from the negotiated session".into(),
                        })?;
                        return Err(SyncError::Handshake(
                            "patch uses a different CRDT mode".into(),
                        ));
                    }
                    let Some((result, generation)) = notify_database_result(
                        apply_patch_to_table_if_generation(
                            manager,
                            table_name,
                            &patch,
                            expected_generation,
                        ),
                        transport,
                    )?
                    else {
                        let error = "sync target changed after its table list was advertised";
                        transport.send(&SyncMessage::Error {
                            message: error.into(),
                        })?;
                        return Err(SyncError::Database(citadel_core::Error::Sync(error.into())));
                    };
                    expected_generation = generation;
                    results.push((table_name.clone(), result.clone()));
                    transport.send(&SyncMessage::PatchAck { result })?;
                }
                SyncMessage::Done if current_table.is_none() => break,
                SyncMessage::Done => {
                    return Err(SyncError::UnexpectedMessage {
                        expected: "TableSyncEnd".into(),
                        actual: "Done".into(),
                    });
                }
                SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
                _ => {
                    transport.send(&SyncMessage::Error {
                        message: "unexpected message in table sync".into(),
                    })?;
                }
            }
        }

        Ok(results)
    }

    /// Pull: diff(remote -> local) via merkle_diff, apply locally.
    fn initiator_pull(
        &self,
        manager: &TxnManager,
        local_reader: &LocalTreeReader<'_>,
        transport: &dyn SyncTransport,
        remote_root: PageId,
        remote_hash: MerkleHash,
    ) -> std::result::Result<ApplyResult, SyncError> {
        let (_, local_hash) = notify_database_result(local_reader.root_info(), transport)?;

        if local_hash == remote_hash && local_hash != UNKNOWN_HASH {
            return Ok(ApplyResult::empty());
        }

        let remote_reader = RemoteTreeReader::new(transport, remote_root, remote_hash);

        // source = remote, target = local
        let diff = notify_database_result(merkle_diff(&remote_reader, local_reader), transport)?;

        if diff.is_empty() {
            return Ok(ApplyResult::empty());
        }

        let patch = SyncPatch::from_diff_owned(self.config.node_id, diff, self.config.crdt_aware);
        let Some((result, _generation)) = notify_database_result(
            apply_patch_if_generation(manager, &patch, local_reader.commit_generation()),
            transport,
        )?
        else {
            return notify_database_result(
                Err(citadel_core::Error::Sync(
                    "sync target changed while its diff was being computed".into(),
                )),
                transport,
            );
        };
        Ok(result)
    }
}

fn single_page_request(page_ids: &[PageId]) -> Option<PageId> {
    match page_ids {
        [page_id] if page_id.is_valid() => Some(*page_id),
        _ => None,
    }
}

fn is_reserved_table(name: &[u8]) -> bool {
    name.starts_with(b"__")
}

fn serialize_patch(
    patch: &SyncPatch,
    transport: &dyn SyncTransport,
) -> std::result::Result<Vec<u8>, SyncError> {
    patch.try_serialize().map_err(|error| {
        let _ = transport.send(&SyncMessage::Error {
            message: error.to_string(),
        });
        SyncError::Patch(error)
    })
}

fn send_patch_and_wait(
    patch: &SyncPatch,
    transport: &dyn SyncTransport,
) -> std::result::Result<ApplyResult, SyncError> {
    transport.send(&SyncMessage::PatchData {
        data: serialize_patch(patch, transport)?,
    })?;
    match transport.recv()? {
        SyncMessage::PatchAck { result } => Ok(result),
        SyncMessage::Error { message } => Err(SyncError::Remote(message)),
        other => Err(SyncError::UnexpectedMessage {
            expected: "PatchAck".into(),
            actual: msg_name(&other).into(),
        }),
    }
}

fn notify_patch_result<T>(
    result: std::result::Result<T, crate::patch::PatchError>,
    transport: &dyn SyncTransport,
) -> std::result::Result<T, SyncError> {
    result.map_err(|error| {
        let _ = transport.send(&SyncMessage::Error {
            message: error.to_string(),
        });
        SyncError::Patch(error)
    })
}

fn notify_database_result<T>(
    result: citadel_core::Result<T>,
    transport: &dyn SyncTransport,
) -> std::result::Result<T, SyncError> {
    result.map_err(|error| {
        let _ = transport.send(&SyncMessage::Error {
            message: error.to_string(),
        });
        SyncError::Database(error)
    })
}

fn validate_table_infos(tables: &[TableInfo]) -> std::result::Result<(), String> {
    let mut names = HashSet::with_capacity(tables.len());
    let mut roots = HashSet::with_capacity(tables.len());
    for table in tables {
        if table.name.is_empty() || table.name.len() > MAX_KEY_SIZE {
            return Err(format!(
                "invalid advertised table name length {}",
                table.name.len()
            ));
        }
        if is_reserved_table(&table.name) {
            return Err(format!(
                "all-table sync does not support reserved table '{}'",
                String::from_utf8_lossy(&table.name)
            ));
        }
        if !names.insert(table.name.as_slice()) {
            return Err(format!(
                "table '{}' was advertised more than once",
                String::from_utf8_lossy(&table.name)
            ));
        }
        if !table.root_page.is_valid() {
            return Err(format!(
                "table '{}' advertised an invalid root page",
                String::from_utf8_lossy(&table.name)
            ));
        }
        if !roots.insert(table.root_page) {
            return Err(format!(
                "root page {} was advertised for more than one table",
                table.root_page.as_u32()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
