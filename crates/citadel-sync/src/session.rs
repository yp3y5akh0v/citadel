use citadel_txn::manager::TxnManager;

use crate::apply::{apply_patch, ApplyResult};
use crate::diff::{merkle_diff, MerkleHash, TreeReader};
use crate::local_reader::LocalTreeReader;
use crate::node_id::NodeId;
use crate::patch::SyncPatch;
use crate::protocol::SyncMessage;
use crate::transport::{msg_name, RemoteTreeReader, SyncError, SyncTransport};

use citadel_core::types::PageId;

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

        // Hello exchange
        transport.send(&SyncMessage::Hello {
            node_id: self.config.node_id,
            root_page: local_root,
            root_hash: local_hash,
        })?;

        let (remote_root, remote_hash, in_sync) = match transport.recv()? {
            SyncMessage::HelloAck { root_page, root_hash, in_sync, .. } => {
                (root_page, root_hash, in_sync)
            }
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => return Err(SyncError::UnexpectedMessage {
                expected: "HelloAck".into(),
                actual: msg_name(&other).into(),
            }),
        };

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

        // Push phase: diff(local → remote), send patch to remote
        if self.config.direction == SyncDirection::Push
            || self.config.direction == SyncDirection::Bidirectional
        {
            let result = self.initiator_push(
                manager, transport, remote_root, remote_hash,
            )?;
            outcome.pushed = Some(result);
        }

        // Pull phase: diff(remote → local), apply patch locally
        if self.config.direction == SyncDirection::Pull
            || self.config.direction == SyncDirection::Bidirectional
        {
            // For bidirectional after push, get updated remote state
            let (pull_root, pull_hash) = if self.config.direction == SyncDirection::Bidirectional {
                transport.send(&SyncMessage::PullRequest)?;
                match transport.recv()? {
                    SyncMessage::PullResponse { root_page, root_hash } => {
                        (root_page, root_hash)
                    }
                    SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
                    other => return Err(SyncError::UnexpectedMessage {
                        expected: "PullResponse".into(),
                        actual: msg_name(&other).into(),
                    }),
                }
            } else {
                (remote_root, remote_hash)
            };

            let result = self.initiator_pull(
                manager, transport, pull_root, pull_hash,
            )?;
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
        let local_reader = LocalTreeReader::new(manager);
        let (local_root, local_hash) = local_reader.root_info().map_err(SyncError::Database)?;

        // Receive Hello
        let remote_hash = match transport.recv()? {
            SyncMessage::Hello { root_hash, .. } => root_hash,
            SyncMessage::Error { message } => return Err(SyncError::Remote(message)),
            other => return Err(SyncError::UnexpectedMessage {
                expected: "Hello".into(),
                actual: msg_name(&other).into(),
            }),
        };

        let in_sync = local_hash == remote_hash;

        transport.send(&SyncMessage::HelloAck {
            node_id: self.config.node_id,
            root_page: local_root,
            root_hash: local_hash,
            in_sync,
        })?;

        if in_sync {
            match transport.recv()? {
                SyncMessage::Done => {}
                _ => {}
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

        // Serve requests until Done
        loop {
            let msg = transport.recv()?;
            match msg {
                SyncMessage::DigestRequest { page_ids } => {
                    let reader = LocalTreeReader::new(manager);
                    let mut digests = Vec::with_capacity(page_ids.len());
                    for pid in &page_ids {
                        match reader.page_digest(*pid) {
                            Ok(d) => digests.push(d),
                            Err(e) => {
                                transport.send(&SyncMessage::Error {
                                    message: e.to_string(),
                                })?;
                                continue;
                            }
                        }
                    }
                    transport.send(&SyncMessage::DigestResponse { digests })?;
                }
                SyncMessage::EntriesRequest { page_ids } => {
                    let reader = LocalTreeReader::new(manager);
                    let mut entries = Vec::new();
                    for pid in &page_ids {
                        match reader.leaf_entries(*pid) {
                            Ok(e) => entries.extend(e),
                            Err(e) => {
                                transport.send(&SyncMessage::Error {
                                    message: e.to_string(),
                                })?;
                                continue;
                            }
                        }
                    }
                    transport.send(&SyncMessage::EntriesResponse { entries })?;
                }
                SyncMessage::PatchData { data } => {
                    let patch = SyncPatch::deserialize(&data).map_err(SyncError::Patch)?;
                    let result = apply_patch(manager, &patch).map_err(SyncError::Database)?;
                    outcome.pushed = Some(result.clone());
                    transport.send(&SyncMessage::PatchAck { result })?;
                }
                SyncMessage::PullRequest => {
                    let reader = LocalTreeReader::new(manager);
                    let (root_page, root_hash) =
                        reader.root_info().map_err(SyncError::Database)?;
                    transport.send(&SyncMessage::PullResponse { root_page, root_hash })?;
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

    /// Push: diff(local → remote) via merkle_diff, send patch.
    fn initiator_push(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
        remote_root: PageId,
        remote_hash: MerkleHash,
    ) -> std::result::Result<ApplyResult, SyncError> {
        let local_reader = LocalTreeReader::new(manager);
        let remote_reader = RemoteTreeReader::new(transport, remote_root, remote_hash);

        // source = local, target = remote
        let diff = merkle_diff(&local_reader, &remote_reader)
            .map_err(SyncError::Database)?;

        if diff.is_empty() {
            return Ok(ApplyResult::empty());
        }

        let patch = SyncPatch::from_diff(self.config.node_id, &diff, self.config.crdt_aware);
        let patch_data = patch.serialize();

        transport.send(&SyncMessage::PatchData { data: patch_data })?;

        match transport.recv()? {
            SyncMessage::PatchAck { result } => Ok(result),
            SyncMessage::Error { message } => Err(SyncError::Remote(message)),
            other => Err(SyncError::UnexpectedMessage {
                expected: "PatchAck".into(),
                actual: msg_name(&other).into(),
            }),
        }
    }

    /// Pull: diff(remote → local) via merkle_diff, apply locally.
    fn initiator_pull(
        &self,
        manager: &TxnManager,
        transport: &dyn SyncTransport,
        remote_root: PageId,
        remote_hash: MerkleHash,
    ) -> std::result::Result<ApplyResult, SyncError> {
        let local_reader = LocalTreeReader::new(manager);
        let (_, local_hash) = local_reader.root_info().map_err(SyncError::Database)?;

        if local_hash == remote_hash {
            return Ok(ApplyResult::empty());
        }

        let remote_reader = RemoteTreeReader::new(transport, remote_root, remote_hash);

        // source = remote, target = local
        let diff = merkle_diff(&remote_reader, &local_reader)
            .map_err(SyncError::Database)?;

        if diff.is_empty() {
            return Ok(ApplyResult::empty());
        }

        let patch = SyncPatch::from_diff(self.config.node_id, &diff, self.config.crdt_aware);
        let result = apply_patch(manager, &patch).map_err(SyncError::Database)?;
        Ok(result)
    }
}
