use citadel_core::types::PageId;

use crate::diff::{DiffEntry, MerkleHash, PageDigest, TreeReader};
use crate::protocol::SyncMessage;

/// Errors from sync transport operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("transport I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),

    #[error("unexpected message: expected {expected}, got {actual}")]
    UnexpectedMessage { expected: String, actual: String },

    #[error("remote error: {0}")]
    Remote(String),

    #[error("transport closed")]
    Closed,

    #[error("database error: {0}")]
    Database(#[from] citadel_core::Error),

    #[error("patch error: {0}")]
    Patch(#[from] crate::patch::PatchError),
}

/// Bidirectional message transport for sync protocol.
///
/// Methods take `&self` (not `&mut self`) to allow shared access between
/// the `SyncSession` and `RemoteTreeReader` during Merkle diff computation.
/// Implementations use interior mutability for stream state.
pub trait SyncTransport: Send {
    /// Send a message to the remote peer.
    fn send(&self, msg: &SyncMessage) -> std::result::Result<(), SyncError>;

    /// Receive the next message from the remote peer.
    fn recv(&self) -> std::result::Result<SyncMessage, SyncError>;

    /// Close the transport connection.
    fn close(&self) -> std::result::Result<(), SyncError>;
}

/// `TreeReader` that reads from a remote database via `SyncTransport`.
///
/// Sends `DigestRequest`/`EntriesRequest` messages and blocks waiting
/// for responses. Used by `merkle_diff()` to compare trees across nodes.
pub struct RemoteTreeReader<'a> {
    transport: &'a dyn SyncTransport,
    root_page: PageId,
    root_hash: MerkleHash,
}

impl<'a> RemoteTreeReader<'a> {
    pub fn new(
        transport: &'a dyn SyncTransport,
        root_page: PageId,
        root_hash: MerkleHash,
    ) -> Self {
        Self { transport, root_page, root_hash }
    }
}

impl TreeReader for RemoteTreeReader<'_> {
    fn root_info(&self) -> citadel_core::Result<(PageId, MerkleHash)> {
        Ok((self.root_page, self.root_hash))
    }

    fn page_digest(&self, page_id: PageId) -> citadel_core::Result<PageDigest> {
        self.transport.send(&SyncMessage::DigestRequest {
            page_ids: vec![page_id],
        }).map_err(sync_to_core)?;

        match self.transport.recv().map_err(sync_to_core)? {
            SyncMessage::DigestResponse { mut digests } if !digests.is_empty() => {
                Ok(digests.remove(0))
            }
            SyncMessage::DigestResponse { .. } => {
                Err(citadel_core::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "empty digest response",
                )))
            }
            SyncMessage::Error { message } => {
                Err(citadel_core::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    message,
                )))
            }
            other => Err(citadel_core::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected DigestResponse, got {}", msg_name(&other)),
            ))),
        }
    }

    fn leaf_entries(&self, page_id: PageId) -> citadel_core::Result<Vec<DiffEntry>> {
        self.transport.send(&SyncMessage::EntriesRequest {
            page_ids: vec![page_id],
        }).map_err(sync_to_core)?;

        match self.transport.recv().map_err(sync_to_core)? {
            SyncMessage::EntriesResponse { entries } => Ok(entries),
            SyncMessage::Error { message } => {
                Err(citadel_core::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    message,
                )))
            }
            other => Err(citadel_core::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected EntriesResponse, got {}", msg_name(&other)),
            ))),
        }
    }
}

fn sync_to_core(e: SyncError) -> citadel_core::Error {
    citadel_core::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

pub(crate) fn msg_name(msg: &SyncMessage) -> &'static str {
    match msg {
        SyncMessage::Hello { .. } => "Hello",
        SyncMessage::HelloAck { .. } => "HelloAck",
        SyncMessage::DigestRequest { .. } => "DigestRequest",
        SyncMessage::DigestResponse { .. } => "DigestResponse",
        SyncMessage::EntriesRequest { .. } => "EntriesRequest",
        SyncMessage::EntriesResponse { .. } => "EntriesResponse",
        SyncMessage::PatchData { .. } => "PatchData",
        SyncMessage::PatchAck { .. } => "PatchAck",
        SyncMessage::Done => "Done",
        SyncMessage::Error { .. } => "Error",
        SyncMessage::PullRequest => "PullRequest",
        SyncMessage::PullResponse { .. } => "PullResponse",
    }
}
