use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Mutex;

use crate::protocol::SyncMessage;
use crate::transport::{SyncError, SyncTransport};

/// In-memory transport for testing sync sessions.
///
/// Uses `mpsc` channels for bidirectional communication.
/// Thread-safe (`Send + Sync`) so each side can be shared with a
/// scoped thread via `&self`.
///
/// Create a connected pair with `MemoryTransport::pair()`.
pub struct MemoryTransport {
    tx: Mutex<mpsc::Sender<Vec<u8>>>,
    rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    closed: AtomicBool,
}

impl MemoryTransport {
    /// Create a connected pair of transports.
    ///
    /// Messages sent on one side are received by the other.
    pub fn pair() -> (Self, Self) {
        let (tx_a, rx_b) = mpsc::channel();
        let (tx_b, rx_a) = mpsc::channel();

        let a = MemoryTransport {
            tx: Mutex::new(tx_a),
            rx: Mutex::new(rx_a),
            closed: AtomicBool::new(false),
        };
        let b = MemoryTransport {
            tx: Mutex::new(tx_b),
            rx: Mutex::new(rx_b),
            closed: AtomicBool::new(false),
        };

        (a, b)
    }
}

impl SyncTransport for MemoryTransport {
    fn send(&self, msg: &SyncMessage) -> std::result::Result<(), SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let data = msg.serialize();
        let tx = self.tx.lock().unwrap();
        tx.send(data).map_err(|_| SyncError::Closed)
    }

    fn recv(&self) -> std::result::Result<SyncMessage, SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let rx = self.rx.lock().unwrap();
        let data = rx.recv().map_err(|_| SyncError::Closed)?;
        Ok(SyncMessage::deserialize(&data)?)
    }

    fn close(&self) -> std::result::Result<(), SyncError> {
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;
    use citadel_core::types::PageId;
    use citadel_core::MERKLE_HASH_SIZE;

    #[test]
    fn pair_send_recv() {
        let (a, b) = MemoryTransport::pair();
        let msg = SyncMessage::Hello {
            node_id: NodeId::from_u64(1),
            root_page: PageId(0),
            root_hash: [0u8; MERKLE_HASH_SIZE],
        };
        a.send(&msg).unwrap();
        let received = b.recv().unwrap();
        match received {
            SyncMessage::Hello { node_id, .. } => {
                assert_eq!(node_id, NodeId::from_u64(1));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn bidirectional() {
        let (a, b) = MemoryTransport::pair();
        a.send(&SyncMessage::Done).unwrap();
        b.send(&SyncMessage::Done).unwrap();

        assert!(matches!(b.recv().unwrap(), SyncMessage::Done));
        assert!(matches!(a.recv().unwrap(), SyncMessage::Done));
    }

    #[test]
    fn ordering_preserved() {
        let (a, b) = MemoryTransport::pair();
        for i in 0..10u64 {
            a.send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(i),
                root_page: PageId(0),
                root_hash: [0u8; MERKLE_HASH_SIZE],
            })
            .unwrap();
        }
        for i in 0..10u64 {
            match b.recv().unwrap() {
                SyncMessage::Hello { node_id, .. } => {
                    assert_eq!(node_id, NodeId::from_u64(i));
                }
                _ => panic!("wrong variant"),
            }
        }
    }

    #[test]
    fn close_prevents_send() {
        let (a, _b) = MemoryTransport::pair();
        a.close().unwrap();
        let err = a.send(&SyncMessage::Done).unwrap_err();
        assert!(matches!(err, SyncError::Closed));
    }

    #[test]
    fn close_prevents_recv() {
        let (a, _b) = MemoryTransport::pair();
        a.close().unwrap();
        let err = a.recv().unwrap_err();
        assert!(matches!(err, SyncError::Closed));
    }

    #[test]
    fn dropped_sender_causes_recv_error() {
        let (a, b) = MemoryTransport::pair();
        drop(a);
        let err = b.recv().unwrap_err();
        assert!(matches!(err, SyncError::Closed));
    }
}
