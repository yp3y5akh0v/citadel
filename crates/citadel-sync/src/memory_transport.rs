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
#[path = "memory_transport_tests.rs"]
mod tests;
