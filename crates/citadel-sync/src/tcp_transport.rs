use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::protocol::SyncMessage;
use crate::transport::{SyncError, SyncTransport};

/// Maximum message size: 64 MiB.
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SEND_DEADLINE: Duration = Duration::from_secs(120);

/// TCP transport for sync sessions.
#[derive(Debug)]
pub struct TcpTransport {
    stream: Mutex<TcpStream>,
    closed: AtomicBool,
}

impl TcpTransport {
    /// Connect to a remote peer at the given address.
    pub fn connect(addr: &str) -> Result<Self, SyncError> {
        Self::connect_timeout(addr, DEFAULT_TIMEOUT)
    }

    /// Connect with a custom timeout.
    pub fn connect_timeout(addr: &str, timeout: Duration) -> Result<Self, SyncError> {
        let addr = addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| SyncError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;
        let stream = TcpStream::connect_timeout(&addr, timeout)?;
        Self::from_stream(stream)
    }

    /// Wrap an existing TCP stream.
    pub fn from_stream(stream: TcpStream) -> Result<Self, SyncError> {
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(DEFAULT_TIMEOUT))?;
        stream.set_write_timeout(Some(DEFAULT_TIMEOUT))?;
        Ok(Self {
            stream: Mutex::new(stream),
            closed: AtomicBool::new(false),
        })
    }
}

/// `write_all` with bounded retries on `WouldBlock` and `Interrupted`.
fn write_all_with_deadline(
    stream: &mut TcpStream,
    mut buf: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !buf.is_empty() {
        match stream.write(buf) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write returned 0",
                ));
            }
            Ok(n) => buf = &buf[n..],
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::Interrupted =>
            {
                if Instant::now() >= deadline {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "send deadline exceeded",
                    ));
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

impl SyncTransport for TcpTransport {
    fn send(&self, msg: &SyncMessage) -> Result<(), SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let data = msg.serialize();
        let len = data.len() as u32;
        let deadline = Instant::now() + SEND_DEADLINE;
        let mut stream = self.stream.lock().unwrap();
        write_all_with_deadline(&mut stream, &len.to_le_bytes(), deadline)?;
        write_all_with_deadline(&mut stream, &data, deadline)?;
        stream.flush()?;
        Ok(())
    }

    fn recv(&self) -> Result<SyncMessage, SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let mut stream = self.stream.lock().unwrap();

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);

        if len > MAX_MESSAGE_SIZE {
            return Err(SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message too large: {len} bytes"),
            )));
        }

        let mut data = vec![0u8; len as usize];
        stream.read_exact(&mut data)?;
        Ok(SyncMessage::deserialize(&data)?)
    }

    fn close(&self) -> Result<(), SyncError> {
        self.closed.store(true, Ordering::Relaxed);
        let stream = self.stream.lock().unwrap();
        stream.shutdown(std::net::Shutdown::Both).ok();
        Ok(())
    }
}

#[cfg(test)]
#[path = "tcp_transport_tests.rs"]
mod tests;
