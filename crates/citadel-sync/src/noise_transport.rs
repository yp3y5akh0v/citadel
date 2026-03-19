use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::protocol::SyncMessage;
use crate::sync_key::SyncKey;
use crate::transport::{SyncError, SyncTransport};

const NOISE_PATTERN: &str = "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s";

/// Max plaintext per Noise message (65535 - 16 byte AEAD tag).
const NOISE_MAX_PAYLOAD: usize = 65535 - 16;

/// Maximum total message size: 64 MiB.
const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

struct NoiseState {
    stream: TcpStream,
    noise: snow::TransportState,
}

/// Noise-encrypted TCP transport for sync sessions.
#[derive(Debug)]
pub struct NoiseTransport {
    state: Mutex<NoiseState>,
    closed: AtomicBool,
}

impl std::fmt::Debug for NoiseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoiseState").finish_non_exhaustive()
    }
}

impl NoiseTransport {
    /// Connect to a remote peer and perform Noise handshake (initiator).
    pub fn connect(addr: &str, key: &SyncKey) -> Result<Self, SyncError> {
        let addr = addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| SyncError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;
        let mut stream = TcpStream::connect_timeout(&addr, DEFAULT_TIMEOUT)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(DEFAULT_TIMEOUT))?;
        stream.set_write_timeout(Some(DEFAULT_TIMEOUT))?;

        let noise = Self::handshake_initiator(&mut stream, key)?;

        Ok(Self {
            state: Mutex::new(NoiseState { stream, noise }),
            closed: AtomicBool::new(false),
        })
    }

    /// Accept an incoming connection and perform Noise handshake (responder).
    pub fn accept(stream: TcpStream, key: &SyncKey) -> Result<Self, SyncError> {
        let mut stream = stream;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(DEFAULT_TIMEOUT))?;
        stream.set_write_timeout(Some(DEFAULT_TIMEOUT))?;

        let noise = Self::handshake_responder(&mut stream, key)?;

        Ok(Self {
            state: Mutex::new(NoiseState { stream, noise }),
            closed: AtomicBool::new(false),
        })
    }

    fn handshake_initiator(
        stream: &mut TcpStream,
        key: &SyncKey,
    ) -> Result<snow::TransportState, SyncError> {
        let mut initiator = snow::Builder::new(NOISE_PATTERN.parse().expect("valid pattern"))
            .psk(0, key.as_bytes())
            .map_err(noise_err)?
            .build_initiator()
            .map_err(noise_err)?;

        let mut buf = [0u8; 65535];

        // -> e, psk
        let len = initiator.write_message(&[], &mut buf).map_err(noise_err)?;
        write_handshake_msg(stream, &buf[..len])?;

        // <- e, ee
        let msg = read_handshake_msg(stream)
            .map_err(|_| SyncError::Handshake("connection lost (wrong sync key?)".into()))?;
        initiator
            .read_message(&msg, &mut buf)
            .map_err(|_| SyncError::Handshake("decryption failed (wrong sync key?)".into()))?;

        initiator.into_transport_mode().map_err(noise_err)
    }

    fn handshake_responder(
        stream: &mut TcpStream,
        key: &SyncKey,
    ) -> Result<snow::TransportState, SyncError> {
        let mut responder = snow::Builder::new(NOISE_PATTERN.parse().expect("valid pattern"))
            .psk(0, key.as_bytes())
            .map_err(noise_err)?
            .build_responder()
            .map_err(noise_err)?;

        let mut buf = [0u8; 65535];

        // <- e, psk
        let msg = read_handshake_msg(stream)
            .map_err(|_| SyncError::Handshake("connection lost (wrong sync key?)".into()))?;
        responder
            .read_message(&msg, &mut buf)
            .map_err(|_| SyncError::Handshake("decryption failed (wrong sync key?)".into()))?;

        // -> e, ee
        let len = responder.write_message(&[], &mut buf).map_err(noise_err)?;
        write_handshake_msg(stream, &buf[..len])?;

        responder.into_transport_mode().map_err(noise_err)
    }
}

impl SyncTransport for NoiseTransport {
    fn send(&self, msg: &SyncMessage) -> Result<(), SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let plaintext = msg.serialize();
        let state = &mut *self.state.lock().unwrap();

        state
            .stream
            .write_all(&(plaintext.len() as u32).to_le_bytes())?;

        let mut offset = 0;
        let mut ct_buf = [0u8; 65535];
        while offset < plaintext.len() {
            let end = (offset + NOISE_MAX_PAYLOAD).min(plaintext.len());
            let ct_len = state
                .noise
                .write_message(&plaintext[offset..end], &mut ct_buf)
                .map_err(noise_err)?;
            state.stream.write_all(&(ct_len as u16).to_le_bytes())?;
            state.stream.write_all(&ct_buf[..ct_len])?;
            offset = end;
        }
        state.stream.flush()?;
        Ok(())
    }

    fn recv(&self) -> Result<SyncMessage, SyncError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(SyncError::Closed);
        }
        let state = &mut *self.state.lock().unwrap();

        let mut len_buf = [0u8; 4];
        state.stream.read_exact(&mut len_buf)?;
        let total_len = u32::from_le_bytes(len_buf);
        if total_len > MAX_MESSAGE_SIZE {
            return Err(SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message too large: {total_len} bytes"),
            )));
        }

        let mut plaintext = Vec::with_capacity(total_len as usize);
        let mut pt_buf = [0u8; 65535];
        while plaintext.len() < total_len as usize {
            let mut chunk_len_buf = [0u8; 2];
            state.stream.read_exact(&mut chunk_len_buf)?;
            let chunk_len = u16::from_le_bytes(chunk_len_buf) as usize;

            let mut ct_buf = vec![0u8; chunk_len];
            state.stream.read_exact(&mut ct_buf)?;

            let pt_len = state
                .noise
                .read_message(&ct_buf, &mut pt_buf)
                .map_err(noise_err)?;
            plaintext.extend_from_slice(&pt_buf[..pt_len]);
        }

        Ok(SyncMessage::deserialize(&plaintext)?)
    }

    fn close(&self) -> Result<(), SyncError> {
        self.closed.store(true, Ordering::Relaxed);
        let state = self.state.lock().unwrap();
        state.stream.shutdown(std::net::Shutdown::Both).ok();
        Ok(())
    }
}

fn write_handshake_msg(stream: &mut TcpStream, data: &[u8]) -> Result<(), SyncError> {
    stream.write_all(&(data.len() as u16).to_le_bytes())?;
    stream.write_all(data)?;
    stream.flush()?;
    Ok(())
}

fn read_handshake_msg(stream: &mut TcpStream) -> Result<Vec<u8>, SyncError> {
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf)?;
    let len = u16::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

fn noise_err(e: snow::Error) -> SyncError {
    SyncError::Handshake(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::NodeId;
    use citadel_core::types::PageId;
    use citadel_core::MERKLE_HASH_SIZE;
    use std::net::TcpListener;
    use std::thread;

    fn loopback_pair(key: &SyncKey) -> (NoiseTransport, NoiseTransport) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let key_clone = key.clone();
        let client =
            thread::spawn(move || NoiseTransport::connect(&addr.to_string(), &key_clone).unwrap());
        let (stream, _) = listener.accept().unwrap();
        let server = NoiseTransport::accept(stream, key).unwrap();
        let client = client.join().unwrap();
        (client, server)
    }

    fn test_key() -> SyncKey {
        SyncKey::from_bytes([0x42u8; 32])
    }

    #[test]
    fn encrypted_roundtrip() {
        let key = test_key();
        let (client, server) = loopback_pair(&key);
        let msg = SyncMessage::Hello {
            node_id: NodeId::from_u64(42),
            root_page: PageId(10),
            root_hash: [1u8; MERKLE_HASH_SIZE],
        };
        client.send(&msg).unwrap();
        match server.recv().unwrap() {
            SyncMessage::Hello {
                node_id,
                root_page,
                root_hash,
            } => {
                assert_eq!(node_id, NodeId::from_u64(42));
                assert_eq!(root_page, PageId(10));
                assert_eq!(root_hash, [1u8; MERKLE_HASH_SIZE]);
            }
            other => panic!("expected Hello, got {:?}", other),
        }
    }

    #[test]
    fn wrong_key_fails() {
        let key_a = SyncKey::from_bytes([0x01u8; 32]);
        let key_b = SyncKey::from_bytes([0x02u8; 32]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client_handle =
            thread::spawn(move || NoiseTransport::connect(&addr.to_string(), &key_a));
        let (stream, _) = listener.accept().unwrap();
        let server_result = NoiseTransport::accept(stream, &key_b);
        let client_result = client_handle.join().unwrap();

        // At least one side should fail
        assert!(
            server_result.is_err() || client_result.is_err(),
            "mismatched keys should cause handshake failure"
        );
    }

    #[test]
    fn bidirectional() {
        let key = test_key();
        let (a, b) = loopback_pair(&key);
        a.send(&SyncMessage::Done).unwrap();
        b.send(&SyncMessage::PullRequest).unwrap();
        assert!(matches!(b.recv().unwrap(), SyncMessage::Done));
        assert!(matches!(a.recv().unwrap(), SyncMessage::PullRequest));
    }

    #[test]
    fn large_message_chunking() {
        let key = test_key();
        let (a, b) = loopback_pair(&key);
        let data = vec![0xABu8; 256 * 1024]; // 256 KiB (requires chunking)
        let data_clone = data.clone();
        // Send/recv must run concurrently: ciphertext exceeds TCP send buffer
        thread::scope(|s| {
            s.spawn(|| {
                a.send(&SyncMessage::PatchData { data: data_clone })
                    .unwrap();
            });
            match b.recv().unwrap() {
                SyncMessage::PatchData { data: received } => {
                    assert_eq!(received.len(), data.len());
                    assert_eq!(received, data);
                }
                _ => panic!("wrong variant"),
            }
        });
    }

    #[test]
    fn close_prevents_send() {
        let key = test_key();
        let (a, _b) = loopback_pair(&key);
        a.close().unwrap();
        assert!(matches!(
            a.send(&SyncMessage::Done).unwrap_err(),
            SyncError::Closed
        ));
    }

    #[test]
    fn close_prevents_recv() {
        let key = test_key();
        let (a, _b) = loopback_pair(&key);
        a.close().unwrap();
        assert!(matches!(a.recv().unwrap_err(), SyncError::Closed));
    }

    #[test]
    fn multiple_messages() {
        let key = test_key();
        let (a, b) = loopback_pair(&key);
        for i in 0..100u64 {
            a.send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(i),
                root_page: PageId(0),
                root_hash: [0u8; MERKLE_HASH_SIZE],
            })
            .unwrap();
        }
        for i in 0..100u64 {
            match b.recv().unwrap() {
                SyncMessage::Hello { node_id, .. } => {
                    assert_eq!(node_id, NodeId::from_u64(i));
                }
                _ => panic!("wrong variant"),
            }
        }
    }
}
