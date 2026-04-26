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
#[path = "noise_transport_tests.rs"]
mod tests;
