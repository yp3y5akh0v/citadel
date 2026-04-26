use base64::Engine;
use zeroize::Zeroize;

const KEY_SIZE: usize = 32;

/// 32-byte pre-shared key for encrypted sync transport.
#[derive(Clone)]
pub struct SyncKey([u8; KEY_SIZE]);

impl SyncKey {
    /// Generate a random sync key.
    pub fn generate() -> Self {
        use rand::RngCore;
        let mut key = [0u8; KEY_SIZE];
        rand::thread_rng().fill_bytes(&mut key);
        Self(key)
    }

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; KEY_SIZE]) -> Self {
        Self(bytes)
    }

    /// Decode from base64.
    pub fn from_base64(s: &str) -> Result<Self, SyncKeyError> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| SyncKeyError(e.to_string()))?;
        if bytes.len() != KEY_SIZE {
            return Err(SyncKeyError(format!(
                "expected {} bytes, got {}",
                KEY_SIZE,
                bytes.len()
            )));
        }
        let mut arr = [0u8; KEY_SIZE];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Encode as base64.
    pub fn to_base64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }

    /// Borrow the raw bytes.
    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.0
    }
}

impl Drop for SyncKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl std::fmt::Debug for SyncKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SyncKey([REDACTED])")
    }
}

impl std::fmt::Display for SyncKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_base64())
    }
}

/// Error decoding a sync key.
#[derive(Debug, Clone)]
pub struct SyncKeyError(pub String);

impl std::fmt::Display for SyncKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid sync key: {}", self.0)
    }
}

impl std::error::Error for SyncKeyError {}

#[cfg(test)]
#[path = "sync_key_tests.rs"]
mod tests;
