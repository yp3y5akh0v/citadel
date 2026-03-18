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
mod tests {
    use super::*;

    #[test]
    fn generate_unique() {
        let a = SyncKey::generate();
        let b = SyncKey::generate();
        assert_ne!(a.0, b.0);
    }

    #[test]
    fn base64_roundtrip() {
        let key = SyncKey::generate();
        let encoded = key.to_base64();
        let decoded = SyncKey::from_base64(&encoded).unwrap();
        assert_eq!(key.0, decoded.0);
    }

    #[test]
    fn from_bytes_roundtrip() {
        let raw = [0xABu8; KEY_SIZE];
        let key = SyncKey::from_bytes(raw);
        assert_eq!(*key.as_bytes(), raw);
    }

    #[test]
    fn invalid_base64_rejected() {
        assert!(SyncKey::from_base64("not-valid-base64!!!").is_err());
    }

    #[test]
    fn wrong_length_rejected() {
        let short = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
        assert!(SyncKey::from_base64(&short).is_err());
    }

    #[test]
    fn debug_redacts() {
        let key = SyncKey::generate();
        let debug = format!("{:?}", key);
        assert_eq!(debug, "SyncKey([REDACTED])");
        assert!(!debug.contains(&key.to_base64()));
    }

    #[test]
    fn display_is_base64() {
        let key = SyncKey::generate();
        assert_eq!(format!("{}", key), key.to_base64());
    }

    #[test]
    fn base64_length_is_44() {
        let key = SyncKey::generate();
        assert_eq!(key.to_base64().len(), 44);
    }
}
