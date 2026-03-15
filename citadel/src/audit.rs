use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

use citadel_core::{
    AUDIT_ENTRY_MAGIC, AUDIT_HEADER_SIZE, AUDIT_LOG_MAGIC, AUDIT_LOG_VERSION, KEY_SIZE, MAC_SIZE,
};

type HmacSha256 = Hmac<Sha256>;

/// Audit log configuration.
#[derive(Debug, Clone)]
pub struct AuditConfig {
    pub enabled: bool,
    pub max_file_size: u64,
    pub max_rotated_files: u32,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_file_size: 10 * 1024 * 1024, // 10 MB
            max_rotated_files: 3,
        }
    }
}

/// Audit event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum AuditEventType {
    DatabaseCreated = 1,
    DatabaseOpened = 2,
    DatabaseClosed = 3,
    PassphraseChanged = 4,
    KeyBackupExported = 5,
    BackupCreated = 6,
    CompactionPerformed = 7,
    IntegrityCheckPerformed = 8,
}

impl AuditEventType {
    fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::DatabaseCreated),
            2 => Some(Self::DatabaseOpened),
            3 => Some(Self::DatabaseClosed),
            4 => Some(Self::PassphraseChanged),
            5 => Some(Self::KeyBackupExported),
            6 => Some(Self::BackupCreated),
            7 => Some(Self::CompactionPerformed),
            8 => Some(Self::IntegrityCheckPerformed),
            _ => None,
        }
    }
}

/// A single audit log entry.
#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub timestamp: u64,
    pub sequence_no: u64,
    pub event_type: AuditEventType,
    pub detail: Vec<u8>,
    pub hmac: [u8; MAC_SIZE],
}

/// Result of verifying an audit log's HMAC chain.
#[derive(Debug)]
pub struct AuditVerifyResult {
    pub entries_verified: u64,
    pub chain_valid: bool,
    pub chain_break_at: Option<u64>,
}

/// Audit log file header (64 bytes).
struct AuditHeader {
    magic: u32,
    version: u32,
    file_id: u64,
    created_at: u64,
    entry_count: u64,
    last_hmac: [u8; MAC_SIZE],
}

impl AuditHeader {
    fn serialize(&self) -> [u8; AUDIT_HEADER_SIZE] {
        let mut buf = [0u8; AUDIT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.created_at.to_le_bytes());
        buf[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[32..64].copy_from_slice(&self.last_hmac);
        buf
    }

    fn deserialize(buf: &[u8; AUDIT_HEADER_SIZE]) -> citadel_core::Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != AUDIT_LOG_MAGIC {
            return Err(citadel_core::Error::InvalidMagic {
                expected: AUDIT_LOG_MAGIC,
                found: magic,
            });
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != AUDIT_LOG_VERSION {
            return Err(citadel_core::Error::UnsupportedVersion(version));
        }
        let mut last_hmac = [0u8; MAC_SIZE];
        last_hmac.copy_from_slice(&buf[32..64]);
        Ok(Self {
            magic,
            version,
            file_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            created_at: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            last_hmac,
        })
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn compute_entry_hmac(
    audit_key: &[u8; KEY_SIZE],
    prev_hmac: &[u8; MAC_SIZE],
    entry_data: &[u8],
) -> [u8; MAC_SIZE] {
    let mut mac = HmacSha256::new_from_slice(audit_key)
        .expect("HMAC key size is always valid");
    mac.update(prev_hmac);
    mac.update(entry_data);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

fn serialize_entry_data(
    timestamp: u64,
    sequence_no: u64,
    event_type: AuditEventType,
    detail: &[u8],
) -> Vec<u8> {
    let detail_len = detail.len() as u16;
    let entry_len = 4 + 8 + 8 + 2 + 2 + detail.len() + MAC_SIZE;
    let mut buf = Vec::with_capacity(entry_len);
    buf.extend_from_slice(&(entry_len as u32).to_le_bytes());
    buf.extend_from_slice(&timestamp.to_le_bytes());
    buf.extend_from_slice(&sequence_no.to_le_bytes());
    buf.extend_from_slice(&(event_type as u16).to_le_bytes());
    buf.extend_from_slice(&detail_len.to_le_bytes());
    buf.extend_from_slice(detail);
    buf
}

/// Internal audit log writer.
pub(crate) struct AuditLog {
    file: File,
    audit_key: [u8; KEY_SIZE],
    prev_hmac: [u8; MAC_SIZE],
    sequence_no: u64,
    entry_count: u64,
    config: AuditConfig,
    path: PathBuf,
    file_id: u64,
}

impl AuditLog {
    pub(crate) fn audit_key(&self) -> &[u8; KEY_SIZE] {
        &self.audit_key
    }

    /// Create a new audit log file.
    pub(crate) fn create(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
    ) -> citadel_core::Result<Self> {
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id,
            created_at: now_nanos(),
            entry_count: 0,
            last_hmac: [0u8; MAC_SIZE],
        };

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.write_all(&header.serialize())?;
        file.sync_data()?;

        Ok(Self {
            file,
            audit_key,
            prev_hmac: [0u8; MAC_SIZE],
            sequence_no: 0,
            entry_count: 0,
            config,
            path: path.to_path_buf(),
            file_id,
        })
    }

    /// Open an existing audit log file, seeking to the end for appending.
    pub(crate) fn open_existing(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
    ) -> citadel_core::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = AuditHeader::deserialize(&header_buf)?;

        if header.file_id != file_id {
            return Err(citadel_core::Error::KeyFileMismatch);
        }

        let mut prev_hmac = [0u8; MAC_SIZE];
        let mut sequence_no = 0u64;
        let mut entry_count = 0u64;

        loop {
            let mut magic_buf = [0u8; 4];
            match file.read_exact(&mut magic_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            if u32::from_le_bytes(magic_buf) != AUDIT_ENTRY_MAGIC {
                break;
            }

            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let entry_len = u32::from_le_bytes(len_buf) as usize;
            if entry_len < 56 {
                break;
            }

            let remaining = entry_len - 4;
            let mut entry_buf = vec![0u8; remaining];
            match file.read_exact(&mut entry_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            sequence_no = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
            prev_hmac.copy_from_slice(&entry_buf[remaining - MAC_SIZE..]);
            entry_count += 1;
        }

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            audit_key,
            prev_hmac,
            sequence_no,
            entry_count,
            config,
            path: path.to_path_buf(),
            file_id,
        })
    }

    /// Log an audit event.
    pub(crate) fn log(
        &mut self,
        event_type: AuditEventType,
        detail: &[u8],
    ) -> citadel_core::Result<()> {
        self.rotate_if_needed()?;

        self.sequence_no += 1;
        let timestamp = now_nanos();
        let entry_data = serialize_entry_data(timestamp, self.sequence_no, event_type, detail);
        let hmac = compute_entry_hmac(&self.audit_key, &self.prev_hmac, &entry_data);

        self.file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes())?;
        self.file.write_all(&entry_data)?;
        self.file.write_all(&hmac)?;
        self.file.sync_data()?;

        self.prev_hmac = hmac;
        self.entry_count += 1;

        self.update_header()?;

        Ok(())
    }

    fn update_header(&mut self) -> citadel_core::Result<()> {
        let pos = self.file.seek(SeekFrom::Current(0))?;

        self.file.seek(SeekFrom::Start(24))?;
        self.file.write_all(&self.entry_count.to_le_bytes())?;
        self.file.write_all(&self.prev_hmac)?;
        self.file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn rotate_if_needed(&mut self) -> citadel_core::Result<()> {
        let file_size = self.file.seek(SeekFrom::End(0))?;
        if file_size < self.config.max_file_size {
            return Ok(());
        }

        self.file.sync_data()?;

        // Shift rotated files: .N → delete, .N-1 → .N, ..., current → .1
        for i in (1..=self.config.max_rotated_files).rev() {
            let src = if i == 1 {
                self.path.clone()
            } else {
                rotated_path(&self.path, i - 1)
            };
            let dst = rotated_path(&self.path, i);

            if i == self.config.max_rotated_files {
                let _ = fs::remove_file(&dst);
            }
            if src.exists() {
                let _ = fs::rename(&src, &dst);
            }
        }

        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: self.file_id,
            created_at: now_nanos(),
            entry_count: 0,
            last_hmac: self.prev_hmac,
        };

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)?;

        file.write_all(&header.serialize())?;
        file.sync_data()?;

        self.file = file;
        self.entry_count = 0;

        Ok(())
    }
}

fn rotated_path(base: &Path, index: u32) -> PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

/// Resolve the audit log path for a database file.
pub(crate) fn resolve_audit_path(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-audit");
    PathBuf::from(name)
}

/// Read all entries from an audit log file (no key needed).
pub fn read_audit_log(path: &Path) -> citadel_core::Result<Vec<AuditEntry>> {
    let mut file = File::open(path)?;

    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let _header = AuditHeader::deserialize(&header_buf)?;

    let mut entries = Vec::new();

    loop {
        let mut magic_buf = [0u8; 4];
        match file.read_exact(&mut magic_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        if u32::from_le_bytes(magic_buf) != AUDIT_ENTRY_MAGIC {
            break;
        }

        let mut len_buf = [0u8; 4];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let entry_len = u32::from_le_bytes(len_buf) as usize;
        if entry_len < 56 {
            break;
        }

        let remaining = entry_len - 4;
        let mut entry_buf = vec![0u8; remaining];
        match file.read_exact(&mut entry_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let timestamp = u64::from_le_bytes(entry_buf[0..8].try_into().unwrap());
        let sequence_no = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
        let event_type_raw = u16::from_le_bytes(entry_buf[16..18].try_into().unwrap());
        let detail_len = u16::from_le_bytes(entry_buf[18..20].try_into().unwrap()) as usize;

        let event_type = match AuditEventType::from_u16(event_type_raw) {
            Some(et) => et,
            None => break,
        };

        if 20 + detail_len + MAC_SIZE != remaining {
            break;
        }

        let detail = entry_buf[20..20 + detail_len].to_vec();
        let mut hmac = [0u8; MAC_SIZE];
        hmac.copy_from_slice(&entry_buf[remaining - MAC_SIZE..]);

        entries.push(AuditEntry {
            timestamp,
            sequence_no,
            event_type,
            detail,
            hmac,
        });
    }

    Ok(entries)
}

/// Verify the HMAC chain of an audit log file.
pub fn verify_audit_log(
    path: &Path,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<AuditVerifyResult> {
    let mut file = File::open(path)?;

    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let _header = AuditHeader::deserialize(&header_buf)?;

    let mut prev_hmac = [0u8; MAC_SIZE];
    let mut entries_verified = 0u64;

    loop {
        let mut magic_buf = [0u8; 4];
        match file.read_exact(&mut magic_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        if u32::from_le_bytes(magic_buf) != AUDIT_ENTRY_MAGIC {
            break;
        }

        let mut len_buf = [0u8; 4];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let entry_len = u32::from_le_bytes(len_buf) as usize;
        if entry_len < 56 {
            break;
        }

        let remaining = entry_len - 4;
        let mut entry_buf = vec![0u8; remaining];
        match file.read_exact(&mut entry_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let sequence_no = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());

        let data_len = remaining - MAC_SIZE;
        let mut entry_data = Vec::with_capacity(4 + data_len);
        entry_data.extend_from_slice(&len_buf);
        entry_data.extend_from_slice(&entry_buf[..data_len]);

        let stored_hmac = &entry_buf[remaining - MAC_SIZE..];
        let expected_hmac = compute_entry_hmac(audit_key, &prev_hmac, &entry_data);

        if stored_hmac != expected_hmac {
            return Ok(AuditVerifyResult {
                entries_verified,
                chain_valid: false,
                chain_break_at: Some(sequence_no),
            });
        }

        prev_hmac.copy_from_slice(stored_hmac);
        entries_verified += 1;
    }

    Ok(AuditVerifyResult {
        entries_verified,
        chain_valid: true,
        chain_break_at: None,
    })
}

/// Scan a corrupted audit log, recovering entries past damaged regions
/// by scanning for per-entry sentinel markers.
pub fn scan_corrupted_audit_log(path: &Path) -> citadel_core::Result<ScanResult> {
    let data = fs::read(path)?;

    if data.len() < AUDIT_HEADER_SIZE {
        return Err(citadel_core::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "audit file too small for header",
        )));
    }
    let header_buf: [u8; AUDIT_HEADER_SIZE] = data[..AUDIT_HEADER_SIZE].try_into().unwrap();
    let _header = AuditHeader::deserialize(&header_buf)?;

    let magic_bytes = AUDIT_ENTRY_MAGIC.to_le_bytes();
    let mut entries = Vec::new();
    let mut corruption_offsets = Vec::new();
    let mut offset = AUDIT_HEADER_SIZE;
    let mut in_corruption = false;

    while offset + 4 <= data.len() {
        if data[offset..offset + 4] != magic_bytes {
            if !in_corruption {
                corruption_offsets.push(offset as u64);
                in_corruption = true;
            }
            offset += 1;
            continue;
        }

        if offset + 8 > data.len() {
            break;
        }
        let entry_len = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        if entry_len < 56 || offset + 4 + entry_len > data.len() {
            if !in_corruption {
                corruption_offsets.push(offset as u64);
                in_corruption = true;
            }
            offset += 1;
            continue;
        }

        let entry_start = offset + 8;
        let remaining = entry_len - 4;

        let event_type_raw = u16::from_le_bytes(
            data[entry_start + 16..entry_start + 18].try_into().unwrap(),
        );
        let detail_len = u16::from_le_bytes(
            data[entry_start + 18..entry_start + 20].try_into().unwrap(),
        ) as usize;

        if AuditEventType::from_u16(event_type_raw).is_none()
            || 20 + detail_len + MAC_SIZE != remaining
        {
            if !in_corruption {
                corruption_offsets.push(offset as u64);
                in_corruption = true;
            }
            offset += 1;
            continue;
        }

        let timestamp = u64::from_le_bytes(data[entry_start..entry_start + 8].try_into().unwrap());
        let sequence_no = u64::from_le_bytes(
            data[entry_start + 8..entry_start + 16].try_into().unwrap(),
        );
        let event_type = AuditEventType::from_u16(event_type_raw).unwrap();
        let detail = data[entry_start + 20..entry_start + 20 + detail_len].to_vec();
        let mut hmac = [0u8; MAC_SIZE];
        hmac.copy_from_slice(&data[entry_start + remaining - MAC_SIZE..entry_start + remaining]);

        entries.push(AuditEntry {
            timestamp,
            sequence_no,
            event_type,
            detail,
            hmac,
        });

        in_corruption = false;
        offset = offset + 4 + entry_len;
    }

    Ok(ScanResult {
        entries,
        corruption_offsets,
    })
}

#[derive(Debug)]
pub struct ScanResult {
    pub entries: Vec<AuditEntry>,
    pub corruption_offsets: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_serialize_deserialize_roundtrip() {
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: 0xDEAD_BEEF,
            created_at: 1234567890,
            entry_count: 42,
            last_hmac: [0xAB; MAC_SIZE],
        };
        let buf = header.serialize();
        let h2 = AuditHeader::deserialize(&buf).unwrap();
        assert_eq!(h2.magic, AUDIT_LOG_MAGIC);
        assert_eq!(h2.version, AUDIT_LOG_VERSION);
        assert_eq!(h2.file_id, 0xDEAD_BEEF);
        assert_eq!(h2.created_at, 1234567890);
        assert_eq!(h2.entry_count, 42);
        assert_eq!(h2.last_hmac, [0xAB; MAC_SIZE]);
    }

    #[test]
    fn header_invalid_magic_rejected() {
        let mut buf = [0u8; AUDIT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let result = AuditHeader::deserialize(&buf);
        assert!(matches!(result, Err(citadel_core::Error::InvalidMagic { .. })));
    }

    #[test]
    fn entry_serialization_roundtrip() {
        let data = serialize_entry_data(999, 1, AuditEventType::DatabaseCreated, &[0x01, 0x02]);
        assert_eq!(data.len(), 4 + 8 + 8 + 2 + 2 + 2);
        let entry_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        assert_eq!(entry_len, 26 + MAC_SIZE);
    }

    #[test]
    fn hmac_chain_deterministic() {
        let key = [0x42u8; KEY_SIZE];
        let prev = [0u8; MAC_SIZE];
        let data = b"test data";
        let h1 = compute_entry_hmac(&key, &prev, data);
        let h2 = compute_entry_hmac(&key, &prev, data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hmac_chain_changes_with_prev() {
        let key = [0x42u8; KEY_SIZE];
        let prev1 = [0u8; MAC_SIZE];
        let prev2 = [0x01u8; MAC_SIZE];
        let data = b"test data";
        let h1 = compute_entry_hmac(&key, &prev1, data);
        let h2 = compute_entry_hmac(&key, &prev2, data);
        assert_ne!(h1, h2);
    }

    #[test]
    fn event_type_roundtrip() {
        for code in 1..=8u16 {
            let et = AuditEventType::from_u16(code).unwrap();
            assert_eq!(et as u16, code);
        }
        assert!(AuditEventType::from_u16(0).is_none());
        assert!(AuditEventType::from_u16(9).is_none());
    }

    #[test]
    fn create_and_log_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[0x00, 0x00]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let entries = read_audit_log(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].event_type, AuditEventType::DatabaseCreated);
        assert_eq!(entries[0].sequence_no, 1);
        assert_eq!(entries[0].detail, vec![0x00, 0x00]);
        assert_eq!(entries[1].event_type, AuditEventType::DatabaseOpened);
        assert_eq!(entries[1].sequence_no, 2);
    }

    #[test]
    fn verify_valid_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        log.log(AuditEventType::PassphraseChanged, &[]).unwrap();
        drop(log);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 3);
        assert!(result.chain_break_at.is_none());
    }

    #[test]
    fn verify_tamper_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let mut data = fs::read(&path).unwrap();
        data[AUDIT_HEADER_SIZE + 4 + 5] ^= 0x01;
        fs::write(&path, &data).unwrap();

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(!result.chain_valid);
        assert_eq!(result.chain_break_at, Some(1));
    }

    #[test]
    fn verify_wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let wrong_key = [0xFF; KEY_SIZE];
        let result = verify_audit_log(&path, &wrong_key).unwrap();
        assert!(!result.chain_valid);
    }

    #[test]
    fn open_existing_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let mut log = AuditLog::open_existing(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let entries = read_audit_log(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence_no, 1);
        assert_eq!(entries[1].sequence_no, 2);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 2);
    }

    #[test]
    fn rotation_triggers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let config = AuditConfig {
            enabled: true,
            max_file_size: 200,
            max_rotated_files: 2,
        };

        let mut log = AuditLog::create(&path, 123, key, config).unwrap();
        for _ in 0..10 {
            log.log(AuditEventType::DatabaseOpened, &[0u8; 50]).unwrap();
        }
        drop(log);

        let rotated = rotated_path(&path, 1);
        assert!(rotated.exists());
        assert!(path.exists());
    }

    #[test]
    fn file_format_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let log = AuditLog::create(&path, 123, key, AuditConfig::default()).unwrap();
        drop(log);

        let data = fs::read(&path).unwrap();
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        assert_eq!(magic, 0x4155_4454);
    }
}
