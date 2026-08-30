use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroize;

use citadel_core::{
    AUDIT_ENTRY_MAGIC, AUDIT_HEADER_SIZE, AUDIT_LOG_MAGIC, AUDIT_LOG_VERSION,
    AUDIT_LOG_VERSION_LEGACY, KEY_SIZE, MAC_SIZE,
};

type HmacSha256 = Hmac<Sha256>;

/// Smallest valid on-disk entry length: len(4), timestamp(8), sequence_no(8),
/// event_type(2), detail_len(2), empty detail, hmac(32).
const MIN_ENTRY_LEN: usize = 56;
/// detail_len is a u16, so no valid entry can exceed this.
const MAX_ENTRY_LEN: usize = MIN_ENTRY_LEN + u16::MAX as usize;

const ROTATION_RECORD_VERSION: u32 = 1;
const ROTATION_PREPARED_MAGIC: [u8; 8] = *b"CTAROT01";
const ROTATION_COMMITTED_MAGIC: [u8; 8] = *b"CTAROTC1";
const ROTATION_RECORD_PREFIX_LEN: usize = 36;
const ROTATION_RECORD_MAC_LEN: usize = 32;
const MAX_ROTATION_RECORD_SIZE: u64 = 16 * 1024 * 1024;

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
            max_file_size: 10 * 1024 * 1024,
            max_rotated_files: 3,
        }
    }
}

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

    /// The name to show a user for this event.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DatabaseCreated => "Database created",
            Self::DatabaseOpened => "Database opened",
            Self::DatabaseClosed => "Database closed",
            Self::PassphraseChanged => "Passphrase changed",
            Self::KeyBackupExported => "Key backup exported",
            Self::BackupCreated => "Backup created",
            Self::CompactionPerformed => "Compaction performed",
            Self::IntegrityCheckPerformed => "Integrity check performed",
        }
    }
}

/// An [`AuditEntry`]'s event-specific detail. Malformed shapes remain `Raw`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditDetail {
    Empty,
    /// The cipher and KDF the database was created with.
    Created {
        cipher: citadel_core::types::CipherId,
        kdf: citadel_core::types::KdfAlgorithm,
        legacy_cipher_encoding: bool,
    },
    /// Where a backup, key export or compaction wrote to.
    Path(String),
    /// How many errors the integrity check found.
    IntegrityErrors(u32),
    /// Bytes that do not decode under this event's shape.
    Raw(Vec<u8>),
}

impl AuditDetail {
    pub fn decode(event: AuditEventType, detail: &[u8]) -> Self {
        use citadel_core::types::{CipherId, KdfAlgorithm};

        let raw = || Self::Raw(detail.to_vec());
        match event {
            AuditEventType::DatabaseOpened
            | AuditEventType::DatabaseClosed
            | AuditEventType::PassphraseChanged => {
                if detail.is_empty() {
                    Self::Empty
                } else {
                    raw()
                }
            }
            AuditEventType::DatabaseCreated => match detail {
                [c @ (0 | 1), k] => match KdfAlgorithm::from_u8(*k) {
                    Some(kdf) => Self::Created {
                        cipher: CipherId::Aes256Ctr,
                        kdf,
                        legacy_cipher_encoding: *c == 1,
                    },
                    None => raw(),
                },
                _ => raw(),
            },
            AuditEventType::BackupCreated
            | AuditEventType::KeyBackupExported
            | AuditEventType::CompactionPerformed => {
                let Some((len_bytes, rest)) = detail.split_at_checked(2) else {
                    return raw();
                };
                let len = u16::from_le_bytes([len_bytes[0], len_bytes[1]]) as usize;
                if rest.len() != len {
                    return raw();
                }
                match std::str::from_utf8(rest) {
                    Ok(s) => Self::Path(s.to_string()),
                    Err(_) => raw(),
                }
            }
            AuditEventType::IntegrityCheckPerformed => match detail.try_into() {
                Ok(b) => Self::IntegrityErrors(u32::from_le_bytes(b)),
                Err(_) => raw(),
            },
        }
    }
}

impl std::fmt::Display for AuditDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => Ok(()),
            Self::Created {
                cipher,
                kdf,
                legacy_cipher_encoding: false,
            } => write!(f, "{}, {}", cipher.as_str(), kdf.as_str()),
            Self::Created {
                kdf,
                legacy_cipher_encoding: true,
                ..
            } => write!(
                f,
                "legacy ChaCha20 request recorded; effective AES-256-CTR, {}",
                kdf.as_str()
            ),
            Self::Path(p) => write!(f, "{p:?}"),
            Self::IntegrityErrors(0) => write!(f, "no errors"),
            Self::IntegrityErrors(1) => write!(f, "1 error"),
            Self::IntegrityErrors(n) => write!(f, "{n} errors"),
            Self::Raw(bytes) => write!(f, "{} undecoded bytes", bytes.len()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub timestamp: u64,
    pub sequence_no: u64,
    pub event_type: AuditEventType,
    pub detail: Vec<u8>,
    pub hmac: [u8; MAC_SIZE],
}

/// Result of verifying an audit log's HMAC chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditVerifyResult {
    pub entries_verified: u64,
    pub chain_valid: bool,
    pub chain_break_at: Option<u64>,
    /// What the header claims the file holds.
    ///
    /// Best-effort consistency, not authenticated evidence: an offline writer can
    /// truncate the tail and lower the count with it. It can reveal accidental or
    /// uncoordinated truncation but cannot prove rollback of the newest segment.
    pub entries_declared: u64,
}

impl AuditVerifyResult {
    /// Entries the header accounts for that are no longer in the file.
    ///
    /// One-directional on purpose: an entry is synced before its header count, so
    /// a crash between the two leaves more entries than declared - benign, and
    /// reported as zero. The count is mutable and unauthenticated, so zero is not
    /// an anti-truncation or anti-rollback guarantee.
    pub fn entries_missing(&self) -> u64 {
        self.entries_declared.saturating_sub(self.entries_verified)
    }
}

/// Audit log file header (64 bytes).
#[derive(Clone)]
struct AuditHeader {
    magic: u32,
    version: u32,
    file_id: u64,
    created_at: u64,
    entry_count: u64,
    /// Bytes 32..64. v2: write-once chain seed for the first entry (zeros for
    /// a first-generation file, the previous file's tip after rotation). v1:
    /// a vestigial tip nothing reads; the v1 chain always seeds from zeros.
    chain_seed: [u8; MAC_SIZE],
}

impl AuditHeader {
    fn serialize(&self) -> [u8; AUDIT_HEADER_SIZE] {
        let mut buf = [0u8; AUDIT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.created_at.to_le_bytes());
        buf[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[32..64].copy_from_slice(&self.chain_seed);
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
        if version != AUDIT_LOG_VERSION && version != AUDIT_LOG_VERSION_LEGACY {
            return Err(citadel_core::Error::UnsupportedVersion(version));
        }
        let mut chain_seed = [0u8; MAC_SIZE];
        chain_seed.copy_from_slice(&buf[32..64]);
        Ok(Self {
            magic,
            version,
            file_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            created_at: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            chain_seed,
        })
    }

    /// Seed for this file's HMAC chain. Legacy v1 files always chain from
    /// zeros; their bytes 32..64 hold a mutable tip, not a seed.
    fn effective_chain_seed(&self) -> [u8; MAC_SIZE] {
        if self.version == AUDIT_LOG_VERSION_LEGACY {
            [0u8; MAC_SIZE]
        } else {
            self.chain_seed
        }
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
    let mut mac = HmacSha256::new_from_slice(audit_key).expect("HMAC key size is always valid");
    mac.update(prev_hmac);
    mac.update(entry_data);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; MAC_SIZE];
    out.copy_from_slice(&result);
    out
}

fn verify_entry_hmac(
    audit_key: &[u8; KEY_SIZE],
    prev_hmac: &[u8; MAC_SIZE],
    entry_data: &[u8],
    tag: &[u8; MAC_SIZE],
) -> bool {
    let mut mac = HmacSha256::new_from_slice(audit_key).expect("HMAC key size is always valid");
    mac.update(prev_hmac);
    mac.update(entry_data);
    mac.verify_slice(tag).is_ok()
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

/// One structurally valid record: everything any reader needs, so the
/// open/read/verify paths share a single framing implementation.
struct ParsedRecord {
    timestamp: u64,
    sequence_no: u64,
    event_type: AuditEventType,
    detail: Vec<u8>,
    hmac: [u8; MAC_SIZE],
    /// Bytes the chain HMAC covers: length prefix + body minus the HMAC.
    hmac_input: Vec<u8>,
    end: u64,
}

enum RawRecord {
    Parsed(Box<ParsedRecord>),
    Malformed,
}

/// Read one record at `start` without crossing `end` (caller seeked there).
/// Any framing fault is Malformed; the internal-consistency check stops a torn
/// record from swallowing later valid records as a phantom. HMAC validity is
/// the caller's job.
fn read_raw_record_before(
    file: &mut File,
    start: u64,
    end: u64,
) -> citadel_core::Result<RawRecord> {
    if start
        .checked_add(8)
        .is_none_or(|prefix_end| prefix_end > end)
    {
        return Ok(RawRecord::Malformed);
    }

    let mut magic_buf = [0u8; 4];
    match file.read_exact(&mut magic_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(RawRecord::Malformed),
        Err(e) => return Err(e.into()),
    }
    if u32::from_le_bytes(magic_buf) != AUDIT_ENTRY_MAGIC {
        return Ok(RawRecord::Malformed);
    }

    let mut len_buf = [0u8; 4];
    match file.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(RawRecord::Malformed),
        Err(e) => return Err(e.into()),
    }
    let entry_len = u32::from_le_bytes(len_buf) as usize;
    if !(MIN_ENTRY_LEN..=MAX_ENTRY_LEN).contains(&entry_len) {
        return Ok(RawRecord::Malformed);
    }
    let Some(record_end) = start
        .checked_add(4)
        .and_then(|offset| offset.checked_add(entry_len as u64))
    else {
        return Ok(RawRecord::Malformed);
    };
    if record_end > end {
        return Ok(RawRecord::Malformed);
    }

    let remaining = entry_len - 4;
    let mut entry_buf = vec![0u8; remaining];
    match file.read_exact(&mut entry_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(RawRecord::Malformed),
        Err(e) => return Err(e.into()),
    }

    let timestamp = u64::from_le_bytes(entry_buf[0..8].try_into().unwrap());
    let sequence_no = u64::from_le_bytes(entry_buf[8..16].try_into().unwrap());
    let event_raw = u16::from_le_bytes(entry_buf[16..18].try_into().unwrap());
    let detail_len = u16::from_le_bytes(entry_buf[18..20].try_into().unwrap()) as usize;
    let Some(event_type) = AuditEventType::from_u16(event_raw) else {
        return Ok(RawRecord::Malformed);
    };
    if entry_len != MIN_ENTRY_LEN + detail_len {
        return Ok(RawRecord::Malformed);
    }

    let data_len = remaining - MAC_SIZE;
    let mut hmac_input = Vec::with_capacity(4 + data_len);
    hmac_input.extend_from_slice(&len_buf);
    hmac_input.extend_from_slice(&entry_buf[..data_len]);
    let mut hmac = [0u8; MAC_SIZE];
    hmac.copy_from_slice(&entry_buf[remaining - MAC_SIZE..]);
    Ok(RawRecord::Parsed(Box::new(ParsedRecord {
        timestamp,
        sequence_no,
        event_type,
        detail: entry_buf[20..20 + detail_len].to_vec(),
        hmac,
        hmac_input,
        end: record_end,
    })))
}

fn find_entry_magic_before(
    file: &mut File,
    mut from: u64,
    end: u64,
) -> citadel_core::Result<Option<u64>> {
    const CHUNK: u64 = 8192;
    let magic = AUDIT_ENTRY_MAGIC.to_le_bytes();
    let mut buf = [0u8; CHUNK as usize];
    while from
        .checked_add(4)
        .is_some_and(|prefix_end| prefix_end <= end)
    {
        file.seek(SeekFrom::Start(from))?;
        let want = (end - from).min(CHUNK) as usize;
        file.read_exact(&mut buf[..want])?;
        if let Some(i) = buf[..want].windows(4).position(|w| w == magic) {
            return Ok(Some(from + i as u64));
        }
        if want < 4 {
            break;
        }
        // Overlap by 3 bytes so a magic spanning two chunks is still found.
        from += (want - 3) as u64;
    }
    Ok(None)
}

/// Create a fresh audit file holding only `header`, durably (content fsync
/// plus directory entry fsync). Shared by create() and rotation.
fn create_header_file(path: &Path, header: &AuditHeader) -> citadel_core::Result<File> {
    create_header_file_with(path, header, |_| Ok(()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderCreateBoundary {
    FileCreated,
    HeaderWritten,
    FileSynced,
    DirectorySynced,
}

fn create_header_file_with(
    path: &Path,
    header: &AuditHeader,
    mut checkpoint: impl FnMut(HeaderCreateBoundary) -> std::io::Result<()>,
) -> citadel_core::Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    let result = (|| -> std::io::Result<()> {
        checkpoint(HeaderCreateBoundary::FileCreated)?;
        file.write_all(&header.serialize())?;
        checkpoint(HeaderCreateBoundary::HeaderWritten)?;
        file.sync_data()?;
        checkpoint(HeaderCreateBoundary::FileSynced)?;
        citadel_io::durable::fsync_directory(path)?;
        checkpoint(HeaderCreateBoundary::DirectorySynced)
    })();
    if let Err(primary) = result {
        drop(file);
        let cleanup =
            remove_if_present(path).and_then(|()| citadel_io::durable::fsync_directory(path));
        return Err(match cleanup {
            Ok(()) => primary.into(),
            Err(cleanup) => citadel_core::Error::Io(std::io::Error::other(format!(
                "audit header creation failed ({primary}); cleanup also failed ({cleanup})"
            ))),
        });
    }
    Ok(file)
}

pub(crate) fn validate_audit_config(config: &AuditConfig) -> citadel_core::Result<()> {
    if config.max_rotated_files == 0 {
        return Err(citadel_core::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "audit max_rotated_files must be at least 1",
        )));
    }
    Ok(())
}

/// Internal audit log writer.
pub(crate) struct AuditLog {
    file: Option<File>,
    audit_key: [u8; KEY_SIZE],
    prev_hmac: [u8; MAC_SIZE],
    sequence_no: u64,
    entry_count: u64,
    config: AuditConfig,
    path: PathBuf,
    file_id: u64,
    /// Format version of the current file; v1 lineages stay v1 across
    /// rotation so released binaries keep opening the database.
    version: u32,
    created_at: u64,
    /// Entries the unauthenticated header claimed at open that the file no
    /// longer held.
    ///
    /// Opening recounts from the records, and the next append writes that recount
    /// back over the header. This preserves an uncoordinated mismatch across the
    /// open event; a writer that also edits the count can erase the signal.
    missing_at_open: u64,
    /// Set when rotation or append rollback cannot re-establish a trustworthy
    /// writer position. Reopening runs recovery before appends resume.
    poisoned: bool,
    /// Builder-owned new histories are removed if initialization never returns
    /// a usable database handle.
    discard_on_drop: bool,
}

impl Drop for AuditLog {
    fn drop(&mut self) {
        if self.discard_on_drop {
            drop(self.file.take());
            let _ = recover_rotation(&self.path, &self.audit_key);
            if let Ok(discovery) = discover_audit_files_from_live(&self.path) {
                for file in discovery.files.into_iter().rev() {
                    let _ = fs::remove_file(file.path);
                }
            }
            let _ = cleanup_unprepared_rotation(&self.path, &rotation_work_path(&self.path));
            let _ = citadel_io::durable::fsync_directory(&self.path);
        }
        self.audit_key.zeroize();
    }
}

impl AuditLog {
    pub(crate) fn audit_key(&self) -> &[u8; KEY_SIZE] {
        &self.audit_key
    }

    pub(crate) fn discard_if_initialization_fails(&mut self) {
        self.discard_on_drop = true;
    }

    pub(crate) fn retain_created_history(&mut self) {
        self.discard_on_drop = false;
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub(crate) fn missing_at_open(&self) -> u64 {
        self.missing_at_open
    }

    /// `v2_required` comes from the authenticated audit-v2 key-file flag. An
    /// un-upgraded database gets a v1 audit header so released binaries can
    /// still open it (a v2 file beside a legacy data file would lock them
    /// out). Protected and new databases get v2.
    pub(crate) fn create(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
        v2_required: bool,
    ) -> citadel_core::Result<Self> {
        validate_audit_config(&config)?;
        recover_rotation(path, &audit_key)?;
        if citadel_io::durable::path_entry_exists(&audit_upgrade_path(path))? {
            return Err(invalid_audit_data(
                "an incomplete audit upgrade exists beside the new audit path",
            ));
        }
        ensure_no_retained_audit_history(path)?;
        let version = if v2_required {
            AUDIT_LOG_VERSION
        } else {
            AUDIT_LOG_VERSION_LEGACY
        };
        let created_at = now_nanos();
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version,
            file_id,
            created_at,
            entry_count: 0,
            chain_seed: [0u8; MAC_SIZE],
        };
        let file = create_header_file(path, &header)?;

        Ok(Self {
            file: Some(file),
            audit_key,
            prev_hmac: [0u8; MAC_SIZE],
            sequence_no: 0,
            entry_count: 0,
            config,
            path: path.to_path_buf(),
            file_id,
            version,
            created_at,
            missing_at_open: 0,
            poisoned: false,
            discard_on_drop: false,
        })
    }

    /// Open an existing audit log file, seeking to the end for appending.
    /// `v2_required` comes from the MAC-covered audit-v2 key-file flag. The
    /// mutable data-header flag can be stamped before a legacy audit file has
    /// been upgraded, so it is deliberately insufficient.
    pub(crate) fn open_existing(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
        v2_required: bool,
    ) -> citadel_core::Result<Self> {
        validate_audit_config(&config)?;
        recover_rotation(path, &audit_key)?;
        recover_abandoned_audit_upgrade(path)?;
        let mut file = citadel_io::durable::open_regular_read_write(path)?;

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = AuditHeader::deserialize(&header_buf)?;

        if header.file_id != file_id {
            return Err(citadel_core::Error::KeyFileMismatch);
        }
        if v2_required && header.version != AUDIT_LOG_VERSION {
            return Err(invalid_audit_data(
                "audit-log downgrade detected: this database requires a v2 audit header",
            ));
        }

        // Seed the chain from the header so the first entry written into a
        // freshly rotated (still empty) file links to the previous file's tip.
        let mut prev_hmac = header.effective_chain_seed();
        let mut sequence_no = 0u64;
        let mut entry_count = 0u64;
        let mut valid_end = AUDIT_HEADER_SIZE as u64;
        let mut cursor = valid_end;
        let scan_end = file.metadata()?.len();

        while cursor < scan_end {
            file.seek(SeekFrom::Start(cursor))?;
            match read_raw_record_before(&mut file, cursor, scan_end)? {
                RawRecord::Parsed(rec) => {
                    sequence_no = rec.sequence_no;
                    prev_hmac = rec.hmac;
                    entry_count += 1;
                    valid_end = rec.end;
                    cursor = rec.end;
                }
                // Resync past garbage: crash recovery can leave a torn record
                // with valid entries appended after it.
                RawRecord::Malformed => {
                    match find_entry_magic_before(&mut file, cursor + 1, scan_end)? {
                        Some(next) => cursor = next,
                        None => break,
                    }
                }
            }
        }

        // Truncate a trailing torn record only when the tail starts at
        // valid_end with an entry magic (an incomplete write) or is too short
        // to hold one. Mid-byte garbage could be a phantom's overhang over
        // scanner-recoverable records, so leave it.
        let eof = file.seek(SeekFrom::End(0))?;
        if eof > valid_end {
            let torn_prefix = if eof - valid_end < 4 {
                true
            } else {
                let mut m = [0u8; 4];
                file.seek(SeekFrom::Start(valid_end))?;
                file.read_exact(&mut m)?;
                u32::from_le_bytes(m) == AUDIT_ENTRY_MAGIC
            };
            if torn_prefix {
                file.set_len(valid_end)?;
                file.sync_data()?;
            }
        }
        file.seek(SeekFrom::End(0))?;

        // Rotation durably creates the successor header before writing the
        // triggering entry. A crash in that window leaves an empty live file with
        // no record to recover the sequence number from, so continue from the
        // retained predecessor rather than restarting at one.
        if entry_count == 0 {
            let discovery = discover_audit_files_from_live(path)?;
            if discovery.suspicious_numeric_name
                || discovery.files.first().map(|file| file.generation) != Some(0)
                || discovery
                    .files
                    .windows(2)
                    .any(|pair| pair[0].generation.checked_add(1) != Some(pair[1].generation))
            {
                return Err(invalid_audit_data(
                    "audit generations are missing, malformed, or non-contiguous",
                ));
            }

            let mut newer_version = header.version;
            let mut newer_seed = header.chain_seed;
            for predecessor in discovery.files.iter().skip(1) {
                let verified = verify_audit_file(&predecessor.path, &audit_key)?;
                if !verified.result.chain_valid || verified.header.file_id != file_id {
                    return Err(invalid_audit_data(
                        "cannot resume sequence from an invalid audit predecessor",
                    ));
                }
                if newer_version == AUDIT_LOG_VERSION
                    && verified.header.version == AUDIT_LOG_VERSION
                    && newer_seed != verified.tip
                {
                    return Err(invalid_audit_data(
                        "audit predecessor seed does not match the older segment tip",
                    ));
                }
                if let Some(last) = verified.last_sequence {
                    sequence_no = last;
                    break;
                }
                newer_version = verified.header.version;
                newer_seed = verified.header.chain_seed;
            }
        }

        Ok(Self {
            file: Some(file),
            audit_key,
            prev_hmac,
            sequence_no,
            entry_count,
            config,
            path: path.to_path_buf(),
            file_id,
            version: header.version,
            created_at: header.created_at,
            missing_at_open: header.entry_count.saturating_sub(entry_count),
            poisoned: false,
            discard_on_drop: false,
        })
    }

    pub(crate) fn log(
        &mut self,
        event_type: AuditEventType,
        detail: &[u8],
    ) -> citadel_core::Result<()> {
        self.log_with_checkpoint(event_type, detail, |_| Ok(()))
    }

    fn log_with_checkpoint(
        &mut self,
        event_type: AuditEventType,
        detail: &[u8],
        mut checkpoint: impl FnMut(AppendBoundary) -> std::io::Result<()>,
    ) -> citadel_core::Result<()> {
        if self.poisoned {
            return Err(invalid_audit_data(
                "audit writer is unavailable after an incomplete append or rotation; reopen the database",
            ));
        }
        if detail.len() > u16::MAX as usize {
            return Err(citadel_core::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "audit entry detail exceeds 65535 bytes",
            )));
        }
        let next_sequence = self
            .sequence_no
            .checked_add(1)
            .ok_or_else(|| invalid_audit_data("audit sequence number overflow"))?;
        if let Err(error) = self.rotate_if_needed() {
            self.poisoned = true;
            return Err(error);
        }
        let next_entry_count = self
            .entry_count
            .checked_add(1)
            .ok_or_else(|| invalid_audit_data("audit entry count overflow"))?;

        let timestamp = now_nanos();
        let entry_data = serialize_entry_data(timestamp, next_sequence, event_type, detail);
        let hmac = compute_entry_hmac(&self.audit_key, &self.prev_hmac, &entry_data);

        let file = self.file.as_mut().ok_or_else(|| {
            invalid_audit_data("audit writer has no live file; reopen the database")
        })?;
        let entry_start = file.seek(SeekFrom::End(0))?;
        let append = (|| -> std::io::Result<()> {
            file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes())?;
            checkpoint(AppendBoundary::MagicWritten)?;
            file.write_all(&entry_data)?;
            checkpoint(AppendBoundary::DataWritten)?;
            file.write_all(&hmac)?;
            checkpoint(AppendBoundary::HmacWritten)?;
            file.sync_data()?;
            checkpoint(AppendBoundary::EntrySynced)
        })();
        if let Err(primary) = append {
            let rollback = file
                .set_len(entry_start)
                .and_then(|()| file.seek(SeekFrom::Start(entry_start)).map(|_| ()))
                .and_then(|()| file.sync_data());
            if let Err(rollback) = rollback {
                self.poisoned = true;
                return Err(citadel_core::Error::Io(std::io::Error::other(format!(
                    "audit append failed ({primary}); rollback also failed ({rollback})"
                ))));
            }
            return Err(primary.into());
        }

        self.sequence_no = next_sequence;
        self.prev_hmac = hmac;
        self.entry_count = next_entry_count;

        // The entry is already durable and the header count is only a hint, so a
        // failed refresh poisons future writes without misreporting this event.
        if self.update_header().is_err() {
            self.poisoned = true;
        }

        Ok(())
    }

    /// Explicit v1 -> v2 header upgrade (part of Database::upgrade_format).
    /// Every v1 file chains from zeros, so the v2 write-once seed is zeros;
    /// one-way: released binaries reject v2 headers afterwards.
    pub(crate) fn upgrade_to_v2(&mut self) -> citadel_core::Result<bool> {
        self.upgrade_to_v2_with(|src, dst| fs::rename(src, dst))
    }

    fn upgrade_to_v2_with(
        &mut self,
        replace: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
    ) -> citadel_core::Result<bool> {
        if self.poisoned || self.file.is_none() {
            return Err(invalid_audit_data(
                "audit writer is unavailable; reopen the database before upgrading",
            ));
        }
        ensure_no_audit_maintenance(&self.path)?;
        if self.version == AUDIT_LOG_VERSION {
            return Ok(false);
        }
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: self.file_id,
            created_at: self.created_at,
            entry_count: self.entry_count,
            chain_seed: [0u8; MAC_SIZE],
        };
        let upgrade_path = audit_upgrade_path(&self.path);
        let mut upgrade_created = false;
        let result = (|| -> citadel_core::Result<()> {
            let source = self.file.as_mut().expect("checked above");
            let permissions = source.metadata()?.permissions();
            source.seek(SeekFrom::Start(AUDIT_HEADER_SIZE as u64))?;

            let mut replacement = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&upgrade_path)?;
            upgrade_created = true;
            replacement.write_all(&header.serialize())?;
            std::io::copy(source, &mut replacement)?;
            replacement.set_permissions(permissions)?;
            replacement.sync_all()?;
            drop(replacement);
            citadel_io::durable::fsync_directory(&upgrade_path)?;
            Ok(())
        })();
        if let Err(error) = result {
            let cleanup = if upgrade_created {
                remove_upgrade_temp(&upgrade_path)
            } else {
                Ok(())
            };
            if cleanup.is_err() {
                self.poisoned = true;
            }
            return Err(rotation_failure(error, cleanup));
        }

        drop(self.file.take());
        if let Err(error) = replace(&upgrade_path, &self.path) {
            let cleanup = remove_upgrade_temp(&upgrade_path);
            if cleanup.is_err() {
                self.poisoned = true;
            }
            match citadel_io::durable::open_regular_read_write(&self.path) {
                Ok(mut original) => {
                    if let Err(reopen) = original.seek(SeekFrom::End(0)) {
                        self.poisoned = true;
                        return Err(citadel_core::Error::Io(std::io::Error::other(format!(
                            "audit upgrade failed ({error}); seeking the original also failed ({reopen})"
                        ))));
                    }
                    self.file = Some(original);
                }
                Err(reopen) => {
                    self.poisoned = true;
                    return Err(citadel_core::Error::Io(std::io::Error::other(format!(
                        "audit upgrade failed ({error}); reopening the original also failed ({reopen})"
                    ))));
                }
            }
            return Err(rotation_failure(error.into(), cleanup));
        }

        let reopened = citadel_io::durable::open_regular_read_write(&self.path);
        let directory_sync = citadel_io::durable::fsync_directory(&self.path);
        match reopened {
            Ok(mut file) => {
                if let Err(error) = file.seek(SeekFrom::End(0)) {
                    self.poisoned = true;
                    return Err(error.into());
                }
                self.file = Some(file);
                self.version = AUDIT_LOG_VERSION;
            }
            Err(error) => {
                self.poisoned = true;
                return Err(error.into());
            }
        }
        if let Err(error) = directory_sync {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(true)
    }

    fn update_header(&mut self) -> citadel_core::Result<()> {
        let file = self.file.as_mut().ok_or_else(|| {
            invalid_audit_data("audit writer has no live file; reopen the database")
        })?;
        let pos = file.stream_position()?;

        // Only entry_count; bytes 32..64 (v2 seed / v1 vestigial tip) must
        // stay as written at create/rotate.
        file.seek(SeekFrom::Start(24))?;
        file.write_all(&self.entry_count.to_le_bytes())?;
        file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn rotate_if_needed(&mut self) -> citadel_core::Result<()> {
        self.rotate_if_needed_with_checkpoint(
            |src, dst| fs::rename(src, dst),
            create_header_file,
            |_| false,
        )
    }

    #[cfg(test)]
    fn rotate_if_needed_with(
        &mut self,
        rename_file: impl FnMut(&Path, &Path) -> std::io::Result<()>,
        create_file: impl FnMut(&Path, &AuditHeader) -> citadel_core::Result<File>,
    ) -> citadel_core::Result<()> {
        self.rotate_if_needed_with_checkpoint(rename_file, create_file, |_| false)
    }

    fn rotate_if_needed_with_checkpoint(
        &mut self,
        mut rename_file: impl FnMut(&Path, &Path) -> std::io::Result<()>,
        mut create_file: impl FnMut(&Path, &AuditHeader) -> citadel_core::Result<File>,
        mut stop_after: impl FnMut(RotationBoundary) -> bool,
    ) -> citadel_core::Result<()> {
        recover_rotation(&self.path, &self.audit_key)?;

        let file = self.file.as_mut().ok_or_else(|| {
            invalid_audit_data("audit writer has no live file; reopen the database")
        })?;
        let file_size = file.seek(SeekFrom::End(0))?;
        if self.entry_count == 0 || file_size < self.config.max_file_size {
            return Ok(());
        }

        file.sync_data()?;

        // v1 successors restart the chain from zeros (staying v1-openable);
        // v2 carries the previous file's tip as the write-once seed. Sequence
        // numbers continue either way, so a dropped generation still shows.
        let successor_seed = if self.version == AUDIT_LOG_VERSION_LEGACY {
            [0u8; MAC_SIZE]
        } else {
            self.prev_hmac
        };
        let created_at = now_nanos();
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: self.version,
            file_id: self.file_id,
            created_at,
            entry_count: 0,
            chain_seed: successor_seed,
        };

        // A fixed adjacent work directory is the durable discovery point. It
        // is fsynced into the parent before any visible generation is moved.
        let work = rotation_work_path(&self.path);
        fs::create_dir(&work)?;
        citadel_io::durable::fsync_directory(&work)?;

        // Build and sync the successor before moving any visible generation.
        let successor_path = rotation_successor_path(&work);
        if let Err(error) = create_file(&successor_path, &header) {
            let _ = cleanup_unprepared_rotation(&self.path, &work);
            return Err(error);
        }
        rotation_checkpoint(&mut stop_after, RotationBoundary::SuccessorPrepared)?;

        // Enumerate the namespace once: probing `0..=retention` hangs on huge
        // settings and misses generations left by a larger previous policy.
        let discovery = match discover_audit_files_from_live(&self.path) {
            Ok(discovery) => discovery,
            Err(error) => {
                let cleanup = cleanup_unprepared_rotation(&self.path, &work);
                return Err(rotation_failure(error, cleanup));
            }
        };
        let generations: Vec<u32> = discovery.files.iter().map(|file| file.generation).collect();
        if discovery.suspicious_numeric_name
            || generations.first() != Some(&0)
            || generations
                .windows(2)
                .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
        {
            let _ = cleanup_unprepared_rotation(&self.path, &work);
            return Err(rotation_invalid(
                "audit generations are missing, malformed, or non-contiguous",
            )
            .into());
        }
        let record = RotationRecord {
            rotation_id: (u128::from(now_nanos()) << 64) | u128::from(rand::random::<u64>()),
            max_rotated_files: self.config.max_rotated_files,
            generations,
        };
        if let Err(error) = write_rotation_record(
            &rotation_prepared_path(&work),
            ROTATION_PREPARED_MAGIC,
            &record,
            &self.audit_key,
        ) {
            let _ = cleanup_unprepared_rotation(&self.path, &work);
            return Err(error.into());
        }
        rotation_checkpoint(&mut stop_after, RotationBoundary::PreparedDurable)?;

        // Move the exact pre-rotation namespace recorded in the authenticated
        // journal. Nothing is deleted while the record is only prepared.
        for &generation in &record.generations {
            let original = audit_generation_path(&self.path, generation);
            let temporary = rotation_generation_path(&work, generation);
            if let Err(error) = rename_file(&original, &temporary) {
                let primary = citadel_core::Error::from(error);
                let rollback =
                    rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
                return Err(rotation_failure(primary, rollback));
            }
            if generation == 0 {
                rotation_checkpoint(&mut stop_after, RotationBoundary::LiveStaged)?;
            }
        }

        // Publish the successor first. Its presence distinguishes publication
        // from the preceding staging phase during deterministic recovery.
        if let Err(error) = rename_file(&successor_path, &self.path) {
            let primary = citadel_core::Error::from(error);
            let rollback = rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
            return Err(rotation_failure(primary, rollback));
        }
        rotation_checkpoint(&mut stop_after, RotationBoundary::SuccessorPublished)?;

        for &generation in record
            .generations
            .iter()
            .filter(|&&generation| generation < record.max_rotated_files)
        {
            let temporary = rotation_generation_path(&work, generation);
            let published = rotated_path(&self.path, generation + 1);
            if let Err(error) = rename_file(&temporary, &published) {
                let primary = citadel_core::Error::from(error);
                let rollback =
                    rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
                return Err(rotation_failure(primary, rollback));
            }
            rotation_checkpoint(
                &mut stop_after,
                RotationBoundary::GenerationPublished(generation),
            )?;
        }

        // Open the successor and durably publish both directories before the
        // commit record. Until that record exists, recovery always rolls back.
        let mut successor_file = match citadel_io::durable::open_regular_read_write(&self.path) {
            Ok(file) => file,
            Err(error) => {
                let primary = citadel_core::Error::from(error);
                let rollback =
                    rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
                return Err(rotation_failure(primary, rollback));
            }
        };
        if let Err(error) = successor_file.seek(SeekFrom::End(0)) {
            drop(successor_file);
            let primary = citadel_core::Error::from(error);
            let rollback = rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
            return Err(rotation_failure(primary, rollback));
        }
        if let Err(error) = sync_rotation_directories(&self.path, &work) {
            drop(successor_file);
            let primary = citadel_core::Error::from(error);
            let rollback = rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
            return Err(rotation_failure(primary, rollback));
        }

        if let Err(error) = write_rotation_record(
            &rotation_committed_path(&work),
            ROTATION_COMMITTED_MAGIC,
            &record,
            &self.audit_key,
        ) {
            drop(successor_file);
            let primary = citadel_core::Error::from(error);
            let rollback = rollback_prepared_rotation(&self.path, &work, &record, &mut rename_file);
            return Err(rotation_failure(primary, rollback));
        }
        rotation_checkpoint(&mut stop_after, RotationBoundary::CommitDurable)?;

        // Past the durable commit record, recovery only rolls forward. Update the
        // live writer before cleanup so an error cannot leave this process
        // appending through the old handle now named `.1`.
        self.file = Some(successor_file);
        self.prev_hmac = successor_seed;
        self.entry_count = 0;
        self.created_at = created_at;

        prepare_committed_cleanup(&self.path, &work, &record)?;
        rotation_checkpoint(&mut stop_after, RotationBoundary::ExpiredTailRemoved)?;
        remove_rotation_journal(&self.path, &work)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendBoundary {
    MagicWritten,
    DataWritten,
    HmacWritten,
    EntrySynced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RotationBoundary {
    SuccessorPrepared,
    PreparedDurable,
    LiveStaged,
    SuccessorPublished,
    GenerationPublished(u32),
    CommitDurable,
    ExpiredTailRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RotationRecord {
    rotation_id: u128,
    max_rotated_files: u32,
    generations: Vec<u32>,
}

fn rotation_checkpoint(
    stop_after: &mut impl FnMut(RotationBoundary) -> bool,
    boundary: RotationBoundary,
) -> citadel_core::Result<()> {
    if stop_after(boundary) {
        return Err(citadel_core::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            format!("simulated audit rotation crash after {boundary:?}"),
        )));
    }
    Ok(())
}

fn audit_generation_path(base: &Path, generation: u32) -> PathBuf {
    if generation == 0 {
        base.to_path_buf()
    } else {
        rotated_path(base, generation)
    }
}

pub(crate) fn rotation_work_path(base: &Path) -> PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(".rotation-work");
    PathBuf::from(name)
}

pub(crate) fn audit_upgrade_path(base: &Path) -> PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(".upgrade");
    PathBuf::from(name)
}

fn remove_upgrade_temp(path: &Path) -> std::io::Result<()> {
    remove_if_present(path)?;
    citadel_io::durable::fsync_directory(path)
}

pub(crate) fn recover_abandoned_audit_upgrade(base: &Path) -> citadel_core::Result<()> {
    let upgrade = audit_upgrade_path(base);
    if !citadel_io::durable::path_entry_exists(&upgrade)? {
        return Ok(());
    }
    if !citadel_io::durable::path_entry_exists(base)? {
        return Err(invalid_audit_data(
            "audit upgrade image exists without a live audit log",
        ));
    }
    drop(citadel_io::durable::open_regular_read(&upgrade)?);
    drop(citadel_io::durable::open_regular_read(base)?);
    remove_upgrade_temp(&upgrade)?;
    Ok(())
}

fn rotation_successor_path(work: &Path) -> PathBuf {
    work.join("new")
}

fn rotation_prepared_path(work: &Path) -> PathBuf {
    work.join("prepared")
}

fn rotation_committed_path(work: &Path) -> PathBuf {
    work.join("committed")
}

fn rotation_generation_path(work: &Path, generation: u32) -> PathBuf {
    work.join(format!("generation-{generation}"))
}

fn rotation_invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn serialize_rotation_record(
    magic: [u8; 8],
    record: &RotationRecord,
    audit_key: &[u8; KEY_SIZE],
) -> std::io::Result<Vec<u8>> {
    let generation_count = u32::try_from(record.generations.len())
        .map_err(|_| rotation_invalid("audit rotation generation list is too large"))?;
    let mut bytes = Vec::with_capacity(
        ROTATION_RECORD_PREFIX_LEN
            + record.generations.len() * std::mem::size_of::<u32>()
            + ROTATION_RECORD_MAC_LEN,
    );
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&ROTATION_RECORD_VERSION.to_le_bytes());
    bytes.extend_from_slice(&record.rotation_id.to_le_bytes());
    bytes.extend_from_slice(&record.max_rotated_files.to_le_bytes());
    bytes.extend_from_slice(&generation_count.to_le_bytes());
    for generation in &record.generations {
        bytes.extend_from_slice(&generation.to_le_bytes());
    }
    let mut mac = <HmacSha256 as Mac>::new_from_slice(audit_key)
        .map_err(|_| rotation_invalid("invalid audit rotation HMAC key"))?;
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    if bytes.len() as u64 > MAX_ROTATION_RECORD_SIZE {
        return Err(rotation_invalid("audit rotation record is too large"));
    }
    Ok(bytes)
}

fn deserialize_rotation_record(
    bytes: &[u8],
    expected_magic: [u8; 8],
    audit_key: &[u8; KEY_SIZE],
) -> std::io::Result<RotationRecord> {
    let minimum_len = ROTATION_RECORD_PREFIX_LEN + ROTATION_RECORD_MAC_LEN;
    if bytes.len() < minimum_len {
        return Err(rotation_invalid("audit rotation record is truncated"));
    }
    if bytes[0..8] != expected_magic {
        return Err(rotation_invalid("audit rotation record has invalid magic"));
    }
    if u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != ROTATION_RECORD_VERSION {
        return Err(rotation_invalid(
            "audit rotation record has unknown version",
        ));
    }
    let generation_count = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
    let expected_len = ROTATION_RECORD_PREFIX_LEN
        .checked_add(
            generation_count
                .checked_mul(std::mem::size_of::<u32>())
                .ok_or_else(|| rotation_invalid("audit rotation record length overflow"))?,
        )
        .and_then(|len| len.checked_add(ROTATION_RECORD_MAC_LEN))
        .ok_or_else(|| rotation_invalid("audit rotation record length overflow"))?;
    if bytes.len() != expected_len {
        return Err(rotation_invalid(
            "audit rotation record has inconsistent generation count",
        ));
    }
    let authenticated_len = expected_len - ROTATION_RECORD_MAC_LEN;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(audit_key)
        .map_err(|_| rotation_invalid("invalid audit rotation HMAC key"))?;
    mac.update(&bytes[..authenticated_len]);
    mac.verify_slice(&bytes[authenticated_len..])
        .map_err(|_| rotation_invalid("audit rotation record authentication failed"))?;

    let rotation_id = u128::from_le_bytes(bytes[12..28].try_into().unwrap());
    let max_rotated_files = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    if max_rotated_files == 0 {
        return Err(rotation_invalid("audit rotation record has zero retention"));
    }
    let mut generations = Vec::with_capacity(generation_count);
    for chunk in bytes[ROTATION_RECORD_PREFIX_LEN..authenticated_len]
        .as_chunks::<4>()
        .0
    {
        generations.push(u32::from_le_bytes(*chunk));
    }
    if generations.first() != Some(&0)
        || generations
            .windows(2)
            .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
    {
        return Err(rotation_invalid(
            "audit rotation record has an invalid generation list",
        ));
    }
    Ok(RotationRecord {
        rotation_id,
        max_rotated_files,
        generations,
    })
}

fn write_rotation_record(
    path: &Path,
    magic: [u8; 8],
    record: &RotationRecord,
    audit_key: &[u8; KEY_SIZE],
) -> std::io::Result<()> {
    let bytes = serialize_rotation_record(magic, record, audit_key)?;
    let temp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_data()?;
    drop(file);
    fs::rename(&temp, path)?;
    citadel_io::durable::fsync_directory(path)
}

fn read_rotation_record(
    path: &Path,
    magic: [u8; 8],
    audit_key: &[u8; KEY_SIZE],
) -> std::io::Result<Option<RotationRecord>> {
    let mut file = match citadel_io::durable::open_regular_read(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > MAX_ROTATION_RECORD_SIZE {
        return Err(rotation_invalid(
            "audit rotation record is not a bounded regular file",
        ));
    }
    let bytes = read_bounded_rotation_record(&mut file, MAX_ROTATION_RECORD_SIZE)?;
    deserialize_rotation_record(&bytes, magic, audit_key).map(Some)
}

fn read_bounded_rotation_record(reader: &mut impl Read, max_len: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let read_limit = max_len
        .checked_add(1)
        .ok_or_else(|| rotation_invalid("audit rotation record bound overflows"))?;
    reader.take(read_limit).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_len {
        return Err(rotation_invalid(
            "audit rotation record is not a bounded regular file",
        ));
    }
    Ok(bytes)
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    if !citadel_io::durable::path_entry_exists(path)? {
        return Ok(());
    }
    drop(citadel_io::durable::open_regular_read(path)?);
    fs::remove_file(path)
}

fn require_regular_file(path: &Path) -> std::io::Result<()> {
    drop(citadel_io::durable::open_regular_read(path)?);
    Ok(())
}

fn sync_rotation_directories(base: &Path, work: &Path) -> std::io::Result<()> {
    citadel_io::durable::fsync_directory(base)?;
    citadel_io::durable::fsync_directory(&work.join(".directory-sync"))
}

fn remove_rotation_control_files(work: &Path) -> std::io::Result<()> {
    for path in [
        rotation_successor_path(work),
        rotation_prepared_path(work),
        rotation_prepared_path(work).with_extension("tmp"),
        rotation_committed_path(work),
        rotation_committed_path(work).with_extension("tmp"),
    ] {
        remove_if_present(&path)?;
    }
    Ok(())
}

fn ensure_work_directory_empty(work: &Path) -> std::io::Result<()> {
    if let Some(entry) = fs::read_dir(work)?.next() {
        let entry = entry?;
        return Err(rotation_invalid(format!(
            "unexpected audit rotation artifact: {}",
            entry.path().display()
        )));
    }
    Ok(())
}

fn cleanup_unprepared_rotation(base: &Path, work: &Path) -> std::io::Result<()> {
    if !citadel_io::durable::path_entry_exists(base)? {
        return Err(rotation_invalid(
            "audit rotation work exists but the live log is missing",
        ));
    }
    require_regular_file(base)?;
    remove_rotation_control_files(work)?;
    ensure_work_directory_empty(work)?;
    fs::remove_dir(work)?;
    citadel_io::durable::fsync_directory(base)
}

fn rollback_prepared_rotation(
    base: &Path,
    work: &Path,
    record: &RotationRecord,
    rename_file: &mut impl FnMut(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let successor = rotation_successor_path(work);
    if citadel_io::durable::path_entry_exists(&successor)? {
        require_regular_file(&successor)?;
        // Publication never started. Restore the prefix that was staged; the
        // remaining originals are still in their visible paths.
        for &generation in record.generations.iter().rev() {
            let temporary = rotation_generation_path(work, generation);
            let original = audit_generation_path(base, generation);
            match (
                citadel_io::durable::path_entry_exists(&temporary)?,
                citadel_io::durable::path_entry_exists(&original)?,
            ) {
                (true, false) => {
                    require_regular_file(&temporary)?;
                    rename_file(&temporary, &original)?;
                }
                (false, true) => require_regular_file(&original)?,
                _ => {
                    return Err(rotation_invalid(format!(
                        "ambiguous audit rotation staging state for generation {generation}"
                    )));
                }
            }
        }
    } else {
        // Successor publication happens only after every original is staged.
        for &generation in record
            .generations
            .iter()
            .rev()
            .filter(|&&generation| generation < record.max_rotated_files)
        {
            let temporary = rotation_generation_path(work, generation);
            if !citadel_io::durable::path_entry_exists(&temporary)? {
                let published = rotated_path(base, generation + 1);
                if !citadel_io::durable::path_entry_exists(&published)? {
                    return Err(rotation_invalid(format!(
                        "missing published audit generation {}",
                        generation + 1
                    )));
                }
                require_regular_file(&published)?;
                rename_file(&published, &temporary)?;
            }
        }
        if !citadel_io::durable::path_entry_exists(base)? {
            return Err(rotation_invalid(
                "published audit successor is missing during rollback",
            ));
        }
        require_regular_file(base)?;
        rename_file(base, &successor)?;
        for &generation in record.generations.iter().rev() {
            let temporary = rotation_generation_path(work, generation);
            let original = audit_generation_path(base, generation);
            if !citadel_io::durable::path_entry_exists(&temporary)?
                || citadel_io::durable::path_entry_exists(&original)?
            {
                return Err(rotation_invalid(format!(
                    "cannot restore audit generation {generation}"
                )));
            }
            require_regular_file(&temporary)?;
            rename_file(&temporary, &original)?;
        }
    }

    remove_if_present(&successor)?;
    sync_rotation_directories(base, work)?;
    remove_rotation_control_files(work)?;
    ensure_work_directory_empty(work)?;
    fs::remove_dir(work)?;
    citadel_io::durable::fsync_directory(base)
}

fn validate_committed_namespace(
    base: &Path,
    work: &Path,
    record: &RotationRecord,
) -> std::io::Result<()> {
    if !citadel_io::durable::path_entry_exists(base)?
        || citadel_io::durable::path_entry_exists(&rotation_successor_path(work))?
    {
        return Err(rotation_invalid(
            "committed audit rotation is missing its live successor",
        ));
    }
    require_regular_file(base)?;
    let discovery = discover_audit_files_from_live(base).map_err(|error| {
        rotation_invalid(format!("cannot inspect committed audit rotation: {error}"))
    })?;
    let actual: Vec<u32> = discovery.files.iter().map(|file| file.generation).collect();
    let expected: Vec<u32> = std::iter::once(0)
        .chain(
            record
                .generations
                .iter()
                .copied()
                .filter(|&generation| generation < record.max_rotated_files)
                .map(|generation| generation + 1),
        )
        .collect();
    if discovery.suspicious_numeric_name || actual != expected {
        return Err(rotation_invalid(
            "committed audit generations do not match the authenticated rotation record",
        ));
    }
    for file in &discovery.files {
        require_regular_file(&file.path)?;
    }
    for &generation in record
        .generations
        .iter()
        .filter(|&&generation| generation < record.max_rotated_files)
    {
        if citadel_io::durable::path_entry_exists(&rotation_generation_path(work, generation))? {
            return Err(rotation_invalid(format!(
                "committed audit generation {generation} was not published"
            )));
        }
    }
    Ok(())
}

fn prepare_committed_cleanup(
    base: &Path,
    work: &Path,
    record: &RotationRecord,
) -> std::io::Result<()> {
    validate_committed_namespace(base, work, record)?;
    // Retention can shrink between opens, so every staged generation at or above
    // the new limit expires; stopping at `max` strands older visible history.
    for &generation in record
        .generations
        .iter()
        .filter(|&&generation| generation >= record.max_rotated_files)
    {
        remove_if_present(&rotation_generation_path(work, generation))?;
    }
    sync_rotation_directories(base, work)
}

fn ensure_only_committed_record(work: &Path) -> std::io::Result<()> {
    let committed = rotation_committed_path(work);
    for entry in fs::read_dir(work)? {
        let path = entry?.path();
        if path != committed {
            return Err(rotation_invalid(format!(
                "unexpected audit rotation artifact: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn remove_rotation_journal(base: &Path, work: &Path) -> std::io::Result<()> {
    // Remove prepared state first and make that deletion durable. The complete
    // committed record remains sufficient to finish cleanup after a crash.
    remove_if_present(&rotation_prepared_path(work))?;
    remove_if_present(&rotation_prepared_path(work).with_extension("tmp"))?;
    remove_if_present(&rotation_committed_path(work).with_extension("tmp"))?;
    citadel_io::durable::fsync_directory(&work.join(".directory-sync"))?;

    // The committed record is removed last. Its preceding directory sync
    // guarantees that an empty surviving work directory is safe to discard.
    ensure_only_committed_record(work)?;
    remove_if_present(&rotation_committed_path(work))?;
    citadel_io::durable::fsync_directory(&work.join(".directory-sync"))?;
    ensure_work_directory_empty(work)?;
    fs::remove_dir(work)?;
    citadel_io::durable::fsync_directory(base)
}

fn finalize_committed_rotation(
    base: &Path,
    work: &Path,
    record: &RotationRecord,
) -> std::io::Result<()> {
    prepare_committed_cleanup(base, work, record)?;
    remove_rotation_journal(base, work)
}

pub(crate) fn recover_rotation(
    base: &Path,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<()> {
    let work = rotation_work_path(base);
    let metadata = match fs::symlink_metadata(&work) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_dir() {
        return Err(rotation_invalid("audit rotation work path is not a directory").into());
    }

    let prepared = read_rotation_record(
        &rotation_prepared_path(&work),
        ROTATION_PREPARED_MAGIC,
        audit_key,
    )?;
    let committed = read_rotation_record(
        &rotation_committed_path(&work),
        ROTATION_COMMITTED_MAGIC,
        audit_key,
    )?;

    match (prepared, committed) {
        (Some(prepared), Some(committed)) => {
            if prepared != committed {
                return Err(rotation_invalid(
                    "audit rotation prepared and committed records disagree",
                )
                .into());
            }
            finalize_committed_rotation(base, &work, &committed)?;
        }
        (_, Some(committed)) => finalize_committed_rotation(base, &work, &committed)?,
        (Some(prepared), None) => {
            rollback_prepared_rotation(base, &work, &prepared, &mut |src, dst| {
                fs::rename(src, dst)
            })?;
        }
        (None, None) => cleanup_unprepared_rotation(base, &work)?,
    }
    Ok(())
}

pub(crate) fn ensure_no_audit_maintenance(base: &Path) -> citadel_core::Result<()> {
    if citadel_io::durable::path_entry_exists(&rotation_work_path(base))? {
        return Err(invalid_audit_data(
            "audit rotation recovery is pending; reopen the database before verification",
        ));
    }
    if citadel_io::durable::path_entry_exists(&audit_upgrade_path(base))? {
        return Err(invalid_audit_data(
            "audit upgrade recovery is pending; reopen the database before verification",
        ));
    }
    Ok(())
}

fn rotation_failure(
    primary: citadel_core::Error,
    rollback: std::io::Result<()>,
) -> citadel_core::Error {
    match rollback {
        Ok(()) => primary,
        Err(rollback_error) => citadel_core::Error::Io(std::io::Error::other(format!(
            "audit rotation failed ({primary}); rollback also failed ({rollback_error})"
        ))),
    }
}

fn rotated_path(base: &Path, index: u32) -> PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

#[derive(Debug, Clone)]
struct AuditFilePath {
    generation: u32,
    path: PathBuf,
}

struct AuditFileDiscovery {
    files: Vec<AuditFilePath>,
    suspicious_numeric_name: bool,
}

fn invalid_audit_data(message: impl Into<String>) -> citadel_core::Error {
    citadel_core::Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

/// Discover every numeric rotation in the containing directory. Stopping at the
/// first absent suffix hides `.2` when `.1` was deleted - exactly the gap a
/// whole-chain verifier must reject.
fn discover_audit_files_from_live(live: &Path) -> citadel_core::Result<AuditFileDiscovery> {
    let live = live.to_path_buf();
    let mut files = Vec::new();
    let mut suspicious_numeric_name = false;
    if citadel_io::durable::path_entry_exists(&live)? {
        files.push(AuditFilePath {
            generation: 0,
            path: live.clone(),
        });
    }

    let parent = live
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let base_name = live
        .file_name()
        .ok_or_else(|| invalid_audit_data("audit log path has no file name"))?
        .as_encoded_bytes();

    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.as_encoded_bytes();
        let Some(suffix) = name
            .strip_prefix(base_name)
            .and_then(|suffix| suffix.strip_prefix(b"."))
        else {
            continue;
        };
        if suffix.is_empty() || !suffix.iter().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let suffix = std::str::from_utf8(suffix).expect("ASCII digits are valid UTF-8");
        let Ok(generation) = suffix.parse::<u32>() else {
            suspicious_numeric_name = true;
            continue;
        };
        if generation == 0 || suffix != generation.to_string() {
            suspicious_numeric_name = true;
            continue;
        }
        if !entry.file_type()?.is_file() {
            suspicious_numeric_name = true;
            continue;
        }
        files.push(AuditFilePath {
            generation,
            path: entry.path(),
        });
    }

    files.sort_by_key(|file| file.generation);
    if files
        .windows(2)
        .any(|pair| pair[0].generation == pair[1].generation)
    {
        suspicious_numeric_name = true;
        files.dedup_by_key(|file| file.generation);
    }
    Ok(AuditFileDiscovery {
        files,
        suspicious_numeric_name,
    })
}

#[cfg(test)]
fn discover_audit_files(data_path: &Path) -> citadel_core::Result<AuditFileDiscovery> {
    discover_audit_files_from_live(&resolve_audit_path(data_path))
}

/// A new live log cannot be created beside retained numeric generations: that
/// detaches the new history while making the old files look like predecessors.
pub(crate) fn ensure_no_retained_audit_history(live: &Path) -> citadel_core::Result<()> {
    let discovery = discover_audit_files_from_live(live)?;
    if discovery.suspicious_numeric_name || discovery.files.iter().any(|file| file.generation != 0)
    {
        return Err(invalid_audit_data(
            "cannot create a new audit log while retained or malformed rotated generations remain",
        ));
    }
    Ok(())
}

/// Caller must exclude in-process audit rotation or hold the data-file lock.
pub(crate) fn audit_log_paths_while_locked(data_path: &Path) -> citadel_core::Result<Vec<PathBuf>> {
    let discovery = discover_audit_files_while_locked(data_path)?;
    if discovery.suspicious_numeric_name {
        return Err(invalid_audit_data(
            "audit history contains a malformed or non-regular generation",
        ));
    }
    for file in &discovery.files {
        citadel_io::durable::open_regular_read(&file.path)?;
    }
    Ok(discovery.files.into_iter().map(|file| file.path).collect())
}

fn discover_audit_files_while_locked(data_path: &Path) -> citadel_core::Result<AuditFileDiscovery> {
    let live = resolve_audit_path(data_path);
    ensure_no_audit_maintenance(&live)?;
    discover_audit_files_from_live(&live)
}

pub(crate) fn resolve_audit_path(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-audit");
    PathBuf::from(name)
}

/// Recover structurally parseable entries without authenticating them.
///
/// This keyless reader resynchronizes past malformed regions and is intended
/// for forensic recovery. Do not present its entries as verified audit facts;
/// use [`Database::visit_verified_audit_history`](crate::Database::visit_verified_audit_history)
/// on an open vault when authenticity matters.
pub fn read_audit_log(path: &Path) -> citadel_core::Result<Vec<AuditEntry>> {
    let mut file = citadel_io::durable::open_regular_read(path)?;

    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let _header = AuditHeader::deserialize(&header_buf)?;

    let mut entries = Vec::new();
    let mut cursor = AUDIT_HEADER_SIZE as u64;
    let scan_end = file.metadata()?.len();

    while cursor < scan_end {
        file.seek(SeekFrom::Start(cursor))?;
        match read_raw_record_before(&mut file, cursor, scan_end)? {
            RawRecord::Parsed(rec) => {
                cursor = rec.end;
                let rec = *rec;
                entries.push(AuditEntry {
                    timestamp: rec.timestamp,
                    sequence_no: rec.sequence_no,
                    event_type: rec.event_type,
                    detail: rec.detail,
                    hmac: rec.hmac,
                });
            }
            RawRecord::Malformed => {
                match find_entry_magic_before(&mut file, cursor + 1, scan_end)? {
                    Some(next) => cursor = next,
                    None => break,
                }
            }
        }
    }

    Ok(entries)
}

struct AuditFileVerification {
    header: AuditHeader,
    result: AuditVerifyResult,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    tip: [u8; MAC_SIZE],
}

fn finish_audit_verification(
    header: AuditHeader,
    entries_verified: u64,
    chain_valid: bool,
    chain_break_at: Option<u64>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    tip: [u8; MAC_SIZE],
) -> AuditFileVerification {
    let entries_declared = header.entry_count;
    AuditFileVerification {
        header,
        result: AuditVerifyResult {
            entries_verified,
            chain_valid,
            chain_break_at,
            entries_declared,
        },
        first_sequence,
        last_sequence,
        tip,
    }
}

/// One streaming pass supplies both the public per-file verdict and the exact
/// verified boundary facts used by whole-chain verification.
fn verify_audit_file(
    path: &Path,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<AuditFileVerification> {
    let mut file = citadel_io::durable::open_regular_read(path)?;
    let end = file.metadata()?.len();
    let header = read_audit_header(&mut file)?;
    verify_audit_reader(&mut file, end, header, audit_key)
}

fn read_audit_header(file: &mut File) -> citadel_core::Result<AuditHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    AuditHeader::deserialize(&header_buf)
}

fn verify_audit_reader(
    file: &mut File,
    end: u64,
    header: AuditHeader,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<AuditFileVerification> {
    // Rotated files chain their first entry from the previous file's tip,
    // recorded in the header at rotation; first-generation and legacy v1
    // files seed zeros.
    let mut prev_hmac = header.effective_chain_seed();
    let mut entries_verified = 0u64;
    let mut first_sequence = None;
    let mut last_seq: Option<u64> = None;
    let mut cursor = AUDIT_HEADER_SIZE as u64;

    loop {
        if cursor >= end {
            return Ok(finish_audit_verification(
                header,
                entries_verified,
                true,
                None,
                first_sequence,
                last_seq,
                prev_hmac,
            ));
        }
        file.seek(SeekFrom::Start(cursor))?;
        match read_raw_record_before(file, cursor, end)? {
            RawRecord::Parsed(rec) => {
                if !verify_entry_hmac(audit_key, &prev_hmac, &rec.hmac_input, &rec.hmac) {
                    return Ok(finish_audit_verification(
                        header,
                        entries_verified,
                        false,
                        Some(rec.sequence_no),
                        first_sequence,
                        last_seq,
                        prev_hmac,
                    ));
                }
                if let Some(previous) = last_seq {
                    if previous.checked_add(1) != Some(rec.sequence_no) {
                        return Ok(finish_audit_verification(
                            header,
                            entries_verified,
                            false,
                            Some(rec.sequence_no),
                            first_sequence,
                            last_seq,
                            prev_hmac,
                        ));
                    }
                }
                first_sequence.get_or_insert(rec.sequence_no);
                prev_hmac = rec.hmac;
                entries_verified += 1;
                last_seq = Some(rec.sequence_no);
                cursor = rec.end;
            }
            RawRecord::Malformed => {
                let mut probe = cursor + 1;
                loop {
                    match find_entry_magic_before(file, probe, end)? {
                        None => {
                            // Clean EOF or a trailing torn fragment. The chain
                            // links, which is not the same as complete: compare
                            // against entries_declared.
                            return Ok(finish_audit_verification(
                                header,
                                entries_verified,
                                true,
                                None,
                                first_sequence,
                                last_seq,
                                prev_hmac,
                            ));
                        }
                        Some(next) => {
                            file.seek(SeekFrom::Start(next))?;
                            if let RawRecord::Parsed(_) = read_raw_record_before(file, next, end)? {
                                return Ok(finish_audit_verification(
                                    header,
                                    entries_verified,
                                    false,
                                    Some(last_seq.map_or(1, |seq| seq.saturating_add(1))),
                                    first_sequence,
                                    last_seq,
                                    prev_hmac,
                                ));
                            }
                            probe = next + 1;
                        }
                    }
                }
            }
        }
    }
}

struct AuditChainSegment {
    file: AuditFilePath,
    header: AuditHeader,
    result: AuditVerifyResult,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    tip: [u8; MAC_SIZE],
}

impl AuditChainSegment {
    fn mark_boundary_invalid(&mut self) {
        self.result.chain_valid = false;
        if self.result.chain_break_at.is_none() {
            self.result.chain_break_at = self.first_sequence;
        }
    }
}

/// Verify the HMAC chain. Garbage followed by more parseable records means
/// records were damaged or excised mid-file (chain reported broken); a
/// trailing torn fragment alone is a benign incomplete write.
pub fn verify_audit_log(
    path: &Path,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<AuditVerifyResult> {
    ensure_no_audit_maintenance(path)?;
    Ok(verify_audit_file(path, audit_key)?.result)
}

/// Verify retained audit generations as one history.
///
/// Per-file verification trusts that file's header seed. This walk also verifies
/// what exists only between files: generations are contiguous, every header's
/// mutable identity names the open database, sequence numbers continue, and each
/// v2 successor seed equals its predecessor's tip.
/// The oldest retained segment has no external anchor, so replacing the whole
/// history (or rolling back the newest segment with its count) stays outside
/// v2's detection boundary.
#[cfg(test)]
fn verify_audit_chain(
    data_path: &Path,
    audit_key: &[u8; KEY_SIZE],
    expected_file_id: u64,
) -> citadel_core::Result<Vec<(PathBuf, AuditVerifyResult)>> {
    ensure_no_audit_maintenance(&resolve_audit_path(data_path))?;
    let discovery = discover_audit_files(data_path)?;
    let suspicious_numeric_name = discovery.suspicious_numeric_name;
    let files = discovery.files;
    if files.is_empty() {
        return Err(invalid_audit_data(
            "audit history is missing; no live or retained generation remains",
        ));
    }
    let mut segments = Vec::with_capacity(files.len());

    for file in files {
        let AuditFileVerification {
            header,
            result,
            first_sequence,
            last_sequence,
            tip,
        } = verify_audit_file(&file.path, audit_key)?;
        segments.push(AuditChainSegment {
            file,
            header,
            result,
            first_sequence,
            last_sequence,
            tip,
        });
    }

    validate_audit_chain_segments(&mut segments, suspicious_numeric_name, expected_file_id);

    Ok(segments
        .into_iter()
        .map(|segment| (segment.file.path, segment.result))
        .collect())
}

fn validate_audit_chain_segments(
    segments: &mut [AuditChainSegment],
    suspicious_numeric_name: bool,
    expected_file_id: u64,
) {
    // Path discovery does not hide a missing live file or a hole like [.0, .2].
    // Attach the boundary failure to the newer existing file, where an otherwise
    // continuous history stops.
    if let Some(first) = segments.first_mut() {
        if first.file.generation != 0 || suspicious_numeric_name {
            first.mark_boundary_invalid();
        }
    }
    for newer_index in 0..segments.len().saturating_sub(1) {
        let (newer_slice, older_slice) = segments.split_at_mut(newer_index + 1);
        let newer = &mut newer_slice[newer_index];
        let older = &older_slice[0];
        if older.file.generation != newer.file.generation.saturating_add(1) {
            newer.mark_boundary_invalid();
            continue;
        }

        // A locally broken file has already identified the failure. Boundary
        // facts beyond it are not trustworthy enough to diagnose a second one.
        if !newer.result.chain_valid || !older.result.chain_valid {
            continue;
        }

        if newer.header.version == AUDIT_LOG_VERSION
            && older.header.version == AUDIT_LOG_VERSION
            && newer.header.chain_seed != older.tip
        {
            newer.mark_boundary_invalid();
            continue;
        }
    }

    // Sequence continuity spans empty generations. This matters for legacy v1
    // segments, which have no cross-file seed to expose an emptied middle file.
    let mut older_last = None;
    for index in (0..segments.len()).rev() {
        if !segments[index].result.chain_valid {
            older_last = None;
            continue;
        }
        let Some(first) = segments[index].first_sequence else {
            continue;
        };
        if older_last.is_some_and(|last: u64| last.checked_add(1) != Some(first)) {
            segments[index].mark_boundary_invalid();
            older_last = None;
        } else {
            older_last = segments[index].last_sequence;
        }
    }

    for segment in segments {
        if segment.header.file_id != expected_file_id {
            segment.mark_boundary_invalid();
        }
    }
}

struct AuditSnapshotFile {
    file: AuditFilePath,
    handle: File,
    end: u64,
    header: AuditHeader,
}

/// Open handles and fixed end offsets captured while the database excludes
/// rotation. Verification and iteration can then release the writer mutex
/// without losing a retained generation or following a replacement path.
pub(crate) struct AuditHistorySnapshot {
    files: Vec<AuditSnapshotFile>,
    suspicious_numeric_name: bool,
}

impl AuditHistorySnapshot {
    pub(crate) fn open_while_locked(data_path: &Path) -> citadel_core::Result<Self> {
        let discovery = discover_audit_files_while_locked(data_path)?;
        if discovery.files.is_empty() {
            return Err(invalid_audit_data(
                "audit history is missing; no live or retained generation remains",
            ));
        }

        let mut snapshots = Vec::with_capacity(discovery.files.len());
        for file in discovery.files {
            let mut handle = citadel_io::durable::open_regular_read(&file.path)?;
            let end = handle.metadata()?.len();
            if end < AUDIT_HEADER_SIZE as u64 {
                return Err(invalid_audit_data("audit file is shorter than its header"));
            }
            let header = read_audit_header(&mut handle)?;
            snapshots.push(AuditSnapshotFile {
                file,
                handle,
                end,
                header,
            });
        }
        Ok(Self {
            files: snapshots,
            suspicious_numeric_name: discovery.suspicious_numeric_name,
        })
    }

    fn verify_segments(
        &mut self,
        audit_key: &[u8; KEY_SIZE],
        expected_file_id: u64,
    ) -> citadel_core::Result<Vec<AuditChainSegment>> {
        let mut segments = Vec::with_capacity(self.files.len());
        for snapshot in &mut self.files {
            let AuditFileVerification {
                header,
                result,
                first_sequence,
                last_sequence,
                tip,
            } = verify_audit_reader(
                &mut snapshot.handle,
                snapshot.end,
                snapshot.header.clone(),
                audit_key,
            )?;
            segments.push(AuditChainSegment {
                file: snapshot.file.clone(),
                header,
                result,
                first_sequence,
                last_sequence,
                tip,
            });
        }
        validate_audit_chain_segments(
            &mut segments,
            self.suspicious_numeric_name,
            expected_file_id,
        );
        Ok(segments)
    }

    pub(crate) fn verify(
        &mut self,
        audit_key: &[u8; KEY_SIZE],
        expected_file_id: u64,
    ) -> citadel_core::Result<Vec<(PathBuf, AuditVerifyResult)>> {
        Ok(self
            .verify_segments(audit_key, expected_file_id)?
            .into_iter()
            .map(|segment| (segment.file.path, segment.result))
            .collect())
    }

    pub(crate) fn verify_and_visit<F>(
        &mut self,
        audit_key: &[u8; KEY_SIZE],
        expected_file_id: u64,
        mut visitor: F,
    ) -> citadel_core::Result<u64>
    where
        F: FnMut(&Path, &AuditEntry) -> citadel_core::Result<()>,
    {
        let segments = self.verify_segments(audit_key, expected_file_id)?;

        for segment in &segments {
            if !segment.result.chain_valid {
                return Err(invalid_audit_data(format!(
                    "audit generation {} failed authentication",
                    segment.file.generation
                )));
            }
            if segment.result.entries_missing() > 0 {
                return Err(invalid_audit_data(format!(
                    "audit generation {} is shorter than its header count",
                    segment.file.generation
                )));
            }
        }

        let mut total = 0u64;
        for index in (0..self.files.len()).rev() {
            let snapshot = &mut self.files[index];
            let expected = segments[index].result.entries_verified;
            let visited = visit_verified_audit_reader(
                &mut snapshot.handle,
                snapshot.end,
                &snapshot.header,
                audit_key,
                expected,
                &snapshot.file.path,
                &mut visitor,
            )?;
            total = total
                .checked_add(visited)
                .ok_or_else(|| invalid_audit_data("audit entry count overflow"))?;
        }
        Ok(total)
    }
}

fn visit_verified_audit_reader<F>(
    file: &mut File,
    end: u64,
    header: &AuditHeader,
    audit_key: &[u8; KEY_SIZE],
    expected: u64,
    path: &Path,
    visitor: &mut F,
) -> citadel_core::Result<u64>
where
    F: FnMut(&Path, &AuditEntry) -> citadel_core::Result<()>,
{
    let mut prev_hmac = header.effective_chain_seed();
    let mut previous_sequence = None;
    let mut visited = 0u64;
    let mut cursor = AUDIT_HEADER_SIZE as u64;
    while cursor < end {
        file.seek(SeekFrom::Start(cursor))?;
        let RawRecord::Parsed(record) = read_raw_record_before(file, cursor, end)? else {
            break;
        };
        if !verify_entry_hmac(audit_key, &prev_hmac, &record.hmac_input, &record.hmac)
            || previous_sequence
                .is_some_and(|sequence: u64| sequence.checked_add(1) != Some(record.sequence_no))
        {
            return Err(invalid_audit_data(
                "audit history changed while its verified snapshot was read",
            ));
        }

        let entry = AuditEntry {
            timestamp: record.timestamp,
            sequence_no: record.sequence_no,
            event_type: record.event_type,
            detail: record.detail,
            hmac: record.hmac,
        };
        visitor(path, &entry)?;
        prev_hmac = entry.hmac;
        previous_sequence = Some(entry.sequence_no);
        visited += 1;
        cursor = record.end;
    }
    if visited != expected {
        return Err(invalid_audit_data(
            "audit history changed while its verified snapshot was read",
        ));
    }
    Ok(visited)
}

/// Scan a corrupted audit log, recovering entries past damaged regions by
/// searching for per-entry sentinel markers.
pub fn scan_corrupted_audit_log(path: &Path) -> citadel_core::Result<ScanResult> {
    let (data, _) = citadel_io::durable::read_regular_file(path)?;

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
        let entry_len =
            u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        if !(MIN_ENTRY_LEN..=MAX_ENTRY_LEN).contains(&entry_len)
            || offset + 4 + entry_len > data.len()
        {
            if !in_corruption {
                corruption_offsets.push(offset as u64);
                in_corruption = true;
            }
            offset += 1;
            continue;
        }

        let entry_start = offset + 8;
        let remaining = entry_len - 4;

        let event_type_raw =
            u16::from_le_bytes(data[entry_start + 16..entry_start + 18].try_into().unwrap());
        let detail_len =
            u16::from_le_bytes(data[entry_start + 18..entry_start + 20].try_into().unwrap())
                as usize;

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
        let sequence_no =
            u64::from_le_bytes(data[entry_start + 8..entry_start + 16].try_into().unwrap());
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

#[derive(Debug, Clone)]
pub struct ScanResult {
    pub entries: Vec<AuditEntry>,
    pub corruption_offsets: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_record_magic_values_are_frozen() {
        assert_eq!(ROTATION_PREPARED_MAGIC, *b"CTAROT01");
        assert_eq!(ROTATION_COMMITTED_MAGIC, *b"CTAROTC1");
    }

    #[test]
    fn rotation_record_reader_never_consumes_past_its_bound() {
        let mut input = std::io::Cursor::new(vec![0u8; 4096]);
        let error = read_bounded_rotation_record(&mut input, 32).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(input.position(), 33);
    }

    #[test]
    fn persistent_audit_assignments_are_frozen() {
        assert_eq!((AUDIT_LOG_VERSION_LEGACY, AUDIT_LOG_VERSION), (1, 2));
        assert_eq!(
            [
                AuditEventType::DatabaseCreated as u16,
                AuditEventType::DatabaseOpened as u16,
                AuditEventType::DatabaseClosed as u16,
                AuditEventType::PassphraseChanged as u16,
                AuditEventType::KeyBackupExported as u16,
                AuditEventType::BackupCreated as u16,
                AuditEventType::CompactionPerformed as u16,
                AuditEventType::IntegrityCheckPerformed as u16,
            ],
            [1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn rotation_record_format_has_a_stable_known_answer() {
        let record = RotationRecord {
            rotation_id: 0x0102_0304_0506_0708,
            max_rotated_files: 2,
            generations: vec![0, 1, 2, 3, 4],
        };
        let bytes =
            serialize_rotation_record(ROTATION_PREPARED_MAGIC, &record, &[0x42; KEY_SIZE]).unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();

        assert_eq!(
            hex,
            "435441524f543031010000000807060504030201000000000000000002000000050000000000000001000000020000000300000004000000f4030e0d6a46f1bba0b6977ad431c2558279b16dd15888997d875717a0970d4f"
        );
        assert_eq!(
            deserialize_rotation_record(&bytes, ROTATION_PREPARED_MAGIC, &[0x42; KEY_SIZE])
                .unwrap(),
            record
        );
    }

    #[test]
    fn audit_entry_format_and_event_tag_have_a_stable_known_answer() {
        let entry = serialize_entry_data(
            0x0102_0304_0506_0708,
            0x1112_1314_1516_1718,
            AuditEventType::CompactionPerformed,
            b"abc",
        );
        let hmac = compute_entry_hmac(&[0x42; KEY_SIZE], &[0x24; MAC_SIZE], &entry);
        let entry_hex: String = entry.iter().map(|byte| format!("{byte:02x}")).collect();
        let hmac_hex: String = hmac.iter().map(|byte| format!("{byte:02x}")).collect();

        assert_eq!(
            entry_hex,
            "3b0000000807060504030201181716151413121107000300616263"
        );
        assert_eq!(
            hmac_hex,
            "95e592de73cb71f42a027cbe659ad084fe4f4299224545afa99b309a117193fb"
        );
    }

    #[test]
    fn audit_snapshot_excludes_entries_appended_after_capture() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("vault.cdl");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42; KEY_SIZE];
        let mut log = AuditLog::create(
            &audit_path,
            7,
            key,
            AuditConfig {
                enabled: true,
                max_file_size: u64::MAX,
                max_rotated_files: 2,
            },
            true,
        )
        .unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        let mut snapshot = AuditHistorySnapshot::open_while_locked(&data_path).unwrap();

        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();

        let mut sequences = Vec::new();
        let visited = snapshot
            .verify_and_visit(&key, 7, |_, entry| {
                sequences.push(entry.sequence_no);
                Ok(())
            })
            .unwrap();
        assert_eq!(visited, 1);
        assert_eq!(sequences, [1]);
    }

    #[test]
    fn audit_snapshot_survives_rotation_of_its_pinned_live_file() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("vault.cdl");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42; KEY_SIZE];
        let mut log = AuditLog::create(
            &audit_path,
            7,
            key,
            AuditConfig {
                enabled: true,
                max_file_size: 100,
                max_rotated_files: 1,
            },
            true,
        )
        .unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        let mut snapshot = AuditHistorySnapshot::open_while_locked(&data_path).unwrap();

        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        log.log(AuditEventType::IntegrityCheckPerformed, &[])
            .unwrap();
        let paths = audit_log_paths_while_locked(&data_path).unwrap();
        assert_eq!(paths.len(), 2);
        let retained_sequences: Vec<u64> = paths
            .iter()
            .rev()
            .flat_map(|path| read_audit_log(path).unwrap())
            .map(|entry| entry.sequence_no)
            .collect();
        assert_eq!(retained_sequences, [2, 3]);

        let mut sequences = Vec::new();
        let visited = snapshot
            .verify_and_visit(&key, 7, |_, entry| {
                sequences.push(entry.sequence_no);
                Ok(())
            })
            .unwrap();
        assert_eq!(visited, 1);
        assert_eq!(sequences, [1]);
    }

    #[test]
    fn audit_snapshot_authenticates_everything_before_the_first_callback() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("vault.cdl");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42; KEY_SIZE];
        let mut log = AuditLog::create(&audit_path, 7, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let last_hmac_byte = fs::metadata(&audit_path).unwrap().len() - 1;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&audit_path)
            .unwrap();
        file.seek(SeekFrom::Start(last_hmac_byte)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(last_hmac_byte)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut snapshot = AuditHistorySnapshot::open_while_locked(&data_path).unwrap();
        let mut callbacks = 0;
        let error = snapshot
            .verify_and_visit(&key, 7, |_, _| {
                callbacks += 1;
                Ok(())
            })
            .unwrap_err();

        assert!(error.to_string().contains("failed authentication"));
        assert_eq!(callbacks, 0);
    }

    #[test]
    fn header_serialize_deserialize_roundtrip() {
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: 0xDEAD_BEEF,
            created_at: 1234567890,
            entry_count: 42,
            chain_seed: [0xAB; MAC_SIZE],
        };
        let buf = header.serialize();
        let hex: String = buf.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            "5444554102000000efbeadde00000000d2029649000000002a00000000000000abababababababababababababababababababababababababababababababab"
        );
        let h2 = AuditHeader::deserialize(&buf).unwrap();
        assert_eq!(h2.magic, AUDIT_LOG_MAGIC);
        assert_eq!(h2.version, AUDIT_LOG_VERSION);
        assert_eq!(h2.file_id, 0xDEAD_BEEF);
        assert_eq!(h2.created_at, 1234567890);
        assert_eq!(h2.entry_count, 42);
        assert_eq!(h2.chain_seed, [0xAB; MAC_SIZE]);
    }

    #[test]
    fn header_invalid_magic_rejected() {
        let mut buf = [0u8; AUDIT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let result = AuditHeader::deserialize(&buf);
        assert!(matches!(
            result,
            Err(citadel_core::Error::InvalidMagic { .. })
        ));
    }

    #[test]
    fn header_rejects_unknown_version() {
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION + 1,
            file_id: 1,
            created_at: 0,
            entry_count: 0,
            chain_seed: [0u8; MAC_SIZE],
        };
        let result = AuditHeader::deserialize(&header.serialize());
        assert!(matches!(
            result,
            Err(citadel_core::Error::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn failed_header_creation_removes_the_new_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: 7,
            created_at: 11,
            entry_count: 0,
            chain_seed: [0u8; MAC_SIZE],
        };

        for boundary in [
            HeaderCreateBoundary::FileCreated,
            HeaderCreateBoundary::HeaderWritten,
            HeaderCreateBoundary::FileSynced,
            HeaderCreateBoundary::DirectorySynced,
        ] {
            let path = dir.path().join(format!("failed-{boundary:?}.audit"));
            let error = create_header_file_with(&path, &header, |seen| {
                if seen == boundary {
                    Err(std::io::Error::other("injected create failure"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();

            assert!(error.to_string().contains("injected create failure"));
            assert!(!path.exists(), "{boundary:?} left a blocking sidecar");
        }
    }

    /// Write a file byte-for-byte as released v1 code did: chain from zeros,
    /// header bytes 32..64 hold the last-HMAC tip.
    fn write_legacy_v1_file(path: &Path, file_id: u64, key: &[u8; KEY_SIZE], entries: usize) {
        let mut tip = [0u8; MAC_SIZE];
        let mut body: Vec<u8> = Vec::new();
        for i in 0..entries {
            let data = serialize_entry_data(
                1_000 + i as u64,
                (i + 1) as u64,
                AuditEventType::DatabaseOpened,
                &[i as u8],
            );
            let hmac = compute_entry_hmac(key, &tip, &data);
            body.extend_from_slice(&AUDIT_ENTRY_MAGIC.to_le_bytes());
            body.extend_from_slice(&data);
            body.extend_from_slice(&hmac);
            tip = hmac;
        }
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION_LEGACY,
            file_id,
            created_at: 999,
            entry_count: entries as u64,
            chain_seed: tip,
        };
        let mut buf = header.serialize().to_vec();
        buf.extend_from_slice(&body);
        fs::write(path, buf).unwrap();
    }

    /// Regression pin: released files carry the chain tip (not zeros) at
    /// bytes 32..64; verification must still seed the v1 chain from zeros.
    #[test]
    fn legacy_v1_file_verifies_with_zero_seed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid, "v1 file must not raise a tamper alarm");
        assert_eq!(result.entries_verified, 3);
        assert_eq!(result.chain_break_at, None);
    }

    /// [valid][garbage][valid] (a crash-recovery shape): open_existing must
    /// resync past the garbage, not truncate the valid tail, and append from
    /// the last surviving record.
    #[test]
    fn open_existing_preserves_records_after_mid_file_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        // Garbage fragment, then two records chained from the last tip.
        let bytes = fs::read(&path).unwrap();
        let mut tip = [0u8; MAC_SIZE];
        tip.copy_from_slice(&bytes[bytes.len() - MAC_SIZE..]);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xFF; 25]).unwrap();
        for seq in 3..=4u64 {
            let data = serialize_entry_data(seq * 100, seq, AuditEventType::DatabaseOpened, &[]);
            let hmac = compute_entry_hmac(&key, &tip, &data);
            file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes()).unwrap();
            file.write_all(&data).unwrap();
            file.write_all(&hmac).unwrap();
            tip = hmac;
        }
        drop(file);
        let len_before = fs::metadata(&path).unwrap().len();

        let mut log =
            AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseClosed, &[]).unwrap();
        drop(log);

        assert!(
            fs::metadata(&path).unwrap().len() > len_before,
            "no valid record may be truncated"
        );
        let scan = scan_corrupted_audit_log(&path).unwrap();
        assert_eq!(scan.entries.len(), 5);
        assert_eq!(
            scan.entries[4].sequence_no, 5,
            "append continues the surviving tail's sequence"
        );
        assert_eq!(scan.corruption_offsets.len(), 1);
    }

    /// Creating the audit file next to an un-upgraded (unflagged) data file
    /// writes a v1 header, so released binaries can still open the database.
    #[test]
    fn create_on_unflagged_database_writes_v1_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-born.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 9, key, AuditConfig::default(), false).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        File::open(&path)
            .unwrap()
            .read_exact(&mut header_buf)
            .unwrap();
        let header = AuditHeader::deserialize(&header_buf).unwrap();
        assert_eq!(header.version, AUDIT_LOG_VERSION_LEGACY);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 1);
    }

    /// A torn record whose intact header claims a length spanning the records
    /// appended after it must not make open_existing truncate them.
    #[test]
    fn phantom_spanning_record_never_truncates_valid_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let bytes = fs::read(&path).unwrap();
        let mut tip = [0u8; MAC_SIZE];
        tip.copy_from_slice(&bytes[bytes.len() - MAC_SIZE..]);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        // Torn record: full consistent header claiming a 60-byte detail,
        // but only the 20 header bytes of the body were written.
        let entry_len = (MIN_ENTRY_LEN + 60) as u32;
        file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes()).unwrap();
        file.write_all(&entry_len.to_le_bytes()).unwrap();
        file.write_all(&2u64.to_le_bytes()).unwrap(); // timestamp
        file.write_all(&2u64.to_le_bytes()).unwrap(); // sequence_no
        file.write_all(&(AuditEventType::DatabaseOpened as u16).to_le_bytes())
            .unwrap();
        file.write_all(&60u16.to_le_bytes()).unwrap(); // detail_len

        // Two real records appended after the crash, chained from the tip.
        for (seq, marker) in [(2u64, b"R1MARKER"), (3u64, b"R2MARKER")] {
            let data = serialize_entry_data(seq * 10, seq, AuditEventType::DatabaseOpened, marker);
            let hmac = compute_entry_hmac(&key, &tip, &data);
            file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes()).unwrap();
            file.write_all(&data).unwrap();
            file.write_all(&hmac).unwrap();
            tip = hmac;
        }
        drop(file);
        let len_before = fs::metadata(&path).unwrap().len();

        let log = AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
        drop(log);

        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            len_before,
            "no byte of the real records may be truncated"
        );
        let survived = fs::read(&path).unwrap();
        for marker in [&b"R1MARKER"[..], &b"R2MARKER"[..]] {
            assert!(
                survived.windows(marker.len()).any(|w| w == marker),
                "appended record bytes must survive reopen"
            );
        }
    }

    /// Unparseable bytes followed by more records mean the chain is broken
    /// there; a trailing torn fragment alone stays benign.
    #[test]
    fn verify_flags_mid_file_garbage_but_not_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        // Trailing torn fragment: benign.
        let clean = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes()).unwrap();
        file.write_all(&[0xAB; 10]).unwrap();
        drop(file);
        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid, "torn tail is not tampering");
        assert_eq!(result.entries_verified, 2);

        // Mid-file garbage followed by a real record: broken chain.
        fs::write(&path, &clean).unwrap();
        let mut tip = [0u8; MAC_SIZE];
        tip.copy_from_slice(&clean[clean.len() - MAC_SIZE..]);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xFF; 25]).unwrap();
        let data = serialize_entry_data(30, 3, AuditEventType::DatabaseClosed, &[]);
        let hmac = compute_entry_hmac(&key, &tip, &data);
        file.write_all(&AUDIT_ENTRY_MAGIC.to_le_bytes()).unwrap();
        file.write_all(&data).unwrap();
        file.write_all(&hmac).unwrap();
        drop(file);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(!result.chain_valid, "mid-file garbage breaks the chain");
        assert_eq!(result.entries_verified, 2);
        assert_eq!(result.chain_break_at, Some(3));
    }

    /// New-code appends continue a v1 file's chain and keep it version 1,
    /// so released binaries can still open and verify it.
    #[test]
    fn legacy_v1_file_appends_and_stays_v1() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);

        let mut log =
            AuditLog::open_existing(&path, 7, key, AuditConfig::default(), false).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        log.log(AuditEventType::DatabaseClosed, &[]).unwrap();
        drop(log);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 5);

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        File::open(&path)
            .unwrap()
            .read_exact(&mut header_buf)
            .unwrap();
        let header = AuditHeader::deserialize(&header_buf).unwrap();
        assert_eq!(header.version, AUDIT_LOG_VERSION_LEGACY);
        assert_eq!(header.entry_count, 5);
    }

    #[test]
    fn legacy_v1_upgrade_to_v2_is_atomic_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);

        let mut log =
            AuditLog::open_existing(&path, 7, key, AuditConfig::default(), false).unwrap();
        assert!(log.upgrade_to_v2().unwrap());
        assert!(!log.upgrade_to_v2().unwrap(), "idempotent");
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        File::open(&path)
            .unwrap()
            .read_exact(&mut header_buf)
            .unwrap();
        let header = AuditHeader::deserialize(&header_buf).unwrap();
        assert_eq!(header.version, AUDIT_LOG_VERSION);
        assert_eq!(header.chain_seed, [0u8; MAC_SIZE]);
        assert_eq!(header.created_at, 999, "creation time preserved");
        assert_eq!(header.entry_count, 4);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 4);
    }

    #[test]
    fn failed_audit_upgrade_publish_preserves_v1_and_can_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);
        let original = fs::read(&path).unwrap();

        let mut log =
            AuditLog::open_existing(&path, 7, key, AuditConfig::default(), false).unwrap();
        let error = log
            .upgrade_to_v2_with(|_, _| Err(std::io::Error::other("publish failed")))
            .unwrap_err();
        assert!(error.to_string().contains("publish failed"));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert!(!audit_upgrade_path(&path).exists());

        assert!(log.upgrade_to_v2().unwrap());
        assert!(verify_audit_log(&path, &key).unwrap().chain_valid);
    }

    #[test]
    fn poisoned_audit_writer_cannot_authenticate_an_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 1);
        let original = fs::read(&path).unwrap();

        let mut log =
            AuditLog::open_existing(&path, 7, key, AuditConfig::default(), false).unwrap();
        log.poisoned = true;
        assert!(log.upgrade_to_v2().is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    /// A v1 lineage stays v1 across rotation; the successor chains from zeros
    /// and verifies, while sequence numbers continue across files.
    #[test]
    fn legacy_v1_rotation_stays_v1_with_zero_seed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);

        let config = AuditConfig {
            max_file_size: 1,
            ..Default::default()
        };
        let mut log = AuditLog::open_existing(&path, 7, key, config, false).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        File::open(&path)
            .unwrap()
            .read_exact(&mut header_buf)
            .unwrap();
        let header = AuditHeader::deserialize(&header_buf).unwrap();
        assert_eq!(header.version, AUDIT_LOG_VERSION_LEGACY);
        assert_eq!(header.chain_seed, [0u8; MAC_SIZE]);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 1);
        let entries = read_audit_log(&path).unwrap();
        assert_eq!(entries[0].sequence_no, 4, "sequence continues");

        // The rotated-out predecessor is untouched and still verifies.
        let rotated = verify_audit_log(&rotated_path(&path, 1), &key).unwrap();
        assert!(rotated.chain_valid);
        assert_eq!(rotated.entries_verified, 3);
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

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[0x00, 0x00])
            .unwrap();
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

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
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

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
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
    fn verify_rejects_authenticated_sequence_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        // The writer normally owns this counter. Force a structurally valid,
        // correctly MACed record that nevertheless skips sequence 2.
        log.sequence_no += 1;
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(!result.chain_valid);
        assert_eq!(result.entries_verified, 1);
        assert_eq!(result.chain_break_at, Some(3));
    }

    #[test]
    fn corrupted_max_sequence_cannot_panic_or_wrap_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let mut bytes = fs::read(&path).unwrap();
        let sequence_offset = AUDIT_HEADER_SIZE + 4 + 4 + 8;
        bytes[sequence_offset..sequence_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        // open_existing deliberately preserves parseable corruption so the
        // verifier can report it, but logging must still error rather than
        // overflow a sequence number read from those bytes.
        let mut log =
            AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
        let before = fs::read(&path).unwrap();
        let error = log.log(AuditEventType::DatabaseOpened, &[]).unwrap_err();
        assert!(error.to_string().contains("sequence number overflow"));
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn verify_wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
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

        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        drop(log);

        let mut log =
            AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
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
    fn failed_append_boundaries_roll_back_before_another_entry_can_follow() {
        for boundary in [
            AppendBoundary::MagicWritten,
            AppendBoundary::DataWritten,
            AppendBoundary::HmacWritten,
            AppendBoundary::EntrySynced,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.citadel-audit");
            let key = [0x42u8; KEY_SIZE];
            let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
            log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
            let before = fs::read(&path).unwrap();

            let error = log
                .log_with_checkpoint(AuditEventType::DatabaseOpened, &[], |reached| {
                    if reached == boundary {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            format!("injected append failure after {boundary:?}"),
                        ))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert!(error.to_string().contains("injected append failure"));
            assert_eq!(fs::read(&path).unwrap(), before, "{boundary:?}");

            log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
            drop(log);
            let entries = read_audit_log(&path).unwrap();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry.sequence_no)
                    .collect::<Vec<_>>(),
                vec![1, 2],
                "{boundary:?}"
            );
            assert!(verify_audit_log(&path, &key).unwrap().chain_valid);
        }
    }

    #[test]
    fn oversized_detail_is_rejected_without_mutating_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        let before = fs::read(&path).unwrap();

        let error = log
            .log(
                AuditEventType::DatabaseOpened,
                &vec![0; u16::MAX as usize + 1],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            citadel_core::Error::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
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

        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        for _ in 0..10 {
            log.log(AuditEventType::DatabaseOpened, &[0u8; 50]).unwrap();
        }
        drop(log);

        let rotated = rotated_path(&path, 1);
        assert!(rotated.exists());
        assert!(path.exists());
    }

    #[test]
    fn failed_live_rotation_rename_preserves_the_current_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 2,
        };
        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        let before = fs::read(&path).unwrap();
        let live = path.clone();

        let error = log
            .rotate_if_needed_with(
                |src, dst| {
                    if src == live {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "injected live rename failure",
                        ))
                    } else {
                        fs::rename(src, dst)
                    }
                },
                create_header_file,
            )
            .unwrap_err();

        assert!(error.to_string().contains("injected live rename failure"));
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(!rotated_path(&path, 1).exists());
    }

    fn rotation_temporary_files(path: &Path) -> Vec<PathBuf> {
        let parent = path.parent().unwrap();
        fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| candidate.to_string_lossy().contains(".rotation-"))
            .collect()
    }

    fn three_generation_rotation_fixture(
        path: &Path,
        key: [u8; KEY_SIZE],
        slots_flagged: bool,
    ) -> AuditLog {
        let config = AuditConfig {
            enabled: true,
            max_file_size: u64::MAX,
            max_rotated_files: 2,
        };
        let mut log = AuditLog::create(path, 123, key, config, slots_flagged).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();

        log.config.max_file_size = 0;
        log.rotate_if_needed().unwrap();
        log.config.max_file_size = u64::MAX;
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();

        log.config.max_file_size = 0;
        log.rotate_if_needed().unwrap();
        log.config.max_file_size = u64::MAX;
        log.log(AuditEventType::PassphraseChanged, &[]).unwrap();

        log.config.max_file_size = 0;
        log
    }

    #[test]
    fn shrinking_retention_removes_every_generation_above_the_new_limit() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("retention.citadel");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: u64::MAX,
            max_rotated_files: 5,
        };
        let mut log = AuditLog::create(&audit_path, 123, key, config, true).unwrap();

        for index in 0..6 {
            log.log(AuditEventType::DatabaseOpened, &[index]).unwrap();
            if index < 5 {
                log.config.max_file_size = 0;
                log.rotate_if_needed().unwrap();
                log.config.max_file_size = u64::MAX;
            }
        }
        assert!(rotated_path(&audit_path, 5).exists());

        log.config.max_rotated_files = 2;
        log.config.max_file_size = 0;
        log.rotate_if_needed().unwrap();
        drop(log);

        assert!(audit_path.exists());
        assert!(rotated_path(&audit_path, 1).exists());
        assert!(rotated_path(&audit_path, 2).exists());
        for generation in 3..=5 {
            assert!(!rotated_path(&audit_path, generation).exists());
        }
        let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
        assert!(verified.iter().all(|(_, result)| result.chain_valid));
        assert_eq!(retained_sequences(&data_path), vec![5, 6]);
    }

    #[test]
    fn audit_log_paths_reports_discovery_errors() {
        let dir = tempfile::tempdir().unwrap();
        let non_directory = dir.path().join("not-a-directory");
        fs::write(&non_directory, b"file").unwrap();

        let error = audit_log_paths_while_locked(&non_directory.join("vault.citadel")).unwrap_err();
        assert!(matches!(error, citadel_core::Error::Io(_)));
    }

    fn retained_sequences(data_path: &Path) -> Vec<u64> {
        let mut sequences = Vec::new();
        for path in audit_log_paths_while_locked(data_path).unwrap() {
            sequences.extend(
                read_audit_log(&path)
                    .unwrap()
                    .into_iter()
                    .map(|entry| entry.sequence_no),
            );
        }
        sequences.sort_unstable();
        sequences
    }

    #[test]
    fn every_rotation_crash_boundary_recovers_without_loss_or_duplication() {
        let boundaries = [
            (RotationBoundary::SuccessorPrepared, false),
            (RotationBoundary::PreparedDurable, false),
            (RotationBoundary::LiveStaged, false),
            (RotationBoundary::SuccessorPublished, false),
            (RotationBoundary::GenerationPublished(0), false),
            (RotationBoundary::GenerationPublished(1), false),
            (RotationBoundary::CommitDurable, true),
            (RotationBoundary::ExpiredTailRemoved, true),
        ];

        for (boundary, committed) in boundaries {
            let dir = tempfile::tempdir().unwrap();
            let data_path = dir.path().join("test.citadel");
            let audit_path = resolve_audit_path(&data_path);
            let key = [0x42u8; KEY_SIZE];
            let mut log = three_generation_rotation_fixture(&audit_path, key, true);

            let error = log
                .rotate_if_needed_with_checkpoint(
                    |src, dst| fs::rename(src, dst),
                    create_header_file,
                    |reached| reached == boundary,
                )
                .unwrap_err();
            assert!(error.to_string().contains("simulated audit rotation crash"));
            drop(log);

            let pending = verify_audit_log(&audit_path, &key).unwrap_err();
            assert!(pending.to_string().contains("rotation recovery is pending"));

            let config = AuditConfig {
                enabled: true,
                max_file_size: u64::MAX,
                max_rotated_files: 2,
            };
            let mut recovered =
                AuditLog::open_existing(&audit_path, 123, key, config, true).unwrap();
            recovered.log(AuditEventType::DatabaseClosed, &[]).unwrap();
            drop(recovered);

            assert!(!rotation_work_path(&audit_path).exists());
            let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
            assert!(verified.iter().all(|(_, result)| result.chain_valid));
            let expected = if committed {
                vec![2, 3, 4]
            } else {
                vec![1, 2, 3, 4]
            };
            assert_eq!(retained_sequences(&data_path), expected, "{boundary:?}");
        }
    }

    #[test]
    fn committed_legacy_rotation_recovery_preserves_v1_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("legacy.citadel");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        let mut log = three_generation_rotation_fixture(&audit_path, key, false);

        log.rotate_if_needed_with_checkpoint(
            |src, dst| fs::rename(src, dst),
            create_header_file,
            |reached| reached == RotationBoundary::CommitDurable,
        )
        .unwrap_err();
        drop(log);

        let config = AuditConfig {
            enabled: true,
            max_file_size: u64::MAX,
            max_rotated_files: 2,
        };
        let mut recovered = AuditLog::open_existing(&audit_path, 123, key, config, false).unwrap();
        recovered.log(AuditEventType::DatabaseClosed, &[]).unwrap();
        drop(recovered);

        for path in audit_log_paths_while_locked(&data_path).unwrap() {
            let mut bytes = [0u8; AUDIT_HEADER_SIZE];
            File::open(path).unwrap().read_exact(&mut bytes).unwrap();
            assert_eq!(
                AuditHeader::deserialize(&bytes).unwrap().version,
                AUDIT_LOG_VERSION_LEGACY
            );
        }
        let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
        assert!(verified.iter().all(|(_, result)| result.chain_valid));
        assert_eq!(retained_sequences(&data_path), vec![2, 3, 4]);
    }

    #[test]
    fn authenticated_rotation_record_tamper_blocks_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let mut log = three_generation_rotation_fixture(&path, key, true);

        log.rotate_if_needed_with_checkpoint(
            |src, dst| fs::rename(src, dst),
            create_header_file,
            |reached| reached == RotationBoundary::PreparedDurable,
        )
        .unwrap_err();
        drop(log);

        let prepared = rotation_prepared_path(&rotation_work_path(&path));
        let mut bytes = fs::read(&prepared).unwrap();
        *bytes.last_mut().unwrap() ^= 0x80;
        fs::write(&prepared, bytes).unwrap();

        let config = AuditConfig {
            enabled: true,
            max_file_size: u64::MAX,
            max_rotated_files: 2,
        };
        let error = match AuditLog::open_existing(&path, 123, key, config, true) {
            Ok(_) => panic!("tampered rotation record must block recovery"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("record authentication failed"));
        assert!(rotation_work_path(&path).exists());
    }

    #[test]
    fn unknown_rotation_artifact_blocks_open_and_is_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig::default();
        let log = AuditLog::create(&path, 123, key, config.clone(), true).unwrap();
        drop(log);

        let work = rotation_work_path(&path);
        fs::create_dir(&work).unwrap();
        let unknown = work.join("not-a-protocol-file");
        fs::write(&unknown, b"preserve me").unwrap();

        let error = match AuditLog::open_existing(&path, 123, key, config, true) {
            Ok(_) => panic!("unknown rotation state must block open"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("unexpected audit rotation artifact"));
        assert_eq!(fs::read(&unknown).unwrap(), b"preserve me");
        let verify_error = verify_audit_log(&path, &key).unwrap_err();
        assert!(verify_error
            .to_string()
            .contains("rotation recovery is pending"));
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_audit_basenames_never_share_generations() {
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let mut first_name = vec![0xff];
        first_name.extend_from_slice(b".citadel-audit");
        let first = dir.path().join(std::ffi::OsString::from_vec(first_name));
        fs::write(&first, b"first live").unwrap();

        let mut second_name = vec![0xfe];
        second_name.extend_from_slice(b".citadel-audit.1");
        let second = dir.path().join(std::ffi::OsString::from_vec(second_name));
        fs::write(&second, b"other vault generation").unwrap();

        let discovery = discover_audit_files_from_live(&first).unwrap();
        assert!(!discovery.suspicious_numeric_name);
        assert_eq!(discovery.files.len(), 1);
        assert_eq!(discovery.files[0].generation, 0);
        assert_eq!(discovery.files[0].path, first);
    }

    #[test]
    fn failed_generation_handoff_restores_live_and_existing_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 2,
        };
        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        fs::write(rotated_path(&path, 1), b"existing generation one").unwrap();
        fs::write(rotated_path(&path, 2), b"existing generation two").unwrap();

        let before = [
            fs::read(&path).unwrap(),
            fs::read(rotated_path(&path, 1)).unwrap(),
            fs::read(rotated_path(&path, 2)).unwrap(),
        ];
        let live = path.clone();
        let first_generation = rotated_path(&path, 1);
        let failed_once = std::cell::Cell::new(false);

        let error = log
            .rotate_if_needed_with(
                |src, dst| {
                    if dst == first_generation && !failed_once.replace(true) {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "injected generation handoff failure",
                        ))
                    } else {
                        fs::rename(src, dst)
                    }
                },
                create_header_file,
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("injected generation handoff failure"));
        assert_eq!(fs::read(&live).unwrap(), before[0]);
        assert_eq!(fs::read(rotated_path(&path, 1)).unwrap(), before[1]);
        assert_eq!(fs::read(rotated_path(&path, 2)).unwrap(), before[2]);
        assert!(rotation_temporary_files(&path).is_empty());

        // The writer still owns the restored live file and its original HMAC
        // tip; a later append must remain usable and chain-valid.
        log.config.max_file_size = u64::MAX;
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);
        let verified = verify_audit_log(&path, &key).unwrap();
        assert!(verified.chain_valid);
        assert_eq!(verified.entries_verified, 2);
    }

    #[test]
    fn failed_successor_creation_precedes_every_visible_handoff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 2,
        };
        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        fs::write(rotated_path(&path, 1), b"existing generation one").unwrap();
        fs::write(rotated_path(&path, 2), b"existing generation two").unwrap();

        let before = [
            fs::read(&path).unwrap(),
            fs::read(rotated_path(&path, 1)).unwrap(),
            fs::read(rotated_path(&path, 2)).unwrap(),
        ];
        let rename_called = std::cell::Cell::new(false);

        let error = log
            .rotate_if_needed_with(
                |src, dst| {
                    rename_called.set(true);
                    fs::rename(src, dst)
                },
                |_, _| {
                    Err(citadel_core::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "injected successor creation failure",
                    )))
                },
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("injected successor creation failure"));
        assert!(!rename_called.get());
        assert_eq!(fs::read(&path).unwrap(), before[0]);
        assert_eq!(fs::read(rotated_path(&path, 1)).unwrap(), before[1]);
        assert_eq!(fs::read(rotated_path(&path, 2)).unwrap(), before[2]);
        assert!(rotation_temporary_files(&path).is_empty());
    }

    #[test]
    fn failed_successor_publication_restores_live_and_existing_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 2,
        };
        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        fs::write(rotated_path(&path, 1), b"existing generation one").unwrap();
        fs::write(rotated_path(&path, 2), b"existing generation two").unwrap();

        let before = [
            fs::read(&path).unwrap(),
            fs::read(rotated_path(&path, 1)).unwrap(),
            fs::read(rotated_path(&path, 2)).unwrap(),
        ];
        let live = path.clone();
        let failed_once = std::cell::Cell::new(false);

        let error = log
            .rotate_if_needed_with(
                |src, dst| {
                    if dst == live && !failed_once.replace(true) {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "injected successor publication failure",
                        ))
                    } else {
                        fs::rename(src, dst)
                    }
                },
                create_header_file,
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("injected successor publication failure"));
        assert_eq!(fs::read(&path).unwrap(), before[0]);
        assert_eq!(fs::read(rotated_path(&path, 1)).unwrap(), before[1]);
        assert_eq!(fs::read(rotated_path(&path, 2)).unwrap(), before[2]);
        assert!(rotation_temporary_files(&path).is_empty());
    }

    #[test]
    fn zero_rotated_generations_is_rejected_before_database_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel");
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 0,
        };

        let builder = crate::DatabaseBuilder::new(&path)
            .passphrase(b"test-passphrase")
            .audit_config(config);
        // Select a FIPS-approved KDF so this test reaches audit validation.
        #[cfg(feature = "fips")]
        let builder = builder
            .kdf_algorithm(citadel_core::types::KdfAlgorithm::Pbkdf2HmacSha256)
            .pbkdf2_iterations(600_000);

        let error = match builder.create() {
            Ok(_) => panic!("zero retention must be rejected before creating the database"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("max_rotated_files must be at least 1"));
        assert!(!path.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn verify_chain_valid_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let config = AuditConfig {
            enabled: true,
            max_file_size: 200,
            max_rotated_files: 2,
        };

        let mut log = AuditLog::create(&path, 123, key, config, true).unwrap();
        for _ in 0..10 {
            log.log(AuditEventType::DatabaseOpened, &[0u8; 50]).unwrap();
        }
        drop(log);

        let current_entries = read_audit_log(&path).unwrap();
        assert!(!current_entries.is_empty());

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, current_entries.len() as u64);
        assert!(result.chain_break_at.is_none());

        // Rotated generations carry their own chain seed and must verify too.
        for i in 1..=2 {
            let rp = rotated_path(&path, i);
            assert!(rp.exists());
            let rotated_result = verify_audit_log(&rp, &key).unwrap();
            assert!(rotated_result.chain_valid);
            assert!(rotated_result.entries_verified > 0);
        }
    }

    #[test]
    fn open_existing_seeds_chain_from_rotated_header() {
        // Simulate a crash right after rotation: the new file holds only a
        // header whose chain seed is the previous file's tip HMAC.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: AUDIT_LOG_VERSION,
            file_id: 123,
            created_at: now_nanos(),
            entry_count: 0,
            chain_seed: [0x77; MAC_SIZE],
        };
        fs::write(&path, header.serialize()).unwrap();

        let mut log =
            AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 1);
    }

    #[test]
    fn empty_successor_recovers_sequence_from_retained_predecessor() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("test");
        let path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        let config = AuditConfig {
            enabled: true,
            max_file_size: 100,
            max_rotated_files: 2,
        };

        let mut log = AuditLog::create(&path, 123, key, config.clone(), true).unwrap();
        log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
        log.rotate_if_needed().unwrap();
        assert!(read_audit_log(&path).unwrap().is_empty());
        drop(log); // crash-window shape: successor header exists, no entry does

        let mut log = AuditLog::open_existing(&path, 123, key, config, true).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let entries = read_audit_log(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence_no, 2);
        let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
        assert!(verified.iter().all(|(_, result)| result.chain_valid));
    }

    #[test]
    fn empty_generations_do_not_hide_the_nearest_sequence_predecessor() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("test");
        let path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&rotated_path(&path, 2), 123, &key, 3);
        write_legacy_v1_file(&rotated_path(&path, 1), 123, &key, 0);
        write_legacy_v1_file(&path, 123, &key, 0);

        let config = AuditConfig {
            max_file_size: 1,
            max_rotated_files: 3,
            ..AuditConfig::default()
        };
        let mut log = AuditLog::open_existing(&path, 123, key, config, false).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let entries = read_audit_log(&path).unwrap();
        assert_eq!(entries[0].sequence_no, 4);
        let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
        assert!(verified.iter().all(|(_, result)| result.chain_valid));
    }

    #[test]
    fn an_emptied_legacy_generation_cannot_hide_a_sequence_gap() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("test");
        let path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 123, &key, 3);
        let config = AuditConfig {
            max_file_size: 1,
            max_rotated_files: 3,
            ..AuditConfig::default()
        };
        let mut log = AuditLog::open_existing(&path, 123, key, config, false).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let middle = rotated_path(&path, 1);
        let mut emptied = fs::read(&middle).unwrap();
        emptied.truncate(AUDIT_HEADER_SIZE);
        emptied[24..32].copy_from_slice(&0u64.to_le_bytes());
        fs::write(&middle, emptied).unwrap();

        let verified = verify_audit_chain(&data_path, &key, 123).unwrap();
        assert!(!verified[0].1.chain_valid);
    }

    #[test]
    fn whole_chain_verification_refuses_missing_history() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("missing.citadel");
        let audit_path = resolve_audit_path(&data_path);
        let key = [0x42u8; KEY_SIZE];
        drop(AuditLog::create(&audit_path, 7, key, AuditConfig::default(), true).unwrap());
        fs::remove_file(&audit_path).unwrap();

        let error = verify_audit_chain(&data_path, &key, 7).unwrap_err();
        assert!(error.to_string().contains("audit history is missing"));
    }

    #[test]
    fn open_existing_truncates_torn_trailing_record() {
        // Torn suffixes a crash mid-log() can leave behind: magic only, and
        // magic + length + partial entry body.
        let magic_only = AUDIT_ENTRY_MAGIC.to_le_bytes().to_vec();
        let mut partial_body = AUDIT_ENTRY_MAGIC.to_le_bytes().to_vec();
        partial_body.extend_from_slice(&60u32.to_le_bytes());
        partial_body.extend_from_slice(&[0xEE; 10]);

        for stray in [magic_only, partial_body] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.citadel-audit");
            let key = [0x42u8; KEY_SIZE];

            let mut log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
            log.log(AuditEventType::DatabaseCreated, &[]).unwrap();
            log.log(AuditEventType::DatabaseClosed, &[]).unwrap();
            drop(log);

            let mut data = fs::read(&path).unwrap();
            let healthy_len = data.len();
            data.extend_from_slice(&stray);
            fs::write(&path, &data).unwrap();

            let mut log =
                AuditLog::open_existing(&path, 123, key, AuditConfig::default(), true).unwrap();
            log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
            drop(log);

            // The stray bytes must be gone: the new entry starts exactly at
            // the previous valid boundary.
            let reopened = fs::read(&path).unwrap();
            assert_eq!(
                u32::from_le_bytes(reopened[healthy_len..healthy_len + 4].try_into().unwrap()),
                AUDIT_ENTRY_MAGIC
            );

            let entries = read_audit_log(&path).unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[2].event_type, AuditEventType::DatabaseOpened);
            assert_eq!(entries[2].sequence_no, 3);

            let result = verify_audit_log(&path, &key).unwrap();
            assert!(result.chain_valid);
            assert_eq!(result.entries_verified, 3);
        }
    }

    #[test]
    fn file_format_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.citadel-audit");
        let key = [0x42u8; KEY_SIZE];

        let log = AuditLog::create(&path, 123, key, AuditConfig::default(), true).unwrap();
        drop(log);

        let data = fs::read(&path).unwrap();
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        assert_eq!(magic, 0x4155_4454);
    }
}
