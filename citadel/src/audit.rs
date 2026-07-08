use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

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

/// Read one record at `start` (caller seeked there). Any framing fault is
/// Malformed; the internal-consistency check (entry_len == 56 + detail_len,
/// known event type) stops a torn record from swallowing later valid records
/// as a phantom. HMAC validity is the caller's job.
fn read_raw_record(file: &mut File, start: u64) -> citadel_core::Result<RawRecord> {
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
        end: start + 4 + entry_len as u64,
    })))
}

/// Scan forward from `from` for the next candidate entry magic. Returns its
/// offset, or None when no candidate exists before EOF.
fn find_entry_magic(file: &mut File, mut from: u64) -> citadel_core::Result<Option<u64>> {
    const CHUNK: u64 = 8192;
    let magic = AUDIT_ENTRY_MAGIC.to_le_bytes();
    let end = file.seek(SeekFrom::End(0))?;
    let mut buf = [0u8; CHUNK as usize];
    while from + 4 <= end {
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
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&header.serialize())?;
    file.sync_data()?;
    citadel_io::durable::fsync_directory(path)?;
    Ok(file)
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
    /// Format version of the current file; v1 lineages stay v1 across
    /// rotation so released binaries keep opening the database.
    version: u32,
    created_at: u64,
}

impl AuditLog {
    pub(crate) fn audit_key(&self) -> &[u8; KEY_SIZE] {
        &self.audit_key
    }

    /// `slots_flagged` is the data file's HEADER_FLAG_SLOTS_V1 state: an
    /// un-upgraded database gets a v1 audit header so released binaries can
    /// still open it (a v2 file beside a legacy data file would lock them
    /// out). Flagged and new databases get v2.
    pub(crate) fn create(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
        slots_flagged: bool,
    ) -> citadel_core::Result<Self> {
        let version = if slots_flagged {
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
            file,
            audit_key,
            prev_hmac: [0u8; MAC_SIZE],
            sequence_no: 0,
            entry_count: 0,
            config,
            path: path.to_path_buf(),
            file_id,
            version,
            created_at,
        })
    }

    /// Open an existing audit log file, seeking to the end for appending.
    pub(crate) fn open_existing(
        path: &Path,
        file_id: u64,
        audit_key: [u8; KEY_SIZE],
        config: AuditConfig,
    ) -> citadel_core::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = AuditHeader::deserialize(&header_buf)?;

        if header.file_id != file_id {
            return Err(citadel_core::Error::KeyFileMismatch);
        }

        // Seed the chain from the header so the first entry written into a
        // freshly rotated (still empty) file links to the previous file's tip.
        let mut prev_hmac = header.effective_chain_seed();
        let mut sequence_no = 0u64;
        let mut entry_count = 0u64;
        let mut valid_end = AUDIT_HEADER_SIZE as u64;
        let mut cursor = valid_end;

        loop {
            file.seek(SeekFrom::Start(cursor))?;
            match read_raw_record(&mut file, cursor)? {
                RawRecord::Parsed(rec) => {
                    sequence_no = rec.sequence_no;
                    prev_hmac = rec.hmac;
                    entry_count += 1;
                    valid_end = rec.end;
                    cursor = rec.end;
                }
                // Resync past garbage: crash recovery can leave a torn record
                // with valid entries appended after it.
                RawRecord::Malformed => match find_entry_magic(&mut file, cursor + 1)? {
                    Some(next) => cursor = next,
                    None => break,
                },
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

        Ok(Self {
            file,
            audit_key,
            prev_hmac,
            sequence_no,
            entry_count,
            config,
            path: path.to_path_buf(),
            file_id,
            version: header.version,
            created_at: header.created_at,
        })
    }

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

    /// Explicit v1 -> v2 header upgrade (part of Database::upgrade_format).
    /// Every v1 file chains from zeros, so the v2 write-once seed is zeros;
    /// one-way: released binaries reject v2 headers afterwards.
    pub(crate) fn upgrade_to_v2(&mut self) -> citadel_core::Result<bool> {
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
        let pos = self.file.stream_position()?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header.serialize())?;
        self.file.sync_data()?;
        self.file.seek(SeekFrom::Start(pos))?;
        // Flip in memory only after the durable write, so a failed upgrade
        // stays retryable.
        self.version = AUDIT_LOG_VERSION;
        Ok(true)
    }

    fn update_header(&mut self) -> citadel_core::Result<()> {
        let pos = self.file.stream_position()?;

        // Only entry_count; bytes 32..64 (v2 seed / v1 vestigial tip) must
        // stay as written at create/rotate.
        self.file.seek(SeekFrom::Start(24))?;
        self.file.write_all(&self.entry_count.to_le_bytes())?;
        self.file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    fn rotate_if_needed(&mut self) -> citadel_core::Result<()> {
        let file_size = self.file.seek(SeekFrom::End(0))?;
        if file_size < self.config.max_file_size {
            return Ok(());
        }

        self.file.sync_data()?;

        // Shift rotated files: .N -> delete, .N-1 -> .N, ..., current -> .1
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

        // v1 successors restart the chain from zeros (staying v1-openable);
        // v2 carries the previous file's tip as the write-once seed. Sequence
        // numbers continue either way, so a dropped generation still shows.
        if self.version == AUDIT_LOG_VERSION_LEGACY {
            self.prev_hmac = [0u8; MAC_SIZE];
        }
        let created_at = now_nanos();
        let header = AuditHeader {
            magic: AUDIT_LOG_MAGIC,
            version: self.version,
            file_id: self.file_id,
            created_at,
            entry_count: 0,
            chain_seed: self.prev_hmac,
        };
        self.file = create_header_file(&self.path, &header)?;
        self.entry_count = 0;
        self.created_at = created_at;

        Ok(())
    }
}

fn rotated_path(base: &Path, index: u32) -> PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

pub(crate) fn resolve_audit_path(data_path: &Path) -> PathBuf {
    let mut name = data_path.as_os_str().to_os_string();
    name.push(".citadel-audit");
    PathBuf::from(name)
}

/// Read all entries from an audit log file (no key needed). Resyncs past
/// unparseable regions like open_existing does, so entries the writer
/// preserved after mid-file garbage are still returned.
pub fn read_audit_log(path: &Path) -> citadel_core::Result<Vec<AuditEntry>> {
    let mut file = File::open(path)?;

    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let _header = AuditHeader::deserialize(&header_buf)?;

    let mut entries = Vec::new();
    let mut cursor = AUDIT_HEADER_SIZE as u64;

    loop {
        file.seek(SeekFrom::Start(cursor))?;
        match read_raw_record(&mut file, cursor)? {
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
            RawRecord::Malformed => match find_entry_magic(&mut file, cursor + 1)? {
                Some(next) => cursor = next,
                None => break,
            },
        }
    }

    Ok(entries)
}

/// Verify the HMAC chain. Garbage followed by more parseable records means
/// records were damaged or excised mid-file (chain reported broken); a
/// trailing torn fragment alone is a benign incomplete write.
pub fn verify_audit_log(
    path: &Path,
    audit_key: &[u8; KEY_SIZE],
) -> citadel_core::Result<AuditVerifyResult> {
    let mut file = File::open(path)?;

    let mut header_buf = [0u8; AUDIT_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let header = AuditHeader::deserialize(&header_buf)?;

    // Rotated files chain their first entry from the previous file's tip,
    // recorded in the header at rotation; first-generation and legacy v1
    // files seed zeros.
    let mut prev_hmac = header.effective_chain_seed();
    let mut entries_verified = 0u64;
    let mut last_seq = 0u64;
    let mut cursor = AUDIT_HEADER_SIZE as u64;

    loop {
        file.seek(SeekFrom::Start(cursor))?;
        match read_raw_record(&mut file, cursor)? {
            RawRecord::Parsed(rec) => {
                let expected = compute_entry_hmac(audit_key, &prev_hmac, &rec.hmac_input);
                if rec.hmac != expected {
                    return Ok(AuditVerifyResult {
                        entries_verified,
                        chain_valid: false,
                        chain_break_at: Some(rec.sequence_no),
                    });
                }
                prev_hmac = rec.hmac;
                entries_verified += 1;
                last_seq = rec.sequence_no;
                cursor = rec.end;
            }
            RawRecord::Malformed => {
                let mut probe = cursor + 1;
                loop {
                    match find_entry_magic(&mut file, probe)? {
                        None => {
                            // Clean EOF or a trailing torn fragment.
                            return Ok(AuditVerifyResult {
                                entries_verified,
                                chain_valid: true,
                                chain_break_at: None,
                            });
                        }
                        Some(next) => {
                            file.seek(SeekFrom::Start(next))?;
                            if matches!(read_raw_record(&mut file, next)?, RawRecord::Parsed(_)) {
                                return Ok(AuditVerifyResult {
                                    entries_verified,
                                    chain_valid: false,
                                    chain_break_at: Some(last_seq + 1),
                                });
                            }
                            probe = next + 1;
                        }
                    }
                }
            }
        }
    }
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
            chain_seed: [0xAB; MAC_SIZE],
        };
        let buf = header.serialize();
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

        let mut log = AuditLog::open_existing(&path, 123, key, AuditConfig::default()).unwrap();
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

        let log = AuditLog::open_existing(&path, 123, key, AuditConfig::default()).unwrap();
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

        let mut log = AuditLog::open_existing(&path, 7, key, AuditConfig::default()).unwrap();
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

    /// upgrade_to_v2 converts the header in place (seed zeros, count and
    /// created_at preserved), verifies afterwards, and is idempotent.
    #[test]
    fn legacy_v1_upgrade_to_v2_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.citadel-audit");
        let key = [0x42u8; KEY_SIZE];
        write_legacy_v1_file(&path, 7, &key, 3);

        let mut log = AuditLog::open_existing(&path, 7, key, AuditConfig::default()).unwrap();
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
        let mut log = AuditLog::open_existing(&path, 7, key, config).unwrap();
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

        let mut log = AuditLog::open_existing(&path, 123, key, AuditConfig::default()).unwrap();
        log.log(AuditEventType::DatabaseOpened, &[]).unwrap();
        drop(log);

        let result = verify_audit_log(&path, &key).unwrap();
        assert!(result.chain_valid);
        assert_eq!(result.entries_verified, 1);
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

            let mut log = AuditLog::open_existing(&path, 123, key, AuditConfig::default()).unwrap();
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
