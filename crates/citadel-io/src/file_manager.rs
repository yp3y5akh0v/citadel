//! File header, dual commit slots, and crash recovery (shadow paging).
//!
//! # Commit-slot integrity
//!
//! Two wire formats coexist. Legacy (pre-v1) is a keyless xxh64 over
//! `[0..SLOT_CHECKSUM]`. V1 keeps that checksum (pre-v1 binaries still open the
//! file) and adds a truncated HMAC-SHA256 over the rest, so tampering with
//! `merkle_root` or the named entries fails [`recover`]. Non-goals: (1)
//! downgrade resistance holds only after a file earns a V1 requirement (the
//! compatibility header bit here, or an authenticated requirement supplied by
//! the facade from its key file) - before that the MAC catches corruption but
//! not a slot re-encoded whole as legacy; (2) no anti-rollback - reverting a
//! complete authentic data/key-file set needs an external freshness anchor.
use citadel_core::types::{PageId, TxnId};
use citadel_core::{Error, Result};
use citadel_core::{
    COMMIT_SLOT_OFFSET, COMMIT_SLOT_SIZE, FILE_HEADER_SIZE, FILE_ID_OFFSET, FORMAT_VERSION,
    GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY, GOD_BYTE_OFFSET, GROWTH_CHUNK_16MB, GROWTH_CHUNK_1MB,
    GROWTH_CHUNK_4MB, GROWTH_THRESHOLD_1GB, GROWTH_THRESHOLD_4MB, GROWTH_THRESHOLD_64MB,
    HEADER_FLAGS_OFFSET, HEADER_FLAG_SLOTS_V1, MAC_KEY_SIZE, MAC_SIZE, MAGIC, MERKLE_HASH_SIZE,
    PAGE_SIZE, SLOT_CATALOG_ROOT, SLOT_CHECKSUM, SLOT_DEK_ID, SLOT_ENCRYPTION_EPOCH,
    SLOT_ENTRY_STALE, SLOT_FORMAT_MARKER, SLOT_HIGH_WATER_MARK, SLOT_MAC, SLOT_MAC_DOMAIN,
    SLOT_MAC_SIZE, SLOT_MARKER_V1, SLOT_MERKLE_ROOT, SLOT_MERKLE_SCHEME,
    SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1, SLOT_NAMED_ENTRIES, SLOT_NAMED_ENTRY_SIZE,
    SLOT_NAMED_MAX_ENTRIES, SLOT_NAMED_MAX_ENTRIES_V1, SLOT_PENDING_FREE_ROOT, SLOT_TOTAL_PAGES,
    SLOT_TREE_DEPTH, SLOT_TREE_ENTRIES, SLOT_TREE_ROOT, SLOT_TXN_ID,
};

use crate::traits::PageIO;

// Canonical access requirements encoded by file format v1. Changing either
// value requires a format-version bump rather than redefining v1 in place.
const FORMAT_V1_MIN_READER_VERSION: u16 = 1;
const FORMAT_V1_MIN_WRITER_VERSION: u16 = 1;

/// Wire format of a commit slot; see the module doc for the boundary each
/// format enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlotFormat {
    /// Pre-v1: keyless xxh64, no MAC. Default so an unsealed slot can never
    /// carry a MAC that fails on reopen.
    #[default]
    Legacy,
    /// Marker + truncated HMAC-SHA256 tail; at most
    /// SLOT_NAMED_MAX_ENTRIES_V1 named entries.
    V1,
    /// Unrecognized marker (corruption or a future format); never verifies.
    Unknown,
}

/// Merkle hash semantics used by one commit generation.
///
/// The marker is per slot rather than global: released writers rewrite a slot
/// from a zeroed buffer, so any later old-writer commit automatically records
/// `Legacy` instead of accidentally preserving a newer scheme declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MerkleScheme {
    /// Released scheme: overflow leaf values hash their physical reference.
    #[default]
    Legacy,
    /// Overflow leaf values hash their logical length and payload digest.
    LogicalOverflowV1,
    /// Unrecognized marker (corruption or a future unsupported scheme).
    Unknown,
}

impl MerkleScheme {
    fn from_marker(marker: u16) -> Self {
        match marker {
            0 => Self::Legacy,
            SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1 => Self::LogicalOverflowV1,
            _ => Self::Unknown,
        }
    }

    fn marker(self) -> u16 {
        match self {
            Self::Legacy => 0,
            Self::LogicalOverflowV1 => SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1,
            // Keep Unknown fail-closed even if a caller tries to serialize it.
            Self::Unknown => u16::MAX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommitSlot {
    pub txn_id: TxnId,
    pub tree_root: PageId,
    pub tree_depth: u16,
    pub merkle_scheme: MerkleScheme,
    pub tree_entries: u64,
    pub catalog_root: PageId,
    pub total_pages: u32,
    pub high_water_mark: u32,
    pub pending_free_root: PageId,
    pub encryption_epoch: u32,
    pub dek_id: [u8; MAC_SIZE],
    pub checksum: u64,
    pub merkle_root: [u8; MERKLE_HASH_SIZE],
    /// (hash, entry_count, root_page, depth) per table
    pub named_table_entries: Vec<(u32, u64, u32, u16)>,
    pub slot_format: SlotFormat,
    pub slot_mac: [u8; SLOT_MAC_SIZE],
}

fn compute_slot_mac(mac_key: &[u8; MAC_KEY_SIZE], data: &[u8]) -> [u8; SLOT_MAC_SIZE] {
    use hmac::{Hmac, Mac};
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(mac_key).expect("HMAC accepts any key length");
    mac.update(SLOT_MAC_DOMAIN);
    mac.update(data);
    let full = mac.finalize().into_bytes();
    let mut out = [0u8; SLOT_MAC_SIZE];
    out.copy_from_slice(&full[..SLOT_MAC_SIZE]);
    out
}

fn mac_eq(a: &[u8; SLOT_MAC_SIZE], b: &[u8; SLOT_MAC_SIZE]) -> bool {
    // subtle carries the optimization barrier a hand-rolled fold lacks, and
    // matches every other MAC-verification site in the workspace.
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

fn named_entry_capacity(format: SlotFormat) -> usize {
    match format {
        SlotFormat::Legacy => SLOT_NAMED_MAX_ENTRIES,
        SlotFormat::V1 | SlotFormat::Unknown => SLOT_NAMED_MAX_ENTRIES_V1,
    }
}

impl CommitSlot {
    /// Pick the wire format and compute the MAC; must run before any slot is
    /// written. Entries over the v1 capacity fall back to legacy (no room for
    /// the MAC once the tail holds the last entry) rather than drop an entry
    /// that may be a table root's sole record.
    pub fn seal(&mut self, mac_key: &[u8; MAC_KEY_SIZE]) {
        if self.named_table_entries.len() <= SLOT_NAMED_MAX_ENTRIES_V1 {
            self.slot_format = SlotFormat::V1;
            let buf = self.serialize();
            self.slot_mac = compute_slot_mac(mac_key, &buf[..SLOT_MAC]);
        } else {
            self.slot_format = SlotFormat::Legacy;
            self.slot_mac = [0u8; SLOT_MAC_SIZE];
        }
    }

    /// Serialize to the fixed on-disk slot layout. Only the first
    /// `named_entry_capacity` entries survive; the txn layer orders stale
    /// (sole-record) entries first so trimming only drops catalog-backed
    /// cache entries.
    pub fn serialize(&self) -> [u8; COMMIT_SLOT_SIZE] {
        let mut buf = [0u8; COMMIT_SLOT_SIZE];
        buf[SLOT_TXN_ID..SLOT_TXN_ID + 8].copy_from_slice(&self.txn_id.as_u64().to_le_bytes());
        buf[SLOT_TREE_ROOT..SLOT_TREE_ROOT + 4]
            .copy_from_slice(&self.tree_root.as_u32().to_le_bytes());
        buf[SLOT_TREE_DEPTH..SLOT_TREE_DEPTH + 2].copy_from_slice(&self.tree_depth.to_le_bytes());
        buf[SLOT_MERKLE_SCHEME..SLOT_MERKLE_SCHEME + 2]
            .copy_from_slice(&self.merkle_scheme.marker().to_le_bytes());
        buf[SLOT_TREE_ENTRIES..SLOT_TREE_ENTRIES + 8]
            .copy_from_slice(&self.tree_entries.to_le_bytes());
        buf[SLOT_CATALOG_ROOT..SLOT_CATALOG_ROOT + 4]
            .copy_from_slice(&self.catalog_root.as_u32().to_le_bytes());
        buf[SLOT_TOTAL_PAGES..SLOT_TOTAL_PAGES + 4]
            .copy_from_slice(&self.total_pages.to_le_bytes());
        buf[SLOT_HIGH_WATER_MARK..SLOT_HIGH_WATER_MARK + 4]
            .copy_from_slice(&self.high_water_mark.to_le_bytes());
        buf[SLOT_PENDING_FREE_ROOT..SLOT_PENDING_FREE_ROOT + 4]
            .copy_from_slice(&self.pending_free_root.as_u32().to_le_bytes());
        buf[SLOT_ENCRYPTION_EPOCH..SLOT_ENCRYPTION_EPOCH + 4]
            .copy_from_slice(&self.encryption_epoch.to_le_bytes());
        buf[SLOT_DEK_ID..SLOT_DEK_ID + MAC_SIZE].copy_from_slice(&self.dek_id);

        let cs = xxhash_rust::xxh64::xxh64(&buf[..SLOT_CHECKSUM], 0);
        buf[SLOT_CHECKSUM..SLOT_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
        buf[SLOT_MERKLE_ROOT..SLOT_MERKLE_ROOT + MERKLE_HASH_SIZE]
            .copy_from_slice(&self.merkle_root);

        let n = self
            .named_table_entries
            .len()
            .min(named_entry_capacity(self.slot_format));
        buf[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2].copy_from_slice(&(n as u16).to_le_bytes());
        for (i, &(hash, count, root, depth)) in self.named_table_entries.iter().take(n).enumerate()
        {
            // Strip the flag on the legacy wire so a pre-v1 binary reads a
            // clean count (legacy entries are all stale anyway).
            let count = if self.slot_format == SlotFormat::V1 {
                count
            } else {
                count & !SLOT_ENTRY_STALE
            };
            let off = SLOT_NAMED_ENTRIES + 2 + i * SLOT_NAMED_ENTRY_SIZE;
            buf[off..off + 4].copy_from_slice(&hash.to_le_bytes());
            buf[off + 4..off + 12].copy_from_slice(&count.to_le_bytes());
            buf[off + 12..off + 16].copy_from_slice(&root.to_le_bytes());
            buf[off + 16..off + 18].copy_from_slice(&depth.to_le_bytes());
        }

        if self.slot_format == SlotFormat::V1 {
            buf[SLOT_FORMAT_MARKER..SLOT_FORMAT_MARKER + 2]
                .copy_from_slice(&SLOT_MARKER_V1.to_le_bytes());
            buf[SLOT_MAC..SLOT_MAC + SLOT_MAC_SIZE].copy_from_slice(&self.slot_mac);
        }

        buf
    }

    pub fn deserialize(buf: &[u8; COMMIT_SLOT_SIZE]) -> Self {
        let mut merkle_root = [0u8; MERKLE_HASH_SIZE];
        merkle_root.copy_from_slice(&buf[SLOT_MERKLE_ROOT..SLOT_MERKLE_ROOT + MERKLE_HASH_SIZE]);

        let named_count = u16::from_le_bytes(
            buf[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let marker = u16::from_le_bytes(
            buf[SLOT_FORMAT_MARKER..SLOT_FORMAT_MARKER + 2]
                .try_into()
                .unwrap(),
        );
        // A legacy-full count reads as Legacy even if the 7th entry's hash
        // bytes collide with the v1 marker: both slots carry those entries,
        // so rejecting the collision would brick the file, and flagged files
        // reject non-V1 slots in recover() anyway.
        let (slot_format, slot_mac) = if named_count > SLOT_NAMED_MAX_ENTRIES {
            (SlotFormat::Unknown, [0u8; SLOT_MAC_SIZE])
        } else if named_count > SLOT_NAMED_MAX_ENTRIES_V1 {
            (SlotFormat::Legacy, [0u8; SLOT_MAC_SIZE])
        } else {
            match marker {
                0 => (SlotFormat::Legacy, [0u8; SLOT_MAC_SIZE]),
                SLOT_MARKER_V1 => (
                    SlotFormat::V1,
                    buf[SLOT_MAC..SLOT_MAC + SLOT_MAC_SIZE].try_into().unwrap(),
                ),
                _ => (SlotFormat::Unknown, [0u8; SLOT_MAC_SIZE]),
            }
        };

        Self {
            txn_id: TxnId(u64::from_le_bytes(
                buf[SLOT_TXN_ID..SLOT_TXN_ID + 8].try_into().unwrap(),
            )),
            tree_root: PageId(u32::from_le_bytes(
                buf[SLOT_TREE_ROOT..SLOT_TREE_ROOT + 4].try_into().unwrap(),
            )),
            tree_depth: u16::from_le_bytes(
                buf[SLOT_TREE_DEPTH..SLOT_TREE_DEPTH + 2]
                    .try_into()
                    .unwrap(),
            ),
            merkle_scheme: MerkleScheme::from_marker(u16::from_le_bytes(
                buf[SLOT_MERKLE_SCHEME..SLOT_MERKLE_SCHEME + 2]
                    .try_into()
                    .unwrap(),
            )),
            tree_entries: u64::from_le_bytes(
                buf[SLOT_TREE_ENTRIES..SLOT_TREE_ENTRIES + 8]
                    .try_into()
                    .unwrap(),
            ),
            catalog_root: PageId(u32::from_le_bytes(
                buf[SLOT_CATALOG_ROOT..SLOT_CATALOG_ROOT + 4]
                    .try_into()
                    .unwrap(),
            )),
            total_pages: u32::from_le_bytes(
                buf[SLOT_TOTAL_PAGES..SLOT_TOTAL_PAGES + 4]
                    .try_into()
                    .unwrap(),
            ),
            high_water_mark: u32::from_le_bytes(
                buf[SLOT_HIGH_WATER_MARK..SLOT_HIGH_WATER_MARK + 4]
                    .try_into()
                    .unwrap(),
            ),
            pending_free_root: PageId(u32::from_le_bytes(
                buf[SLOT_PENDING_FREE_ROOT..SLOT_PENDING_FREE_ROOT + 4]
                    .try_into()
                    .unwrap(),
            )),
            encryption_epoch: u32::from_le_bytes(
                buf[SLOT_ENCRYPTION_EPOCH..SLOT_ENCRYPTION_EPOCH + 4]
                    .try_into()
                    .unwrap(),
            ),
            dek_id: buf[SLOT_DEK_ID..SLOT_DEK_ID + MAC_SIZE].try_into().unwrap(),
            checksum: u64::from_le_bytes(buf[SLOT_CHECKSUM..SLOT_CHECKSUM + 8].try_into().unwrap()),
            merkle_root,
            named_table_entries: {
                let n = named_count.min(named_entry_capacity(slot_format));
                let mut entries = Vec::with_capacity(n);
                for i in 0..n {
                    let off = SLOT_NAMED_ENTRIES + 2 + i * SLOT_NAMED_ENTRY_SIZE;
                    let hash = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                    let count = u64::from_le_bytes(buf[off + 4..off + 12].try_into().unwrap());
                    let root = u32::from_le_bytes(buf[off + 12..off + 16].try_into().unwrap());
                    let depth = u16::from_le_bytes(buf[off + 16..off + 18].try_into().unwrap());
                    entries.push((hash, count, root, depth));
                }
                entries
            },
            slot_format,
            slot_mac,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        if self.slot_format == SlotFormat::Unknown || self.merkle_scheme == MerkleScheme::Unknown {
            return false;
        }
        let buf = self.serialize();
        let computed = xxhash_rust::xxh64::xxh64(&buf[..SLOT_CHECKSUM], 0);
        self.checksum == computed
    }

    /// Keyed check over the whole slot except the MAC, covering `merkle_root`
    /// and the named entries that the keyless checksum omits. Legacy slots
    /// pass vacuously so pre-v1 files keep opening (see the module doc).
    pub fn verify_mac(&self, mac_key: &[u8; MAC_KEY_SIZE]) -> bool {
        if self.merkle_scheme == MerkleScheme::Unknown {
            return false;
        }
        match self.slot_format {
            SlotFormat::Legacy => true,
            SlotFormat::Unknown => false,
            SlotFormat::V1 => {
                let buf = self.serialize();
                let expected = compute_slot_mac(mac_key, &buf[..SLOT_MAC]);
                mac_eq(&expected, &self.slot_mac)
            }
        }
    }

    /// Cached entry count for a named table, without the SLOT_ENTRY_STALE
    /// flag bit.
    pub fn named_entry_count(&self, name: &[u8]) -> Option<u64> {
        let h = table_name_hash(name);
        self.named_table_entries
            .iter()
            .find(|&&(hash, ..)| hash == h)
            .map(|&(_, count, ..)| count & !SLOT_ENTRY_STALE)
    }

    /// Cached root + depth for a named table. None if not in slot.
    pub fn named_entry_root(&self, name: &[u8]) -> Option<(PageId, u16)> {
        let h = table_name_hash(name);
        self.named_table_entries
            .iter()
            .find(|&&(hash, ..)| hash == h)
            .and_then(|&(_, _, root, depth)| {
                // (0, 0) is the absent-cache sentinel. Page zero itself is a
                // valid reclaimed B-tree root, whose depth is always nonzero.
                if root != 0 || depth != 0 {
                    Some((PageId(root), depth))
                } else {
                    None
                }
            })
    }

    /// Whether the slot is the sole durable record of this table's root.
    /// Legacy entries are all stale, but only ones the slot actually carries:
    /// a hash it does not list derives from the catalog, and minting a
    /// phantom stale entry for it would overflow the capacity and drop a real
    /// sole-record root in serialize().
    pub fn entry_is_stale(&self, hash: u32) -> bool {
        let carried = self.named_table_entries.iter().find(|&&(h, ..)| h == hash);
        match (self.slot_format, carried) {
            (SlotFormat::Legacy, Some(_)) => true,
            (SlotFormat::Legacy, None) => false,
            (_, Some(&(_, count, ..))) => count & SLOT_ENTRY_STALE != 0,
            (_, None) => false,
        }
    }
}

pub fn table_name_hash(name: &[u8]) -> u32 {
    xxhash_rust::xxh64::xxh64(name, 0x7461626C) as u32
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub magic: u32,
    pub format_version: u32,
    pub page_size: u32,
    pub body_size: u32,
    pub min_reader_ver: u16,
    pub min_writer_ver: u16,
    pub god_byte: u8,
    /// HEADER_FLAG_* bits; pre-flag files carry 0 (the byte was padding).
    pub flags: u8,
    pub file_id: u64,
    pub slots: [CommitSlot; 2],
}

impl FileHeader {
    pub fn serialize(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[12..16].copy_from_slice(&self.body_size.to_le_bytes());
        buf[16..18].copy_from_slice(&self.min_reader_ver.to_le_bytes());
        buf[18..20].copy_from_slice(&self.min_writer_ver.to_le_bytes());
        buf[GOD_BYTE_OFFSET] = self.god_byte;
        buf[HEADER_FLAGS_OFFSET] = self.flags;
        buf[FILE_ID_OFFSET..FILE_ID_OFFSET + 8].copy_from_slice(&self.file_id.to_le_bytes());

        let slot0 = self.slots[0].serialize();
        let slot1 = self.slots[1].serialize();
        buf[COMMIT_SLOT_OFFSET..COMMIT_SLOT_OFFSET + COMMIT_SLOT_SIZE].copy_from_slice(&slot0);
        buf[COMMIT_SLOT_OFFSET + COMMIT_SLOT_SIZE..COMMIT_SLOT_OFFSET + 2 * COMMIT_SLOT_SIZE]
            .copy_from_slice(&slot1);

        buf
    }

    pub fn deserialize(buf: &[u8; FILE_HEADER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::InvalidMagic {
                expected: MAGIC,
                found: magic,
            });
        }

        let format_version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if format_version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(format_version));
        }

        let page_size = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let body_size = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let min_reader_ver = u16::from_le_bytes(buf[16..18].try_into().unwrap());
        let min_writer_ver = u16::from_le_bytes(buf[18..20].try_into().unwrap());
        if page_size != PAGE_SIZE as u32
            || body_size != citadel_core::BODY_SIZE as u32
            || min_reader_ver != FORMAT_V1_MIN_READER_VERSION
            || min_writer_ver != FORMAT_V1_MIN_WRITER_VERSION
        {
            return Err(Error::DatabaseCorrupted);
        }

        let slot0_buf: [u8; COMMIT_SLOT_SIZE] = buf
            [COMMIT_SLOT_OFFSET..COMMIT_SLOT_OFFSET + COMMIT_SLOT_SIZE]
            .try_into()
            .unwrap();
        let slot1_buf: [u8; COMMIT_SLOT_SIZE] = buf
            [COMMIT_SLOT_OFFSET + COMMIT_SLOT_SIZE..COMMIT_SLOT_OFFSET + 2 * COMMIT_SLOT_SIZE]
            .try_into()
            .unwrap();

        Ok(Self {
            magic,
            format_version,
            page_size,
            body_size,
            min_reader_ver,
            min_writer_ver,
            god_byte: buf[GOD_BYTE_OFFSET],
            flags: buf[HEADER_FLAGS_OFFSET],
            file_id: u64::from_le_bytes(
                buf[FILE_ID_OFFSET..FILE_ID_OFFSET + 8].try_into().unwrap(),
            ),
            slots: [
                CommitSlot::deserialize(&slot0_buf),
                CommitSlot::deserialize(&slot1_buf),
            ],
        })
    }

    pub fn new(file_id: u64, dek_id: [u8; MAC_SIZE]) -> Self {
        let slot = CommitSlot {
            txn_id: TxnId(0),
            tree_root: PageId(0),
            tree_depth: 0,
            merkle_scheme: MerkleScheme::LogicalOverflowV1,
            tree_entries: 0,
            catalog_root: PageId::INVALID,
            total_pages: 0,
            high_water_mark: 0,
            pending_free_root: PageId::INVALID,
            encryption_epoch: 1,
            dek_id,
            merkle_root: [0u8; MERKLE_HASH_SIZE],
            ..Default::default()
        };

        Self {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            body_size: citadel_core::BODY_SIZE as u32,
            min_reader_ver: FORMAT_V1_MIN_READER_VERSION,
            min_writer_ver: FORMAT_V1_MIN_WRITER_VERSION,
            god_byte: 0,
            // New files only ever write sealed V1 slots.
            flags: HEADER_FLAG_SLOTS_V1,
            file_id,
            slots: [slot.clone(), slot],
        }
    }

    #[inline]
    pub fn active_slot(&self) -> usize {
        (self.god_byte & GOD_BIT_ACTIVE_SLOT) as usize
    }

    #[inline]
    pub fn inactive_slot(&self) -> usize {
        1 - self.active_slot()
    }

    #[inline]
    pub fn recovery_required(&self) -> bool {
        self.god_byte & GOD_BIT_RECOVERY != 0
    }
}

pub fn read_god_byte(io: &dyn PageIO) -> Result<u8> {
    let mut buf = [0u8; 1];
    io.read_at(GOD_BYTE_OFFSET as u64, &mut buf)?;
    Ok(buf[0])
}

pub fn write_god_byte(io: &dyn PageIO, value: u8) -> Result<()> {
    io.write_at(GOD_BYTE_OFFSET as u64, &[value])
}

pub fn read_file_header(io: &dyn PageIO) -> Result<FileHeader> {
    let mut buf = [0u8; FILE_HEADER_SIZE];
    io.read_at(0, &mut buf)?;
    FileHeader::deserialize(&buf)
}

pub fn write_file_header(io: &dyn PageIO, header: &FileHeader) -> Result<()> {
    let buf = header.serialize();
    io.write_at(0, &buf)
}

pub fn write_commit_slot(io: &dyn PageIO, slot_index: usize, slot: &CommitSlot) -> Result<()> {
    let offset = COMMIT_SLOT_OFFSET + slot_index * COMMIT_SLOT_SIZE;
    let buf = slot.serialize();
    io.write_at(offset as u64, &buf)
}

pub fn read_commit_slot(io: &dyn PageIO, slot_index: usize) -> Result<CommitSlot> {
    let offset = COMMIT_SLOT_OFFSET + slot_index * COMMIT_SLOT_SIZE;
    let mut buf = [0u8; COMMIT_SLOT_SIZE];
    io.read_at(offset as u64, &mut buf)?;
    Ok(CommitSlot::deserialize(&buf))
}

#[inline]
pub fn page_offset(page_id: PageId) -> u64 {
    FILE_HEADER_SIZE as u64 + page_id.as_u32() as u64 * PAGE_SIZE as u64
}

pub fn read_header_flags(io: &dyn PageIO) -> Result<u8> {
    let mut buf = [0u8; 1];
    io.read_at(HEADER_FLAGS_OFFSET as u64, &mut buf)?;
    Ok(buf[0])
}

/// Stamp HEADER_FLAG_SLOTS_V1 (one-way) once both slots are checksum-valid,
/// authenticated V1 records, so legacy slots are rejected thereafter. Returns
/// whether the flag is set. A lost write just re-runs on the next open.
pub fn mark_slots_v1_if_upgraded(io: &dyn PageIO, mac_key: &[u8; MAC_KEY_SIZE]) -> Result<bool> {
    let header = read_file_header(io)?;
    if header.flags & HEADER_FLAG_SLOTS_V1 != 0 {
        return Ok(true);
    }
    let both_v1 = header.slots.iter().all(|slot| {
        slot.slot_format == SlotFormat::V1 && slot.verify_checksum() && slot.verify_mac(mac_key)
    });
    if both_v1 {
        io.write_at(
            HEADER_FLAGS_OFFSET as u64,
            &[header.flags | HEADER_FLAG_SLOTS_V1],
        )?;
        io.fsync()?;
    }
    Ok(both_v1)
}

pub fn recover(io: &dyn PageIO, mac_key: &[u8; MAC_KEY_SIZE]) -> Result<(usize, CommitSlot)> {
    recover_with_v1_requirement(io, mac_key, false)
}

/// Recover with an optional requirement authenticated outside the mutable
/// data header (currently the facade's MAC-covered key-file marker).
///
/// The legacy [`recover`] entry point remains header-only for lower-level
/// callers that manage no key file. A protected caller must never retry that
/// path after this one rejects a downgrade.
pub fn recover_with_v1_requirement(
    io: &dyn PageIO,
    mac_key: &[u8; MAC_KEY_SIZE],
    authenticated_v1_required: bool,
) -> Result<(usize, CommitSlot)> {
    // Read and validate one coherent header image before recovery can mutate
    // the selector byte. This also protects lower-level TxnManager callers
    // that do not pass through the facade's header check.
    let header = read_file_header(io)?;
    let god_byte = header.god_byte;
    let active = (god_byte & GOD_BIT_ACTIVE_SLOT) as usize;
    let inactive = 1 - active;

    let slot_active = header.slots[active].clone();
    let slot_inactive = header.slots[inactive].clone();

    // A checksum-valid legacy slot in a flagged file is downgrade evidence
    // (rollback or a pre-v1 binary wrote it). Refuse loudly rather than
    // silently open the older generation.
    let v1_required = authenticated_v1_required || header.flags & HEADER_FLAG_SLOTS_V1 != 0;
    if v1_required {
        for slot in [&slot_active, &slot_inactive] {
            if slot.slot_format == SlotFormat::Legacy && slot.verify_checksum() {
                return Err(Error::SlotDowngradeDetected);
            }
        }
    }
    let format_ok = |slot: &CommitSlot| {
        slot.merkle_scheme != MerkleScheme::Unknown
            && (!v1_required || slot.slot_format == SlotFormat::V1)
    };

    let active_valid =
        format_ok(&slot_active) && slot_active.verify_checksum() && slot_active.verify_mac(mac_key);
    let inactive_valid = format_ok(&slot_inactive)
        && slot_inactive.verify_checksum()
        && slot_inactive.verify_mac(mac_key);

    let (chosen_slot_idx, chosen_slot) = match (active_valid, inactive_valid) {
        (true, _) => (active, slot_active),
        (false, true) => (inactive, slot_inactive),
        (false, false) => return Err(Error::DatabaseCorrupted),
    };

    if chosen_slot.high_water_mark > 0 {
        if chosen_slot.tree_root.as_u32() > 0
            && chosen_slot.tree_root.as_u32() >= chosen_slot.high_water_mark
        {
            return Err(Error::PageOutOfBounds(chosen_slot.tree_root));
        }
        if chosen_slot.pending_free_root != PageId::INVALID
            && chosen_slot.pending_free_root.as_u32() >= chosen_slot.high_water_mark
        {
            return Err(Error::PageOutOfBounds(chosen_slot.pending_free_root));
        }
    }

    if god_byte & GOD_BIT_RECOVERY != 0 {
        let new_god_byte = (chosen_slot_idx as u8) & GOD_BIT_ACTIVE_SLOT; // clear bit 1
        write_god_byte(io, new_god_byte)?;
        io.fsync()?;
    }

    Ok((chosen_slot_idx, chosen_slot))
}

pub fn growth_chunk(current_size: u64) -> u64 {
    if current_size < GROWTH_THRESHOLD_4MB {
        GROWTH_CHUNK_1MB
    } else if current_size < GROWTH_THRESHOLD_64MB {
        GROWTH_CHUNK_4MB
    } else if current_size < GROWTH_THRESHOLD_1GB {
        GROWTH_CHUNK_16MB
    } else {
        std::cmp::max(GROWTH_CHUNK_16MB, current_size / 100)
    }
}

pub fn ensure_file_size(io: &dyn PageIO, needed_offset: u64) -> Result<()> {
    let current_size = io.file_size()?;
    let needed_size = needed_offset + PAGE_SIZE as u64;
    if current_size >= needed_size {
        return Ok(());
    }
    let chunk = growth_chunk(current_size);
    let new_size = std::cmp::max(needed_size, current_size + chunk);
    io.truncate(new_size)
}

#[cfg(test)]
#[path = "file_manager_tests.rs"]
mod tests;
