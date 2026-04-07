use citadel_core::types::{PageId, TxnId};
use citadel_core::{Error, Result};
use citadel_core::{
    COMMIT_SLOT_OFFSET, COMMIT_SLOT_SIZE, FILE_HEADER_SIZE, FILE_ID_OFFSET, FORMAT_VERSION,
    GOD_BIT_ACTIVE_SLOT, GOD_BIT_RECOVERY, GOD_BYTE_OFFSET, GROWTH_CHUNK_16MB, GROWTH_CHUNK_1MB,
    GROWTH_CHUNK_4MB, GROWTH_THRESHOLD_1GB, GROWTH_THRESHOLD_4MB, GROWTH_THRESHOLD_64MB, MAC_SIZE,
    MAGIC, MERKLE_HASH_SIZE, PAGE_SIZE, SLOT_CATALOG_ROOT, SLOT_CHECKSUM, SLOT_DEK_ID,
    SLOT_ENCRYPTION_EPOCH, SLOT_HIGH_WATER_MARK, SLOT_MERKLE_ROOT, SLOT_NAMED_ENTRIES,
    SLOT_NAMED_ENTRY_SIZE, SLOT_NAMED_MAX_ENTRIES, SLOT_PENDING_FREE_ROOT, SLOT_TOTAL_PAGES,
    SLOT_TREE_DEPTH, SLOT_TREE_ENTRIES, SLOT_TREE_ROOT, SLOT_TXN_ID,
};

use crate::traits::PageIO;

#[derive(Debug, Clone, Default)]
pub struct CommitSlot {
    pub txn_id: TxnId,
    pub tree_root: PageId,
    pub tree_depth: u16,
    pub tree_entries: u64,
    pub catalog_root: PageId,
    pub total_pages: u32,
    pub high_water_mark: u32,
    pub pending_free_root: PageId,
    pub encryption_epoch: u32,
    pub dek_id: [u8; MAC_SIZE],
    pub checksum: u64,
    pub merkle_root: [u8; MERKLE_HASH_SIZE],
    pub named_table_entries: Vec<(u32, u64)>,
}

impl CommitSlot {
    pub fn serialize(&self) -> [u8; COMMIT_SLOT_SIZE] {
        let mut buf = [0u8; COMMIT_SLOT_SIZE];
        buf[SLOT_TXN_ID..SLOT_TXN_ID + 8].copy_from_slice(&self.txn_id.as_u64().to_le_bytes());
        buf[SLOT_TREE_ROOT..SLOT_TREE_ROOT + 4]
            .copy_from_slice(&self.tree_root.as_u32().to_le_bytes());
        buf[SLOT_TREE_DEPTH..SLOT_TREE_DEPTH + 2].copy_from_slice(&self.tree_depth.to_le_bytes());
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

        let n = self.named_table_entries.len().min(SLOT_NAMED_MAX_ENTRIES);
        buf[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2].copy_from_slice(&(n as u16).to_le_bytes());
        for (i, &(hash, count)) in self.named_table_entries.iter().take(n).enumerate() {
            let off = SLOT_NAMED_ENTRIES + 2 + i * SLOT_NAMED_ENTRY_SIZE;
            buf[off..off + 4].copy_from_slice(&hash.to_le_bytes());
            buf[off + 4..off + 12].copy_from_slice(&count.to_le_bytes());
        }

        buf
    }

    pub fn deserialize(buf: &[u8; COMMIT_SLOT_SIZE]) -> Self {
        let mut merkle_root = [0u8; MERKLE_HASH_SIZE];
        merkle_root.copy_from_slice(&buf[SLOT_MERKLE_ROOT..SLOT_MERKLE_ROOT + MERKLE_HASH_SIZE]);

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
                let n = u16::from_le_bytes(
                    buf[SLOT_NAMED_ENTRIES..SLOT_NAMED_ENTRIES + 2]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let n = n.min(SLOT_NAMED_MAX_ENTRIES);
                let mut entries = Vec::with_capacity(n);
                for i in 0..n {
                    let off = SLOT_NAMED_ENTRIES + 2 + i * SLOT_NAMED_ENTRY_SIZE;
                    let hash = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                    let count = u64::from_le_bytes(buf[off + 4..off + 12].try_into().unwrap());
                    entries.push((hash, count));
                }
                entries
            },
        }
    }

    pub fn verify_checksum(&self) -> bool {
        let buf = self.serialize();
        let computed = xxhash_rust::xxh64::xxh64(&buf[..SLOT_CHECKSUM], 0);
        self.checksum == computed
    }

    pub fn named_entry_count(&self, name: &[u8]) -> Option<u64> {
        let h = table_name_hash(name);
        self.named_table_entries
            .iter()
            .find(|&&(hash, _)| hash == h)
            .map(|&(_, count)| count)
    }
}

pub fn table_name_hash(name: &[u8]) -> u32 {
    xxhash_rust::xxh64::xxh64(name, 0x7461626C) as u32
}

pub struct FileHeader {
    pub magic: u32,
    pub format_version: u32,
    pub page_size: u32,
    pub body_size: u32,
    pub min_reader_ver: u16,
    pub min_writer_ver: u16,
    pub god_byte: u8,
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
            page_size: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            body_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            min_reader_ver: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
            min_writer_ver: u16::from_le_bytes(buf[18..20].try_into().unwrap()),
            god_byte: buf[GOD_BYTE_OFFSET],
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
            tree_entries: 0,
            catalog_root: PageId::INVALID,
            total_pages: 0,
            high_water_mark: 0,
            pending_free_root: PageId::INVALID,
            encryption_epoch: 1,
            dek_id,
            checksum: 0,
            merkle_root: [0u8; MERKLE_HASH_SIZE],
            named_table_entries: Vec::new(),
        };

        Self {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            body_size: citadel_core::BODY_SIZE as u32,
            min_reader_ver: 1,
            min_writer_ver: 1,
            god_byte: 0,
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

pub fn recover(io: &dyn PageIO) -> Result<(usize, CommitSlot)> {
    let god_byte = read_god_byte(io)?;
    let active = (god_byte & GOD_BIT_ACTIVE_SLOT) as usize;
    let inactive = 1 - active;

    let slot_active = read_commit_slot(io, active)?;
    let slot_inactive = read_commit_slot(io, inactive)?;

    let active_valid = slot_active.verify_checksum();
    let inactive_valid = slot_inactive.verify_checksum();

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
mod tests {
    use super::*;

    #[test]
    fn commit_slot_serialize_roundtrip() {
        let slot = CommitSlot {
            txn_id: TxnId(42),
            tree_root: PageId(10),
            tree_depth: 3,
            tree_entries: 1000,
            catalog_root: PageId(11),
            total_pages: 100,
            high_water_mark: 99,
            pending_free_root: PageId(50),
            encryption_epoch: 1,
            dek_id: [0xAA; MAC_SIZE],
            checksum: 0,
            merkle_root: [0xBB; MERKLE_HASH_SIZE],
            named_table_entries: vec![(0x12345678, 500)],
        };

        let buf = slot.serialize();
        let slot2 = CommitSlot::deserialize(&buf);

        assert_eq!(slot2.txn_id, TxnId(42));
        assert_eq!(slot2.tree_root, PageId(10));
        assert_eq!(slot2.tree_depth, 3);
        assert_eq!(slot2.tree_entries, 1000);
        assert_eq!(slot2.catalog_root, PageId(11));
        assert_eq!(slot2.total_pages, 100);
        assert_eq!(slot2.high_water_mark, 99);
        assert_eq!(slot2.pending_free_root, PageId(50));
        assert_eq!(slot2.encryption_epoch, 1);
        assert_eq!(slot2.dek_id, [0xAA; MAC_SIZE]);
        assert_eq!(slot2.merkle_root, [0xBB; MERKLE_HASH_SIZE]);
        assert_eq!(slot2.named_table_entries, vec![(0x12345678, 500)]);
    }

    #[test]
    fn commit_slot_checksum() {
        let slot = CommitSlot {
            txn_id: TxnId(1),
            tree_root: PageId(5),
            tree_depth: 1,
            tree_entries: 10,
            catalog_root: PageId(0),
            total_pages: 5,
            high_water_mark: 4,
            pending_free_root: PageId::INVALID,
            encryption_epoch: 1,
            dek_id: [0; MAC_SIZE],
            checksum: 0,
            merkle_root: [0; MERKLE_HASH_SIZE],
            named_table_entries: Vec::new(),
        };

        let buf = slot.serialize();
        let slot2 = CommitSlot::deserialize(&buf);
        assert!(slot2.verify_checksum());

        let mut tampered = buf;
        tampered[0] ^= 0x01;
        let slot3 = CommitSlot::deserialize(&tampered);
        assert!(!slot3.verify_checksum());
    }

    #[test]
    fn file_header_serialize_roundtrip() {
        let dek_id = [0xBB; MAC_SIZE];
        let header = FileHeader::new(0x1234, dek_id);

        let buf = header.serialize();
        let header2 = FileHeader::deserialize(&buf).unwrap();

        assert_eq!(header2.magic, MAGIC);
        assert_eq!(header2.format_version, FORMAT_VERSION);
        assert_eq!(header2.page_size, PAGE_SIZE as u32);
        assert_eq!(header2.file_id, 0x1234);
        assert_eq!(header2.god_byte, 0);
        assert_eq!(header2.active_slot(), 0);
        assert!(!header2.recovery_required());
    }

    #[test]
    fn file_header_invalid_magic() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let result = FileHeader::deserialize(&buf);
        assert!(matches!(result, Err(Error::InvalidMagic { .. })));
    }

    #[test]
    fn god_byte_active_slot() {
        let mut header = FileHeader::new(0, [0; MAC_SIZE]);
        assert_eq!(header.active_slot(), 0);
        assert_eq!(header.inactive_slot(), 1);

        header.god_byte = 0x01; // active = slot 1
        assert_eq!(header.active_slot(), 1);
        assert_eq!(header.inactive_slot(), 0);
    }

    #[test]
    fn god_byte_recovery_flag() {
        let mut header = FileHeader::new(0, [0; MAC_SIZE]);
        assert!(!header.recovery_required());

        header.god_byte = GOD_BIT_RECOVERY; // recovery + slot 0
        assert!(header.recovery_required());
        assert_eq!(header.active_slot(), 0);

        header.god_byte = GOD_BIT_RECOVERY | GOD_BIT_ACTIVE_SLOT; // recovery + slot 1
        assert!(header.recovery_required());
        assert_eq!(header.active_slot(), 1);
    }

    #[test]
    fn page_offset_calculation() {
        assert_eq!(page_offset(PageId(0)), FILE_HEADER_SIZE as u64);
        assert_eq!(
            page_offset(PageId(1)),
            FILE_HEADER_SIZE as u64 + PAGE_SIZE as u64
        );
        assert_eq!(
            page_offset(PageId(10)),
            FILE_HEADER_SIZE as u64 + 10 * PAGE_SIZE as u64
        );
    }

    #[test]
    fn growth_chunk_sizes() {
        assert_eq!(growth_chunk(0), GROWTH_CHUNK_1MB);
        assert_eq!(growth_chunk(1_000_000), GROWTH_CHUNK_1MB);
        assert_eq!(growth_chunk(GROWTH_THRESHOLD_4MB), GROWTH_CHUNK_4MB);
        assert_eq!(growth_chunk(GROWTH_THRESHOLD_64MB), GROWTH_CHUNK_16MB);
        assert_eq!(growth_chunk(GROWTH_THRESHOLD_1GB), GROWTH_CHUNK_16MB);
        assert_eq!(
            growth_chunk(10 * GROWTH_THRESHOLD_1GB),
            10 * GROWTH_THRESHOLD_1GB / 100
        );
    }
}
