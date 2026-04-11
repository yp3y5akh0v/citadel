// File magic number: 0xC17AD3EL ("CITADEL" without vowels)
pub const MAGIC: u32 = 0xC17A_D3E1;
pub const FORMAT_VERSION: u32 = 1;

// Key file magic: "KEYS" in ASCII
pub const KEY_FILE_MAGIC: u32 = 0x4B45_5953;
pub const KEY_FILE_VERSION: u32 = 1;

// On-disk page sizes
pub const PAGE_SIZE: usize = 8208; // IV(16) + ciphertext(8160) + MAC(32)
pub const BODY_SIZE: usize = 8160; // decrypted page body
pub const IV_SIZE: usize = 16;
pub const MAC_SIZE: usize = 32; // HMAC-SHA256

// Page body layout
pub const PAGE_HEADER_SIZE: usize = 64;
pub const USABLE_SIZE: usize = BODY_SIZE - PAGE_HEADER_SIZE; // 8096 bytes for cells
pub const CHECKSUM_SIZE: usize = 8; // xxHash64

// File header
pub const FILE_HEADER_SIZE: usize = 512;
pub const GOD_BYTE_OFFSET: usize = 20;
pub const FILE_ID_OFFSET: usize = 24;
pub const COMMIT_SLOT_OFFSET: usize = 32;
pub const COMMIT_SLOT_SIZE: usize = 240;

// God byte bits
pub const GOD_BIT_ACTIVE_SLOT: u8 = 0x01; // bit 0: active commit slot (0 or 1)
pub const GOD_BIT_RECOVERY: u8 = 0x02; // bit 1: recovery_required flag

// Key size constants
pub const KEY_SIZE: usize = 32; // AES-256 key = 32 bytes
pub const REK_SIZE: usize = 32; // Root Encryption Key
pub const DEK_SIZE: usize = 32; // Data Encryption Key
pub const MAC_KEY_SIZE: usize = 32; // HMAC key
pub const WRAPPED_KEY_SIZE: usize = 40; // AES-KW(32B key) = 40B (32 + 8 integrity)
pub const ARGON2_SALT_SIZE: usize = 16;

// Key file layout
pub const KEY_FILE_SIZE: usize = 172;

// HKDF info strings for domain separation
pub const HKDF_INFO_DEK: &[u8] = b"citadel-dek-v1";
pub const HKDF_INFO_MAC_KEY: &[u8] = b"citadel-mac-key-v1";
pub const HKDF_INFO_KEYFILE_MAC: &[u8] = b"citadel-keyfile-mac";
pub const HKDF_INFO_KMS_MASTER: &[u8] = b"citadel-master-key";
pub const HKDF_KMS_SALT: &[u8] = b"citadel-v1";

// PBKDF2 (FIPS mode)
pub const PBKDF2_MIN_ITERATIONS: u32 = 600_000;

// Key backup file
pub const KEY_BACKUP_MAGIC: u32 = 0x4B45_5942; // "KEYB"
pub const KEY_BACKUP_VERSION: u32 = 1;
pub const KEY_BACKUP_SIZE: usize = 124;
pub const HKDF_INFO_BACKUP_MAC: &[u8] = b"citadel-backup-mac";

// B+ tree limits
pub const MAX_KEY_SIZE: usize = 2048;
pub const MAX_INLINE_VALUE_SIZE: usize = 1920;
pub const OVERFLOW_THRESHOLD: usize = MAX_INLINE_VALUE_SIZE;

// Pending-free entry size
pub const PENDING_FREE_ENTRY_SIZE: usize = 12; // page_id(4) + freed_at_txn(8)
pub const PENDING_FREE_ENTRIES_PER_PAGE: usize = USABLE_SIZE / PENDING_FREE_ENTRY_SIZE; // 674

// Merkle hash (inline in page header, BLAKE3 truncated to 28 bytes = 224 bits)
pub const MERKLE_HASH_SIZE: usize = 28;
pub const MERKLE_HASH_OFFSET: usize = 36; // page header offset [36..64]
pub const SLOT_MERKLE_ROOT: usize = 84; // CommitSlot offset [84..112]

// Audit log file
pub const AUDIT_LOG_MAGIC: u32 = 0x4155_4454; // "AUDT"
pub const AUDIT_LOG_VERSION: u32 = 1;
pub const AUDIT_HEADER_SIZE: usize = 64;
pub const AUDIT_ENTRY_MAGIC: u32 = 0x454E_5452; // "ENTR" - per-entry sentinel for scanning past corruption
pub const HKDF_INFO_AUDIT_KEY: &[u8] = b"citadel-audit-key-v1";

// Buffer pool defaults
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 256; // pages (2 MiB decrypted)

// Commit slot field offsets (relative to slot start)
pub const SLOT_TXN_ID: usize = 0;
pub const SLOT_TREE_ROOT: usize = 8;
pub const SLOT_TREE_DEPTH: usize = 12;
pub const SLOT_TREE_ENTRIES: usize = 16;
pub const SLOT_CATALOG_ROOT: usize = 24;
pub const SLOT_TOTAL_PAGES: usize = 28;
pub const SLOT_HIGH_WATER_MARK: usize = 32;
pub const SLOT_PENDING_FREE_ROOT: usize = 36;
pub const SLOT_ENCRYPTION_EPOCH: usize = 40;
pub const SLOT_DEK_ID: usize = 44;
pub const SLOT_CHECKSUM: usize = 76;

// Named table entry counts in CommitSlot [112..240]
pub const SLOT_NAMED_ENTRIES: usize = 112;
pub const SLOT_NAMED_ENTRY_SIZE: usize = 12;
pub const SLOT_NAMED_MAX_ENTRIES: usize =
    (COMMIT_SLOT_SIZE - SLOT_NAMED_ENTRIES - 2) / SLOT_NAMED_ENTRY_SIZE;

// File growth chunk sizes
pub const GROWTH_CHUNK_1MB: u64 = 1024 * 1024;
pub const GROWTH_CHUNK_4MB: u64 = 4 * 1024 * 1024;
pub const GROWTH_CHUNK_16MB: u64 = 16 * 1024 * 1024;
pub const GROWTH_THRESHOLD_4MB: u64 = 4 * 1024 * 1024;
pub const GROWTH_THRESHOLD_64MB: u64 = 64 * 1024 * 1024;
pub const GROWTH_THRESHOLD_1GB: u64 = 1024 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_consistency() {
        assert_eq!(PAGE_SIZE, IV_SIZE + BODY_SIZE + MAC_SIZE);
    }

    #[test]
    fn usable_size_consistency() {
        assert_eq!(USABLE_SIZE, BODY_SIZE - PAGE_HEADER_SIZE);
        assert_eq!(USABLE_SIZE, 8096);
    }

    #[test]
    fn two_cells_per_page_invariant() {
        // Guarantees 2 cells fit per page for B+ tree splits
        // branch cell: child(4) + key_len(2) + key(2048) = 2054
        // leaf cell: key_len(2) + val_len(4) + key(2048) + val_type(1) + value(1920) = 3975
        // 2 * max(2054, 3975) = 7950 <= 8096
        let max_branch_cell = 4 + 2 + MAX_KEY_SIZE;
        let max_leaf_cell = 2 + 4 + MAX_KEY_SIZE + 1 + MAX_INLINE_VALUE_SIZE;
        let max_cell = max_branch_cell.max(max_leaf_cell);
        assert!(2 * max_cell <= USABLE_SIZE, "2 cells must fit in one page");
    }

    #[test]
    fn file_header_fits() {
        let needed = COMMIT_SLOT_OFFSET + 2 * COMMIT_SLOT_SIZE;
        assert!(
            needed <= FILE_HEADER_SIZE,
            "commit slots must fit in header"
        );
    }

    #[test]
    fn pending_free_entries_per_page() {
        assert_eq!(PENDING_FREE_ENTRIES_PER_PAGE, 674);
    }
}
