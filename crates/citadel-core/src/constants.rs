// File magic number: 0xC17AD3E1 ("CITADEL" without vowels)
pub const MAGIC: u32 = 0xC17A_D3E1;
pub const FORMAT_VERSION: u32 = 1;

// Key file magic: "KEYS" in ASCII
pub const KEY_FILE_MAGIC: u32 = 0x4B45_5953;
pub const KEY_FILE_VERSION: u32 = 1;

pub const PAGE_SIZE: usize = 8208; // IV(16) + ciphertext(8160) + MAC(32)
pub const BODY_SIZE: usize = 8160; // decrypted page body
pub const IV_SIZE: usize = 16;
pub const MAC_SIZE: usize = 32; // HMAC-SHA256

pub const PAGE_HEADER_SIZE: usize = 64;
pub const USABLE_SIZE: usize = BODY_SIZE - PAGE_HEADER_SIZE; // 8096 bytes for cells
pub const CHECKSUM_SIZE: usize = 8; // xxHash64

pub const FILE_HEADER_SIZE: usize = 512;
pub const GOD_BYTE_OFFSET: usize = 20;
// Header flags byte (previously zero padding, so pre-flag files read as 0).
pub const HEADER_FLAGS_OFFSET: usize = 21;
// One-way: set once both slots are V1, then recover() rejects legacy
// (MAC-less) slots, closing the re-encode-as-legacy downgrade.
pub const HEADER_FLAG_SLOTS_V1: u8 = 0x01;
pub const FILE_ID_OFFSET: usize = 24;
pub const COMMIT_SLOT_OFFSET: usize = 32;
pub const COMMIT_SLOT_SIZE: usize = 240;

pub const GOD_BIT_ACTIVE_SLOT: u8 = 0x01; // bit 0: active commit slot (0 or 1)
pub const GOD_BIT_RECOVERY: u8 = 0x02; // bit 1: recovery_required flag

pub const KEY_SIZE: usize = 32; // AES-256 key = 32 bytes
pub const REK_SIZE: usize = 32; // Root Encryption Key
pub const DEK_SIZE: usize = 32; // Data Encryption Key
pub const MAC_KEY_SIZE: usize = 32; // HMAC key
pub const WRAPPED_KEY_SIZE: usize = 40; // AES-KW(32B key) = 40B (32 + 8 integrity)
pub const ARGON2_SALT_SIZE: usize = 16;

pub const KEY_FILE_SIZE: usize = 172;

// HKDF info strings for domain separation.
pub const HKDF_INFO_DEK: &[u8] = b"citadel-dek-v1";
pub const HKDF_INFO_MAC_KEY: &[u8] = b"citadel-mac-key-v1";
pub const HKDF_INFO_KEYFILE_MAC: &[u8] = b"citadel-keyfile-mac";
pub const HKDF_INFO_KMS_MASTER: &[u8] = b"citadel-master-key";
pub const HKDF_KMS_SALT: &[u8] = b"citadel-v1";

pub const REGION_STORE_MAGIC: u32 = 0x5247_4E53; // "RGNS"
pub const REGION_STORE_VERSION: u32 = 1;
pub const REGION_STORE_BLOCK: usize = 512; // sector-aligned header/slot copy size
pub const REGION_STORE_PREALLOC_SLOTS: u32 = 64; // initial capacity (grows append-only)
pub const HKDF_INFO_REGION_STORE_MAC: &[u8] = b"citadel-region-store-mac-v1";
pub const HKDF_INFO_REGION_WRAP: &[u8] = b"citadel-region-wrap-v1";
pub const HKDF_INFO_RCK_DEK: &[u8] = b"citadel-rck-dek-v1";
pub const HKDF_INFO_RCK_MAC: &[u8] = b"citadel-rck-mac-v1";
// Per-atom erasure: each atom's random content key (ACK) is AES-KW-wrapped
// under a KEK derived from the region RCK and stored as the sole copy in the
// atom key store, so destroying that slot erases one atom and destroying the
// RCK erases the whole region.
pub const HKDF_INFO_ATOM_WRAP: &[u8] = b"citadel-atom-wrap-v1";
// Keyed-idempotency tag MAC key: derived from the region RCK under its own
// label (never the atom-wrap KEK) so no plaintext equality tag reaches disk.
pub const HKDF_INFO_IDENTITY_MAC: &[u8] = b"citadel-identity-mac-v1";
pub const ATOM_STORE_MAGIC: u32 = 0x4154_4D53; // "ATMS"
pub const ATOM_STORE_VERSION: u32 = 1;
pub const ATOM_STORE_PREALLOC_SLOTS: u32 = 256; // initial capacity (grows append-only)

pub const PBKDF2_MIN_ITERATIONS: u32 = 600_000;

pub const KEY_BACKUP_MAGIC: u32 = 0x4B45_5942; // "KEYB"
pub const KEY_BACKUP_VERSION: u32 = 1;
pub const KEY_BACKUP_SIZE: usize = 124;
pub const HKDF_INFO_BACKUP_MAC: &[u8] = b"citadel-backup-mac";

pub const MAX_KEY_SIZE: usize = 2048;
pub const MAX_INLINE_VALUE_SIZE: usize = 1920;
pub const OVERFLOW_THRESHOLD: usize = MAX_INLINE_VALUE_SIZE;
/// Hard cap including overflow chains; bounded by the cell's `val_len: u32`.
pub const MAX_VALUE_SIZE: usize = 1 << 30; // 1 GiB

pub const PENDING_FREE_ENTRY_SIZE: usize = 12; // page_id(4) + freed_at_txn(8)
pub const PENDING_FREE_ENTRIES_PER_PAGE: usize =
    (USABLE_SIZE - core::mem::size_of::<u32>()) / PENDING_FREE_ENTRY_SIZE; // 674

// Merkle hash: BLAKE3 truncated to 28 bytes (224 bits) to fit inline in the
// page header.
pub const MERKLE_HASH_SIZE: usize = 28;
pub const MERKLE_HASH_OFFSET: usize = 36; // page header offset [36..64]
pub const SLOT_MERKLE_ROOT: usize = 84; // CommitSlot offset [84..112]

pub const AUDIT_LOG_MAGIC: u32 = 0x4155_4454; // "AUDT"

// v1 (released): header bytes 32..64 are a vestigial tip, chain seeds from
// zeros. v2: those bytes are a write-once chain seed. v1 files stay v1.
pub const AUDIT_LOG_VERSION_LEGACY: u32 = 1;
pub const AUDIT_LOG_VERSION: u32 = 2;
pub const AUDIT_HEADER_SIZE: usize = 64;
pub const AUDIT_ENTRY_MAGIC: u32 = 0x454E_5452; // "ENTR" - per-entry sentinel for scanning past corruption
pub const HKDF_INFO_AUDIT_KEY: &[u8] = b"citadel-audit-key-v1";

pub const DEFAULT_BUFFER_POOL_SIZE: usize = 256; // pages (2 MiB decrypted)

// Commit slot field offsets (relative to slot start).
pub const SLOT_TXN_ID: usize = 0;
pub const SLOT_TREE_ROOT: usize = 8;
pub const SLOT_TREE_DEPTH: usize = 12;
// Merkle hash scheme for this commit generation. Released writers left these
// reserved bytes zero, so zero remains the legacy overflow-reference scheme.
pub const SLOT_MERKLE_SCHEME: usize = 14;
pub const SLOT_MERKLE_SCHEME_LOGICAL_OVERFLOW_V1: u16 = 1;
pub const SLOT_TREE_ENTRIES: usize = 16;
pub const SLOT_CATALOG_ROOT: usize = 24;
pub const SLOT_TOTAL_PAGES: usize = 28;
pub const SLOT_HIGH_WATER_MARK: usize = 32;
pub const SLOT_PENDING_FREE_ROOT: usize = 36;
pub const SLOT_ENCRYPTION_EPOCH: usize = 40;
pub const SLOT_DEK_ID: usize = 44;
pub const SLOT_CHECKSUM: usize = 76;

// Named table entries in CommitSlot [112..240], 18 bytes each
pub const SLOT_NAMED_ENTRIES: usize = 112;
pub const SLOT_NAMED_ENTRY_SIZE: usize = 18;
pub const SLOT_NAMED_MAX_ENTRIES: usize =
    (COMMIT_SLOT_SIZE - SLOT_NAMED_ENTRIES - 2) / SLOT_NAMED_ENTRY_SIZE;

// V1 (authenticated) slot tail: [222..224] a format marker, [224..240] a
// truncated HMAC over [0..SLOT_MAC]. Legacy slots hold a 7th entry or zeros
// there, so both pre-v1 forms stay distinguishable.
pub const SLOT_FORMAT_MARKER: usize = 222;
pub const SLOT_MARKER_V1: u16 = 0xC17A;
pub const SLOT_MAC: usize = 224;
pub const SLOT_MAC_SIZE: usize = COMMIT_SLOT_SIZE - SLOT_MAC;
pub const SLOT_NAMED_MAX_ENTRIES_V1: usize =
    (SLOT_FORMAT_MARKER - SLOT_NAMED_ENTRIES - 2) / SLOT_NAMED_ENTRY_SIZE;
pub const SLOT_MAC_DOMAIN: &[u8] = b"citadel-slot-mac-v1";
// High bit of an entry's count: marks it the sole record of its root (never
// dropped on serialize). In count, not depth, so a downgrade reader treating
// it as a statistic can't misread it as tree height. Wire-compatible: real
// counts never reach 2^63 and old files never set it.
pub const SLOT_ENTRY_STALE: u64 = 1 << 63;

pub const GROWTH_CHUNK_1MB: u64 = 1024 * 1024;
pub const GROWTH_CHUNK_4MB: u64 = 4 * 1024 * 1024;
pub const GROWTH_CHUNK_16MB: u64 = 16 * 1024 * 1024;
pub const GROWTH_THRESHOLD_4MB: u64 = 4 * 1024 * 1024;
pub const GROWTH_THRESHOLD_64MB: u64 = 64 * 1024 * 1024;
pub const GROWTH_THRESHOLD_1GB: u64 = 1024 * 1024 * 1024;

#[cfg(test)]
#[path = "constants_tests.rs"]
mod tests;
