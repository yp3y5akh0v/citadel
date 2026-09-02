use citadel_buffer::btree::BTree;
use citadel_core::types::PageId;
use citadel_core::Result;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;

pub const TABLE_DESCRIPTOR_SIZE: usize = 20;

const RESOLVED_CATALOG_LIMIT: usize = 256;

/// Exact-name descriptors for one immutable catalog generation, without
/// commit-slot overrides. Old snapshots retain their own cache after DDL.
#[derive(Default)]
pub(crate) struct ResolvedCatalog {
    tables: Mutex<FxHashMap<Vec<u8>, TableDescriptor>>,
}

impl ResolvedCatalog {
    pub(crate) fn resolve(
        &self,
        name: &[u8],
        load: impl FnOnce() -> Result<TableDescriptor>,
    ) -> Result<TableDescriptor> {
        if let Some(descriptor) = self.tables.lock().get(name).cloned() {
            return Ok(descriptor);
        }

        // Do not hold the cache lock during I/O or cancellation checks.
        let descriptor = load()?;
        let mut tables = self.tables.lock();
        if tables.len() < RESOLVED_CATALOG_LIMIT {
            tables.insert(name.to_vec(), descriptor.clone());
        }
        Ok(descriptor)
    }
}

/// On-disk descriptor for a named table, stored as a value in the catalog B+ tree.
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    pub root_page: PageId,
    pub entry_count: u64,
    pub depth: u16,
    pub flags: u16,
}

impl TableDescriptor {
    pub fn serialize(&self) -> [u8; TABLE_DESCRIPTOR_SIZE] {
        let mut buf = [0u8; TABLE_DESCRIPTOR_SIZE];
        buf[0..4].copy_from_slice(&self.root_page.as_u32().to_le_bytes());
        buf[4..12].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[12..14].copy_from_slice(&self.depth.to_le_bytes());
        buf[14..16].copy_from_slice(&self.flags.to_le_bytes());
        // [16..20] reserved
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        // Preserve the public prefix-decoding contract: a caller that already
        // validated its record may supply the 16-byte prefix with no reserved
        // tail, or a longer future record this version cannot read in full.
        Self {
            root_page: PageId(u32::from_le_bytes(buf[0..4].try_into().unwrap())),
            entry_count: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            depth: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            flags: u16::from_le_bytes(buf[14..16].try_into().unwrap()),
        }
    }

    /// Decode a catalog descriptor only when its complete on-disk shape is
    /// present. Catalog lookup paths consume untrusted page contents, so they
    /// must not use the indexing/panic contract of [`Self::deserialize`].
    pub fn try_deserialize(buf: &[u8]) -> Option<Self> {
        let buf: &[u8; TABLE_DESCRIPTOR_SIZE] = buf.try_into().ok()?;
        Some(Self {
            root_page: PageId(u32::from_le_bytes(buf[0..4].try_into().unwrap())),
            entry_count: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            depth: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            flags: u16::from_le_bytes(buf[14..16].try_into().unwrap()),
        })
    }

    pub fn from_tree(tree: &BTree) -> Self {
        Self {
            root_page: tree.root,
            entry_count: tree.entry_count,
            depth: tree.depth,
            flags: 0,
        }
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
