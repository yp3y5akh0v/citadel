use citadel_core::types::PageId;
use citadel_buffer::btree::BTree;

pub const TABLE_DESCRIPTOR_SIZE: usize = 20;

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
        Self {
            root_page: PageId(u32::from_le_bytes(buf[0..4].try_into().unwrap())),
            entry_count: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            depth: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            flags: u16::from_le_bytes(buf[14..16].try_into().unwrap()),
        }
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
mod tests {
    use super::*;

    #[test]
    fn serialize_deserialize_roundtrip() {
        let desc = TableDescriptor {
            root_page: PageId(42),
            entry_count: 1000,
            depth: 3,
            flags: 0,
        };
        let buf = desc.serialize();
        assert_eq!(buf.len(), TABLE_DESCRIPTOR_SIZE);

        let desc2 = TableDescriptor::deserialize(&buf);
        assert_eq!(desc2.root_page, PageId(42));
        assert_eq!(desc2.entry_count, 1000);
        assert_eq!(desc2.depth, 3);
        assert_eq!(desc2.flags, 0);
    }
}
