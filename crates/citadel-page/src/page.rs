use citadel_core::types::{PageFlags, PageId, PageType, TxnId};
use citadel_core::{
    BODY_SIZE, CHECKSUM_SIZE, MERKLE_HASH_OFFSET, MERKLE_HASH_SIZE, PAGE_HEADER_SIZE, USABLE_SIZE,
};

/// Decrypted page body (8160 bytes).
///
/// Layout:
/// [0..8]     checksum (xxHash64 of bytes [8..8160])
/// [8..12]    page_id (u32)
/// [12..14]   page_type (u16)
/// [14..16]   flags (u16)
/// [16..24]   txn_id (u64)
/// [24..26]   num_cells (u16)
/// [26..28]   cell_area_start (u16) — where cell data begins (grows down from 8160)
/// [28..30]   free_space (u16)
/// [30..34]   right_child (u32) — rightmost child (branch) / 0 (leaf/overflow)
/// [34..36]   _reserved (u16)
/// [36..64]   merkle_hash (28B, BLAKE3 truncated) — Merkle tree hash for sync
/// [64..8160] cell data area (slotted page)
#[derive(Clone)]
pub struct Page {
    pub data: [u8; BODY_SIZE],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [0u8; BODY_SIZE],
        }
    }
}

impl Page {
    /// Create a new empty page.
    pub fn new(page_id: PageId, page_type: PageType, txn_id: TxnId) -> Self {
        let mut data = [0u8; BODY_SIZE];

        // page_id
        data[8..12].copy_from_slice(&page_id.as_u32().to_le_bytes());
        // page_type
        data[12..14].copy_from_slice(&(page_type as u16).to_le_bytes());
        // flags
        data[14..16].copy_from_slice(&PageFlags::NONE.0.to_le_bytes());
        // txn_id
        data[16..24].copy_from_slice(&txn_id.as_u64().to_le_bytes());
        // num_cells = 0
        data[24..26].copy_from_slice(&0u16.to_le_bytes());
        // cell_area_start = BODY_SIZE (no cells yet, start from end)
        data[26..28].copy_from_slice(&(BODY_SIZE as u16).to_le_bytes());
        // free_space = USABLE_SIZE (entire cell area available)
        data[28..30].copy_from_slice(&(USABLE_SIZE as u16).to_le_bytes());
        // right_child = 0
        data[30..34].copy_from_slice(&0u32.to_le_bytes());

        let mut page = Self { data };
        page.update_checksum();
        page
    }

    /// Create a page from raw decrypted bytes.
    pub fn from_bytes(data: [u8; BODY_SIZE]) -> Self {
        Self { data }
    }

    // --- Header field accessors ---

    pub fn checksum(&self) -> u64 {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }

    pub fn page_id(&self) -> PageId {
        PageId(u32::from_le_bytes(self.data[8..12].try_into().unwrap()))
    }

    pub fn set_page_id(&mut self, id: PageId) {
        self.data[8..12].copy_from_slice(&id.as_u32().to_le_bytes());
    }

    pub fn page_type(&self) -> Option<PageType> {
        PageType::from_u16(u16::from_le_bytes(self.data[12..14].try_into().unwrap()))
    }

    pub fn page_type_raw(&self) -> u16 {
        u16::from_le_bytes(self.data[12..14].try_into().unwrap())
    }

    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[12..14].copy_from_slice(&(pt as u16).to_le_bytes());
    }

    pub fn flags(&self) -> PageFlags {
        PageFlags(u16::from_le_bytes(self.data[14..16].try_into().unwrap()))
    }

    pub fn set_flags(&mut self, flags: PageFlags) {
        self.data[14..16].copy_from_slice(&flags.0.to_le_bytes());
    }

    pub fn txn_id(&self) -> TxnId {
        TxnId(u64::from_le_bytes(self.data[16..24].try_into().unwrap()))
    }

    pub fn set_txn_id(&mut self, txn_id: TxnId) {
        self.data[16..24].copy_from_slice(&txn_id.as_u64().to_le_bytes());
    }

    pub fn num_cells(&self) -> u16 {
        u16::from_le_bytes(self.data[24..26].try_into().unwrap())
    }

    pub fn set_num_cells(&mut self, n: u16) {
        self.data[24..26].copy_from_slice(&n.to_le_bytes());
    }

    pub fn cell_area_start(&self) -> u16 {
        u16::from_le_bytes(self.data[26..28].try_into().unwrap())
    }

    pub fn set_cell_area_start(&mut self, offset: u16) {
        self.data[26..28].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn free_space(&self) -> u16 {
        u16::from_le_bytes(self.data[28..30].try_into().unwrap())
    }

    pub fn set_free_space(&mut self, space: u16) {
        self.data[28..30].copy_from_slice(&space.to_le_bytes());
    }

    pub fn right_child(&self) -> PageId {
        PageId(u32::from_le_bytes(self.data[30..34].try_into().unwrap()))
    }

    pub fn set_right_child(&mut self, child: PageId) {
        self.data[30..34].copy_from_slice(&child.as_u32().to_le_bytes());
    }

    /// Get the Merkle hash stored in this page's header.
    pub fn merkle_hash(&self) -> [u8; MERKLE_HASH_SIZE] {
        let end = MERKLE_HASH_OFFSET + MERKLE_HASH_SIZE;
        self.data[MERKLE_HASH_OFFSET..end].try_into().unwrap()
    }

    /// Set the Merkle hash in this page's header.
    pub fn set_merkle_hash(&mut self, hash: &[u8; MERKLE_HASH_SIZE]) {
        let end = MERKLE_HASH_OFFSET + MERKLE_HASH_SIZE;
        self.data[MERKLE_HASH_OFFSET..end].copy_from_slice(hash);
    }

    // --- Slotted page operations ---

    /// Get the offset of cell pointer at index `i`.
    /// Cell pointers start at offset 64, each is 2 bytes.
    #[inline]
    fn cell_ptr_offset(i: u16) -> usize {
        PAGE_HEADER_SIZE + (i as usize) * 2
    }

    /// Get the cell data offset stored in cell pointer at index `i`.
    pub fn cell_offset(&self, i: u16) -> u16 {
        let off = Self::cell_ptr_offset(i);
        u16::from_le_bytes(self.data[off..off + 2].try_into().unwrap())
    }

    /// Set the cell data offset at pointer index `i`.
    pub fn set_cell_offset(&mut self, i: u16, offset: u16) {
        let off = Self::cell_ptr_offset(i);
        self.data[off..off + 2].copy_from_slice(&offset.to_le_bytes());
    }

    /// Get a slice of cell data at the given offset and length.
    pub fn cell_data(&self, offset: u16, len: usize) -> &[u8] {
        let start = offset as usize;
        &self.data[start..start + len]
    }

    /// Get a mutable slice of cell data at the given offset and length.
    pub fn cell_data_mut(&mut self, offset: u16, len: usize) -> &mut [u8] {
        let start = offset as usize;
        &mut self.data[start..start + len]
    }

    /// Calculate available space for a new cell (cell pointer + cell data).
    /// Available = cell_area_start - (PAGE_HEADER_SIZE + num_cells * 2) - 2
    /// The -2 accounts for the new cell pointer we need to add.
    pub fn available_space(&self) -> usize {
        let ptrs_end = PAGE_HEADER_SIZE + (self.num_cells() as usize) * 2;
        let cell_start = self.cell_area_start() as usize;
        if cell_start <= ptrs_end + 2 {
            0
        } else {
            cell_start - ptrs_end - 2 // 2 bytes for new cell pointer
        }
    }

    /// Write cell data at the bottom of the cell area, return the offset.
    /// Updates cell_area_start and free_space.
    pub fn write_cell(&mut self, data: &[u8]) -> Option<u16> {
        let cell_len = data.len();
        if self.available_space() < cell_len {
            return None;
        }

        let new_start = self.cell_area_start() as usize - cell_len;
        self.data[new_start..new_start + cell_len].copy_from_slice(data);
        self.set_cell_area_start(new_start as u16);

        let n = self.num_cells();
        self.set_cell_offset(n, new_start as u16);
        self.set_num_cells(n + 1);

        let free = self.free_space() as usize - cell_len - 2; // cell data + cell pointer
        self.set_free_space(free as u16);

        Some(new_start as u16)
    }

    /// Insert cell data at position `idx` in the sorted cell pointer array.
    /// Shifts existing pointers at idx..num_cells right by one slot.
    /// Cell data is appended at the bottom of the cell area.
    /// Returns the data offset, or None if not enough space.
    pub fn insert_cell_at(&mut self, idx: u16, cell_data: &[u8]) -> Option<u16> {
        let cell_len = cell_data.len();
        if self.available_space() < cell_len {
            return None;
        }

        // Write cell data at bottom of cell area
        let new_start = self.cell_area_start() as usize - cell_len;
        self.data[new_start..new_start + cell_len].copy_from_slice(cell_data);
        self.set_cell_area_start(new_start as u16);

        let n = self.num_cells();

        // Shift cell pointers [idx..n] right by 2 bytes to make room
        if idx < n {
            let src_start = Self::cell_ptr_offset(idx);
            let src_end = Self::cell_ptr_offset(n);
            self.data.copy_within(src_start..src_end, src_start + 2);
        }

        // Write new cell pointer at idx
        self.set_cell_offset(idx, new_start as u16);
        self.set_num_cells(n + 1);

        let free = self.free_space() as usize - cell_len - 2;
        self.set_free_space(free as u16);

        Some(new_start as u16)
    }

    /// Delete cell at position `idx`. Shifts pointers left.
    /// The cell data becomes a hole (not reclaimed until compact).
    /// `cell_len` is the size of the cell data being removed.
    pub fn delete_cell_at(&mut self, idx: u16, cell_len: usize) {
        let n = self.num_cells();
        assert!(idx < n, "delete_cell_at: index out of bounds");

        // Shift cell pointers [idx+1..n] left by 2 bytes
        if idx + 1 < n {
            let src_start = Self::cell_ptr_offset(idx + 1);
            let src_end = Self::cell_ptr_offset(n);
            let dst_start = Self::cell_ptr_offset(idx);
            self.data.copy_within(src_start..src_end, dst_start);
        }

        self.set_num_cells(n - 1);
        // Free space increases by cell_len (data hole) + 2 (pointer slot)
        // Note: cell data is not compacted, just the pointer is removed.
        // free_space tracks total usable space including holes.
        let free = self.free_space() as usize + cell_len + 2;
        self.set_free_space(free as u16);
    }

    /// Rebuild page from a list of cell data blobs. Clears all cells and re-inserts.
    /// Used during CoW and split operations.
    pub fn rebuild_cells(&mut self, cells: &[&[u8]]) {
        // Reset cell area
        self.set_num_cells(0);
        self.set_cell_area_start(BODY_SIZE as u16);
        self.set_free_space(USABLE_SIZE as u16);

        // Re-insert all cells
        for cell_data in cells {
            self.write_cell(cell_data)
                .expect("rebuild_cells: cell data should fit");
        }
    }

    // --- Checksum ---

    /// Compute xxHash64 of bytes [8..8160] (everything after checksum field).
    pub fn compute_checksum(&self) -> u64 {
        xxhash_rust::xxh64::xxh64(&self.data[CHECKSUM_SIZE..], 0)
    }

    /// Update the stored checksum to match current page contents.
    pub fn update_checksum(&mut self) {
        let cs = self.compute_checksum();
        self.data[0..CHECKSUM_SIZE].copy_from_slice(&cs.to_le_bytes());
    }

    /// Verify the stored checksum matches the page contents.
    pub fn verify_checksum(&self) -> bool {
        self.checksum() == self.compute_checksum()
    }

    /// Get the full page body as bytes (for encryption).
    pub fn as_bytes(&self) -> &[u8; BODY_SIZE] {
        &self.data
    }

    /// Get the full page body as mutable bytes.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; BODY_SIZE] {
        &mut self.data
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("page_id", &self.page_id())
            .field("page_type", &self.page_type())
            .field("txn_id", &self.txn_id())
            .field("num_cells", &self.num_cells())
            .field("free_space", &self.free_space())
            .field("checksum_valid", &self.verify_checksum())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_fields() {
        let page = Page::new(PageId(42), PageType::Leaf, TxnId(1));
        assert_eq!(page.page_id(), PageId(42));
        assert_eq!(page.page_type(), Some(PageType::Leaf));
        assert_eq!(page.txn_id(), TxnId(1));
        assert_eq!(page.num_cells(), 0);
        assert_eq!(page.cell_area_start(), BODY_SIZE as u16);
        assert_eq!(page.free_space(), USABLE_SIZE as u16);
        assert_eq!(page.right_child(), PageId(0));
        assert_eq!(page.flags(), PageFlags::NONE);
    }

    #[test]
    fn checksum_roundtrip() {
        let page = Page::new(PageId(1), PageType::Branch, TxnId(5));
        assert!(page.verify_checksum());
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        page.update_checksum();
        assert!(page.verify_checksum());

        // Corrupt a data byte
        page.data[100] ^= 0xFF;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn write_cell_and_read_back() {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        let cell = b"hello world";
        let offset = page.write_cell(cell).unwrap();

        assert_eq!(page.num_cells(), 1);
        assert_eq!(page.cell_offset(0), offset);
        assert_eq!(page.cell_data(offset, cell.len()), cell);
    }

    #[test]
    fn multiple_cells() {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        let cell1 = b"first";
        let cell2 = b"second";
        let cell3 = b"third";

        let o1 = page.write_cell(cell1).unwrap();
        let o2 = page.write_cell(cell2).unwrap();
        let o3 = page.write_cell(cell3).unwrap();

        assert_eq!(page.num_cells(), 3);
        // Cells grow downward, so later cells have lower offsets
        assert!(o2 < o1);
        assert!(o3 < o2);

        assert_eq!(page.cell_data(o1, cell1.len()), cell1);
        assert_eq!(page.cell_data(o2, cell2.len()), cell2);
        assert_eq!(page.cell_data(o3, cell3.len()), cell3);
    }

    #[test]
    fn available_space_decreases() {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        let initial = page.available_space();

        let cell = vec![0u8; 100];
        page.write_cell(&cell).unwrap();

        let after = page.available_space();
        assert_eq!(after, initial - 100 - 2); // cell data + cell pointer
    }

    #[test]
    fn page_full_returns_none() {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        let big_cell = vec![0u8; page.available_space() + 1];
        assert!(page.write_cell(&big_cell).is_none());
    }

    #[test]
    fn set_flags() {
        let mut page = Page::new(PageId(1), PageType::Branch, TxnId(1));
        let mut flags = page.flags();
        flags.set(PageFlags::IS_ROOT);
        page.set_flags(flags);
        assert!(page.flags().contains(PageFlags::IS_ROOT));
    }

    #[test]
    fn right_child_roundtrip() {
        let mut page = Page::new(PageId(1), PageType::Branch, TxnId(1));
        page.set_right_child(PageId(999));
        assert_eq!(page.right_child(), PageId(999));
    }

    #[test]
    fn page_debug_display() {
        let page = Page::new(PageId(42), PageType::Leaf, TxnId(7));
        let dbg = format!("{:?}", page);
        assert!(dbg.contains("PageId(42)"));
    }

    #[test]
    fn from_bytes_preserves_data() {
        let page = Page::new(PageId(5), PageType::Leaf, TxnId(3));
        let bytes = *page.as_bytes();
        let page2 = Page::from_bytes(bytes);
        assert_eq!(page2.page_id(), PageId(5));
        assert_eq!(page2.txn_id(), TxnId(3));
        assert!(page2.verify_checksum());
    }
}
