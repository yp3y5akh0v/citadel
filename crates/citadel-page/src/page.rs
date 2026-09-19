use citadel_core::types::{PageFlags, PageId, PageType, TxnId};
use citadel_core::{
    BODY_SIZE, CHECKSUM_SIZE, MERKLE_HASH_OFFSET, MERKLE_HASH_SIZE, PAGE_HEADER_SIZE, USABLE_SIZE,
};

/// A malformed slotted-page cell layout. The message stays behind a private field
/// so callers can report corruption detail without the individual checks becoming
/// public compatibility surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellDecodeError {
    detail: String,
}

impl CellDecodeError {
    pub(crate) fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for CellDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for CellDecodeError {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CellSpan {
    pub index: usize,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CellOffsetOrder {
    Ascending,
    Descending,
    Unordered,
}

/// Borrowed pointer offsets whose complete array has already passed validation.
pub(crate) struct CheckedCellOffsets<'a> {
    offsets: std::slice::Iter<'a, [u8; 2]>,
    order: CellOffsetOrder,
}

impl CheckedCellOffsets<'_> {
    pub(crate) fn order(&self) -> CellOffsetOrder {
        self.order
    }
}

impl Iterator for CheckedCellOffsets<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        self.offsets
            .next()
            .map(|pointer| u16::from_le_bytes(*pointer) as usize)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

impl ExactSizeIterator for CheckedCellOffsets<'_> {}

/// Validate the entire cell-pointer array before exposing borrowed offsets.
pub(crate) fn checked_cell_offsets(page: &Page) -> Result<CheckedCellOffsets<'_>, CellDecodeError> {
    let count = page.num_cells() as usize;
    let pointer_bytes = count.checked_mul(2).ok_or_else(|| {
        CellDecodeError::new(format!(
            "cell pointer count {count} overflows address space"
        ))
    })?;
    let pointer_end = PAGE_HEADER_SIZE.checked_add(pointer_bytes).ok_or_else(|| {
        CellDecodeError::new(format!(
            "cell pointer count {count} overflows address space"
        ))
    })?;
    if pointer_end > BODY_SIZE {
        return Err(CellDecodeError::new(format!(
            "cell pointer array ends at {pointer_end}, beyond page body {BODY_SIZE}"
        )));
    }

    let cell_area_start = page.cell_area_start() as usize;
    if cell_area_start < pointer_end || cell_area_start > BODY_SIZE {
        return Err(CellDecodeError::new(format!(
            "cell area starts at {cell_area_start}, outside {pointer_end}..={BODY_SIZE}"
        )));
    }

    let pointers = page.data[PAGE_HEADER_SIZE..pointer_end].as_chunks::<2>().0;
    let mut ascending = true;
    let mut descending = true;
    let mut previous = None;
    for (index, pointer) in pointers.iter().enumerate() {
        let offset = u16::from_le_bytes(*pointer) as usize;
        if offset < cell_area_start || offset >= BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "cell {index} offset {offset} lies outside cell area {cell_area_start}..{BODY_SIZE}"
            )));
        }
        if let Some(previous) = previous {
            ascending &= previous < offset;
            descending &= previous > offset;
        }
        previous = Some(offset);
    }
    let order = if ascending {
        CellOffsetOrder::Ascending
    } else if descending {
        CellOffsetOrder::Descending
    } else {
        CellOffsetOrder::Unordered
    };
    Ok(CheckedCellOffsets {
        offsets: pointers.iter(),
        order,
    })
}

/// Accumulate layout evidence while the shared cell parser visits logical order.
/// Ordered pointers need only adjacent spans; arbitrary or duplicate pointers
/// retain the existing sorted-span validation.
pub(crate) enum CellLayout {
    Ordered {
        descending: bool,
        cell_count: usize,
        cell_bytes: Option<usize>,
        previous: Option<CellSpan>,
        overlap: Option<(CellSpan, CellSpan)>,
    },
    Unordered(Vec<CellSpan>),
}

impl CellLayout {
    pub(crate) fn new(offsets: &CheckedCellOffsets<'_>) -> Self {
        match offsets.order() {
            CellOffsetOrder::Ascending | CellOffsetOrder::Descending => Self::Ordered {
                descending: offsets.order() == CellOffsetOrder::Descending,
                cell_count: offsets.len(),
                cell_bytes: Some(0),
                previous: None,
                overlap: None,
            },
            CellOffsetOrder::Unordered => Self::Unordered(Vec::with_capacity(offsets.len())),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, span: CellSpan) {
        match self {
            Self::Ordered {
                descending,
                cell_bytes,
                previous,
                overlap,
                ..
            } => {
                *cell_bytes = cell_bytes.and_then(|total| total.checked_add(span.end - span.start));
                if let Some(previous) = *previous {
                    let (lower, upper) = if *descending {
                        (span, previous)
                    } else {
                        (previous, span)
                    };
                    if spans_overlap(lower, upper) && (*descending || overlap.is_none()) {
                        // Descending traversal encounters lower physical pairs later.
                        // Retain the last overlap there, and the first when ascending.
                        *overlap = Some((lower, upper));
                    }
                }
                *previous = Some(span);
            }
            Self::Unordered(spans) => spans.push(span),
        }
    }

    pub(crate) fn finish(self, page: &Page) -> Result<(), CellDecodeError> {
        match self {
            Self::Ordered {
                cell_count,
                cell_bytes,
                overlap,
                ..
            } => {
                if let Some((lower, upper)) = overlap {
                    return Err(overlap_error(lower, upper));
                }
                validate_cell_accounting(page, cell_count, cell_bytes)
            }
            Self::Unordered(mut spans) => validate_cell_layout(page, &mut spans),
        }
    }
}

#[inline]
fn spans_overlap(lower: CellSpan, upper: CellSpan) -> bool {
    lower.end > upper.start
}

fn overlap_error(lower: CellSpan, upper: CellSpan) -> CellDecodeError {
    CellDecodeError::new(format!(
        "cells {} and {} overlap at byte {}",
        lower.index, upper.index, upper.start
    ))
}

/// Validate relationships common to every slotted-page cell format: live cells may
/// not overlap, and free space must account exactly for the pointer array and cells.
pub(crate) fn validate_cell_layout(
    page: &Page,
    spans: &mut [CellSpan],
) -> Result<(), CellDecodeError> {
    spans.sort_unstable_by_key(|span| span.start);
    if let Some(pair) = spans
        .windows(2)
        .find(|pair| spans_overlap(pair[0], pair[1]))
    {
        return Err(overlap_error(pair[0], pair[1]));
    }

    let cell_bytes = spans.iter().try_fold(0usize, |total, span| {
        total.checked_add(span.end - span.start)
    });
    validate_cell_accounting(page, spans.len(), cell_bytes)
}

fn validate_cell_accounting(
    page: &Page,
    cell_count: usize,
    cell_bytes: Option<usize>,
) -> Result<(), CellDecodeError> {
    let pointer_bytes = cell_count
        .checked_mul(2)
        .ok_or_else(|| CellDecodeError::new("free-space pointer accounting overflow"))?;
    let cell_bytes =
        cell_bytes.ok_or_else(|| CellDecodeError::new("free-space cell accounting overflow"))?;
    let expected = USABLE_SIZE
        .checked_sub(pointer_bytes)
        .and_then(|space| space.checked_sub(cell_bytes))
        .ok_or_else(|| CellDecodeError::new("live cells exceed usable page space"))?;
    let recorded = page.free_space() as usize;
    if recorded != expected {
        return Err(CellDecodeError::new(format!(
            "free-space accounting records {recorded} bytes, expected {expected}"
        )));
    }
    Ok(())
}

/// Decrypted page body (8160 bytes).
///
/// Layout:
/// [0..8]     checksum (xxHash64 of bytes [8..8160])
/// [8..12]    page_id (u32)
/// [12..14]   page_type (u16)
/// [14..16]   flags (u16)
/// [16..24]   txn_id (u64)
/// [24..26]   num_cells (u16)
/// [26..28]   cell_area_start (u16) - where cell data begins (grows down from 8160)
/// [28..30]   free_space (u16)
/// [30..34]   right_child (u32) - rightmost child (branch) / 0 (leaf/overflow)
/// [34..36]   _reserved (u16)
/// [36..64]   merkle_hash (28B, BLAKE3 truncated) - Merkle tree hash for sync
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
    pub fn new(page_id: PageId, page_type: PageType, txn_id: TxnId) -> Self {
        let mut page = Self::new_for_write(page_id, page_type, txn_id);
        page.update_checksum();
        page
    }

    /// Construct a fully initialized page for a writer that will modify it.
    ///
    /// The checksum field remains zero. Call `update_checksum` after the final
    /// mutation and before persisting or publishing the page to readers.
    /// Use `new` when an immediately checksummed empty page is required.
    pub fn new_for_write(page_id: PageId, page_type: PageType, txn_id: TxnId) -> Self {
        let mut data = [0u8; BODY_SIZE];

        data[8..12].copy_from_slice(&page_id.as_u32().to_le_bytes());
        data[12..14].copy_from_slice(&(page_type as u16).to_le_bytes());
        data[14..16].copy_from_slice(&PageFlags::NONE.0.to_le_bytes());
        data[16..24].copy_from_slice(&txn_id.as_u64().to_le_bytes());
        data[24..26].copy_from_slice(&0u16.to_le_bytes());
        data[26..28].copy_from_slice(&(BODY_SIZE as u16).to_le_bytes());
        data[28..30].copy_from_slice(&(USABLE_SIZE as u16).to_le_bytes());
        data[30..34].copy_from_slice(&0u32.to_le_bytes());

        Self { data }
    }

    /// Validate a disk page before exposing unchecked cell access to normal
    /// readers. Locally constructed writer pages already satisfy these layout
    /// invariants; integrity tools may retain the raw bytes for detailed errors.
    /// Call after authentication, decryption, and checksum verification.
    pub fn validate_for_read(&self, expected_id: PageId) -> citadel_core::Result<()> {
        use citadel_core::Error;
        if self.page_id() != expected_id {
            return Err(Error::DatabaseCorrupted);
        }
        match self.page_type() {
            Some(PageType::Leaf) => {
                crate::leaf_node::validate_cells_checked(self)
                    .map_err(|_| Error::DatabaseCorrupted)?;
            }
            Some(PageType::Branch) => {
                crate::branch_node::validate_cells_checked(self)
                    .map_err(|_| Error::DatabaseCorrupted)?;
            }
            // Their dedicated chain decoders validate payload and link bounds.
            Some(PageType::Overflow | PageType::PendingFree) => {}
            None => return Err(Error::InvalidPageType(self.page_type_raw(), expected_id)),
        }
        Ok(())
    }

    pub fn from_bytes(data: [u8; BODY_SIZE]) -> Self {
        Self { data }
    }

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

    pub fn merkle_hash(&self) -> [u8; MERKLE_HASH_SIZE] {
        let end = MERKLE_HASH_OFFSET + MERKLE_HASH_SIZE;
        self.data[MERKLE_HASH_OFFSET..end].try_into().unwrap()
    }

    pub fn set_merkle_hash(&mut self, hash: &[u8; MERKLE_HASH_SIZE]) {
        let end = MERKLE_HASH_OFFSET + MERKLE_HASH_SIZE;
        self.data[MERKLE_HASH_OFFSET..end].copy_from_slice(hash);
    }

    #[inline]
    fn cell_ptr_offset(i: u16) -> usize {
        PAGE_HEADER_SIZE + (i as usize) * 2
    }

    pub fn cell_offset(&self, i: u16) -> u16 {
        let off = Self::cell_ptr_offset(i);
        u16::from_le_bytes(self.data[off..off + 2].try_into().unwrap())
    }

    pub fn set_cell_offset(&mut self, i: u16, offset: u16) {
        let off = Self::cell_ptr_offset(i);
        self.data[off..off + 2].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn cell_data(&self, offset: u16, len: usize) -> &[u8] {
        let start = offset as usize;
        &self.data[start..start + len]
    }

    pub fn cell_data_mut(&mut self, offset: u16, len: usize) -> &mut [u8] {
        let start = offset as usize;
        &mut self.data[start..start + len]
    }

    pub fn available_space(&self) -> usize {
        let ptrs_end = PAGE_HEADER_SIZE + (self.num_cells() as usize) * 2;
        let cell_start = self.cell_area_start() as usize;
        if cell_start <= ptrs_end + 2 {
            0
        } else {
            cell_start - ptrs_end - 2 // 2 bytes for new cell pointer
        }
    }

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

    /// Insert cell at `idx`, shifting pointers right. Returns data offset.
    pub fn insert_cell_at(&mut self, idx: u16, cell_data: &[u8]) -> Option<u16> {
        let cell_len = cell_data.len();
        if self.available_space() < cell_len {
            return None;
        }

        let new_start = self.cell_area_start() as usize - cell_len;
        self.data[new_start..new_start + cell_len].copy_from_slice(cell_data);
        self.set_cell_area_start(new_start as u16);

        let n = self.num_cells();
        if idx < n {
            let src_start = Self::cell_ptr_offset(idx);
            let src_end = Self::cell_ptr_offset(n);
            self.data.copy_within(src_start..src_end, src_start + 2);
        }

        self.set_cell_offset(idx, new_start as u16);
        self.set_num_cells(n + 1);

        let free = self.free_space() as usize - cell_len - 2;
        self.set_free_space(free as u16);

        Some(new_start as u16)
    }

    pub fn insert_cell_direct<F>(&mut self, idx: u16, cell_len: usize, write_fn: F) -> Option<u16>
    where
        F: FnOnce(&mut [u8]),
    {
        if self.available_space() < cell_len {
            return None;
        }

        let new_start = self.cell_area_start() as usize - cell_len;
        write_fn(&mut self.data[new_start..new_start + cell_len]);
        self.set_cell_area_start(new_start as u16);

        let n = self.num_cells();
        if idx < n {
            let src_start = Self::cell_ptr_offset(idx);
            let src_end = Self::cell_ptr_offset(n);
            self.data.copy_within(src_start..src_end, src_start + 2);
        }

        self.set_cell_offset(idx, new_start as u16);
        self.set_num_cells(n + 1);

        let free = self.free_space() as usize - cell_len - 2;
        self.set_free_space(free as u16);

        Some(new_start as u16)
    }

    /// Delete cell at `idx`. Data becomes a hole until compact.
    pub fn delete_cell_at(&mut self, idx: u16, cell_len: usize) {
        let n = self.num_cells();
        assert!(idx < n, "delete_cell_at: index out of bounds");

        if idx + 1 < n {
            let src_start = Self::cell_ptr_offset(idx + 1);
            let src_end = Self::cell_ptr_offset(n);
            let dst_start = Self::cell_ptr_offset(idx);
            self.data.copy_within(src_start..src_end, dst_start);
        }

        self.set_num_cells(n - 1);
        let free = self.free_space() as usize + cell_len + 2;
        self.set_free_space(free as u16);
    }

    /// Clear all cells and re-insert from `cells`.
    pub fn rebuild_cells(&mut self, cells: &[&[u8]]) {
        self.set_num_cells(0);
        self.set_cell_area_start(BODY_SIZE as u16);
        self.set_free_space(USABLE_SIZE as u16);

        for cell_data in cells {
            self.write_cell(cell_data)
                .expect("rebuild_cells: cell data should fit");
        }
    }

    /// Pack live cells in logical order without allocating per-cell buffers.
    /// The cell format supplies lengths; the page owns offsets and free space.
    #[cold]
    #[inline(never)]
    pub(crate) fn compact_cells(&mut self, cell_size: impl Fn(&Page, u16) -> usize) {
        let source = self.clone();
        self.rebuild_cells(&[]);
        for index in 0..source.num_cells() {
            let offset = source.cell_offset(index);
            self.write_cell(source.cell_data(offset, cell_size(&source, index)))
                .expect("compact_cells: existing cells should fit");
        }
    }

    pub fn compute_checksum(&self) -> u64 {
        xxhash_rust::xxh64::xxh64(&self.data[CHECKSUM_SIZE..], 0)
    }

    pub fn update_checksum(&mut self) {
        let cs = self.compute_checksum();
        self.data[0..CHECKSUM_SIZE].copy_from_slice(&cs.to_le_bytes());
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum() == self.compute_checksum()
    }

    pub fn as_bytes(&self) -> &[u8; BODY_SIZE] {
        &self.data
    }

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
#[path = "page_tests.rs"]
mod tests;
