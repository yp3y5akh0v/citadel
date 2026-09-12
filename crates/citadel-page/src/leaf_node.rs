//! Leaf node ops. Cell: `[key_len:u16][val_len:u32][key][val_type:u8][value]`

use crate::page::{checked_cell_offsets, validate_cell_layout, CellDecodeError, CellSpan, Page};
use citadel_core::types::{PageId, PageType, ValueType};
use citadel_core::{BODY_SIZE, MAX_VALUE_SIZE};

/// Fixed-size fields in a leaf cell (key_len: 2 + val_len: 4 + val_type: 1).
const LEAF_CELL_FIXED: usize = 7;

#[derive(Debug, Clone, Copy)]
pub struct LeafCell<'a> {
    pub key: &'a [u8],
    pub val_type: ValueType,
    pub value: &'a [u8],
}

/// Overflow value metadata stored in leaf cell when val_type == Overflow.
pub struct OverflowRef {
    pub first_page: PageId,
    pub total_len: u32,
}

impl OverflowRef {
    pub fn from_bytes(data: &[u8]) -> Self {
        assert!(data.len() >= 8);
        Self {
            first_page: PageId(u32::from_le_bytes(data[0..4].try_into().unwrap())),
            total_len: u32::from_le_bytes(data[4..8].try_into().unwrap()),
        }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&self.first_page.as_u32().to_le_bytes());
        buf[4..8].copy_from_slice(&self.total_len.to_le_bytes());
        buf
    }
}

pub fn read_cell(page: &Page, i: u16) -> LeafCell<'_> {
    let offset = page.cell_offset(i) as usize;
    let key_len = u16::from_le_bytes(page.data[offset..offset + 2].try_into().unwrap()) as usize;
    let val_len =
        u32::from_le_bytes(page.data[offset + 2..offset + 6].try_into().unwrap()) as usize;
    let key = &page.data[offset + 6..offset + 6 + key_len];
    let val_type_byte = page.data[offset + 6 + key_len];
    let val_type = ValueType::from_u8(val_type_byte).unwrap_or(ValueType::Inline);
    let value = &page.data[offset + 6 + key_len + 1..offset + 6 + key_len + 1 + val_len];
    LeafCell {
        key,
        val_type,
        value,
    }
}

/// Decode and validate every leaf cell without unchecked indexing. Rejects unknown
/// value kinds rather than treating them as inline data, enforces key ordering, and
/// proves overflow references have their fixed width and a usable page and length.
pub fn read_cells_checked(page: &Page) -> Result<Vec<LeafCell<'_>>, CellDecodeError> {
    if page.page_type() != Some(PageType::Leaf) {
        return Err(CellDecodeError::new(format!(
            "checked leaf decode received page type {}",
            page.page_type_raw()
        )));
    }

    let offsets = checked_cell_offsets(page)?;
    let mut cells = Vec::with_capacity(offsets.len());
    let mut spans = Vec::with_capacity(offsets.len());
    for (index, offset) in offsets.enumerate() {
        let fixed_end = offset.checked_add(6).ok_or_else(|| {
            CellDecodeError::new(format!("leaf cell {index} header length overflows"))
        })?;
        if fixed_end > BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} header ends at {fixed_end}, beyond page body {BODY_SIZE}"
            )));
        }
        let key_len = u16::from_le_bytes([page.data[offset], page.data[offset + 1]]) as usize;
        let value_len = u32::from_le_bytes([
            page.data[offset + 2],
            page.data[offset + 3],
            page.data[offset + 4],
            page.data[offset + 5],
        ]) as usize;
        let value_type_offset = fixed_end.checked_add(key_len).ok_or_else(|| {
            CellDecodeError::new(format!("leaf cell {index} key length overflows"))
        })?;
        if value_type_offset >= BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} key ends at {value_type_offset}, leaving no value type byte"
            )));
        }
        let value_start = value_type_offset + 1;
        let end = value_start.checked_add(value_len).ok_or_else(|| {
            CellDecodeError::new(format!("leaf cell {index} value length overflows"))
        })?;
        if end > BODY_SIZE {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} value ends at {end}, beyond page body {BODY_SIZE}"
            )));
        }
        let raw_type = page.data[value_type_offset];
        let val_type = ValueType::from_u8(raw_type).ok_or_else(|| {
            CellDecodeError::new(format!(
                "leaf cell {index} has invalid value type {raw_type}"
            ))
        })?;
        spans.push(CellSpan {
            index,
            start: offset,
            end,
        });
        cells.push(LeafCell {
            key: &page.data[fixed_end..value_type_offset],
            val_type,
            value: &page.data[value_start..end],
        });
    }
    validate_cell_layout(page, &mut spans)?;

    if let Some((index, _)) = cells
        .windows(2)
        .enumerate()
        .find(|(_, pair)| pair[0].key >= pair[1].key)
    {
        return Err(CellDecodeError::new(format!(
            "leaf keys {index} and {} are not strictly ordered",
            index + 1
        )));
    }

    for (index, cell) in cells.iter().enumerate() {
        if cell.val_type != ValueType::Overflow {
            continue;
        }
        if cell.value.len() != 8 {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} overflow reference has {} bytes instead of 8",
                cell.value.len()
            )));
        }
        let reference = OverflowRef::from_bytes(cell.value);
        if reference.first_page.as_u32() == 0 || !reference.first_page.is_valid() {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} overflow reference has invalid first page {}",
                reference.first_page
            )));
        }
        if reference.total_len as usize > MAX_VALUE_SIZE {
            return Err(CellDecodeError::new(format!(
                "leaf cell {index} overflow length {} exceeds {MAX_VALUE_SIZE}",
                reference.total_len
            )));
        }
    }

    Ok(cells)
}

/// Get the total byte size of a leaf cell.
pub fn cell_size(key_len: usize, val_len: usize) -> usize {
    LEAF_CELL_FIXED + key_len + val_len
}

pub fn get_cell_size(page: &Page, i: u16) -> usize {
    let offset = page.cell_offset(i) as usize;
    let key_len = u16::from_le_bytes(page.data[offset..offset + 2].try_into().unwrap()) as usize;
    let val_len =
        u32::from_le_bytes(page.data[offset + 2..offset + 6].try_into().unwrap()) as usize;
    LEAF_CELL_FIXED + key_len + val_len
}

/// Build a leaf cell into a byte buffer.
pub fn build_cell(key: &[u8], val_type: ValueType, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(LEAF_CELL_FIXED + key.len() + value.len());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.push(val_type as u8);
    buf.extend_from_slice(value);
    buf
}

/// Read the raw cell bytes at index `i` (for rebuilding pages during CoW/split).
pub fn read_cell_bytes(page: &Page, i: u16) -> Vec<u8> {
    let size = get_cell_size(page, i);
    let offset = page.cell_offset(i) as usize;
    page.data[offset..offset + size].to_vec()
}

/// Binary search for key. Ok(index) if found, Err(index) for insertion point.
pub fn search(page: &Page, search_key: &[u8]) -> Result<u16, u16> {
    let n = page.num_cells();
    let mut lo = 0u16;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = read_cell(page, mid);
        match search_key.cmp(cell.key) {
            std::cmp::Ordering::Less => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
            std::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    Err(lo)
}

#[inline]
fn write_cell_into(slot: &mut [u8], key: &[u8], val_type: ValueType, value: &[u8]) {
    slot[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    slot[2..6].copy_from_slice(&(value.len() as u32).to_le_bytes());
    slot[6..6 + key.len()].copy_from_slice(key);
    slot[6 + key.len()] = val_type as u8;
    slot[7 + key.len()..7 + key.len() + value.len()].copy_from_slice(value);
}

/// In-place value overwrite if same length. Returns false if sizes differ.
pub fn update_value_in_place(page: &mut Page, idx: u16, val_type: ValueType, value: &[u8]) -> bool {
    let offset = page.cell_offset(idx) as usize;
    let key_len = u16::from_le_bytes(page.data[offset..offset + 2].try_into().unwrap()) as usize;
    let old_val_len =
        u32::from_le_bytes(page.data[offset + 2..offset + 6].try_into().unwrap()) as usize;
    if value.len() != old_val_len {
        return false;
    }
    let val_start = offset + 6 + key_len;
    page.data[val_start] = val_type as u8;
    page.data[val_start + 1..val_start + 1 + value.len()].copy_from_slice(value);
    true
}

/// Caller must guarantee `key > last_cell.key` (strictly greater than current
/// max). Skips the binary search since the insert position is always the end.
pub fn insert_append_direct(
    page: &mut Page,
    key: &[u8],
    val_type: ValueType,
    value: &[u8],
) -> bool {
    let pos = page.num_cells();
    let total = LEAF_CELL_FIXED + key.len() + value.len();

    if page
        .insert_cell_direct(pos, total, |slot| {
            write_cell_into(slot, key, val_type, value);
        })
        .is_some()
    {
        return true;
    }

    let cell_len_with_ptr = total + 2;
    if (page.free_space() as usize) >= cell_len_with_ptr {
        compact_page(page);
        return page
            .insert_cell_direct(pos, total, |slot| {
                write_cell_into(slot, key, val_type, value);
            })
            .is_some();
    }

    false
}

pub fn insert_direct(page: &mut Page, key: &[u8], val_type: ValueType, value: &[u8]) -> bool {
    let pos = match search(page, key) {
        Ok(idx) => {
            let old_size = get_cell_size(page, idx);
            page.delete_cell_at(idx, old_size);
            idx
        }
        Err(idx) => idx,
    };

    let total = LEAF_CELL_FIXED + key.len() + value.len();

    if page
        .insert_cell_direct(pos, total, |slot| {
            write_cell_into(slot, key, val_type, value);
        })
        .is_some()
    {
        return true;
    }

    let cell_len_with_ptr = total + 2;
    if (page.free_space() as usize) >= cell_len_with_ptr {
        compact_page(page);
        return page
            .insert_cell_direct(pos, total, |slot| {
                write_cell_into(slot, key, val_type, value);
            })
            .is_some();
    }

    false
}

pub fn replace_at(
    page: &mut Page,
    idx: u16,
    key: &[u8],
    val_type: ValueType,
    value: &[u8],
) -> bool {
    if update_value_in_place(page, idx, val_type, value) {
        return true;
    }

    let old_size = get_cell_size(page, idx);
    page.delete_cell_at(idx, old_size);
    let total = LEAF_CELL_FIXED + key.len() + value.len();
    if page
        .insert_cell_direct(idx, total, |slot| {
            write_cell_into(slot, key, val_type, value)
        })
        .is_some()
    {
        return true;
    }

    let cell_len_with_ptr = total + 2;
    if (page.free_space() as usize) >= cell_len_with_ptr {
        compact_page(page);
        return page
            .insert_cell_direct(idx, total, |slot| {
                write_cell_into(slot, key, val_type, value)
            })
            .is_some();
    }

    false
}

/// Insert key-value at sorted position. Returns false if not enough space.
pub fn insert(page: &mut Page, key: &[u8], val_type: ValueType, value: &[u8]) -> bool {
    let pos = match search(page, key) {
        Ok(idx) => {
            // Key exists - update in place by deleting old and re-inserting
            let old_size = get_cell_size(page, idx);
            page.delete_cell_at(idx, old_size);
            idx
        }
        Err(idx) => idx,
    };

    let cell = build_cell(key, val_type, value);
    if page.insert_cell_at(pos, &cell).is_some() {
        return true;
    }

    // Compact fragmented space and retry
    let cell_len_with_ptr = cell.len() + 2;
    if (page.free_space() as usize) >= cell_len_with_ptr {
        compact_page(page);
        return page.insert_cell_at(pos, &cell).is_some();
    }

    false
}

/// Compact a leaf page by rebuilding its cell data, eliminating holes.
fn compact_page(page: &mut Page) {
    let n = page.num_cells();
    let cells: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let offset = page.cell_offset(i) as usize;
            let sz = get_cell_size(page, i);
            page.data[offset..offset + sz].to_vec()
        })
        .collect();
    let refs: Vec<&[u8]> = cells.iter().map(|c| c.as_slice()).collect();
    page.rebuild_cells(&refs);
}

/// Delete a cell at an index already resolved in this page.
///
/// The index remains valid across a page clone, but not across a cell insertion
/// or deletion. Panics if the index is outside the page's current cells.
#[inline]
pub fn delete_at(page: &mut Page, idx: u16) {
    assert!(idx < page.num_cells(), "delete_at: index out of bounds");
    let cell_sz = get_cell_size(page, idx);
    page.delete_cell_at(idx, cell_sz);
}

/// Delete a key. Returns true if found and deleted.
pub fn delete(page: &mut Page, key: &[u8]) -> bool {
    match search(page, key) {
        Ok(idx) => {
            delete_at(page, idx);
            true
        }
        Err(_) => false,
    }
}

/// Split at midpoint. Returns (separator_key, right_cells).
pub fn split(page: &Page) -> (Vec<u8>, Vec<Vec<u8>>) {
    let n = page.num_cells() as usize;
    let split_point = n / 2;

    // Separator = first key of right half
    let sep_cell = read_cell(page, split_point as u16);
    let sep_key = sep_cell.key.to_vec();

    let mut right_cells = Vec::with_capacity(n - split_point);
    for i in split_point..n {
        right_cells.push(read_cell_bytes(page, i as u16));
    }

    (sep_key, right_cells)
}

#[cfg(test)]
#[path = "leaf_node_tests.rs"]
mod tests;
