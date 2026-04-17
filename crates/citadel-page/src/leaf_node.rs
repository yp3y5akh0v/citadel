//! Leaf node ops. Cell: `[key_len:u16][val_len:u32][key][val_type:u8][value]`

use crate::page::Page;
use citadel_core::types::{PageId, ValueType};

/// Fixed-size fields in a leaf cell (key_len: 2 + val_len: 4 + val_type: 1).
const LEAF_CELL_FIXED: usize = 7;

/// A parsed leaf cell.
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

/// Read leaf cell at index `i`.
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

/// Get the total byte size of a leaf cell.
pub fn cell_size(key_len: usize, val_len: usize) -> usize {
    LEAF_CELL_FIXED + key_len + val_len
}

/// Get the cell byte size for the cell at index `i`.
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

/// Delete a key. Returns true if found and deleted.
pub fn delete(page: &mut Page, key: &[u8]) -> bool {
    match search(page, key) {
        Ok(idx) => {
            let cell_sz = get_cell_size(page, idx);
            page.delete_cell_at(idx, cell_sz);
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

    // Collect right cells
    let mut right_cells = Vec::with_capacity(n - split_point);
    for i in split_point..n {
        right_cells.push(read_cell_bytes(page, i as u16));
    }

    (sep_key, right_cells)
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_core::types::{PageType, TxnId};

    #[test]
    fn read_write_leaf_cell() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        let cell = build_cell(b"hello", ValueType::Inline, b"world");
        page.write_cell(&cell).unwrap();

        let parsed = read_cell(&page, 0);
        assert_eq!(parsed.key, b"hello");
        assert_eq!(parsed.val_type, ValueType::Inline);
        assert_eq!(parsed.value, b"world");
    }

    #[test]
    fn insert_maintains_sorted_order() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));

        // Insert out of order
        assert!(insert(&mut page, b"dog", ValueType::Inline, b"woof"));
        assert!(insert(&mut page, b"ant", ValueType::Inline, b"tiny"));
        assert!(insert(&mut page, b"cat", ValueType::Inline, b"meow"));
        assert!(insert(&mut page, b"fox", ValueType::Inline, b"sly"));

        assert_eq!(page.num_cells(), 4);

        // Verify sorted order
        assert_eq!(read_cell(&page, 0).key, b"ant");
        assert_eq!(read_cell(&page, 1).key, b"cat");
        assert_eq!(read_cell(&page, 2).key, b"dog");
        assert_eq!(read_cell(&page, 3).key, b"fox");
    }

    #[test]
    fn search_found_and_not_found() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        insert(&mut page, b"b", ValueType::Inline, b"2");
        insert(&mut page, b"d", ValueType::Inline, b"4");
        insert(&mut page, b"f", ValueType::Inline, b"6");

        assert_eq!(search(&page, b"b"), Ok(0));
        assert_eq!(search(&page, b"d"), Ok(1));
        assert_eq!(search(&page, b"f"), Ok(2));

        assert_eq!(search(&page, b"a"), Err(0));
        assert_eq!(search(&page, b"c"), Err(1));
        assert_eq!(search(&page, b"e"), Err(2));
        assert_eq!(search(&page, b"g"), Err(3));
    }

    #[test]
    fn insert_update_existing_key() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        insert(&mut page, b"key", ValueType::Inline, b"value1");
        insert(&mut page, b"key", ValueType::Inline, b"value2");

        assert_eq!(page.num_cells(), 1);
        let cell = read_cell(&page, 0);
        assert_eq!(cell.value, b"value2");
    }

    #[test]
    fn delete_key() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        insert(&mut page, b"a", ValueType::Inline, b"1");
        insert(&mut page, b"b", ValueType::Inline, b"2");
        insert(&mut page, b"c", ValueType::Inline, b"3");

        assert!(delete(&mut page, b"b"));
        assert_eq!(page.num_cells(), 2);
        assert_eq!(read_cell(&page, 0).key, b"a");
        assert_eq!(read_cell(&page, 1).key, b"c");

        assert!(!delete(&mut page, b"b")); // already deleted
    }

    #[test]
    fn leaf_split() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        insert(&mut page, b"a", ValueType::Inline, b"1");
        insert(&mut page, b"b", ValueType::Inline, b"2");
        insert(&mut page, b"c", ValueType::Inline, b"3");
        insert(&mut page, b"d", ValueType::Inline, b"4");

        let (sep_key, right_cells) = split(&page);

        // Split at 4/2 = 2: left keeps [a,b], right gets [c,d]
        assert_eq!(sep_key, b"c");
        assert_eq!(right_cells.len(), 2);
    }

    #[test]
    fn overflow_ref_roundtrip() {
        let oref = OverflowRef {
            first_page: PageId(42),
            total_len: 65536,
        };
        let bytes = oref.to_bytes();
        let parsed = OverflowRef::from_bytes(&bytes);
        assert_eq!(parsed.first_page, PageId(42));
        assert_eq!(parsed.total_len, 65536);
    }

    #[test]
    fn tombstone_cell() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        insert(&mut page, b"key", ValueType::Tombstone, b"");

        let cell = read_cell(&page, 0);
        assert_eq!(cell.val_type, ValueType::Tombstone);
        assert_eq!(cell.value.len(), 0);
    }

    #[test]
    fn cell_size_calculation() {
        assert_eq!(cell_size(5, 10), 7 + 5 + 10);
        assert_eq!(cell_size(2048, 1920), 7 + 2048 + 1920);
    }
}
