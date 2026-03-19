//! Branch (interior) node operations for the B+ tree.
//!
//! Branch cell format: `[child: u32][key_len: u16][key_data: var]`
//! Total cell size: 6 + key_len
//!
//! A branch node with n cells has n+1 children:
//! - cell[i].child handles keys where key[i-1] <= k < key[i] (key[-1] = -∞)
//! - right_child handles keys where key[n-1] <= k

use crate::page::Page;
use citadel_core::types::PageId;

/// Size of fixed fields in a branch cell (child: 4 + key_len: 2).
const BRANCH_CELL_FIXED: usize = 6;

/// A parsed branch cell.
pub struct BranchCell<'a> {
    pub child: PageId,
    pub key: &'a [u8],
}

/// Read branch cell at index `i`.
pub fn read_cell(page: &Page, i: u16) -> BranchCell<'_> {
    let offset = page.cell_offset(i) as usize;
    let child = u32::from_le_bytes(page.data[offset..offset + 4].try_into().unwrap());
    let key_len =
        u16::from_le_bytes(page.data[offset + 4..offset + 6].try_into().unwrap()) as usize;
    let key = &page.data[offset + 6..offset + 6 + key_len];
    BranchCell {
        child: PageId(child),
        key,
    }
}

/// Get the total byte size of a branch cell on disk.
pub fn cell_size(key_len: usize) -> usize {
    BRANCH_CELL_FIXED + key_len
}

/// Build a branch cell into a byte buffer.
pub fn build_cell(child: PageId, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(BRANCH_CELL_FIXED + key.len());
    buf.extend_from_slice(&child.as_u32().to_le_bytes());
    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Get the cell byte size for the cell at index `i`.
pub fn get_cell_size(page: &Page, i: u16) -> usize {
    let offset = page.cell_offset(i) as usize;
    let key_len =
        u16::from_le_bytes(page.data[offset + 4..offset + 6].try_into().unwrap()) as usize;
    BRANCH_CELL_FIXED + key_len
}

/// Binary search for the child to descend to for a given search key.
///
/// Returns the PageId of the child whose subtree contains the search key.
/// For keys < cell[0].key → cell[0].child
/// For keys >= cell[n-1].key → right_child
pub fn search(page: &Page, search_key: &[u8]) -> PageId {
    let n = page.num_cells();
    for i in 0..n {
        let cell = read_cell(page, i);
        if search_key < cell.key {
            return cell.child;
        }
    }
    page.right_child()
}

/// Binary search returning the child index (0..=num_cells).
/// Index num_cells means right_child.
pub fn search_child_index(page: &Page, search_key: &[u8]) -> usize {
    let n = page.num_cells() as usize;
    // Binary search for the first key > search_key
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = read_cell(page, mid as u16);
        if search_key < cell.key {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Get the child PageId at a given child index (0..=num_cells).
pub fn get_child(page: &Page, child_idx: usize) -> PageId {
    let n = page.num_cells() as usize;
    if child_idx < n {
        read_cell(page, child_idx as u16).child
    } else {
        page.right_child()
    }
}

/// Insert a separator key and right child into the branch page.
///
/// After a child at `child_idx` splits, we need to insert the separator.
/// The left child stays at `child_idx`, and we insert (right_child, sep_key).
///
/// For child_idx < num_cells:
///   - old cell[child_idx] had (old_child, old_key)
///   - We set cell[child_idx] = (left_child, sep_key)
///   - We insert new cell (right_child, old_key) at child_idx + 1
///
/// For child_idx == num_cells (right_child split):
///   - We append cell (left_child, sep_key)
///   - We set right_child = right_child
///
/// Returns false if not enough space.
pub fn insert_separator(
    page: &mut Page,
    child_idx: usize,
    left_child: PageId,
    sep_key: &[u8],
    right_child: PageId,
) -> bool {
    let n = page.num_cells() as usize;

    if child_idx < n {
        // Read old key before modifying
        let old_cell = read_cell(page, child_idx as u16);
        let old_key = old_cell.key.to_vec();
        let old_cell_size = get_cell_size(page, child_idx as u16);

        // Build the new cell that replaces cell[child_idx]
        let new_cell = build_cell(left_child, sep_key);
        // Build the cell to insert at child_idx + 1
        let insert_cell = build_cell(right_child, &old_key);

        // Check if we have space for the size difference + new cell + pointer
        // We need: new_cell.len - old_cell_size + insert_cell.len + 2 (for new ptr)
        if page.available_space() + old_cell_size < new_cell.len() + insert_cell.len() + 2 {
            // Not enough space — caller must split this branch
            return false;
        }

        // Strategy: rebuild all cells to avoid fragmentation issues.
        // Collect all cells, modify, and rebuild.
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(n + 1);
        for i in 0..n {
            if i == child_idx {
                cells.push(new_cell.clone());
            } else {
                let c = read_cell(page, i as u16);
                cells.push(build_cell(c.child, c.key));
            }
        }
        cells.insert(child_idx + 1, insert_cell);

        let rc = page.right_child();
        let cell_refs: Vec<&[u8]> = cells.iter().map(|c| c.as_slice()).collect();
        page.rebuild_cells(&cell_refs);
        page.set_right_child(rc);
    } else {
        // right_child split
        let new_cell = build_cell(left_child, sep_key);
        if page.available_space() < new_cell.len() {
            return false;
        }
        page.write_cell(&new_cell);
        page.set_right_child(right_child);
    }

    true
}

/// Split a branch page. Returns (separator_key, right_page_cells, right_child).
///
/// The split point is at num_cells / 2.
/// - Left page keeps cells [0..split_point], right_child = promoted_cell.child
/// - Promoted: key = cells[split_point].key
/// - Right page gets cells [split_point+1..n], right_child = old right_child
pub fn split(page: &Page) -> (Vec<u8>, Vec<Vec<u8>>, PageId, PageId) {
    let n = page.num_cells() as usize;
    let split_point = n / 2;

    // Read the promoted cell
    let promoted = read_cell(page, split_point as u16);
    let sep_key = promoted.key.to_vec();
    let promoted_child = promoted.child; // becomes left page's right_child

    // Collect right page cells
    let mut right_cells = Vec::with_capacity(n - split_point - 1);
    for i in (split_point + 1)..n {
        let c = read_cell(page, i as u16);
        right_cells.push(build_cell(c.child, c.key));
    }

    let old_right_child = page.right_child(); // becomes right page's right_child

    (sep_key, right_cells, promoted_child, old_right_child)
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_core::types::{PageType, TxnId};

    fn make_branch_page(keys: &[&[u8]], children: &[PageId], right_child: PageId) -> Page {
        assert_eq!(keys.len(), children.len());
        let mut page = Page::new(PageId(0), PageType::Branch, TxnId(1));
        for (key, child) in keys.iter().zip(children.iter()) {
            let cell = build_cell(*child, key);
            page.write_cell(&cell).unwrap();
        }
        page.set_right_child(right_child);
        page
    }

    #[test]
    fn read_write_branch_cell() {
        let mut page = Page::new(PageId(0), PageType::Branch, TxnId(1));
        let cell = build_cell(PageId(5), b"hello");
        page.write_cell(&cell).unwrap();

        let parsed = read_cell(&page, 0);
        assert_eq!(parsed.child, PageId(5));
        assert_eq!(parsed.key, b"hello");
    }

    #[test]
    fn search_finds_correct_child() {
        let page = make_branch_page(
            &[b"cat", b"dog", b"fox"],
            &[PageId(1), PageId(2), PageId(3)],
            PageId(4),
        );

        assert_eq!(search(&page, b"ant"), PageId(1)); // < "cat"
        assert_eq!(search(&page, b"cat"), PageId(2)); // >= "cat", < "dog"
        assert_eq!(search(&page, b"cow"), PageId(2)); // >= "cat", < "dog"
        assert_eq!(search(&page, b"dog"), PageId(3)); // >= "dog", < "fox"
        assert_eq!(search(&page, b"elk"), PageId(3)); // >= "dog", < "fox"
        assert_eq!(search(&page, b"fox"), PageId(4)); // >= "fox"
        assert_eq!(search(&page, b"zebra"), PageId(4)); // >= "fox"
    }

    #[test]
    fn search_child_index_binary_search() {
        let page = make_branch_page(
            &[b"b", b"d", b"f", b"h"],
            &[PageId(1), PageId(2), PageId(3), PageId(4)],
            PageId(5),
        );

        assert_eq!(search_child_index(&page, b"a"), 0);
        assert_eq!(search_child_index(&page, b"b"), 1);
        assert_eq!(search_child_index(&page, b"c"), 1);
        assert_eq!(search_child_index(&page, b"d"), 2);
        assert_eq!(search_child_index(&page, b"g"), 3);
        assert_eq!(search_child_index(&page, b"h"), 4);
        assert_eq!(search_child_index(&page, b"z"), 4);
    }

    #[test]
    fn insert_separator_middle() {
        let mut page = make_branch_page(&[b"b", b"f"], &[PageId(1), PageId(2)], PageId(3));

        // Child at index 1 (PageId(2), handles [b, f)) splits with separator "d"
        let ok = insert_separator(
            &mut page,
            1,
            PageId(20), // left child (CoW'd PageId(2))
            b"d",       // separator
            PageId(21), // right child (new page)
        );
        assert!(ok);

        assert_eq!(page.num_cells(), 3);
        // Verify: [(1, "b"), (20, "d"), (21, "f")], rc=3
        let c0 = read_cell(&page, 0);
        assert_eq!(c0.child, PageId(1));
        assert_eq!(c0.key, b"b");

        let c1 = read_cell(&page, 1);
        assert_eq!(c1.child, PageId(20));
        assert_eq!(c1.key, b"d");

        let c2 = read_cell(&page, 2);
        assert_eq!(c2.child, PageId(21));
        assert_eq!(c2.key, b"f");

        assert_eq!(page.right_child(), PageId(3));
    }

    #[test]
    fn insert_separator_right_child() {
        let mut page = make_branch_page(&[b"b"], &[PageId(1)], PageId(2));

        // right_child (PageId(2)) splits with separator "e"
        let ok = insert_separator(
            &mut page,
            1,          // child_idx == num_cells means right_child
            PageId(20), // left (CoW'd old right_child)
            b"e",       // separator
            PageId(21), // new right_child
        );
        assert!(ok);

        assert_eq!(page.num_cells(), 2);
        let c1 = read_cell(&page, 1);
        assert_eq!(c1.child, PageId(20));
        assert_eq!(c1.key, b"e");
        assert_eq!(page.right_child(), PageId(21));
    }

    #[test]
    fn split_branch() {
        let page = make_branch_page(
            &[b"b", b"d", b"f", b"h", b"j"],
            &[PageId(1), PageId(2), PageId(3), PageId(4), PageId(5)],
            PageId(6),
        );

        let (sep_key, right_cells, left_rc, right_rc) = split(&page);

        // Split point = 5/2 = 2, so cell[2] (f) is promoted
        assert_eq!(sep_key, b"f");
        assert_eq!(left_rc, PageId(3)); // promoted cell's child
        assert_eq!(right_rc, PageId(6)); // old right_child

        // Right cells: cells [3..5] = [(4, "h"), (5, "j")]
        assert_eq!(right_cells.len(), 2);
    }
}
