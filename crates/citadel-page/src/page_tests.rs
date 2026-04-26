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
