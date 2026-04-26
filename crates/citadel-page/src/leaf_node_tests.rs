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

    assert!(insert(&mut page, b"dog", ValueType::Inline, b"woof"));
    assert!(insert(&mut page, b"ant", ValueType::Inline, b"tiny"));
    assert!(insert(&mut page, b"cat", ValueType::Inline, b"meow"));
    assert!(insert(&mut page, b"fox", ValueType::Inline, b"sly"));

    assert_eq!(page.num_cells(), 4);

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
