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
fn checked_reader_accepts_a_well_formed_leaf() {
    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut page, b"alpha", ValueType::Inline, b"one");
    insert(&mut page, b"beta", ValueType::Tombstone, b"");

    let cells = read_cells_checked(&page).unwrap();
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0].key, b"alpha");
    assert_eq!(cells[1].val_type, ValueType::Tombstone);
}

#[test]
fn checked_reader_rejects_invalid_value_type_without_defaulting_to_inline() {
    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut page, b"key", ValueType::Inline, b"value");
    let offset = page.cell_offset(0) as usize;
    page.data[offset + 6 + b"key".len()] = u8::MAX;

    let decoded = std::panic::catch_unwind(|| read_cells_checked(&page));
    assert!(decoded.is_ok(), "checked decoding must not unwind");
    let error = decoded.unwrap().unwrap_err();
    assert!(error.to_string().contains("invalid value type"));
}

#[test]
fn checked_reader_rejects_truncated_and_overlapping_cells() {
    let mut truncated = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut truncated, b"key", ValueType::Inline, b"value");
    let offset = truncated.cell_offset(0) as usize;
    truncated.data[offset + 2..offset + 6].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(read_cells_checked(&truncated)
        .unwrap_err()
        .to_string()
        .contains("value ends"));

    let mut overlapping = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut overlapping, b"alpha", ValueType::Inline, b"one");
    insert(&mut overlapping, b"beta", ValueType::Inline, b"two");
    overlapping.set_cell_offset(1, overlapping.cell_offset(0));
    assert!(read_cells_checked(&overlapping)
        .unwrap_err()
        .to_string()
        .contains("overlap"));
}

#[test]
fn checked_reader_rejects_free_space_and_key_order_corruption() {
    let mut free_space = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut free_space, b"key", ValueType::Inline, b"value");
    free_space.set_free_space(free_space.free_space() + 1);
    assert!(read_cells_checked(&free_space)
        .unwrap_err()
        .to_string()
        .contains("free-space accounting"));

    let mut unordered = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    unordered.write_cell(&build_cell(b"beta", ValueType::Inline, b"two"));
    unordered.write_cell(&build_cell(b"alpha", ValueType::Inline, b"one"));
    assert!(read_cells_checked(&unordered)
        .unwrap_err()
        .to_string()
        .contains("not strictly ordered"));
}

#[test]
fn checked_reader_rejects_malformed_overflow_references() {
    let mut wrong_width = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    insert(&mut wrong_width, b"key", ValueType::Overflow, &[0u8; 7]);
    assert!(read_cells_checked(&wrong_width)
        .unwrap_err()
        .to_string()
        .contains("instead of 8"));

    let mut zero_page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    let reference = OverflowRef {
        first_page: PageId(0),
        total_len: 4096,
    };
    insert(
        &mut zero_page,
        b"key",
        ValueType::Overflow,
        &reference.to_bytes(),
    );
    assert!(read_cells_checked(&zero_page)
        .unwrap_err()
        .to_string()
        .contains("invalid first page"));
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

#[test]
fn delete_at_preserves_remaining_cells_and_fragment_accounting() {
    let overflow = OverflowRef {
        first_page: PageId(42),
        total_len: 4096,
    }
    .to_bytes();
    let entries = [
        (b"a".as_slice(), ValueType::Inline, b"first".as_slice()),
        (b"b".as_slice(), ValueType::Overflow, overflow.as_slice()),
        (
            b"c".as_slice(),
            ValueType::Inline,
            b"last and longer".as_slice(),
        ),
    ];
    for removed in 0..entries.len() {
        let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
        for &(key, kind, value) in &entries {
            assert!(insert(&mut page, key, kind, value));
        }
        let cell_area_start = page.cell_area_start();
        delete_at(&mut page, removed as u16);
        assert_eq!(page.cell_area_start(), cell_area_start);
        let cells = read_cells_checked(&page).unwrap();
        let actual: Vec<_> = cells
            .iter()
            .map(|cell| (cell.key, cell.val_type, cell.value))
            .collect();
        let expected: Vec<_> = entries
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| (i != removed).then_some(*entry))
            .collect();
        assert_eq!(actual, expected);
        assert!(!delete(&mut page, entries[removed].0));

        // A later insertion must coexist with the hole and shifted pointers.
        assert!(insert(&mut page, b"bb", ValueType::Inline, &[7; 128]));
        let cells = read_cells_checked(&page).unwrap();
        assert_eq!(cells.len(), 3);
        assert_eq!(cells.iter().filter(|cell| cell.key == b"bb").count(), 1);
    }
}
