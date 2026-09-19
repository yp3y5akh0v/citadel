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
fn search_distinguishes_binary_prefix_keys_with_mixed_values() {
    let overflow = OverflowRef {
        first_page: PageId(42),
        total_len: 4096,
    }
    .to_bytes();
    let payload = [7u8; 1024];
    let entries = [
        (b"".as_slice(), ValueType::Inline, b"empty key".as_slice()),
        (b"\0".as_slice(), ValueType::Tombstone, b"".as_slice()),
        (b"a".as_slice(), ValueType::Overflow, overflow.as_slice()),
        (b"a\0".as_slice(), ValueType::Inline, payload.as_slice()),
        (b"alphabet".as_slice(), ValueType::Inline, b"".as_slice()),
        (b"b".as_slice(), ValueType::Tombstone, b"".as_slice()),
        (
            b"\xff\xff".as_slice(),
            ValueType::Inline,
            b"last".as_slice(),
        ),
    ];
    let mut page = Page::new(PageId(0), PageType::Leaf, TxnId(1));
    assert_eq!(search(&page, b"a"), Err(0));
    // Reverse insertion separates physical placement from logical key order.
    for &(key, kind, value) in entries.iter().rev() {
        assert!(insert_direct(&mut page, key, kind, value));
    }
    read_cells_checked(&page).unwrap();

    for (index, &(key, kind, value)) in entries.iter().enumerate() {
        assert_eq!(read_key(&page, index as u16), key);
        let found = search(&page, key).unwrap();
        assert_eq!(found, index as u16);
        let cell = read_cell(&page, found);
        assert_eq!((cell.key, cell.val_type, cell.value), (key, kind, value));
    }
    for (key, position) in [
        (b"\0\0".as_slice(), 2),
        (b"a\0\0".as_slice(), 4),
        (b"al".as_slice(), 4),
        (b"alphabet\0".as_slice(), 5),
        (b"\xff".as_slice(), 6),
        (b"\xff\xff\0".as_slice(), 7),
    ] {
        assert_eq!(search(&page, key), Err(position), "key={key:?}");
    }
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

#[test]
fn checked_vacancy_hint_preserves_order_and_existing_key_semantics() {
    let mut empty = Page::new(PageId(7), PageType::Leaf, TxnId(1));
    assert!(insert_direct_with_hint(
        &mut empty,
        0,
        b"d",
        ValueType::Inline,
        b"first"
    ));
    assert_eq!(read_cell(&empty, 0).key, b"d");

    let mut original = Page::new(PageId(7), PageType::Leaf, TxnId(1));
    assert!(insert_direct(
        &mut original,
        b"b",
        ValueType::Inline,
        b"old"
    ));
    assert!(insert_direct(
        &mut original,
        b"d",
        ValueType::Tombstone,
        b""
    ));
    assert!(insert_direct(
        &mut original,
        b"f",
        ValueType::Inline,
        b"old"
    ));
    for key in [b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
        for hint in [0, 1, 2, 3, u16::MAX] {
            let mut expected = original.clone();
            let mut hinted = original.clone();
            assert!(insert_direct(
                &mut expected,
                key,
                ValueType::Inline,
                b"new value"
            ));
            assert!(insert_direct_with_hint(
                &mut hinted,
                hint,
                key,
                ValueType::Inline,
                b"new value"
            ));
            assert_eq!(
                hinted.as_bytes(),
                expected.as_bytes(),
                "key={key:?}, hint={hint}"
            );
            let cells = read_cells_checked(&hinted).unwrap();
            assert_eq!(
                cells.len(),
                if matches!(key, b"b" | b"d" | b"f") {
                    3
                } else {
                    4
                }
            );
            let found = cells.iter().find(|cell| cell.key == key).unwrap();
            assert_eq!(found.val_type, ValueType::Inline);
            assert_eq!(found.value, b"new value");
        }
    }
}

#[test]
fn checked_vacancy_hint_compacts_holes_and_preserves_full_page_failure() {
    let mut packed = Page::new(PageId(7), PageType::Leaf, TxnId(1));
    let payload = [0x52; 64];
    let mut count = 0u16;
    while insert_append_direct(
        &mut packed,
        &(count * 2).to_be_bytes(),
        ValueType::Inline,
        &payload,
    ) {
        count += 1;
    }
    assert!(count > 80 && count < 200);

    // A truly full page must stay byte-identical when a new key cannot fit.
    let before = packed.as_bytes().to_vec();
    let absent = 3u16.to_be_bytes();
    let vacancy = search(&packed, &absent).unwrap_err();
    assert!(!insert_direct_with_hint(
        &mut packed,
        vacancy,
        &absent,
        ValueType::Inline,
        &[0; 512]
    ));
    assert_eq!(packed.as_bytes().as_slice(), before);

    for index in (0..count).step_by(2) {
        assert!(delete(&mut packed, &(index * 2).to_be_bytes()));
    }
    let larger = [0x61; 384];
    let total = cell_size(absent.len(), larger.len());
    assert!(packed.available_space() < total);
    assert!(packed.free_space() as usize >= total + 2);
    let vacancy = search(&packed, &absent).unwrap_err();
    assert!(vacancy > 0 && vacancy < packed.num_cells());
    let mut expected = packed.clone();
    assert!(insert_direct(
        &mut expected,
        &absent,
        ValueType::Inline,
        &larger
    ));
    assert!(insert_direct_with_hint(
        &mut packed,
        vacancy,
        &absent,
        ValueType::Inline,
        &larger
    ));
    assert_eq!(packed.as_bytes(), expected.as_bytes());
    let cells = read_cells_checked(&packed).unwrap();
    assert_eq!(cells.len(), count as usize / 2 + 1);
    for index in (1..count).step_by(2) {
        let index = search(&packed, &(index * 2).to_be_bytes()).unwrap();
        assert_eq!(read_cell(&packed, index).value, payload);
    }

    // A duplicate falls back even with an in-range hint. Keep the existing
    // low-level contract: replacement can remove its old cell before false.
    let oversized = vec![0x73; citadel_core::USABLE_SIZE];
    let mut expected = packed.clone();
    let mut hinted = packed;
    assert!(!insert_direct(
        &mut expected,
        &absent,
        ValueType::Inline,
        &oversized
    ));
    assert!(!insert_direct_with_hint(
        &mut hinted,
        vacancy,
        &absent,
        ValueType::Inline,
        &oversized
    ));
    assert_eq!(hinted.as_bytes(), expected.as_bytes());
    assert!(search(&hinted, &absent).is_err());
    read_cells_checked(&hinted).unwrap();
}
