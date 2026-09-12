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

#[test]
fn checked_offsets_of_an_empty_page_are_exhausted() {
    let page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
    let mut offsets = checked_cell_offsets(&page).unwrap();
    assert_eq!(offsets.len(), 0);
    assert_eq!(offsets.size_hint(), (0, Some(0)));
    assert_eq!(offsets.next(), None);
    assert_eq!(offsets.next(), None);
}

#[test]
fn checked_offsets_preserve_pointer_order_and_remaining_length() {
    let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
    let first = page.write_cell(b"first").unwrap();
    let second = page.write_cell(b"second").unwrap();
    let third = page.write_cell(b"third").unwrap();
    page.set_cell_offset(0, third);
    page.set_cell_offset(1, first);
    page.set_cell_offset(2, second);

    let mut offsets = checked_cell_offsets(&page).unwrap();
    for (remaining, expected) in [(3, third), (2, first), (1, second)] {
        assert_eq!(offsets.len(), remaining);
        assert_eq!(offsets.size_hint(), (remaining, Some(remaining)));
        assert_eq!(offsets.next(), Some(expected as usize));
    }
    assert_eq!(offsets.len(), 0);
    assert_eq!(offsets.next(), None);
}

#[test]
fn checked_offsets_reject_invalid_pointer_metadata_without_panicking() {
    let mut oversized = Page::new(PageId(1), PageType::Leaf, TxnId(1));
    oversized.set_num_cells(u16::MAX);
    let result = std::panic::catch_unwind(|| checked_cell_offsets(&oversized).map(Iterator::count));
    assert!(result
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("pointer array"));

    for cell_area_start in [PAGE_HEADER_SIZE + 1, BODY_SIZE + 1] {
        let mut page = Page::new(PageId(1), PageType::Leaf, TxnId(1));
        page.write_cell(b"cell").unwrap();
        page.set_cell_area_start(cell_area_start as u16);
        let result = std::panic::catch_unwind(|| checked_cell_offsets(&page).map(Iterator::count));
        assert!(result
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cell area starts"));
    }
}

#[test]
fn checked_offsets_validate_late_pointers_before_returning_an_iterator() {
    let mut valid = Page::new(PageId(1), PageType::Leaf, TxnId(1));
    valid.write_cell(b"first").unwrap();
    valid.write_cell(b"second").unwrap();
    valid.write_cell(b"third").unwrap();
    for bad_offset in [0, valid.cell_area_start() - 1, BODY_SIZE as u16, u16::MAX] {
        let mut page = valid.clone();
        page.set_cell_offset(2, bad_offset);
        let error = checked_cell_offsets(&page)
            .err()
            .expect("invalid last pointer");
        assert!(error.to_string().contains("cell 2 offset"));
    }
}

#[test]
fn checked_cell_readers_validate_all_pointers_before_decoding_cells() {
    for page_type in [PageType::Leaf, PageType::Branch] {
        let mut page = Page::new(PageId(1), page_type, TxnId(1));
        page.write_cell(b"x").unwrap();
        page.write_cell(b"y").unwrap();
        page.set_cell_offset(1, BODY_SIZE as u16);
        let error = match page_type {
            PageType::Leaf => crate::leaf_node::read_cells_checked(&page).unwrap_err(),
            PageType::Branch => crate::branch_node::read_cells_checked(&page).unwrap_err(),
            _ => unreachable!(),
        };
        assert!(error.to_string().contains("cell 1 offset"));
    }
}

#[test]
fn writable_constructor_preserves_initialized_format_and_public_checksum_contract() {
    for page_type in [
        PageType::Leaf,
        PageType::Branch,
        PageType::Overflow,
        PageType::PendingFree,
    ] {
        let checksummed = Page::new(PageId(42), page_type, TxnId(17));
        assert!(
            checksummed.verify_checksum(),
            "Page::new must return a checksummed page"
        );

        let mut writable = Page::new_for_write(PageId(42), page_type, TxnId(17));
        assert_eq!(writable.checksum(), 0);
        assert_eq!(
            &writable.as_bytes()[CHECKSUM_SIZE..],
            &checksummed.as_bytes()[CHECKSUM_SIZE..]
        );
        assert!(writable.as_bytes()[PAGE_HEADER_SIZE..]
            .iter()
            .all(|&byte| byte == 0));

        writable.update_checksum();
        assert!(writable.verify_checksum());
        assert_eq!(writable.as_bytes(), checksummed.as_bytes());
    }
}

fn assert_cell_validation_modes(page: &Page, expected_error: Option<&str>) {
    let (decoded, validated) = match page.page_type().unwrap() {
        PageType::Leaf => (
            crate::leaf_node::read_cells_checked(page).map(|_| ()),
            crate::leaf_node::validate_cells_checked(page),
        ),
        PageType::Branch => (
            crate::branch_node::read_cells_checked(page).map(|_| ()),
            crate::branch_node::validate_cells_checked(page),
        ),
        _ => unreachable!(),
    };
    assert_eq!(validated, decoded);
    assert_eq!(
        page.validate_for_read(page.page_id()).is_ok(),
        expected_error.is_none()
    );
    if let Some(expected) = expected_error {
        let error = decoded.unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected {expected:?}, got {error:?}"
        );
    } else {
        decoded.unwrap();
    }
}

fn raw_validation_page(kind: PageType, cells: &[Vec<u8>], right_child: PageId) -> Page {
    let mut page = Page::new(PageId(10), kind, TxnId(1));
    page.set_right_child(right_child);
    for cell in cells {
        page.write_cell(cell).unwrap();
    }
    page
}

#[test]
fn leaf_validation_modes_preserve_combined_corruption_priority() {
    use crate::leaf_node::{build_cell, OverflowRef};
    use citadel_core::types::ValueType;

    let malformed = raw_validation_page(
        PageType::Leaf,
        &[
            build_cell(b"a", ValueType::Overflow, &[0; 7]),
            build_cell(b"b", ValueType::Inline, b"value"),
        ],
        PageId(0),
    );
    assert_cell_validation_modes(
        &malformed,
        Some("leaf cell 0 overflow reference has 7 bytes"),
    );

    let mut later_parse = malformed.clone();
    let offset = later_parse.cell_offset(1) as usize;
    later_parse.data[offset + 2..offset + 6].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_cell_validation_modes(&later_parse, Some("leaf cell 1 value"));

    let mut bad_layout = malformed.clone();
    bad_layout.set_free_space(bad_layout.free_space() + 1);
    assert_cell_validation_modes(&bad_layout, Some("free-space accounting"));

    let mut overlap = malformed.clone();
    overlap.set_cell_offset(1, overlap.cell_offset(0));
    assert_cell_validation_modes(&overlap, Some("overlap"));

    let unordered = raw_validation_page(
        PageType::Leaf,
        &[
            build_cell(b"b", ValueType::Overflow, &[0; 7]),
            build_cell(b"a", ValueType::Inline, b"value"),
        ],
        PageId(0),
    );
    assert_cell_validation_modes(
        &unordered,
        Some("leaf keys 0 and 1 are not strictly ordered"),
    );

    let mut invalid_type = malformed.clone();
    let offset = invalid_type.cell_offset(0) as usize;
    invalid_type.data[offset + 7] = u8::MAX;
    assert_cell_validation_modes(
        &invalid_type,
        Some("leaf cell 0 has invalid value type 255"),
    );
    invalid_type.set_cell_offset(1, BODY_SIZE as u16);
    assert_cell_validation_modes(&invalid_type, Some("cell 1 offset"));

    for first_page in [PageId(0), PageId::INVALID] {
        let reference = OverflowRef {
            first_page,
            total_len: 1,
        }
        .to_bytes();
        let page = raw_validation_page(
            PageType::Leaf,
            &[build_cell(b"key", ValueType::Overflow, &reference)],
            PageId(0),
        );
        assert_cell_validation_modes(&page, Some("overflow reference has invalid first page"));
    }
    let reference = OverflowRef {
        first_page: PageId(7),
        total_len: citadel_core::MAX_VALUE_SIZE as u32 + 1,
    }
    .to_bytes();
    let page = raw_validation_page(
        PageType::Leaf,
        &[build_cell(b"key", ValueType::Overflow, &reference)],
        PageId(0),
    );
    assert_cell_validation_modes(&page, Some("overflow length"));
}

#[test]
fn branch_validation_modes_preserve_combined_corruption_priority() {
    use crate::branch_node::build_cell;

    let malformed = raw_validation_page(
        PageType::Branch,
        &[
            build_cell(PageId::INVALID, b"a"),
            build_cell(PageId(7), b"b"),
        ],
        PageId(8),
    );
    assert_cell_validation_modes(&malformed, Some("branch child 0 is invalid"));

    let mut later_parse = malformed.clone();
    let offset = later_parse.cell_offset(1) as usize;
    later_parse.data[offset + 4..offset + 6].copy_from_slice(&u16::MAX.to_le_bytes());
    assert_cell_validation_modes(&later_parse, Some("branch cell 1 key"));

    let mut bad_layout = malformed.clone();
    bad_layout.set_free_space(bad_layout.free_space() + 1);
    assert_cell_validation_modes(&bad_layout, Some("free-space accounting"));

    let mut overlap = malformed.clone();
    overlap.set_cell_offset(1, overlap.cell_offset(0));
    assert_cell_validation_modes(&overlap, Some("overlap"));

    let unordered = raw_validation_page(
        PageType::Branch,
        &[
            build_cell(PageId::INVALID, b"b"),
            build_cell(PageId(7), b"a"),
        ],
        PageId(8),
    );
    assert_cell_validation_modes(
        &unordered,
        Some("branch separator keys 0 and 1 are not strictly ordered"),
    );

    let invalid_right_child = raw_validation_page(
        PageType::Branch,
        &[build_cell(PageId(7), b"a"), build_cell(PageId(7), b"b")],
        PageId::INVALID,
    );
    assert_cell_validation_modes(&invalid_right_child, Some("branch child 2 is invalid"));

    let duplicates = raw_validation_page(
        PageType::Branch,
        &[
            build_cell(PageId(9), b"a"),
            build_cell(PageId(9), b"b"),
            build_cell(PageId(2), b"c"),
            build_cell(PageId(2), b"d"),
        ],
        PageId(5),
    );
    assert_cell_validation_modes(&duplicates, Some("branch child duplicates page page:2"));

    for (child, expected) in [
        (PageId::INVALID, "branch child 0 is invalid"),
        (PageId(10), "branch child 0 points back to page page:10"),
    ] {
        let empty = raw_validation_page(PageType::Branch, &[], child);
        assert_cell_validation_modes(&empty, Some(expected));
    }
}

#[test]
fn validation_modes_accept_logical_order_independent_of_physical_layout() {
    use crate::{branch_node, leaf_node};
    use citadel_core::types::ValueType;

    let reference = leaf_node::OverflowRef {
        first_page: PageId(7),
        total_len: 1,
    }
    .to_bytes();
    let mut leaf = raw_validation_page(
        PageType::Leaf,
        &[
            leaf_node::build_cell(b"c", ValueType::Overflow, &reference),
            leaf_node::build_cell(b"a", ValueType::Inline, b"value"),
            leaf_node::build_cell(b"b", ValueType::Tombstone, b""),
        ],
        PageId(0),
    );
    let mut branch = raw_validation_page(
        PageType::Branch,
        &[
            branch_node::build_cell(PageId(0), b"c"),
            branch_node::build_cell(PageId(8), b"a"),
            branch_node::build_cell(PageId(2), b"b"),
        ],
        PageId(7),
    );
    for page in [&mut leaf, &mut branch] {
        let offsets = [
            page.cell_offset(0),
            page.cell_offset(1),
            page.cell_offset(2),
        ];
        for (index, offset) in [offsets[1], offsets[2], offsets[0]].into_iter().enumerate() {
            page.set_cell_offset(index as u16, offset);
        }
        assert_cell_validation_modes(page, None);
    }
    let keys: Vec<_> = leaf_node::read_cells_checked(&leaf)
        .unwrap()
        .into_iter()
        .map(|cell| cell.key)
        .collect();
    assert_eq!(keys, [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]);
    let children: Vec<_> = branch_node::read_cells_checked(&branch)
        .unwrap()
        .into_iter()
        .map(|cell| cell.child)
        .collect();
    assert_eq!(children, [PageId(8), PageId(2), PageId(0)]);

    let area_start = leaf.cell_area_start();
    leaf_node::delete_at(&mut leaf, 1);
    assert_eq!(leaf.cell_area_start(), area_start);
    assert_cell_validation_modes(&leaf, None);
    assert!(leaf_node::insert(
        &mut leaf,
        b"bb",
        ValueType::Inline,
        &[7; 128]
    ));
    assert_cell_validation_modes(&leaf, None);

    for kind in [PageType::Leaf, PageType::Branch] {
        assert_cell_validation_modes(&raw_validation_page(kind, &[], PageId(0)), None);
    }
}
