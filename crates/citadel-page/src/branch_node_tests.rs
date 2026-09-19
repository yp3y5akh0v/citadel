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
fn checked_reader_accepts_a_well_formed_branch() {
    let mut page = make_branch_page(&[b"cat", b"dog"], &[PageId(1), PageId(2)], PageId(3));
    page.set_page_id(PageId(10));

    let cells = read_cells_checked(&page).unwrap();
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0].key, b"cat");
    assert_eq!(cells[1].child, PageId(2));
}

#[test]
fn checked_reader_rejects_an_out_of_bounds_pointer_array_without_panicking() {
    let mut page = make_branch_page(&[b"key"], &[PageId(1)], PageId(2));
    page.set_page_id(PageId(10));
    page.set_num_cells(u16::MAX);

    let decoded = std::panic::catch_unwind(|| read_cells_checked(&page));
    assert!(decoded.is_ok(), "checked decoding must not unwind");
    let error = decoded.unwrap().unwrap_err();
    assert!(error.to_string().contains("pointer array"));
}

#[test]
fn checked_reader_rejects_overlapping_cells() {
    let mut page = make_branch_page(&[b"cat", b"dog"], &[PageId(1), PageId(2)], PageId(3));
    page.set_page_id(PageId(10));
    page.set_cell_offset(1, page.cell_offset(0));

    let error = read_cells_checked(&page).unwrap_err();
    assert!(error.to_string().contains("overlap"));
}

#[test]
fn checked_reader_rejects_unordered_keys_and_bad_children() {
    let mut unordered = make_branch_page(&[b"dog", b"cat"], &[PageId(1), PageId(2)], PageId(3));
    unordered.set_page_id(PageId(10));
    assert!(read_cells_checked(&unordered)
        .unwrap_err()
        .to_string()
        .contains("not strictly ordered"));

    let mut duplicate = make_branch_page(&[b"cat"], &[PageId(1)], PageId(1));
    duplicate.set_page_id(PageId(10));
    assert!(read_cells_checked(&duplicate)
        .unwrap_err()
        .to_string()
        .contains("duplicates"));

    let mut self_ref = make_branch_page(&[b"cat"], &[PageId(10)], PageId(2));
    self_ref.set_page_id(PageId(10));
    assert!(read_cells_checked(&self_ref)
        .unwrap_err()
        .to_string()
        .contains("points back"));
}

#[test]
fn checked_reader_preserves_separator_order_when_children_are_unsorted() {
    let page = make_branch_page(
        &[b"ant", b"cat", b"dog"],
        &[PageId(9), PageId(2), PageId(5)],
        PageId(1),
    );
    let cells = read_cells_checked(&page).unwrap();
    let actual: Vec<_> = cells.iter().map(|cell| (cell.child, cell.key)).collect();
    assert_eq!(
        actual,
        vec![
            (PageId(9), b"ant".as_slice()),
            (PageId(2), b"cat".as_slice()),
            (PageId(5), b"dog".as_slice()),
        ]
    );
}

#[test]
fn checked_reader_rejects_nonadjacent_duplicate_children() {
    for (children, right) in [
        ([PageId(7), PageId(2), PageId(7)], PageId(4)),
        ([PageId(7), PageId(2), PageId(4)], PageId(7)),
    ] {
        let page = make_branch_page(&[b"ant", b"cat", b"dog"], &children, right);
        assert!(read_cells_checked(&page)
            .unwrap_err()
            .to_string()
            .contains(&format!("duplicates page {}", PageId(7))));
    }
}

#[test]
fn checked_reader_validates_cell_and_right_children() {
    for (child, right, expected) in [
        (PageId::INVALID, PageId(2), "child 0 is invalid"),
        (PageId(1), PageId::INVALID, "child 1 is invalid"),
        (PageId(0), PageId(2), "child 0 points back"),
        (PageId(1), PageId(0), "child 1 points back"),
    ] {
        let page = make_branch_page(&[b"cat"], &[child], right);
        assert!(read_cells_checked(&page)
            .unwrap_err()
            .to_string()
            .contains(expected));
    }
}

#[test]
fn checked_reader_validates_the_only_child_of_an_empty_branch() {
    let mut page = make_branch_page(&[], &[], PageId(0));
    page.set_page_id(PageId(10));
    assert!(read_cells_checked(&page).unwrap().is_empty());
    for (right, expected) in [
        (PageId::INVALID, "child 0 is invalid"),
        (PageId(10), "child 0 points back"),
    ] {
        page.set_right_child(right);
        assert!(read_cells_checked(&page)
            .unwrap_err()
            .to_string()
            .contains(expected));
    }
}

#[test]
fn search_finds_correct_child() {
    let page = make_branch_page(
        &[b"cat", b"dog", b"fox"],
        &[PageId(1), PageId(2), PageId(3)],
        PageId(4),
    );

    let find = |key: &[u8]| get_child(&page, search_child_index(&page, key));
    assert_eq!(find(b"ant"), PageId(1)); // < "cat"
    assert_eq!(find(b"cat"), PageId(2)); // >= "cat", < "dog"
    assert_eq!(find(b"cow"), PageId(2)); // >= "cat", < "dog"
    assert_eq!(find(b"dog"), PageId(3)); // >= "dog", < "fox"
    assert_eq!(find(b"elk"), PageId(3)); // >= "dog", < "fox"
    assert_eq!(find(b"fox"), PageId(4)); // >= "fox"
    assert_eq!(find(b"zebra"), PageId(4)); // >= "fox"
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
fn binary_prefix_separators_route_to_the_correct_child() {
    let keys: &[&[u8]] = &[b"", b"\0", b"a", b"a\0", b"alphabet", b"\xff"];
    let children = [
        PageId(17),
        PageId(3),
        PageId(88),
        PageId(55),
        PageId(34),
        PageId(2),
    ];
    let right_child = PageId(4096);
    let page = make_branch_page(keys, &children, right_child);
    read_cells_checked(&page).unwrap();

    for (index, (&key, &child)) in keys.iter().zip(&children).enumerate() {
        assert_eq!(read_key(&page, index as u16), key);
        assert_eq!(get_child(&page, index), child);
        let cell = read_cell(&page, index as u16);
        assert_eq!((cell.key, cell.child), (key, child));
    }
    assert_eq!(get_child(&page, keys.len()), right_child);

    for (key, child) in [
        (b"".as_slice(), PageId(3)),
        (b"\0".as_slice(), PageId(88)),
        (b"\0\0".as_slice(), PageId(88)),
        (b"a".as_slice(), PageId(55)),
        (b"a\0".as_slice(), PageId(34)),
        (b"a\0\0".as_slice(), PageId(34)),
        (b"al".as_slice(), PageId(34)),
        (b"alphabet".as_slice(), PageId(2)),
        (b"alphabet\0".as_slice(), PageId(2)),
        (b"\xff".as_slice(), right_child),
        (b"\xff\0".as_slice(), right_child),
    ] {
        assert_eq!(
            get_child(&page, search_child_index(&page, key)),
            child,
            "key={key:?}"
        );
    }

    let empty = make_branch_page(&[], &[], right_child);
    assert_eq!(search_child_index(&empty, b"any key"), 0);
    assert_eq!(get_child(&empty, 0), right_child);
}

#[test]
fn insert_separator_middle() {
    let mut page = make_branch_page(&[b"b", b"f"], &[PageId(1), PageId(2)], PageId(3));

    let ok = insert_separator(
        &mut page,
        1,
        PageId(20), // left child (CoW'd PageId(2))
        b"d",       // separator
        PageId(21), // right child (new page)
    );
    assert!(ok);

    assert_eq!(page.num_cells(), 3);
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

    assert_eq!(sep_key, b"f");
    assert_eq!(left_rc, PageId(3));
    assert_eq!(right_rc, PageId(6));

    assert_eq!(right_cells.len(), 2);
}
