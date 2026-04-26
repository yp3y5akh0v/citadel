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
