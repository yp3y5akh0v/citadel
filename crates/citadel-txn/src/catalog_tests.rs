use super::*;

#[test]
fn serialize_deserialize_roundtrip() {
    let desc = TableDescriptor {
        root_page: PageId(42),
        entry_count: 1000,
        depth: 3,
        flags: 0,
    };
    let buf = desc.serialize();
    assert_eq!(buf.len(), TABLE_DESCRIPTOR_SIZE);

    let desc2 = TableDescriptor::deserialize(&buf);
    assert_eq!(desc2.root_page, PageId(42));
    assert_eq!(desc2.entry_count, 1000);
    assert_eq!(desc2.depth, 3);
    assert_eq!(desc2.flags, 0);
}
