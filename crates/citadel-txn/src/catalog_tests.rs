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

#[test]
fn checked_deserialize_rejects_truncated_and_extended_descriptors() {
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE - 1]).is_none());
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE + 1]).is_none());
    assert!(TableDescriptor::try_deserialize(&[0; TABLE_DESCRIPTOR_SIZE]).is_some());
}

#[test]
fn legacy_deserialize_keeps_its_prefix_decoding_contract() {
    let desc = TableDescriptor {
        root_page: PageId(17),
        entry_count: 23,
        depth: 2,
        flags: 5,
    };
    let encoded = desc.serialize();

    let from_prefix = TableDescriptor::deserialize(&encoded[..16]);
    let mut extended = encoded.to_vec();
    extended.extend_from_slice(b"future");
    let from_extended = TableDescriptor::deserialize(&extended);

    for decoded in [from_prefix, from_extended] {
        assert_eq!(decoded.root_page, desc.root_page);
        assert_eq!(decoded.entry_count, desc.entry_count);
        assert_eq!(decoded.depth, desc.depth);
        assert_eq!(decoded.flags, desc.flags);
    }
}
