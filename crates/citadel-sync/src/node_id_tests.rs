use super::*;

#[test]
fn random_generates_unique_ids() {
    let ids: Vec<NodeId> = (0..100).map(|_| NodeId::random()).collect();
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            assert_ne!(ids[i], ids[j], "collision at indices {i} and {j}");
        }
    }
}

#[test]
fn u64_roundtrip() {
    let id = NodeId::from_u64(0xDEADBEEF_CAFEBABE);
    assert_eq!(id.as_u64(), 0xDEADBEEF_CAFEBABE);
}

#[test]
fn bytes_roundtrip() {
    let id = NodeId::from_u64(0x0123_4567_89AB_CDEF);
    let bytes = id.to_bytes();
    let id2 = NodeId::from_bytes(bytes);
    assert_eq!(id, id2);
}

#[test]
fn bytes_big_endian() {
    let id = NodeId::from_u64(0x0102_0304_0506_0708);
    let bytes = id.to_bytes();
    assert_eq!(bytes, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
}

#[test]
fn ordering_consistent() {
    let a = NodeId::from_u64(100);
    let b = NodeId::from_u64(200);
    assert!(a < b);

    let c = NodeId::from_u64(100);
    assert_eq!(a, c);
}

#[test]
fn display_hex() {
    let id = NodeId::from_u64(0xFF);
    assert_eq!(format!("{id}"), "00000000000000ff");
}

#[test]
fn debug_hex() {
    let id = NodeId::from_u64(0xFF);
    assert_eq!(format!("{id:?}"), "NodeId(00000000000000ff)");
}

#[test]
fn hash_consistency() {
    use std::collections::HashSet;
    let a = NodeId::from_u64(42);
    let b = NodeId::from_u64(42);
    let c = NodeId::from_u64(43);

    let mut set = HashSet::new();
    set.insert(a);
    assert!(set.contains(&b));
    assert!(!set.contains(&c));
}
