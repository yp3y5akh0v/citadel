use super::*;
use crate::hlc::HlcTimestamp;
use crate::node_id::NodeId;

const SECOND: i64 = 1_000_000_000;

fn meta(wall_ns: i64, logical: i32, node: u64) -> CrdtMeta {
    CrdtMeta::new(HlcTimestamp::new(wall_ns, logical), NodeId::from_u64(node))
}

#[test]
fn meta_new_and_accessors() {
    let ts = HlcTimestamp::new(1000 * SECOND, 5);
    let nid = NodeId::from_u64(42);
    let m = CrdtMeta::new(ts, nid);
    assert_eq!(m.timestamp, ts);
    assert_eq!(m.node_id, nid);
}

#[test]
fn meta_bytes_roundtrip() {
    let m = meta(1000 * SECOND, 42, 0xDEADBEEF);
    let bytes = m.to_bytes();
    assert_eq!(bytes.len(), CRDT_META_SIZE);
    let m2 = CrdtMeta::from_bytes(&bytes);
    assert_eq!(m, m2);
}

#[test]
fn meta_bytes_roundtrip_zero() {
    let m = meta(0, 0, 0);
    let bytes = m.to_bytes();
    let m2 = CrdtMeta::from_bytes(&bytes);
    assert_eq!(m, m2);
}

#[test]
fn meta_bytes_roundtrip_max() {
    let m = meta(i64::MAX, i32::MAX, u64::MAX);
    let bytes = m.to_bytes();
    let m2 = CrdtMeta::from_bytes(&bytes);
    assert_eq!(m, m2);
}

#[test]
fn meta_debug_format() {
    let m = meta(1_000_000_000, 5, 255);
    let s = format!("{m:?}");
    assert!(s.contains("CrdtMeta"));
    assert!(s.contains("HLC"));
    assert!(s.contains("NodeId"));
}

#[test]
fn lww_higher_timestamp_wins() {
    let a = meta(1000 * SECOND, 0, 1);
    let b = meta(1001 * SECOND, 0, 1);
    assert!(b.wins_over(&a));
    assert!(!a.wins_over(&b));
}

#[test]
fn lww_higher_logical_wins() {
    let a = meta(1000 * SECOND, 5, 1);
    let b = meta(1000 * SECOND, 6, 1);
    assert!(b.wins_over(&a));
    assert!(!a.wins_over(&b));
}

#[test]
fn lww_node_id_tiebreaker() {
    let a = meta(1000 * SECOND, 5, 100);
    let b = meta(1000 * SECOND, 5, 200);
    assert!(b.wins_over(&a));
    assert!(!a.wins_over(&b));
}

#[test]
fn lww_equal_entries() {
    let a = meta(1000 * SECOND, 5, 100);
    let b = meta(1000 * SECOND, 5, 100);
    assert!(!a.wins_over(&b));
    assert!(!b.wins_over(&a));
    assert_eq!(a.lww_cmp(&b), std::cmp::Ordering::Equal);
}

#[test]
fn lww_timestamp_dominates_node_id() {
    let a = meta(1001 * SECOND, 0, 1);
    let b = meta(1000 * SECOND, 0, u64::MAX);
    assert!(a.wins_over(&b));
}

#[test]
fn merge_local_wins() {
    let local = meta(1001 * SECOND, 0, 1);
    let remote = meta(1000 * SECOND, 0, 1);
    assert_eq!(lww_merge(&local, &remote), MergeResult::Local);
}

#[test]
fn merge_remote_wins() {
    let local = meta(1000 * SECOND, 0, 1);
    let remote = meta(1001 * SECOND, 0, 1);
    assert_eq!(lww_merge(&local, &remote), MergeResult::Remote);
}

#[test]
fn merge_equal() {
    let local = meta(1000 * SECOND, 5, 100);
    let remote = meta(1000 * SECOND, 5, 100);
    assert_eq!(lww_merge(&local, &remote), MergeResult::Equal);
}

#[test]
fn merge_commutativity() {
    let entries = [
        meta(1000 * SECOND, 0, 1),
        meta(1000 * SECOND, 0, 2),
        meta(1001 * SECOND, 0, 1),
        meta(1000 * SECOND, 1, 1),
    ];

    for a in &entries {
        for b in &entries {
            let ab = lww_merge(a, b);
            let ba = lww_merge(b, a);
            match (ab, ba) {
                (MergeResult::Local, MergeResult::Remote) => {}
                (MergeResult::Remote, MergeResult::Local) => {}
                (MergeResult::Equal, MergeResult::Equal) => {}
                _ => panic!("commutativity violated for {a:?} vs {b:?}: {ab:?} vs {ba:?}"),
            }
        }
    }
}

#[test]
fn merge_associativity() {
    let a = meta(1000 * SECOND, 0, 1);
    let b = meta(1001 * SECOND, 5, 2);
    let c = meta(1001 * SECOND, 5, 3);

    fn winner(local: &CrdtMeta, remote: &CrdtMeta) -> CrdtMeta {
        match lww_merge(local, remote) {
            MergeResult::Local | MergeResult::Equal => *local,
            MergeResult::Remote => *remote,
        }
    }

    let ab = winner(&a, &b);
    let ab_c = winner(&ab, &c);

    let bc = winner(&b, &c);
    let a_bc = winner(&a, &bc);

    assert_eq!(ab_c, a_bc, "associativity violated");
}

#[test]
fn merge_idempotency() {
    let a = meta(1000 * SECOND, 5, 42);
    assert_eq!(lww_merge(&a, &a), MergeResult::Equal);
}

#[test]
fn entry_kind_roundtrip() {
    assert_eq!(EntryKind::from_u8(0), Some(EntryKind::Put));
    assert_eq!(EntryKind::from_u8(1), Some(EntryKind::Tombstone));
    assert_eq!(EntryKind::from_u8(2), None);
    assert_eq!(EntryKind::from_u8(255), None);
}

#[test]
fn encode_decode_put_roundtrip() {
    let m = meta(1000 * SECOND, 5, 42);
    let user_val = b"hello world";
    let encoded = encode_lww_value(&m, EntryKind::Put, user_val);

    assert_eq!(encoded.len(), CRDT_HEADER_SIZE + user_val.len());

    let decoded = decode_lww_value(&encoded).unwrap();
    assert_eq!(decoded.meta, m);
    assert_eq!(decoded.kind, EntryKind::Put);
    assert_eq!(decoded.user_value, user_val);
}

#[test]
fn encode_decode_tombstone_roundtrip() {
    let m = meta(1000 * SECOND, 5, 42);
    let encoded = encode_lww_value(&m, EntryKind::Tombstone, b"");

    assert_eq!(encoded.len(), CRDT_HEADER_SIZE);

    let decoded = decode_lww_value(&encoded).unwrap();
    assert_eq!(decoded.meta, m);
    assert_eq!(decoded.kind, EntryKind::Tombstone);
    assert_eq!(decoded.user_value.len(), 0);
}

#[test]
fn encode_tombstone_ignores_user_value() {
    let m = meta(1000 * SECOND, 5, 42);
    let encoded = encode_lww_value(&m, EntryKind::Tombstone, b"should be ignored");
    assert_eq!(encoded.len(), CRDT_HEADER_SIZE);
}

#[test]
fn encode_decode_empty_value() {
    let m = meta(1000 * SECOND, 0, 1);
    let encoded = encode_lww_value(&m, EntryKind::Put, b"");

    assert_eq!(encoded.len(), CRDT_HEADER_SIZE);

    let decoded = decode_lww_value(&encoded).unwrap();
    assert_eq!(decoded.kind, EntryKind::Put);
    assert_eq!(decoded.user_value.len(), 0);
}

#[test]
fn encode_decode_large_value() {
    let m = meta(1000 * SECOND, 0, 1);
    let user_val = vec![0xAB; 4096];
    let encoded = encode_lww_value(&m, EntryKind::Put, &user_val);

    assert_eq!(encoded.len(), CRDT_HEADER_SIZE + 4096);

    let decoded = decode_lww_value(&encoded).unwrap();
    assert_eq!(decoded.user_value, &user_val[..]);
}

#[test]
fn decode_too_short() {
    let err = decode_lww_value(&[0u8; 10]).unwrap_err();
    assert!(matches!(err, DecodeError::TooShort { .. }));
}

#[test]
fn decode_invalid_entry_kind() {
    let mut data = [0u8; CRDT_HEADER_SIZE];
    data[0] = 255; // invalid
    let err = decode_lww_value(&data).unwrap_err();
    assert!(matches!(err, DecodeError::InvalidEntryKind(255)));
}

#[test]
fn header_size_constant() {
    assert_eq!(CRDT_HEADER_SIZE, 24);
    assert_eq!(CRDT_META_SIZE, 20);
    assert_eq!(1 + 3 + 12 + 8, CRDT_HEADER_SIZE);
}

#[test]
fn merge_encoded_values() {
    let local_meta = meta(1000 * SECOND, 0, 1);
    let remote_meta = meta(1001 * SECOND, 0, 2);

    let local_encoded = encode_lww_value(&local_meta, EntryKind::Put, b"local");
    let remote_encoded = encode_lww_value(&remote_meta, EntryKind::Put, b"remote");

    let local_decoded = decode_lww_value(&local_encoded).unwrap();
    let remote_decoded = decode_lww_value(&remote_encoded).unwrap();

    let result = lww_merge(&local_decoded.meta, &remote_decoded.meta);
    assert_eq!(result, MergeResult::Remote);
}

#[test]
fn tombstone_wins_over_put_with_lower_timestamp() {
    let put_meta = meta(1000 * SECOND, 0, 1);
    let del_meta = meta(1001 * SECOND, 0, 1);

    let put_encoded = encode_lww_value(&put_meta, EntryKind::Put, b"value");
    let del_encoded = encode_lww_value(&del_meta, EntryKind::Tombstone, b"");

    let put_decoded = decode_lww_value(&put_encoded).unwrap();
    let del_decoded = decode_lww_value(&del_encoded).unwrap();

    let result = lww_merge(&put_decoded.meta, &del_decoded.meta);
    assert_eq!(result, MergeResult::Remote);
    assert_eq!(del_decoded.kind, EntryKind::Tombstone);
}

#[test]
fn put_wins_over_tombstone_with_lower_timestamp() {
    let del_meta = meta(1000 * SECOND, 0, 1);
    let put_meta = meta(1001 * SECOND, 0, 1);

    let del_encoded = encode_lww_value(&del_meta, EntryKind::Tombstone, b"");
    let put_encoded = encode_lww_value(&put_meta, EntryKind::Put, b"value");

    let del_decoded = decode_lww_value(&del_encoded).unwrap();
    let put_decoded = decode_lww_value(&put_encoded).unwrap();

    let result = lww_merge(&del_decoded.meta, &put_decoded.meta);
    assert_eq!(result, MergeResult::Remote);
    assert_eq!(put_decoded.kind, EntryKind::Put);
}

#[test]
fn encoded_format_put() {
    let m = CrdtMeta::new(
        HlcTimestamp::new(0x0102_0304_0506_0708, 0x090A0B0C),
        NodeId::from_u64(0x1112_1314_1516_1718),
    );
    let encoded = encode_lww_value(&m, EntryKind::Put, b"\xAA\xBB");

    assert_eq!(encoded[0], 0x00);
    assert_eq!(&encoded[1..4], &[0, 0, 0]);
    assert_eq!(
        &encoded[4..12],
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
    assert_eq!(&encoded[12..16], &[0x09, 0x0A, 0x0B, 0x0C]);
    assert_eq!(
        &encoded[16..24],
        &[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18]
    );
    assert_eq!(&encoded[24..26], &[0xAA, 0xBB]);
}

#[test]
fn encoded_format_tombstone() {
    let m = meta(1000 * SECOND, 0, 1);
    let encoded = encode_lww_value(&m, EntryKind::Tombstone, b"");
    assert_eq!(encoded[0], 0x01);
    assert_eq!(encoded.len(), CRDT_HEADER_SIZE);
}

#[test]
fn merge_many_entries_finds_latest() {
    let entries: Vec<CrdtMeta> = (0..100)
        .map(|i| meta(1000 * SECOND + i as i64, 0, i as u64))
        .collect();

    let mut winner = entries[0];
    for e in &entries[1..] {
        if lww_merge(&winner, e) == MergeResult::Remote {
            winner = *e;
        }
    }

    assert_eq!(winner.timestamp.wall_time(), 1000 * SECOND + 99);
    assert_eq!(winner.node_id.as_u64(), 99);
}

#[test]
fn merge_reverse_order_same_result() {
    let entries: Vec<CrdtMeta> = (0..100)
        .map(|i| meta(1000 * SECOND + i as i64, 0, i as u64))
        .collect();

    let mut fwd_winner = entries[0];
    for e in &entries[1..] {
        if lww_merge(&fwd_winner, e) == MergeResult::Remote {
            fwd_winner = *e;
        }
    }

    let mut rev_winner = entries[99];
    for e in entries[..99].iter().rev() {
        if lww_merge(&rev_winner, e) == MergeResult::Remote {
            rev_winner = *e;
        }
    }

    assert_eq!(fwd_winner, rev_winner);
}

#[test]
fn merge_shuffled_order_same_result() {
    use std::collections::BTreeSet;

    let entries: Vec<CrdtMeta> = (0..50)
        .map(|i| meta(1000 * SECOND + (i * 7 % 50) as i64, 0, i as u64))
        .collect();

    let expected = entries.iter().max_by(|a, b| a.lww_cmp(b)).unwrap();

    let mut winner = entries[0];
    for e in &entries[1..] {
        if lww_merge(&winner, e) == MergeResult::Remote {
            winner = *e;
        }
    }

    assert_eq!(winner, *expected);

    let sorted: BTreeSet<u64> = entries
        .iter()
        .map(|e| e.timestamp.wall_time() as u64)
        .collect();
    assert!(sorted.len() <= entries.len());
}
