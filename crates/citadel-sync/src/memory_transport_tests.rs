use super::*;
use crate::node_id::NodeId;
use citadel_core::types::PageId;
use citadel_core::MERKLE_HASH_SIZE;

#[test]
fn pair_send_recv() {
    let (a, b) = MemoryTransport::pair();
    let msg = SyncMessage::Hello {
        node_id: NodeId::from_u64(1),
        root_page: PageId(0),
        root_hash: [0u8; MERKLE_HASH_SIZE],
    };
    a.send(&msg).unwrap();
    let received = b.recv().unwrap();
    match received {
        SyncMessage::Hello { node_id, .. } => {
            assert_eq!(node_id, NodeId::from_u64(1));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn bidirectional() {
    let (a, b) = MemoryTransport::pair();
    a.send(&SyncMessage::Done).unwrap();
    b.send(&SyncMessage::Done).unwrap();

    assert!(matches!(b.recv().unwrap(), SyncMessage::Done));
    assert!(matches!(a.recv().unwrap(), SyncMessage::Done));
}

#[test]
fn ordering_preserved() {
    let (a, b) = MemoryTransport::pair();
    for i in 0..10u64 {
        a.send(&SyncMessage::Hello {
            node_id: NodeId::from_u64(i),
            root_page: PageId(0),
            root_hash: [0u8; MERKLE_HASH_SIZE],
        })
        .unwrap();
    }
    for i in 0..10u64 {
        match b.recv().unwrap() {
            SyncMessage::Hello { node_id, .. } => {
                assert_eq!(node_id, NodeId::from_u64(i));
            }
            _ => panic!("wrong variant"),
        }
    }
}

#[test]
fn close_prevents_send() {
    let (a, _b) = MemoryTransport::pair();
    a.close().unwrap();
    let err = a.send(&SyncMessage::Done).unwrap_err();
    assert!(matches!(err, SyncError::Closed));
}

#[test]
fn close_prevents_recv() {
    let (a, _b) = MemoryTransport::pair();
    a.close().unwrap();
    let err = a.recv().unwrap_err();
    assert!(matches!(err, SyncError::Closed));
}

#[test]
fn dropped_sender_causes_recv_error() {
    let (a, b) = MemoryTransport::pair();
    drop(a);
    let err = b.recv().unwrap_err();
    assert!(matches!(err, SyncError::Closed));
}
