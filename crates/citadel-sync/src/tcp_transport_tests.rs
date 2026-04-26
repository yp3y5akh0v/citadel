use super::*;
use crate::node_id::NodeId;
use citadel_core::types::PageId;
use citadel_core::MERKLE_HASH_SIZE;
use std::net::TcpListener;
use std::thread;

fn loopback_pair() -> (TcpTransport, TcpTransport) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = thread::spawn(move || TcpTransport::connect(&addr.to_string()).unwrap());
    let (stream, _) = listener.accept().unwrap();
    let server = TcpTransport::from_stream(stream).unwrap();
    let client = client.join().unwrap();
    (client, server)
}

#[test]
fn connect_and_exchange() {
    let (client, server) = loopback_pair();
    let msg = SyncMessage::Hello {
        node_id: NodeId::from_u64(42),
        root_page: PageId(10),
        root_hash: [1u8; MERKLE_HASH_SIZE],
    };
    client.send(&msg).unwrap();
    match server.recv().unwrap() {
        SyncMessage::Hello {
            node_id,
            root_page,
            root_hash,
        } => {
            assert_eq!(node_id, NodeId::from_u64(42));
            assert_eq!(root_page, PageId(10));
            assert_eq!(root_hash, [1u8; MERKLE_HASH_SIZE]);
        }
        other => panic!("expected Hello, got {:?}", other),
    }
}

#[test]
fn bidirectional() {
    let (a, b) = loopback_pair();
    a.send(&SyncMessage::Done).unwrap();
    b.send(&SyncMessage::PullRequest).unwrap();

    assert!(matches!(b.recv().unwrap(), SyncMessage::Done));
    assert!(matches!(a.recv().unwrap(), SyncMessage::PullRequest));
}

#[test]
fn roundtrip_all_types() {
    let (a, b) = loopback_pair();

    let messages = vec![
        SyncMessage::Hello {
            node_id: NodeId::from_u64(1),
            root_page: PageId(0),
            root_hash: [0u8; MERKLE_HASH_SIZE],
        },
        SyncMessage::HelloAck {
            node_id: NodeId::from_u64(2),
            root_page: PageId(5),
            root_hash: [2u8; MERKLE_HASH_SIZE],
            in_sync: false,
        },
        SyncMessage::DigestRequest {
            page_ids: vec![PageId(1), PageId(2)],
        },
        SyncMessage::EntriesRequest {
            page_ids: vec![PageId(3)],
        },
        SyncMessage::PatchData {
            data: vec![1, 2, 3, 4, 5],
        },
        SyncMessage::PatchAck {
            result: crate::apply::ApplyResult {
                entries_applied: 5,
                entries_skipped: 1,
                entries_equal: 0,
            },
        },
        SyncMessage::Done,
        SyncMessage::Error {
            message: "test error".into(),
        },
        SyncMessage::PullRequest,
        SyncMessage::PullResponse {
            root_page: PageId(99),
            root_hash: [9u8; MERKLE_HASH_SIZE],
        },
    ];

    for msg in &messages {
        a.send(msg).unwrap();
    }

    for expected in &messages {
        let received = b.recv().unwrap();
        let expected_bytes = expected.serialize();
        let received_bytes = received.serialize();
        assert_eq!(expected_bytes, received_bytes);
    }
}

#[test]
fn large_payload() {
    let (a, b) = loopback_pair();
    let data = vec![0xABu8; 1024 * 1024]; // 1 MiB
    let send_data = data.clone();
    let sender = thread::spawn(move || {
        a.send(&SyncMessage::PatchData { data: send_data }).unwrap();
    });
    match b.recv().unwrap() {
        SyncMessage::PatchData { data: received } => {
            assert_eq!(received.len(), data.len());
            assert_eq!(received, data);
        }
        _ => panic!("wrong variant"),
    }
    sender.join().unwrap();
}

#[test]
fn close_prevents_send() {
    let (a, _b) = loopback_pair();
    a.close().unwrap();
    let err = a.send(&SyncMessage::Done).unwrap_err();
    assert!(matches!(err, SyncError::Closed));
}

#[test]
fn close_prevents_recv() {
    let (a, _b) = loopback_pair();
    a.close().unwrap();
    let err = a.recv().unwrap_err();
    assert!(matches!(err, SyncError::Closed));
}

#[test]
fn connection_refused() {
    let err = TcpTransport::connect("127.0.0.1:1").unwrap_err();
    assert!(matches!(err, SyncError::Io(_)));
}

#[test]
fn multiple_messages() {
    let (a, b) = loopback_pair();
    for i in 0..100u64 {
        a.send(&SyncMessage::Hello {
            node_id: NodeId::from_u64(i),
            root_page: PageId(0),
            root_hash: [0u8; MERKLE_HASH_SIZE],
        })
        .unwrap();
    }
    for i in 0..100u64 {
        match b.recv().unwrap() {
            SyncMessage::Hello { node_id, .. } => {
                assert_eq!(node_id, NodeId::from_u64(i));
            }
            _ => panic!("wrong variant"),
        }
    }
}
