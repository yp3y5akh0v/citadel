use super::*;
use crate::node_id::NodeId;
use citadel_core::types::PageId;
use citadel_core::MERKLE_HASH_SIZE;
use std::net::TcpListener;
use std::thread;

fn loopback_pair(key: &SyncKey) -> (NoiseTransport, NoiseTransport) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let key_clone = key.clone();
    let client =
        thread::spawn(move || NoiseTransport::connect(&addr.to_string(), &key_clone).unwrap());
    let (stream, _) = listener.accept().unwrap();
    let server = NoiseTransport::accept(stream, key).unwrap();
    let client = client.join().unwrap();
    (client, server)
}

fn test_key() -> SyncKey {
    SyncKey::from_bytes([0x42u8; 32])
}

#[test]
fn encrypted_roundtrip() {
    let key = test_key();
    let (client, server) = loopback_pair(&key);
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
fn wrong_key_fails() {
    let key_a = SyncKey::from_bytes([0x01u8; 32]);
    let key_b = SyncKey::from_bytes([0x02u8; 32]);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client_handle = thread::spawn(move || NoiseTransport::connect(&addr.to_string(), &key_a));
    let (stream, _) = listener.accept().unwrap();
    let server_result = NoiseTransport::accept(stream, &key_b);
    let client_result = client_handle.join().unwrap();

    assert!(
        server_result.is_err() || client_result.is_err(),
        "mismatched keys should cause handshake failure"
    );
}

#[test]
fn bidirectional() {
    let key = test_key();
    let (a, b) = loopback_pair(&key);
    a.send(&SyncMessage::Done).unwrap();
    b.send(&SyncMessage::PullRequest).unwrap();
    assert!(matches!(b.recv().unwrap(), SyncMessage::Done));
    assert!(matches!(a.recv().unwrap(), SyncMessage::PullRequest));
}

#[test]
fn large_message_chunking() {
    let key = test_key();
    let (a, b) = loopback_pair(&key);
    let data = vec![0xABu8; 256 * 1024];
    let data_clone = data.clone();
    thread::scope(|s| {
        s.spawn(|| {
            a.send(&SyncMessage::PatchData { data: data_clone })
                .unwrap();
        });
        match b.recv().unwrap() {
            SyncMessage::PatchData { data: received } => {
                assert_eq!(received.len(), data.len());
                assert_eq!(received, data);
            }
            _ => panic!("wrong variant"),
        }
    });
}

#[test]
fn close_prevents_send() {
    let key = test_key();
    let (a, _b) = loopback_pair(&key);
    a.close().unwrap();
    assert!(matches!(
        a.send(&SyncMessage::Done).unwrap_err(),
        SyncError::Closed
    ));
}

#[test]
fn close_prevents_recv() {
    let key = test_key();
    let (a, _b) = loopback_pair(&key);
    a.close().unwrap();
    assert!(matches!(a.recv().unwrap_err(), SyncError::Closed));
}

#[test]
fn multiple_messages() {
    let key = test_key();
    let (a, b) = loopback_pair(&key);
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
