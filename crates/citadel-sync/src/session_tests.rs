use super::*;
use crate::memory_transport::MemoryTransport;
use crate::patch::PatchEntry;
use crate::transport::SyncTransport;
use crate::{EntryKind, LocalTreeReader};
use citadel_core::constants::{DEK_SIZE, MAC_KEY_SIZE, MAC_SIZE};
use citadel_io::mmap_io::MmapPageIO;

fn test_manager(path: &std::path::Path) -> TxnManager {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    let io = Box::new(MmapPageIO::try_new(file).unwrap());
    TxnManager::create(
        io,
        [0x42; DEK_SIZE],
        [0x43; MAC_KEY_SIZE],
        1,
        0x1234,
        [0x44; MAC_SIZE],
        256,
    )
    .unwrap()
}

fn session(node: u64, crdt_aware: bool) -> SyncSession {
    SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(node),
        direction: SyncDirection::Push,
        crdt_aware,
    })
}

fn put_patch(key: &[u8], value: &[u8]) -> SyncPatch {
    SyncPatch {
        source_node: NodeId::from_u64(9),
        entries: vec![PatchEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            kind: EntryKind::Put,
            crdt_meta: None,
        }],
        crdt_aware: false,
    }
}

#[test]
fn stale_default_patch_cannot_overwrite_a_post_advertisement_commit() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();

    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(1),
                root_page: PageId(1),
                root_hash: [9; citadel_core::MERKLE_HASH_SIZE],
                crdt_aware: false,
            })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::HelloAck { .. }
        ));

        let mut concurrent = manager.begin_write().unwrap();
        concurrent.insert(b"k", b"newer-local").unwrap();
        concurrent.commit().unwrap();

        client
            .send(&SyncMessage::PatchData {
                data: put_patch(b"k", b"stale-remote").try_serialize().unwrap(),
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(handle.join().unwrap().is_err());
    });

    assert_eq!(
        manager.begin_read().get(b"k").unwrap().as_deref(),
        Some(b"newer-local".as_slice())
    );
}

#[test]
fn stale_named_patch_cannot_overwrite_a_post_table_list_commit() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let mut setup = manager.begin_write().unwrap();
    setup.create_table(b"data").unwrap();
    setup.table_insert(b"data", b"k", b"advertised").unwrap();
    setup.commit().unwrap();

    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.handle_table_sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::TableListRequest { crdt_aware: false })
            .unwrap();
        let SyncMessage::TableListResponse { tables } = client.recv().unwrap() else {
            panic!("expected table list");
        };
        let advertised = tables.iter().find(|table| table.name == b"data").unwrap();

        let mut concurrent = manager.begin_write().unwrap();
        concurrent
            .table_insert(b"data", b"k", b"newer-local")
            .unwrap();
        concurrent.commit().unwrap();

        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"data".to_vec(),
                root_page: advertised.root_page,
                root_hash: advertised.root_hash,
            })
            .unwrap();
        client
            .send(&SyncMessage::PatchData {
                data: put_patch(b"k", b"stale-remote").try_serialize().unwrap(),
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(handle.join().unwrap().is_err());
    });

    assert_eq!(
        manager
            .begin_read()
            .table_get(b"data", b"k")
            .unwrap()
            .as_deref(),
        Some(b"newer-local".as_slice())
    );
}

#[test]
fn multi_table_responder_enforces_scope_and_reserved_name_state() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();

    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.handle_table_sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::TableListRequest { crdt_aware: false })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::TableListResponse { .. }
        ));

        client
            .send(&SyncMessage::PatchData {
                data: put_patch(b"default", b"must-not-land")
                    .try_serialize()
                    .unwrap(),
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        client
            .send(&SyncMessage::DigestRequest {
                page_ids: vec![PageId(1)],
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));

        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"__temp_hidden".to_vec(),
                root_page: PageId(1),
                root_hash: UNKNOWN_HASH,
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));

        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"remote_empty".to_vec(),
                root_page: PageId(1),
                root_hash: UNKNOWN_HASH,
            })
            .unwrap();
        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"nested".to_vec(),
                root_page: PageId(2),
                root_hash: UNKNOWN_HASH,
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        client
            .send(&SyncMessage::PatchData {
                data: SyncPatch::empty(NodeId::from_u64(1))
                    .try_serialize()
                    .unwrap(),
            })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::PatchAck { result } if result == ApplyResult::empty()
        ));
        client
            .send(&SyncMessage::TableSyncEnd {
                table_name: b"wrong".to_vec(),
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        client
            .send(&SyncMessage::TableSyncEnd {
                table_name: b"remote_empty".to_vec(),
            })
            .unwrap();
        client.send(&SyncMessage::Done).unwrap();
        assert!(handle.join().unwrap().is_ok());
    });

    let mut read = manager.begin_read();
    assert!(read.get(b"default").unwrap().is_none());
    assert!(read
        .list_tables()
        .unwrap()
        .iter()
        .any(|(name, _)| name == b"remote_empty"));
}

#[test]
fn empty_local_table_is_sent_as_an_acknowledged_patch() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let mut setup = manager.begin_write().unwrap();
    setup.create_table(b"empty").unwrap();
    setup.commit().unwrap();

    let initiator = session(1, false);
    let (client, server) = MemoryTransport::pair();
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| initiator.sync_tables_as_initiator(&manager, &client));

        assert!(matches!(
            server.recv().unwrap(),
            SyncMessage::TableListRequest { crdt_aware: false }
        ));
        server
            .send(&SyncMessage::TableListResponse { tables: Vec::new() })
            .unwrap();

        let SyncMessage::TableSyncBegin {
            table_name,
            root_page,
            ..
        } = server.recv().unwrap()
        else {
            panic!("expected table scope");
        };
        assert_eq!(table_name, b"empty");
        assert!(root_page.is_valid());

        let SyncMessage::PatchData { data } = server.recv().unwrap() else {
            panic!("empty table creation must use the acknowledged patch path");
        };
        assert!(SyncPatch::deserialize(&data).unwrap().is_empty());
        server
            .send(&SyncMessage::PatchAck {
                result: ApplyResult::empty(),
            })
            .unwrap();
        assert!(matches!(
            server.recv().unwrap(),
            SyncMessage::TableSyncEnd { table_name } if table_name == b"empty"
        ));
        assert!(matches!(server.recv().unwrap(), SyncMessage::Done));

        let results = handle.join().unwrap().unwrap();
        assert_eq!(results, vec![(b"empty".to_vec(), ApplyResult::empty())]);
    });
}

#[test]
fn table_sync_end_has_no_database_side_effect_after_patch_ack() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let mut setup = manager.begin_write().unwrap();
    setup.create_table(b"data").unwrap();
    setup.commit().unwrap();

    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.handle_table_sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::TableListRequest { crdt_aware: false })
            .unwrap();
        let SyncMessage::TableListResponse { tables } = client.recv().unwrap() else {
            panic!("expected table list");
        };
        let advertised = tables.iter().find(|table| table.name == b"data").unwrap();
        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"data".to_vec(),
                root_page: advertised.root_page,
                root_hash: advertised.root_hash,
            })
            .unwrap();
        client
            .send(&SyncMessage::PatchData {
                data: put_patch(b"remote", b"value").try_serialize().unwrap(),
            })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::PatchAck { .. }
        ));

        let mut concurrent = manager.begin_write().unwrap();
        concurrent
            .table_insert(b"data", b"concurrent", b"value")
            .unwrap();
        concurrent.commit().unwrap();

        client
            .send(&SyncMessage::TableSyncEnd {
                table_name: b"data".to_vec(),
            })
            .unwrap();
        client.send(&SyncMessage::Done).unwrap();
        assert!(handle.join().unwrap().is_ok());
    });

    let mut read = manager.begin_read();
    assert_eq!(
        read.table_get(b"data", b"remote").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(
        read.table_get(b"data", b"concurrent").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
}

#[test]
fn malformed_default_patch_is_reported_to_the_peer() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();

    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(1),
                root_page: PageId(1),
                root_hash: [9; citadel_core::MERKLE_HASH_SIZE],
                crdt_aware: false,
            })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::HelloAck { in_sync: false, .. }
        ));
        client
            .send(&SyncMessage::PatchData { data: vec![0] })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(matches!(handle.join().unwrap(), Err(SyncError::Patch(_))));
    });
}

#[test]
fn malformed_named_patch_is_reported_to_the_peer() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let responder = session(2, false);
    let (client, server) = MemoryTransport::pair();

    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.handle_table_sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::TableListRequest { crdt_aware: false })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::TableListResponse { .. }
        ));
        client
            .send(&SyncMessage::TableSyncBegin {
                table_name: b"remote".to_vec(),
                root_page: PageId(1),
                root_hash: UNKNOWN_HASH,
            })
            .unwrap();
        client
            .send(&SyncMessage::PatchData { data: vec![0] })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(matches!(handle.join().unwrap(), Err(SyncError::Patch(_))));
    });
}

#[test]
fn handshake_flags_and_in_sync_phase_are_verified() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));

    // A responder rejects a CRDT-mode mismatch before advertising pages.
    let (client, server) = MemoryTransport::pair();
    let responder = session(2, false);
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(1),
                root_page: PageId(1),
                root_hash: UNKNOWN_HASH,
                crdt_aware: true,
            })
            .unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(matches!(
            handle.join().unwrap(),
            Err(SyncError::Handshake(_))
        ));
    });

    // Once roots are declared equal, only Done is a valid next message.
    let reader = LocalTreeReader::new(&manager);
    let (root_page, root_hash) = reader.root_info().unwrap();
    drop(reader);
    let (client, server) = MemoryTransport::pair();
    let responder = session(2, false);
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| responder.sync_as_responder(&manager, &server));
        client
            .send(&SyncMessage::Hello {
                node_id: NodeId::from_u64(1),
                root_page,
                root_hash,
                crdt_aware: false,
            })
            .unwrap();
        assert!(matches!(
            client.recv().unwrap(),
            SyncMessage::HelloAck { in_sync: true, .. }
        ));
        client.send(&SyncMessage::PullRequest).unwrap();
        assert!(matches!(client.recv().unwrap(), SyncMessage::Error { .. }));
        assert!(matches!(
            handle.join().unwrap(),
            Err(SyncError::UnexpectedMessage { .. })
        ));
    });
}

#[test]
fn initiator_rejects_an_inconsistent_in_sync_ack() {
    let dir = tempfile::tempdir().unwrap();
    let manager = test_manager(&dir.path().join("test.db"));
    let initiator = session(1, false);
    let (client, server) = MemoryTransport::pair();

    std::thread::scope(|scope| {
        let handle = scope.spawn(|| initiator.sync_as_initiator(&manager, &client));
        let SyncMessage::Hello {
            root_page,
            root_hash,
            ..
        } = server.recv().unwrap()
        else {
            panic!("expected Hello");
        };
        let expected_in_sync = root_hash != UNKNOWN_HASH;
        server
            .send(&SyncMessage::HelloAck {
                node_id: NodeId::from_u64(2),
                root_page,
                root_hash,
                in_sync: !expected_in_sync,
                crdt_aware: false,
            })
            .unwrap();
        assert!(matches!(
            handle.join().unwrap(),
            Err(SyncError::Handshake(_))
        ));
    });
}

#[test]
fn duplicate_names_or_roots_in_a_table_list_are_rejected() {
    let base = TableInfo {
        name: b"one".to_vec(),
        root_page: PageId(7),
        root_hash: UNKNOWN_HASH,
    };
    let duplicate_name = TableInfo {
        name: b"one".to_vec(),
        root_page: PageId(8),
        root_hash: UNKNOWN_HASH,
    };
    assert!(validate_table_infos(&[base.clone(), duplicate_name]).is_err());

    let duplicate_root = TableInfo {
        name: b"two".to_vec(),
        ..base.clone()
    };
    assert!(validate_table_infos(&[base, duplicate_root]).is_err());
}

#[test]
fn terminal_diff_error_is_reported_to_the_waiting_peer() {
    let (initiator, responder) = MemoryTransport::pair();
    let result: std::result::Result<(), SyncError> = notify_database_result(
        Err(citadel_core::Error::Sync(
            "diff exceeds sync payload limit".into(),
        )),
        &initiator,
    );
    assert!(matches!(result, Err(SyncError::Database(_))));
    assert!(matches!(
        responder.recv().unwrap(),
        SyncMessage::Error { message } if message.contains("payload limit")
    ));
}
