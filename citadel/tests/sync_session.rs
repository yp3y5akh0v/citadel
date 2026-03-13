use std::collections::BTreeMap;
use std::thread;

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sync::{
    MemoryTransport, NodeId, SyncConfig, SyncDirection, SyncOutcome, SyncSession,
};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"sync-session-test")
        .argon2_profile(Argon2Profile::Iot)
}

fn collect_all(db: &Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut data = BTreeMap::new();
    let mut rtx = db.begin_read();
    rtx.for_each(|k, v| {
        data.insert(k.to_vec(), v.to_vec());
        Ok(())
    })
    .unwrap();
    data
}

fn insert_range(db: &Database, start: u32, end: u32) {
    let mut wtx = db.begin_write().unwrap();
    for i in start..end {
        wtx.insert(&i.to_be_bytes(), &(i * 7).to_le_bytes())
            .unwrap();
    }
    wtx.commit().unwrap();
}

fn sync_push(
    initiator_db: &Database,
    responder_db: &Database,
) -> (SyncOutcome, SyncOutcome) {
    let (t_init, t_resp) = MemoryTransport::pair();

    let init_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(1),
        direction: SyncDirection::Push,
        crdt_aware: false,
    });
    let resp_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(2),
        direction: SyncDirection::Push,
        crdt_aware: false,
    });

    let init_mgr = initiator_db.manager();
    let resp_mgr = responder_db.manager();

    thread::scope(|s| {
        let h1 = s.spawn(|| init_session.sync_as_initiator(init_mgr, &t_init).unwrap());
        let h2 = s.spawn(|| resp_session.sync_as_responder(resp_mgr, &t_resp).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    })
}

fn sync_pull(
    initiator_db: &Database,
    responder_db: &Database,
) -> (SyncOutcome, SyncOutcome) {
    let (t_init, t_resp) = MemoryTransport::pair();

    let init_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(1),
        direction: SyncDirection::Pull,
        crdt_aware: false,
    });
    let resp_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(2),
        direction: SyncDirection::Pull,
        crdt_aware: false,
    });

    let init_mgr = initiator_db.manager();
    let resp_mgr = responder_db.manager();

    thread::scope(|s| {
        let h1 = s.spawn(|| init_session.sync_as_initiator(init_mgr, &t_init).unwrap());
        let h2 = s.spawn(|| resp_session.sync_as_responder(resp_mgr, &t_resp).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    })
}

fn sync_bidi(
    initiator_db: &Database,
    responder_db: &Database,
) -> (SyncOutcome, SyncOutcome) {
    let (t_init, t_resp) = MemoryTransport::pair();

    let init_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(1),
        direction: SyncDirection::Bidirectional,
        crdt_aware: false,
    });
    let resp_session = SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(2),
        direction: SyncDirection::Bidirectional,
        crdt_aware: false,
    });

    let init_mgr = initiator_db.manager();
    let resp_mgr = responder_db.manager();

    thread::scope(|s| {
        let h1 = s.spawn(|| init_session.sync_as_initiator(init_mgr, &t_init).unwrap());
        let h2 = s.spawn(|| resp_session.sync_as_responder(resp_mgr, &t_resp).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    })
}

// ============================================================
// Push tests
// ============================================================

#[test]
fn push_identical_dbs_already_in_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let (init_out, resp_out) = sync_push(&db1, &db2);
    assert!(init_out.already_in_sync);
    assert!(resp_out.already_in_sync);
    assert!(init_out.pushed.is_none());
    assert!(resp_out.pushed.is_none());
}

#[test]
fn push_identical_nonempty_dbs() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for db in [&db1, &db2] {
        insert_range(db, 0, 20);
    }

    let (init_out, resp_out) = sync_push(&db1, &db2);
    assert!(init_out.already_in_sync);
    assert!(resp_out.already_in_sync);
}

#[test]
fn push_one_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 1);

    let (init_out, _) = sync_push(&db1, &db2);
    assert!(!init_out.already_in_sync);
    let pushed = init_out.pushed.unwrap();
    assert!(pushed.entries_applied > 0);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn push_many_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 100);

    let (init_out, _) = sync_push(&db1, &db2);
    assert!(!init_out.already_in_sync);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn push_empty_to_populated() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db2, 0, 50);

    // Push from empty db1 → populated db2: no changes to push
    let (init_out, _) = sync_push(&db1, &db2);
    assert!(!init_out.already_in_sync);

    // db2 still has its data
    assert_eq!(collect_all(&db2).len(), 50);
}

#[test]
fn push_populated_to_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 50);

    let (init_out, _) = sync_push(&db1, &db2);
    assert!(!init_out.already_in_sync);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Pull tests
// ============================================================

#[test]
fn pull_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db2, 0, 50);

    // Initiator (db1) pulls from responder (db2)
    let (init_out, _) = sync_pull(&db1, &db2);
    assert!(!init_out.already_in_sync);
    let pulled = init_out.pulled.unwrap();
    assert!(pulled.entries_applied > 0);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

#[test]
fn pull_identical_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let (init_out, _) = sync_pull(&db1, &db2);
    assert!(init_out.already_in_sync);
}

// ============================================================
// Bidirectional tests
// ============================================================

#[test]
fn bidirectional_disjoint_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 25);
    insert_range(&db2, 100, 125);

    let (init_out, _) = sync_bidi(&db1, &db2);
    assert!(!init_out.already_in_sync);

    // After bidirectional sync, both databases should have all entries
    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1.len(), 50);
    assert_eq!(data2.len(), 50);
    assert_eq!(data1, data2);
}

#[test]
fn bidirectional_one_side_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 30);

    let (init_out, _) = sync_bidi(&db1, &db2);
    assert!(!init_out.already_in_sync);

    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Incremental sync tests
// ============================================================

#[test]
fn incremental_sync_3_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Round 1: push 10 entries
    insert_range(&db1, 0, 10);
    sync_push(&db1, &db2);
    assert_eq!(collect_all(&db1), collect_all(&db2));

    // Round 2: push 10 more
    insert_range(&db1, 10, 20);
    sync_push(&db1, &db2);
    assert_eq!(collect_all(&db1), collect_all(&db2));

    // Round 3: push 10 more
    insert_range(&db1, 20, 30);
    let (init_out, _) = sync_push(&db1, &db2);
    assert!(!init_out.already_in_sync);
    assert_eq!(collect_all(&db1), collect_all(&db2));
    assert_eq!(collect_all(&db2).len(), 30);
}

#[test]
fn sync_after_sync_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 20);
    sync_push(&db1, &db2);

    // Second sync should detect in-sync
    let (init_out, _) = sync_push(&db1, &db2);
    assert!(init_out.already_in_sync);
}

// ============================================================
// Value update tests
// ============================================================

#[test]
fn push_value_update() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Insert same key in both
    insert_range(&db1, 0, 5);
    insert_range(&db2, 0, 5);

    // Update a value on db1
    {
        let mut wtx = db1.begin_write().unwrap();
        wtx.insert(&0u32.to_be_bytes(), b"updated-value").unwrap();
        wtx.commit().unwrap();
    }

    sync_push(&db1, &db2);
    assert_eq!(collect_all(&db1), collect_all(&db2));
}

// ============================================================
// Large dataset test
// ============================================================

#[test]
fn push_large_dataset_500_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db2 = fast_builder(&dir.path().join("b.db")).create().unwrap();

    insert_range(&db1, 0, 500);
    sync_push(&db1, &db2);

    let data1 = collect_all(&db1);
    let data2 = collect_all(&db2);
    assert_eq!(data1.len(), 500);
    assert_eq!(data1, data2);
}

// ============================================================
// Persistence test
// ============================================================

#[test]
fn sync_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path1 = dir.path().join("a.db");
    let path2 = dir.path().join("b.db");

    {
        let db1 = fast_builder(&path1).create().unwrap();
        let db2 = fast_builder(&path2).create().unwrap();

        insert_range(&db1, 0, 30);
        sync_push(&db1, &db2);
    }

    // Reopen both databases
    let db1 = fast_builder(&path1).open().unwrap();
    let db2 = fast_builder(&path2).open().unwrap();

    assert_eq!(collect_all(&db1), collect_all(&db2));
    assert_eq!(collect_all(&db2).len(), 30);
}
