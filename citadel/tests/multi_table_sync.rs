use std::thread;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sync::{MemoryTransport, NodeId, SyncConfig, SyncDirection, SyncSession};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"sync-test")
        .argon2_profile(Argon2Profile::Iot)
}

fn session(node: u64) -> SyncSession {
    SyncSession::new(SyncConfig {
        node_id: NodeId::from_u64(node),
        direction: SyncDirection::Push,
        crdt_aware: false,
    })
}

#[test]
fn sync_identical_tables() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    // Both have same data
    for db in [&db_a, &db_b] {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.table_insert(b"users", b"k1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);

    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        // No changes needed - tables are identical
        assert!(results.is_empty());
    });
}

#[test]
fn sync_one_table_push() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    // A has data, B has empty table
    let mut wtx = db_a.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.table_insert(b"data", b"k1", b"v1").unwrap();
    wtx.table_insert(b"data", b"k2", b"v2").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db_b.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.commit().unwrap();

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"data");
        assert_eq!(results[0].1.entries_applied, 2);
    });

    // Verify B now has the data
    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"data", b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(rtx.table_get(b"data", b"k2").unwrap().unwrap(), b"v2");
}

#[test]
fn sync_multiple_tables() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    let mut wtx = db_a.begin_write().unwrap();
    wtx.create_table(b"alpha").unwrap();
    wtx.create_table(b"beta").unwrap();
    wtx.table_insert(b"alpha", b"a1", b"100").unwrap();
    wtx.table_insert(b"beta", b"b1", b"200").unwrap();
    wtx.commit().unwrap();

    // B has the tables but empty
    let mut wtx = db_b.begin_write().unwrap();
    wtx.create_table(b"alpha").unwrap();
    wtx.create_table(b"beta").unwrap();
    wtx.commit().unwrap();

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        assert_eq!(results.len(), 2);
    });

    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"alpha", b"a1").unwrap().unwrap(), b"100");
    assert_eq!(rtx.table_get(b"beta", b"b1").unwrap().unwrap(), b"200");
}

#[test]
fn sync_disjoint_tables() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    // A has "only_a" table
    let mut wtx = db_a.begin_write().unwrap();
    wtx.create_table(b"only_a").unwrap();
    wtx.table_insert(b"only_a", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    // B has "only_b" table
    let mut wtx = db_b.begin_write().unwrap();
    wtx.create_table(b"only_b").unwrap();
    wtx.table_insert(b"only_b", b"k", b"v").unwrap();
    wtx.commit().unwrap();

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        // A has "only_a" which B doesn't - should push it
        assert!(results.iter().any(|(name, _)| name == b"only_a"));
    });

    // B should now have "only_a" data
    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"only_a", b"k").unwrap().unwrap(), b"v");
}

#[test]
fn sync_empty_tables_no_crash() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    // Neither has tables
    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        assert!(results.is_empty());
    });
}

#[test]
fn sync_skips_index_tables() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    // Create a table and an index-like table
    let mut wtx = db_a.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.create_table(b"__idx_data_name").unwrap();
    wtx.table_insert(b"data", b"k", b"v").unwrap();
    wtx.table_insert(b"__idx_data_name", b"idx_k", b"idx_v")
        .unwrap();
    wtx.commit().unwrap();

    let mut wtx = db_b.begin_write().unwrap();
    wtx.create_table(b"data").unwrap();
    wtx.commit().unwrap();

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        let results = h.join().unwrap();
        // Should sync "data" but NOT "__idx_data_name"
        assert!(results.iter().any(|(name, _)| name == b"data"));
        assert!(!results.iter().any(|(name, _)| name.starts_with(b"__idx_")));
    });
}

#[test]
fn sync_preserves_unshared_data() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir_a.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir_b.path().join("b.db")).create().unwrap();

    let mut wtx = db_a.begin_write().unwrap();
    wtx.create_table(b"shared").unwrap();
    wtx.table_insert(b"shared", b"from_a", b"val_a").unwrap();
    wtx.commit().unwrap();

    let mut wtx = db_b.begin_write().unwrap();
    wtx.create_table(b"shared").unwrap();
    wtx.table_insert(b"shared", b"from_b", b"val_b").unwrap();
    wtx.commit().unwrap();

    // Also put something in B's default tree
    let mut wtx = db_b.begin_write().unwrap();
    wtx.insert(b"default_key", b"default_val").unwrap();
    wtx.commit().unwrap();

    let (ta, tb) = MemoryTransport::pair();
    let sess_a = session(1);
    let sess_b = session(2);
    let mgr_a = db_a.manager();
    let mgr_b = db_b.manager();

    thread::scope(|s| {
        let h = s.spawn(|| sess_a.sync_tables_as_initiator(mgr_a, &ta).unwrap());
        sess_b.handle_table_sync_as_responder(mgr_b, &tb).unwrap();
        h.join().unwrap();
    });

    // B should have A's data AND keep its own
    let mut rtx = db_b.begin_read();
    assert_eq!(
        rtx.table_get(b"shared", b"from_a").unwrap().unwrap(),
        b"val_a"
    );
    assert_eq!(
        rtx.table_get(b"shared", b"from_b").unwrap().unwrap(),
        b"val_b"
    );
    // Default tree untouched
    assert_eq!(rtx.get(b"default_key").unwrap().unwrap(), b"default_val");
}
