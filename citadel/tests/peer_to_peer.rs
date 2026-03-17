use std::net::TcpListener;
use std::thread;

use citadel::{Argon2Profile, DatabaseBuilder};

fn fast_builder(path: &std::path::Path) -> DatabaseBuilder {
    DatabaseBuilder::new(path)
        .passphrase(b"p2p-test")
        .argon2_profile(Argon2Profile::Iot)
}

fn listen_random_port() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn addr_of(listener: &TcpListener) -> String {
    listener.local_addr().unwrap().to_string()
}

// ============================================================
// NodeId persistence
// ============================================================

#[test]
fn node_id_generated_and_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    let id1 = {
        let db = fast_builder(&path).create().unwrap();
        db.node_id().unwrap()
    };

    let id2 = {
        let db = fast_builder(&path).open().unwrap();
        db.node_id().unwrap()
    };

    assert_eq!(id1, id2);
}

#[test]
fn node_id_stable_within_session() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    let id1 = db.node_id().unwrap();
    let id2 = db.node_id().unwrap();
    assert_eq!(id1, id2);
}

#[test]
fn node_id_unique_per_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let id_a = db_a.node_id().unwrap();
    let id_b = db_b.node_id().unwrap();
    assert_ne!(id_a, id_b);
}

// ============================================================
// Basic TCP sync
// ============================================================

#[test]
fn sync_to_pushes_kv_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Insert data into A
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        wtx.table_insert(b"data", b"k1", b"v1").unwrap();
        wtx.table_insert(b"data", b"k2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        db_a.sync_to(&addr).unwrap();
    });

    // Verify B has the data
    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"data", b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(rtx.table_get(b"data", b"k2").unwrap().unwrap(), b"v2");
}

#[test]
fn sync_to_creates_missing_table_on_responder() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // A has a table that B doesn't
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"only_a").unwrap();
        wtx.table_insert(b"only_a", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        assert!(!outcome.tables_synced.is_empty());
    });

    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"only_a", b"k").unwrap().unwrap(), b"v");
}

#[test]
fn sync_identical_databases_no_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Same data in both
    for db in [&db_a, &db_b] {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.table_insert(b"t", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        assert!(outcome.tables_synced.is_empty());
    });
}

#[test]
fn sync_multiple_tables_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"users").unwrap();
        wtx.create_table(b"orders").unwrap();
        wtx.table_insert(b"users", b"u1", b"Alice").unwrap();
        wtx.table_insert(b"orders", b"o1", b"item-A").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        assert_eq!(outcome.tables_synced.len(), 2);
    });

    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"users", b"u1").unwrap().unwrap(), b"Alice");
    assert_eq!(rtx.table_get(b"orders", b"o1").unwrap().unwrap(), b"item-A");
}

// ============================================================
// Incremental sync
// ============================================================

#[test]
fn sync_incremental_two_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // Round 1
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        wtx.table_insert(b"data", b"k1", b"v1").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        db_a.sync_to(&addr).unwrap();
    });

    // Round 2: add more data
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.table_insert(b"data", b"k2", b"v2").unwrap();
        wtx.commit().unwrap();
    }

    let listener2 = listen_random_port();
    let addr2 = addr_of(&listener2);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener2.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        db_a.sync_to(&addr2).unwrap();
    });

    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"data", b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(rtx.table_get(b"data", b"k2").unwrap().unwrap(), b"v2");
}

// ============================================================
// Sync preserves existing data
// ============================================================

#[test]
fn sync_preserves_responder_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    // A has table "alpha"
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"alpha").unwrap();
        wtx.table_insert(b"alpha", b"a1", b"100").unwrap();
        wtx.commit().unwrap();
    }

    // B has table "beta" and some default tree data
    {
        let mut wtx = db_b.begin_write().unwrap();
        wtx.create_table(b"beta").unwrap();
        wtx.table_insert(b"beta", b"b1", b"200").unwrap();
        wtx.insert(b"default-key", b"default-val").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        db_a.sync_to(&addr).unwrap();
    });

    // B should have A's data AND keep its own
    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"alpha", b"a1").unwrap().unwrap(), b"100");
    assert_eq!(rtx.table_get(b"beta", b"b1").unwrap().unwrap(), b"200");
    assert_eq!(rtx.get(b"default-key").unwrap().unwrap(), b"default-val");
}

// ============================================================
// Sync skips index tables
// ============================================================

#[test]
fn sync_skips_index_tables_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"data").unwrap();
        wtx.create_table(b"__idx_data_name").unwrap();
        wtx.table_insert(b"data", b"k", b"v").unwrap();
        wtx.table_insert(b"__idx_data_name", b"idx_k", b"idx_v").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        // Should sync "data" but NOT "__idx_data_name"
        assert!(outcome.tables_synced.iter().any(|(n, _)| n == b"data"));
        assert!(!outcome.tables_synced.iter().any(|(n, _)| n.starts_with(b"__idx_")));
    });
}

// ============================================================
// Persistence after sync
// ============================================================

#[test]
fn sync_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path_a = dir.path().join("a.db");
    let path_b = dir.path().join("b.db");

    {
        let db_a = fast_builder(&path_a).create().unwrap();
        let db_b = fast_builder(&path_b).create().unwrap();

        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.table_insert(b"t", b"k", b"v").unwrap();
        wtx.commit().unwrap();

        let listener = listen_random_port();
        let addr = addr_of(&listener);

        thread::scope(|s| {
            s.spawn(|| {
                let (stream, _) = listener.accept().unwrap();
                db_b.handle_sync(stream).unwrap();
            });
            db_a.sync_to(&addr).unwrap();
        });
    }

    // Reopen and verify
    let db_b = fast_builder(&path_b).open().unwrap();
    let mut rtx = db_b.begin_read();
    assert_eq!(rtx.table_get(b"t", b"k").unwrap().unwrap(), b"v");
}

// ============================================================
// Connection error handling
// ============================================================

#[test]
fn sync_to_connection_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fast_builder(&dir.path().join("test.db")).create().unwrap();

    // No listener on this port
    let result = db.sync_to("127.0.0.1:1");
    assert!(result.is_err());
}

// ============================================================
// Empty databases
// ============================================================

#[test]
fn sync_empty_databases_no_crash() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        assert!(outcome.tables_synced.is_empty());
    });
}

// ============================================================
// Node ID survives sync
// ============================================================

#[test]
fn node_id_survives_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    let id_a_before = db_a.node_id().unwrap();
    let id_b_before = db_b.node_id().unwrap();

    // Insert data and sync
    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"t").unwrap();
        wtx.table_insert(b"t", b"k", b"v").unwrap();
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        db_a.sync_to(&addr).unwrap();
    });

    // Node IDs should be unchanged
    assert_eq!(db_a.node_id().unwrap(), id_a_before);
    assert_eq!(db_b.node_id().unwrap(), id_b_before);
}

// ============================================================
// Large table sync
// ============================================================

#[test]
fn sync_large_table_100_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    {
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(b"big").unwrap();
        for i in 0..100u32 {
            let key = format!("key-{:04}", i);
            let val = format!("value-{}", i * 7);
            wtx.table_insert(b"big", key.as_bytes(), val.as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let listener = listen_random_port();
    let addr = addr_of(&listener);

    thread::scope(|s| {
        s.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            db_b.handle_sync(stream).unwrap();
        });
        let outcome = db_a.sync_to(&addr).unwrap();
        let synced = outcome.tables_synced.iter().find(|(n, _)| n == b"big").unwrap();
        assert_eq!(synced.1, 100);
    });

    // Spot-check a few entries
    let mut rtx = db_b.begin_read();
    assert_eq!(
        rtx.table_get(b"big", b"key-0000").unwrap().unwrap(),
        b"value-0"
    );
    assert_eq!(
        rtx.table_get(b"big", b"key-0099").unwrap().unwrap(),
        b"value-693"
    );
}

// ============================================================
// Multiple sync rounds over TCP
// ============================================================

#[test]
fn three_sync_rounds_over_tcp() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fast_builder(&dir.path().join("a.db")).create().unwrap();
    let db_b = fast_builder(&dir.path().join("b.db")).create().unwrap();

    for round in 0..3u32 {
        let table_name = format!("table_{}", round);
        let mut wtx = db_a.begin_write().unwrap();
        wtx.create_table(table_name.as_bytes()).unwrap();
        wtx.table_insert(table_name.as_bytes(), b"k", b"v").unwrap();
        wtx.commit().unwrap();

        let listener = listen_random_port();
        let addr = addr_of(&listener);

        thread::scope(|s| {
            s.spawn(|| {
                let (stream, _) = listener.accept().unwrap();
                db_b.handle_sync(stream).unwrap();
            });
            db_a.sync_to(&addr).unwrap();
        });
    }

    // B should have all 3 tables
    let mut rtx = db_b.begin_read();
    for i in 0..3u32 {
        let table_name = format!("table_{}", i);
        assert_eq!(
            rtx.table_get(table_name.as_bytes(), b"k").unwrap().unwrap(),
            b"v"
        );
    }
}
