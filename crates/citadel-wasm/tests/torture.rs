use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::Connection;

fn create_db() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn create_db_small_cache(cache: usize) -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"torture-test")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(cache)
        .create_in_memory()
        .unwrap()
}

struct SimpleRng(u32);
impl SimpleRng {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
}

// ============================================================
// BTreeMap reference tests
// ============================================================

#[test]
fn reference_500_txns_random_ops() {
    let db = create_db();
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = SimpleRng(12345);

    for txn in 0..500u32 {
        let mut wtx = db.begin_write().unwrap();
        let num_ops = 1 + rng.next() % 8;

        for _ in 0..num_ops {
            let key_id = rng.next() % 200;
            let key = format!("k{key_id:04}").into_bytes();

            if rng.next() % 10 < 7 {
                let val = format!("t{txn}-v{}", rng.next() % 100).into_bytes();
                wtx.insert(&key, &val).unwrap();
                expected.insert(key, val);
            } else if expected.contains_key(&key) {
                wtx.delete(&key).unwrap();
                expected.remove(&key);
            }
        }

        wtx.commit().unwrap();

        if txn % 100 == 99 {
            let mut rtx = db.begin_read();
            assert_eq!(
                rtx.entry_count(),
                expected.len() as u64,
                "count mismatch at txn {txn}"
            );
            for (k, v) in &expected {
                assert_eq!(
                    rtx.get(k).unwrap(),
                    Some(v.clone()),
                    "value mismatch at txn {txn}"
                );
            }
        }
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), expected.len() as u64);
    let mut scan = Vec::new();
    rtx.for_each(|k, v| {
        scan.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();

    let expected: Vec<_> = expected.into_iter().collect();
    assert_eq!(scan.len(), expected.len());
    for (s, o) in scan.iter().zip(expected.iter()) {
        assert_eq!(s, o, "cursor scan mismatch");
    }
}

#[test]
fn small_cache_forces_eviction() {
    let db = create_db_small_cache(8);
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = SimpleRng(54321);

    for txn in 0..200u32 {
        let mut wtx = db.begin_write().unwrap();
        let num_ops = 1 + rng.next() % 6;

        for _ in 0..num_ops {
            let key_id = rng.next() % 150;
            let key = format!("k{key_id:04}").into_bytes();

            if rng.next() % 10 < 7 {
                let val = format!("t{txn}-v{}", rng.next() % 100).into_bytes();
                wtx.insert(&key, &val).unwrap();
                expected.insert(key, val);
            } else if expected.contains_key(&key) {
                wtx.delete(&key).unwrap();
                expected.remove(&key);
            }
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), expected.len() as u64);
    for (k, v) in &expected {
        assert_eq!(
            rtx.get(k).unwrap(),
            Some(v.clone()),
            "eviction-forced mismatch for key {:?}",
            String::from_utf8_lossy(k)
        );
    }
}

#[test]
fn small_cache_multi_batch() {
    let db = create_db_small_cache(8);

    for batch in 0..20u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            let key = format!("b{batch:02}-k{i:02}");
            wtx.insert(key.as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1000);
    // Verify first and last batch survived eviction
    assert_eq!(rtx.get(b"b00-k00").unwrap(), Some(b"v".to_vec()));
    assert_eq!(rtx.get(b"b19-k49").unwrap(), Some(b"v".to_vec()));
    // Spot check middle
    assert_eq!(rtx.get(b"b10-k25").unwrap(), Some(b"v".to_vec()));
}

// ============================================================
// MVCC snapshot isolation
// ============================================================

#[test]
fn snapshot_isolation_during_heavy_writes() {
    let db = create_db();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut snap = db.begin_read();

    for round in 0..10u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.insert(
                format!("k{i:04}").as_bytes(),
                format!("round{round}").as_bytes(),
            )
            .unwrap();
        }
        wtx.commit().unwrap();
    }

    // Snapshot still sees original values
    assert_eq!(snap.entry_count(), 200);
    for i in 0..200u32 {
        let key = format!("k{i:04}");
        assert_eq!(
            snap.get(key.as_bytes()).unwrap(),
            Some(b"v1".to_vec()),
            "snapshot corrupted for key {key}"
        );
    }
}

#[test]
fn multiple_snapshots_different_versions() {
    let db = create_db();

    // Version 1: 10 keys
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..10u32 {
            wtx.insert(format!("k{i}").as_bytes(), b"v1").unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r1 = db.begin_read();

    // Version 2: add 10 more
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 10..20u32 {
            wtx.insert(format!("k{i}").as_bytes(), b"v2").unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r2 = db.begin_read();

    // Version 3: delete first 5
    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..5u32 {
            wtx.delete(format!("k{i}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }
    let mut r3 = db.begin_read();

    assert_eq!(r1.entry_count(), 10);
    assert_eq!(r2.entry_count(), 20);
    assert_eq!(r3.entry_count(), 15);

    assert_eq!(r1.get(b"k0").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(r2.get(b"k15").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(r3.get(b"k0").unwrap(), None);
    assert_eq!(r3.get(b"k5").unwrap(), Some(b"v1".to_vec()));
}

// ============================================================
// Abort correctness
// ============================================================

#[test]
fn many_aborts_then_commit() {
    let db = create_db();

    for _ in 0..50 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"aborted", b"should-not-exist").unwrap();
        wtx.abort();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"real", b"data").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1);
    assert_eq!(rtx.get(b"aborted").unwrap(), None);
    assert_eq!(rtx.get(b"real").unwrap(), Some(b"data".to_vec()));
}

#[test]
fn drop_write_txn_releases_lock() {
    let db = create_db();

    for _ in 0..100 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"temp", b"v").unwrap();
    }

    let mut wtx = db.begin_write().unwrap();
    wtx.insert(b"final", b"v").unwrap();
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 1);
    assert_eq!(rtx.get(b"final").unwrap(), Some(b"v".to_vec()));
}

// ============================================================
// Insert/delete churn with verification
// ============================================================

#[test]
fn insert_delete_half_verify_remaining() {
    let db = create_db();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..500u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        for i in (0..500u32).step_by(2) {
            wtx.delete(format!("k{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 250);

    for i in (1..500u32).step_by(2) {
        let key = format!("k{i:04}");
        let val = format!("v{i:04}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "odd key {key} should exist"
        );
    }
    for i in (0..500u32).step_by(2) {
        let key = format!("k{i:04}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            None,
            "even key {key} should be deleted"
        );
    }
}

#[test]
fn rapid_overwrite_100_txns() {
    let db = create_db();

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..50u32 {
            wtx.insert(format!("k{i:02}").as_bytes(), b"original")
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    for round in 0..100u32 {
        let mut wtx = db.begin_write().unwrap();
        let val = format!("round{round:03}");
        for i in 0..50u32 {
            wtx.insert(format!("k{i:02}").as_bytes(), val.as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 50);
    for i in 0..50u32 {
        let key = format!("k{i:02}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(b"round099".to_vec()),
            "key {key}"
        );
    }
}

#[test]
fn insert_delete_reinsert_50_cycles() {
    let db = create_db();

    for round in 0..50u32 {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"cycle-key", format!("round-{round}").as_bytes())
            .unwrap();
        wtx.commit().unwrap();

        let mut wtx = db.begin_write().unwrap();
        wtx.delete(b"cycle-key").unwrap();
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        wtx.insert(b"cycle-key", b"final").unwrap();
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.get(b"cycle-key").unwrap(), Some(b"final".to_vec()));
    assert_eq!(rtx.entry_count(), 1);
}

// ============================================================
// Named tables
// ============================================================

#[test]
fn named_tables_reference() {
    let db = create_db();
    let mut expected_maps: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = vec![BTreeMap::new(); 5];

    let mut wtx = db.begin_write().unwrap();
    for t in 0..5 {
        wtx.create_table(format!("t{t}").as_bytes()).unwrap();
    }

    for t in 0..5u8 {
        let table = format!("t{t}");
        for i in 0u32..80 {
            let key = format!("k{i:04}").into_bytes();
            let val = vec![t; 32];
            wtx.table_insert(table.as_bytes(), &key, &val).unwrap();
            expected_maps[t as usize].insert(key, val);
        }
    }
    wtx.commit().unwrap();

    // Delete half from tables 0 and 2
    let mut wtx = db.begin_write().unwrap();
    for t in [0u8, 2] {
        let table = format!("t{t}");
        for i in (0u32..80).step_by(2) {
            let key = format!("k{i:04}").into_bytes();
            wtx.table_delete(table.as_bytes(), &key).unwrap();
            expected_maps[t as usize].remove(&key);
        }
    }
    wtx.commit().unwrap();

    let mut rtx = db.begin_read();
    for t in 0..5u8 {
        let table = format!("t{t}");
        let exp = &expected_maps[t as usize];

        let mut actual = Vec::new();
        rtx.table_for_each(table.as_bytes(), |k, v| {
            actual.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();

        let expected: Vec<_> = exp.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(actual.len(), expected.len(), "table {table} count mismatch");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a, e, "table {table} data mismatch");
        }
    }
}

#[test]
fn create_drop_recreate_30_tables() {
    let db = create_db();

    {
        let mut wtx = db.begin_write().unwrap();
        for t in 0..30u32 {
            wtx.create_table(format!("t{t:03}").as_bytes()).unwrap();
            wtx.table_insert(
                format!("t{t:03}").as_bytes(),
                b"k",
                format!("v{t}").as_bytes(),
            )
            .unwrap();
        }
        wtx.commit().unwrap();
    }

    // Drop even
    {
        let mut wtx = db.begin_write().unwrap();
        for t in (0..30u32).step_by(2) {
            wtx.drop_table(format!("t{t:03}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    for t in (1..30u32).step_by(2) {
        let name = format!("t{t:03}");
        assert_eq!(
            rtx.table_get(name.as_bytes(), b"k").unwrap(),
            Some(format!("v{t}").into_bytes()),
            "table {name}"
        );
    }
    for t in (0..30u32).step_by(2) {
        let name = format!("t{t:03}");
        assert!(
            rtx.table_get(name.as_bytes(), b"k").is_err(),
            "dropped table {name} should not exist"
        );
    }
}

// ============================================================
// Concurrent readers (thread safety with in-memory)
// ============================================================

#[test]
fn concurrent_readers_threaded() {
    let db = Arc::new(create_db());

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..1000u32 {
            wtx.insert(format!("k{i:05}").as_bytes(), format!("v{i:05}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut handles = Vec::new();
    for thread_id in 0..4u32 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let mut rtx = db_clone.begin_read();
            assert_eq!(rtx.entry_count(), 1000);
            let start = thread_id * 250;
            for i in start..start + 250 {
                let key = format!("k{i:05}");
                let val = format!("v{i:05}");
                assert_eq!(
                    rtx.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "thread {thread_id}, key {key}"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================
// SQL with small cache (full stack through MemoryPageIO)
// ============================================================

#[test]
fn sql_crud_small_cache() {
    let db = create_db_small_cache(16);
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE stress (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER NOT NULL)",
    )
    .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..500 {
        conn.execute(&format!(
            "INSERT INTO stress (id, name, score) VALUES ({i}, 'user_{i}', {})",
            i * 10
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM stress").unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(500));

    conn.execute("UPDATE stress SET score = score + 1 WHERE id < 250")
        .unwrap();

    let qr = conn.query("SELECT score FROM stress WHERE id = 0").unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(1));

    let qr = conn
        .query("SELECT score FROM stress WHERE id = 499")
        .unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(4990));

    conn.execute("DELETE FROM stress WHERE id >= 250").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM stress").unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(250));
}

#[test]
fn sql_join_small_cache() {
    let db = create_db_small_cache(16);
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    conn.execute(
        "CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL)",
    )
    .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..50 {
        conn.execute(&format!(
            "INSERT INTO authors (id, name) VALUES ({i}, 'Author_{i}')"
        ))
        .unwrap();
        for j in 0..4 {
            let book_id = i * 4 + j;
            conn.execute(&format!(
                "INSERT INTO books (id, author_id, title) VALUES ({book_id}, {i}, 'Book_{book_id}')"
            ))
            .unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT a.name, COUNT(*) FROM authors a JOIN books b ON a.id = b.author_id GROUP BY a.name ORDER BY a.name LIMIT 5")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for row in &qr.rows {
        assert_eq!(row[1], citadel_sql::types::Value::Integer(4));
    }
}

#[test]
fn sql_index_small_cache() {
    let db = create_db_small_cache(16);
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE indexed (id INTEGER PRIMARY KEY, cat TEXT NOT NULL, val INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_cat ON indexed (cat)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..300 {
        let cat = match i % 3 {
            0 => "alpha",
            1 => "beta",
            _ => "gamma",
        };
        conn.execute(&format!(
            "INSERT INTO indexed (id, cat, val) VALUES ({i}, '{cat}', {i})"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query("SELECT COUNT(*) FROM indexed WHERE cat = 'alpha'")
        .unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(100));

    let qr = conn
        .query("EXPLAIN SELECT * FROM indexed WHERE cat = 'beta'")
        .unwrap();
    let plan_text = format!("{}", qr.rows[0][0]);
    assert!(
        plan_text.contains("USING INDEX"),
        "expected index scan, got: {plan_text}"
    );
}

#[test]
fn sql_subquery_small_cache() {
    let db = create_db_small_cache(16);
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for i in 0..100 {
        conn.execute(&format!(
            "INSERT INTO scores (id, val) VALUES ({i}, {})",
            i % 10
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = conn
        .query(
            "SELECT COUNT(*) FROM scores WHERE val IN (SELECT DISTINCT val FROM scores WHERE val > 5)",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(40));

    let qr = conn
        .query("SELECT COUNT(*) FROM scores WHERE val > (SELECT AVG(val) FROM scores)")
        .unwrap();
    assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(50));
}

#[test]
fn sql_prepared_params_small_cache() {
    let db = create_db_small_cache(16);
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE params (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();

    for i in 0..100 {
        conn.execute_params(
            "INSERT INTO params (id, val) VALUES ($1, $2)",
            &[
                citadel_sql::types::Value::Integer(i),
                citadel_sql::types::Value::Text(format!("val_{i}").into()),
            ],
        )
        .unwrap();
    }

    for i in 0..100i64 {
        let qr = conn
            .query_params(
                "SELECT val FROM params WHERE id = $1",
                &[citadel_sql::types::Value::Integer(i)],
            )
            .unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(
            qr.rows[0][0],
            citadel_sql::types::Value::Text(format!("val_{i}").into())
        );
    }
}

// ============================================================
// Integrity check after heavy churn
// ============================================================

#[test]
fn integrity_after_heavy_churn() {
    let db = create_db();

    for round in 0..5u32 {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            let key = format!("r{round}-{i:04}");
            wtx.insert(key.as_bytes(), b"data").unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..200u32 {
            wtx.delete(format!("r0-{i:04}").as_bytes()).unwrap();
            wtx.delete(format!("r1-{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(
        report.errors.is_empty(),
        "integrity errors: {:?}",
        report.errors
    );
    assert!(report.pages_checked > 0);
    assert_eq!(db.stats().entry_count, 600);
}

#[test]
fn integrity_small_cache_after_churn() {
    let db = create_db_small_cache(8);

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..300u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
                .unwrap();
        }
        wtx.commit().unwrap();
    }

    {
        let mut wtx = db.begin_write().unwrap();
        for i in (0..300u32).step_by(3) {
            wtx.delete(format!("k{i:04}").as_bytes()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let report = db.integrity_check().unwrap();
    assert!(
        report.errors.is_empty(),
        "integrity errors after churn with small cache: {:?}",
        report.errors
    );
    assert_eq!(db.stats().entry_count, 200);
}

// ============================================================
// Large values through in-memory path
// ============================================================

#[test]
fn large_values_near_overflow_threshold() {
    let db = create_db();
    let big_val = vec![0x42u8; 1800];

    {
        let mut wtx = db.begin_write().unwrap();
        for i in 0..100u32 {
            wtx.insert(format!("k{i:04}").as_bytes(), &big_val).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    assert_eq!(rtx.entry_count(), 100);
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        assert_eq!(
            rtx.get(key.as_bytes()).unwrap(),
            Some(big_val.clone()),
            "key {key}"
        );
    }
}

// ============================================================
// SQL multi-table isolation
// ============================================================

#[test]
fn sql_multi_table_create_use_drop() {
    let db = create_db();
    let mut conn = Connection::open(&db).unwrap();

    for t in 0..5 {
        conn.execute(&format!(
            "CREATE TABLE tbl_{t} (id INTEGER PRIMARY KEY, data TEXT NOT NULL)"
        ))
        .unwrap();
    }

    for t in 0..5 {
        conn.execute("BEGIN").unwrap();
        for i in 0..20 {
            conn.execute(&format!(
                "INSERT INTO tbl_{t} (id, data) VALUES ({i}, 'tbl{t}_row{i}')"
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    for t in 0..5 {
        let qr = conn
            .query(&format!("SELECT COUNT(*) FROM tbl_{t}"))
            .unwrap();
        assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(20));
    }

    conn.execute("DROP TABLE tbl_2").unwrap();
    assert!(conn.execute("SELECT * FROM tbl_2").is_err());

    for t in [0, 1, 3, 4] {
        let qr = conn
            .query(&format!("SELECT COUNT(*) FROM tbl_{t}"))
            .unwrap();
        assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(20));
    }
}

// ============================================================
// Cursor scan ordering with small cache
// ============================================================

#[test]
fn cursor_scan_sorted_order_small_cache() {
    let db = create_db_small_cache(8);

    {
        let mut wtx = db.begin_write().unwrap();
        // Insert in reverse order to stress the tree
        for i in (0..200u32).rev() {
            wtx.insert(format!("k{i:04}").as_bytes(), b"v").unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut rtx = db.begin_read();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0u32;
    rtx.for_each(|k, _| {
        if let Some(p) = &prev {
            assert!(k > p.as_slice(), "keys not sorted");
        }
        prev = Some(k.to_vec());
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 200);
}

// ============================================================
// Repeated create/drop table cycles (SQL)
// ============================================================

#[test]
fn repeated_create_drop_table_sql() {
    let db = create_db();
    let mut conn = Connection::open(&db).unwrap();

    for round in 0..20 {
        conn.execute("CREATE TABLE temp (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .unwrap();
        for i in 0..10 {
            conn.execute(&format!("INSERT INTO temp (id, v) VALUES ({i}, {round})"))
                .unwrap();
        }
        let qr = conn.query("SELECT COUNT(*) FROM temp").unwrap();
        assert_eq!(qr.rows[0][0], citadel_sql::types::Value::Integer(10));
        conn.execute("DROP TABLE temp").unwrap();
    }
}
