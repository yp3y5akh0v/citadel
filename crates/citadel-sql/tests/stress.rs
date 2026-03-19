//! Stress tests: bulk operations, resource management, repeated open/close cycles.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

// ── Bulk insert/select ─────────────────────────────────────────────

#[test]
fn insert_1000_rows_verify_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();

    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 7))
            .unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1000));

    // Verify each row
    for i in 0..1000i64 {
        let qr = conn
            .query(&format!("SELECT val FROM t WHERE id = {i}"))
            .unwrap();
        assert_eq!(qr.rows.len(), 1, "row {i} missing");
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(i * 7),
            "wrong value for row {i}"
        );
    }
}

#[test]
fn insert_then_delete_half_verify_remaining() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    for i in 0..500 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    // Delete even-numbered rows
    for i in (0..500).step_by(2) {
        conn.execute(&format!("DELETE FROM t WHERE id = {i}"))
            .unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(250));

    // Verify only odd rows remain
    for i in 0..500 {
        let qr = conn
            .query(&format!("SELECT id FROM t WHERE id = {i}"))
            .unwrap();
        if i % 2 == 0 {
            assert_eq!(qr.rows.len(), 0, "even row {i} should be deleted");
        } else {
            assert_eq!(qr.rows.len(), 1, "odd row {i} should exist");
        }
    }
}

#[test]
fn update_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 0..200 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 0)"))
            .unwrap();
    }

    // Update all rows
    conn.execute("UPDATE t SET val = id * 3").unwrap();

    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 200);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64));
        assert_eq!(row[1], Value::Integer(i as i64 * 3));
    }
}

// ── Many sequential transactions ───────────────────────────────────

#[test]
fn many_sequential_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, batch INTEGER)")
        .unwrap();

    // Each INSERT is its own transaction (auto-commit)
    let mut id = 0;
    for batch in 0..50 {
        for _ in 0..10 {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, {batch})"))
                .unwrap();
            id += 1;
        }
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(500));

    // Verify batch assignments
    for batch in 0..50 {
        let qr = conn
            .query(&format!("SELECT COUNT(*) FROM t WHERE batch = {batch}"))
            .unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(10),
            "batch {batch} should have 10 rows"
        );
    }
}

// ── Create/drop table cycles ───────────────────────────────────────

#[test]
fn create_drop_cycle_50_times() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    for i in 0..50 {
        let name = format!("table_{i}");
        conn.execute(&format!(
            "CREATE TABLE {name} (id INTEGER NOT NULL PRIMARY KEY, val TEXT)"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO {name} VALUES (1, 'round_{i}')"))
            .unwrap();

        let qr = conn
            .query(&format!("SELECT val FROM {name} WHERE id = 1"))
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Text(format!("round_{i}")));

        conn.execute(&format!("DROP TABLE {name}")).unwrap();
    }

    assert!(conn.tables().is_empty());
}

#[test]
fn recreate_same_table_with_different_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Create with 2 columns
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    // Drop and recreate with different schema (3 columns, different types)
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, score REAL, active BOOLEAN)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 99.5, TRUE)")
        .unwrap();

    let qr = conn
        .query("SELECT score, active FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(99.5));
    assert_eq!(qr.rows[0][1], Value::Boolean(true));
}

// ── Many tables simultaneously ─────────────────────────────────────

#[test]
fn create_50_tables_all_active() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    for i in 0..50 {
        conn.execute(&format!(
            "CREATE TABLE t_{i} (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)"
        ))
        .unwrap();
        conn.execute(&format!("INSERT INTO t_{i} VALUES ({i}, {})", i * 100))
            .unwrap();
    }

    assert_eq!(conn.tables().len(), 50);

    // Verify each table independently
    for i in 0..50 {
        let qr = conn
            .query(&format!("SELECT val FROM t_{i} WHERE id = {i}"))
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(i * 100));
    }
}

// ── Persistence stress ─────────────────────────────────────────────

#[test]
fn repeated_open_close_cycles() {
    let dir = tempfile::tempdir().unwrap();

    // Create initial data
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    }

    // Open/close 20 times, adding data each time
    for cycle in 2..=21 {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(cycle - 1),
            "wrong count at cycle {cycle}"
        );

        conn.execute(&format!("INSERT INTO t VALUES ({cycle}, {})", cycle * 10))
            .unwrap();
    }

    // Final verification
    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(21));
    }
}

#[test]
fn write_close_reopen_verify_10_cycles() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, data TEXT)")
            .unwrap();
    }

    for cycle in 0..10 {
        // Open and write
        {
            let db = open_db(dir.path());
            let mut conn = Connection::open(&db).unwrap();
            for j in 0..10 {
                let id = cycle * 10 + j;
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({id}, 'cycle_{cycle}_item_{j}')"
                ))
                .unwrap();
            }
        }

        // Reopen and verify
        {
            let db = open_db(dir.path());
            let mut conn = Connection::open(&db).unwrap();
            let expected_count = (cycle + 1) * 10;
            let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
            assert_eq!(
                qr.rows[0][0],
                Value::Integer(expected_count as i64),
                "wrong count after cycle {cycle}"
            );
        }
    }
}

// ── Multiple connections to same database ──────────────────────────

#[test]
fn multiple_connections_same_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    let mut conn1 = Connection::open(&db).unwrap();
    conn1
        .execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    // Second connection should see the table
    let mut conn2 = Connection::open(&db).unwrap();
    let qr = conn2.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn connection_sees_writes_from_other_connection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    let mut conn1 = Connection::open(&db).unwrap();
    conn1
        .execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    let mut conn2 = Connection::open(&db).unwrap();

    // conn2 should see conn1's data
    let qr = conn2.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));

    // conn1 writes more
    conn1.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    // conn2 should see the new data
    let qr = conn2.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ── Delete then re-insert ──────────────────────────────────────────

#[test]
fn delete_all_reinsert_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, gen INTEGER)")
        .unwrap();

    for gen in 0..10 {
        // Insert 50 rows
        for i in 0..50 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {gen})"))
                .unwrap();
        }

        let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(50),
            "wrong count in generation {gen}"
        );

        // Delete all
        conn.execute("DELETE FROM t").unwrap();

        let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(0),
            "table should be empty after delete in generation {gen}"
        );
    }
}

// ── Large text values ──────────────────────────────────────────────

#[test]
fn large_text_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, data TEXT)")
        .unwrap();

    // Insert progressively larger strings (up to 500 chars)
    for i in 1..=20 {
        let size = i * 25;
        let text = "x".repeat(size);
        conn.execute(&format!("INSERT INTO t VALUES ({i}, '{text}')"))
            .unwrap();
    }

    // Verify each
    for i in 1..=20 {
        let size = i * 25;
        let qr = conn
            .query(&format!("SELECT data FROM t WHERE id = {i}"))
            .unwrap();
        match &qr.rows[0][0] {
            Value::Text(s) => assert_eq!(s.len(), size, "wrong length for row {i}"),
            other => panic!("expected Text for row {i}, got {other:?}"),
        }
    }
}

#[test]
fn value_near_max_inline_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, data TEXT)")
        .unwrap();

    // Try a value that might be close to MAX_INLINE_VALUE_SIZE (1920 bytes)
    // Row encoding adds overhead (type tag + length), so the actual text
    // that fits is less than 1920 bytes
    let text = "A".repeat(1800);
    conn.execute(&format!("INSERT INTO t VALUES (1, '{text}')"))
        .unwrap();

    let qr = conn.query("SELECT data FROM t WHERE id = 1").unwrap();
    match &qr.rows[0][0] {
        Value::Text(s) => assert_eq!(s.len(), 1800),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn value_exceeding_max_inline_size_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, data TEXT)")
        .unwrap();

    // 1920 is MAX_INLINE_VALUE_SIZE. Row encoding overhead:
    // col_count(2) + null_bitmap(1) + type_tag(1) + data_len(4) + data
    // So a text value of 1920 bytes would produce an encoded row of 1928 bytes
    let text = "B".repeat(1920);
    let result = conn.execute(&format!("INSERT INTO t VALUES (1, '{text}')"));
    assert!(
        result.is_err(),
        "value exceeding max inline size should be rejected"
    );
}

// ── Many columns ───────────────────────────────────────────────────

#[test]
fn table_with_20_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Build CREATE TABLE with 20 columns
    let mut cols: Vec<String> = vec!["id INTEGER NOT NULL PRIMARY KEY".into()];
    for i in 1..20 {
        cols.push(format!("col_{i} INTEGER"));
    }
    let create = format!("CREATE TABLE wide ({})", cols.join(", "));
    conn.execute(&create).unwrap();

    // Insert a row with all columns
    let mut vals: Vec<String> = vec!["1".into()];
    for i in 1..20 {
        vals.push(format!("{}", i * 10));
    }
    let insert = format!("INSERT INTO wide VALUES ({})", vals.join(", "));
    conn.execute(&insert).unwrap();

    // Verify SELECT *
    let qr = conn.query("SELECT * FROM wide WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0].len(), 20);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    for i in 1..20 {
        assert_eq!(qr.rows[0][i], Value::Integer(i as i64 * 10));
    }
}

#[test]
fn table_with_many_nullable_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // 15 nullable columns
    let mut cols: Vec<String> = vec!["id INTEGER NOT NULL PRIMARY KEY".into()];
    for i in 1..=15 {
        cols.push(format!("nullable_{i} INTEGER"));
    }
    let create = format!("CREATE TABLE sparse ({})", cols.join(", "));
    conn.execute(&create).unwrap();

    // Insert with only PK
    conn.execute("INSERT INTO sparse (id) VALUES (1)").unwrap();

    // Verify all nullable columns are NULL
    let qr = conn.query("SELECT * FROM sparse WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0].len(), 16);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    for i in 1..=15 {
        assert!(qr.rows[0][i].is_null(), "col {i} should be NULL");
    }

    // Insert with some columns set
    conn.execute("INSERT INTO sparse (id, nullable_5, nullable_10) VALUES (2, 50, 100)")
        .unwrap();

    let qr = conn
        .query("SELECT nullable_5, nullable_10 FROM sparse WHERE id = 2")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));
    assert_eq!(qr.rows[0][1], Value::Integer(100));
}

// ── ORDER BY on many rows ──────────────────────────────────────────

#[test]
fn order_by_500_rows_ascending() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    // Insert in reverse order
    for i in (0..500).rev() {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", 500 - i))
            .unwrap();
    }

    let qr = conn.query("SELECT id FROM t ORDER BY id ASC").unwrap();
    assert_eq!(qr.rows.len(), 500);
    for (idx, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(idx as i64));
    }
}

#[test]
fn order_by_text_500_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();

    for i in 0..500 {
        let name = format!("item_{:05}", 499 - i); // insert in reverse lexicographic order
        conn.execute(&format!("INSERT INTO t VALUES ({i}, '{name}')"))
            .unwrap();
    }

    let qr = conn.query("SELECT name FROM t ORDER BY name ASC").unwrap();
    assert_eq!(qr.rows.len(), 500);
    for i in 0..499 {
        let a = &qr.rows[i][0];
        let b = &qr.rows[i + 1][0];
        assert!(a <= b, "sort order broken at index {i}: {a:?} > {b:?}");
    }
}

// ── Group by with many groups ──────────────────────────────────────

#[test]
fn group_by_100_groups() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp INTEGER NOT NULL, val INTEGER)",
    )
    .unwrap();

    let mut id = 0;
    for grp in 0..100 {
        for val in 0..5 {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, {grp}, {val})"))
                .unwrap();
            id += 1;
        }
    }

    let qr = conn
        .query("SELECT grp, COUNT(*), SUM(val) FROM t GROUP BY grp ORDER BY grp")
        .unwrap();
    assert_eq!(qr.rows.len(), 100);

    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64), "wrong group at index {i}");
        assert_eq!(row[1], Value::Integer(5), "wrong count for group {i}");
        assert_eq!(row[2], Value::Integer(10), "wrong sum for group {i}"); // 0+1+2+3+4=10
    }
}

// ── Parameterized integer range ────────────────────────────────────

#[test]
fn integer_range_minus_500_to_499_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    for i in -500..500 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1000));

    // Verify ordering
    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1000);
    for (idx, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(-500 + idx as i64));
    }
}

// ── Interleaved operations ─────────────────────────────────────────

#[test]
fn interleaved_insert_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    for round in 0..20 {
        let base = round * 10;

        // Insert 10 rows
        for j in 0..10 {
            let id = base + j;
            conn.execute(&format!("INSERT INTO t VALUES ({id}, 0)"))
                .unwrap();
        }

        // Update the first 5
        for j in 0..5 {
            let id = base + j;
            conn.execute(&format!("UPDATE t SET val = 1 WHERE id = {id}"))
                .unwrap();
        }

        // Delete the last 3
        for j in 7..10 {
            let id = base + j;
            conn.execute(&format!("DELETE FROM t WHERE id = {id}"))
                .unwrap();
        }
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    // 20 rounds * (10 inserted - 3 deleted) = 140
    assert_eq!(qr.rows[0][0], Value::Integer(140));

    // Verify updates: first 5 of each round have val=1, next 2 have val=0
    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100)); // 20 * 5

    let qr = conn.query("SELECT COUNT(*) FROM t WHERE val = 0").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(40)); // 20 * 2
}

// ── Schema persistence across many cycles ──────────────────────────

#[test]
fn schema_persists_with_multiple_tables_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute(
            "CREATE TABLE posts (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER, body TEXT)",
        )
        .unwrap();
        conn.execute("CREATE TABLE tags (id INTEGER NOT NULL PRIMARY KEY, label TEXT NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO posts VALUES (1, 1, 'Hello world')")
            .unwrap();
        conn.execute("INSERT INTO tags VALUES (1, 'rust')").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let mut tables = conn.tables();
        tables.sort();
        assert_eq!(tables, vec!["posts", "tags", "users"]);

        let qr = conn.query("SELECT name FROM users WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));

        let qr = conn.query("SELECT body FROM posts WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("Hello world".into()));

        let qr = conn.query("SELECT label FROM tags WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("rust".into()));

        // Drop one table, add another
        conn.execute("DROP TABLE posts").unwrap();
        conn.execute("CREATE TABLE comments (id INTEGER NOT NULL PRIMARY KEY, text TEXT)")
            .unwrap();
        conn.execute("INSERT INTO comments VALUES (1, 'Nice!')")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let mut tables = conn.tables();
        tables.sort();
        assert_eq!(tables, vec!["comments", "tags", "users"]);

        // posts table should be gone
        let result = conn.query("SELECT * FROM posts");
        assert!(result.is_err());

        let qr = conn
            .query("SELECT text FROM comments WHERE id = 1")
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("Nice!".into()));
    }
}

// ── Bulk multi-row insert ──────────────────────────────────────────

#[test]
fn multi_row_insert_100_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    // Build a single INSERT with 100 value tuples
    for batch in 0..5 {
        let mut values = Vec::new();
        for j in 0..100 {
            let id = batch * 100 + j;
            values.push(format!("({id}, {})", id * 2));
        }
        let sql = format!("INSERT INTO t VALUES {}", values.join(", "));
        match conn.execute(&sql) {
            Ok(ExecutionResult::RowsAffected(n)) => assert_eq!(n, 100),
            other => panic!("expected RowsAffected(100), got {other:?}"),
        }
    }

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(500));
}

// ── Mixed type columns stress ──────────────────────────────────────

#[test]
fn all_types_in_one_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE mixed (
            id INTEGER NOT NULL PRIMARY KEY,
            int_col INTEGER,
            real_col REAL,
            text_col TEXT,
            bool_col BOOLEAN
        )",
    )
    .unwrap();

    for i in 0..100 {
        let real_val = i as f64 * 0.1;
        let text_val = format!("item_{i}");
        let bool_val = if i % 2 == 0 { "TRUE" } else { "FALSE" };
        conn.execute(&format!(
            "INSERT INTO mixed VALUES ({i}, {}, {real_val}, '{text_val}', {bool_val})",
            i * 100
        ))
        .unwrap();
    }

    // Query each type with WHERE
    let qr = conn
        .query("SELECT COUNT(*) FROM mixed WHERE int_col >= 5000")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));

    let qr = conn
        .query("SELECT COUNT(*) FROM mixed WHERE real_col > 5.0")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(49)); // 51..99 -> 49 rows (5.1, 5.2, ..., 9.9)

    let qr = conn
        .query("SELECT COUNT(*) FROM mixed WHERE bool_col = TRUE")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(50));

    // Aggregation over mixed
    let qr = conn.query("SELECT SUM(int_col) FROM mixed").unwrap();
    // Sum of 0, 100, 200, ..., 9900 = 100 * (0+1+...+99) = 100 * 4950 = 495000
    assert_eq!(qr.rows[0][0], Value::Integer(495000));

    let qr = conn
        .query("SELECT MIN(text_col), MAX(text_col) FROM mixed")
        .unwrap();
    // Lexicographic: "item_0" < "item_1" < ... "item_99"
    assert_eq!(qr.rows[0][0], Value::Text("item_0".into()));
    assert_eq!(qr.rows[0][1], Value::Text("item_99".into()));
}

// ── Update all rows then verify persistence ────────────────────────

#[test]
fn update_all_then_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
            .unwrap();
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 0)"))
                .unwrap();
        }
        conn.execute("UPDATE t SET val = id * 2").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 100);
        for (i, row) in qr.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Integer(i as i64));
            assert_eq!(row[1], Value::Integer(i as i64 * 2), "wrong val for id {i}");
        }
    }
}

// ── Rapid successive queries ───────────────────────────────────────

#[test]
fn rapid_queries_500() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    for i in 0..50 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * i))
            .unwrap();
    }

    // Execute 500 queries rapidly
    for _ in 0..10 {
        for i in 0..50 {
            let qr = conn
                .query(&format!("SELECT val FROM t WHERE id = {i}"))
                .unwrap();
            assert_eq!(qr.rows[0][0], Value::Integer(i * i));
        }
    }
}

// ── DISTINCT stress ─────────────────────────────────────────────────

#[test]
fn distinct_many_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();

    for i in 0..500 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 10))
            .unwrap();
    }

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64));
    }
}

#[test]
fn distinct_multi_column_many_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)",
    )
    .unwrap();

    for i in 0..300 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {}, {})", i % 5, i % 7))
            .unwrap();
    }

    let qr = conn
        .query("SELECT DISTINCT a, b FROM t ORDER BY a, b")
        .unwrap();
    assert_eq!(qr.rows.len(), 35);
}

#[test]
fn distinct_with_limit_on_large_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();

    for i in 0..1000 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 50))
            .unwrap();
    }

    let qr = conn
        .query("SELECT DISTINCT val FROM t ORDER BY val LIMIT 5")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64));
    }
}
