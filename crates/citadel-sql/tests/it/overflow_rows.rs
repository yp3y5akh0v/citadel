//! Regressions for overflow-blind write paths: UPDATE / ON CONFLICT DO UPDATE
//! over rows whose encoded size exceeds the inline threshold (1920 bytes), and
//! deep-tree ascending deletes.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        other => panic!("expected RowsAffected, got {other:?}"),
    }
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

/// Growing UPDATE on a packed leaf: previously the old cell was deleted and
/// the too-large replacement silently dropped (row loss).
#[test]
fn growing_update_moves_row_to_overflow_without_loss() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
        .unwrap();
    let filler = "a".repeat(120);
    for i in 1..=60 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, '{filler}')"))
            .unwrap();
    }

    let big3k = "x".repeat(3000);
    let n = rows_affected(
        conn.execute(&format!("UPDATE t SET s = '{big3k}' WHERE id = 5"))
            .unwrap(),
    );
    assert_eq!(n, 1);

    let big10k = "y".repeat(10_000);
    let n = rows_affected(
        conn.execute(&format!("UPDATE t SET s = '{big10k}' WHERE id = 7"))
            .unwrap(),
    );
    assert_eq!(n, 1);

    let qr = query(&conn, "SELECT s FROM t WHERE id = 5");
    assert_eq!(qr.rows[0][0], Value::Text(big3k.into()));
    let qr = query(&conn, "SELECT s FROM t WHERE id = 7");
    assert_eq!(qr.rows[0][0], Value::Text(big10k.into()));
    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(60));

    // Shrink an overflow row back to a small value.
    let n = rows_affected(
        conn.execute("UPDATE t SET s = 'small' WHERE id = 7")
            .unwrap(),
    );
    assert_eq!(n, 1);
    let qr = query(&conn, "SELECT s FROM t WHERE id = 7");
    assert_eq!(qr.rows[0][0], Value::Text("small".into()));
    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(60));
}

/// Fused ON CONFLICT DO UPDATE lane (no indexes) over an overflow-stored row:
/// previously failed with InvalidValue("truncated column value").
#[test]
fn fused_do_update_over_overflow_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, data TEXT)")
        .unwrap();
    let big = "o".repeat(5000);
    conn.execute(&format!("INSERT INTO t VALUES (1, 10, '{big}')"))
        .unwrap();

    // Fast-path patch (n = n + 1) decodes the OLD row.
    let n = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 0, 'x') ON CONFLICT(id) DO UPDATE SET n = n + 1")
            .unwrap(),
    );
    assert_eq!(n, 1);
    let qr = query(&conn, "SELECT n, data FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(11));
    assert_eq!(qr.rows[0][1], Value::Text(big.clone().into()));

    // Generic closure path replacing the overflow column with a small value.
    let n = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0, 'y') ON CONFLICT(id) DO UPDATE SET data = excluded.data",
        )
        .unwrap(),
    );
    assert_eq!(n, 1);
    let qr = query(&conn, "SELECT n, data FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(11));
    assert_eq!(qr.rows[0][1], Value::Text("y".into()));
}

/// Fused DO UPDATE producing a 9000-char value: previously a reproduced panic
/// in split_leaf_with_insert (index out of bounds / rebuild_cells expect).
#[test]
fn fused_do_update_producing_oversized_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'small')").unwrap();

    let big9k = "z".repeat(9000);
    let n = rows_affected(
        conn.execute(&format!(
            "INSERT INTO t VALUES (1, '{big9k}') ON CONFLICT(id) DO UPDATE SET data = excluded.data"
        ))
        .unwrap(),
    );
    assert_eq!(n, 1);
    let qr = query(&conn, "SELECT data FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text(big9k.clone().into()));

    // Insert branch of the same lane on an empty table with an oversized row.
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    let n = rows_affected(
        conn.execute(&format!(
            "INSERT INTO t2 VALUES (1, '{big9k}') ON CONFLICT(id) DO UPDATE SET data = excluded.data"
        ))
        .unwrap(),
    );
    assert_eq!(n, 1);
    let qr = query(&conn, "SELECT data FROM t2 WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text(big9k.clone().into()));
}

/// Non-fused ON CONFLICT lane (index present) fetches the existing row via
/// insert_or_fetch: previously it decoded the raw 8-byte OverflowRef as the
/// old row (error or garbage write-back).
#[test]
fn indexed_do_update_over_overflow_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT, idx_col INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX i ON t (idx_col)").unwrap();
    let big = "b".repeat(5000);
    conn.execute(&format!("INSERT INTO t VALUES (1, '{big}', 2)"))
        .unwrap();

    let n = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 'x', 5) ON CONFLICT(id) DO UPDATE SET idx_col = idx_col + 1",
        )
        .unwrap(),
    );
    assert_eq!(n, 1);

    let qr = query(&conn, "SELECT data, idx_col FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text(big.clone().into()));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
    // Index must have been maintained from the real old row.
    let qr = query(&conn, "SELECT id FROM t WHERE idx_col = 3");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    let qr = query(&conn, "SELECT id FROM t WHERE idx_col = 2");
    assert!(qr.rows.is_empty());
}

/// Patch-safe UPDATE lane (fixed-width non-null SET target) over rows whose
/// encoded size exceeds the inline threshold: previously errored with
/// InvalidValue("truncated column value") on the 8-byte OverflowRef.
#[test]
fn patch_safe_update_over_overflow_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, blob TEXT)")
        .unwrap();
    let big = "q".repeat(5000);
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, '{big}')"))
            .unwrap();
    }

    let n = rows_affected(conn.execute("UPDATE t SET n = n + 1 WHERE id > 0").unwrap());
    assert_eq!(n, 10);

    let qr = query(&conn, "SELECT id, n, blob FROM t ORDER BY id");
    assert_eq!(qr.rows.len(), 10);
    for (i, row) in qr.rows.iter().enumerate() {
        let id = (i + 1) as i64;
        assert_eq!(row[0], Value::Integer(id));
        assert_eq!(row[1], Value::Integer(id + 1));
        assert_eq!(row[2], Value::Text(big.clone().into()));
    }
}

/// Compiled autocommit ranged-UPDATE lane growing a variable-width column:
/// previously panicked copying the re-encoded (larger) row into the
/// fixed-length patch buffer; the lane must fall back to delete+reinsert.
#[test]
fn ranged_update_growing_text_takes_safe_lane() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT NOT NULL)")
        .unwrap();
    for i in 1..=8 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'a')"))
            .unwrap();
    }

    // Plain autocommit execute compiles into the ranged lane.
    let grown = "G".repeat(40);
    let n = rows_affected(
        conn.execute(&format!(
            "UPDATE t SET s = '{grown}' WHERE id >= 1 AND id < 100"
        ))
        .unwrap(),
    );
    assert_eq!(n, 8);
    let qr = query(&conn, "SELECT s FROM t WHERE id = 3");
    assert_eq!(qr.rows[0][0], Value::Text(grown.clone().into()));

    // Prepared parameterized variant, growing inline rows into overflow.
    let big6k = "H".repeat(6000);
    let stmt = conn
        .prepare("UPDATE t SET s = $1 WHERE id >= 1 AND id < 100")
        .unwrap();
    let n = stmt.execute(&[Value::Text(big6k.clone().into())]).unwrap();
    assert_eq!(n, 8);

    // Grow again over the now overflow-stored rows.
    let big8k = "I".repeat(8000);
    let n = stmt.execute(&[Value::Text(big8k.clone().into())]).unwrap();
    assert_eq!(n, 8);
    let qr = query(&conn, "SELECT s FROM t ORDER BY id");
    assert_eq!(qr.rows.len(), 8);
    for row in &qr.rows {
        assert_eq!(row[0], Value::Text(big8k.clone().into()));
    }
    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(8));
}

/// Depth-3 tree (2000-char TEXT keys) drained by ascending deletes: previously
/// the tree depth underflowed (debug panic / persisted 65535 in release).
#[test]
fn deep_tree_ascending_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY)").unwrap();
    let make_key = |i: u32| format!("{i:04}{}", "k".repeat(1996));
    for i in 0..40u32 {
        conn.execute(&format!("INSERT INTO t VALUES ('{}')", make_key(i)))
            .unwrap();
    }

    for i in 0..40u32 {
        let n = rows_affected(
            conn.execute(&format!("DELETE FROM t WHERE k = '{}'", make_key(i)))
                .unwrap(),
        );
        assert_eq!(n, 1, "row {i} should delete");
    }

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(0));

    // Table stays usable after the full drain.
    conn.execute("INSERT INTO t VALUES ('after')").unwrap();
    let qr = query(&conn, "SELECT k FROM t");
    assert_eq!(qr.rows[0][0], Value::Text("after".into()));
}
