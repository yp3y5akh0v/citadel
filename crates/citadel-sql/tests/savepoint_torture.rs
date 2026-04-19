//! Torture tests for SAVEPOINT / RELEASE / ROLLBACK TO.
//!
//! Covers deep nesting, name shadowing, DML/DDL interleave, index invariants,
//! error recovery, cursor visibility, persistence, and randomized fuzzing.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"savepoint-torture")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"savepoint-torture")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn count(conn: &mut Connection<'_>, sql: &str) -> i64 {
    let qr = conn.query(sql).unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        v => panic!("expected integer count, got {v:?}"),
    }
}

fn setup(conn: &mut Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
        .unwrap();
}

fn assert_ok(r: ExecutionResult) {
    match r {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section 1: Deep nesting
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn deep_10_levels_rollback_to_top() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute(&format!("SAVEPOINT l{i}")).unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    conn.execute("ROLLBACK TO l1").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn deep_20_levels_rollback_to_middle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    for i in 1..=20 {
        conn.execute(&format!("SAVEPOINT l{i}")).unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    conn.execute("ROLLBACK TO l10").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 9);
}

#[test]
fn deep_50_levels_release_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    for i in 1..=50 {
        conn.execute(&format!("SAVEPOINT l{i}")).unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    for i in (1..=50).rev() {
        conn.execute(&format!("RELEASE l{i}")).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 50);
}

#[test]
fn deep_alternating_release_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    for i in 1..=20 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'x', {i})"
        ))
        .unwrap();
        conn.execute(&format!("SAVEPOINT sp{i}")).unwrap();
    }
    // No insert happens after each sp_i, so rollback-to is a no-op on data.
    for i in (1..=20).rev() {
        conn.execute(&format!("RELEASE sp{i}")).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 20);
}

#[test]
fn deep_rollback_then_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT base").unwrap();
    for _cycle in 0..10 {
        for i in 1..=5 {
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({i}, 'cycle', {i})"
            ))
            .unwrap();
        }
        conn.execute("ROLLBACK TO base").unwrap();
    }
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 2: Name shadowing torture
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn shadow_10_times_same_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    for i in 1..=10 {
        conn.execute("SAVEPOINT sp").unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    // Peel one level per step: ROLLBACK TO undoes its row, RELEASE drops
    // the innermost sp so the next iteration hits the level below.
    for _ in 0..10 {
        conn.execute("ROLLBACK TO sp").unwrap();
        conn.execute("RELEASE sp").unwrap();
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn shadow_release_outer_via_inner_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'outer', 1)")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'inner', 2)")
        .unwrap();
    conn.execute("RELEASE sp").unwrap(); // peels only innermost
    conn.execute("ROLLBACK TO sp").unwrap(); // now hits outer
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn shadow_rollback_does_not_peel() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'outer', 1)")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'inner', 2)")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    // Innermost sp is preserved; second rollback is a no-op at this level.
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    conn.execute("RELEASE sp").unwrap(); // peels inner
    conn.execute("ROLLBACK TO sp").unwrap(); // hits outer
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn shadow_interleaved_with_unique_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
        .unwrap();
    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 2)")
        .unwrap();
    conn.execute("SAVEPOINT a").unwrap(); // shadow
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'a2', 3)")
        .unwrap();
    conn.execute("ROLLBACK TO a").unwrap(); // hits inner a
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
    conn.execute("RELEASE a").unwrap(); // peels inner a
    conn.execute("ROLLBACK TO b").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    conn.execute("ROLLBACK TO a").unwrap(); // hits outer a
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 3: DML × savepoint interleave
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn insert_update_delete_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=5 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'base', {i})"
        ))
        .unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (6, 'new', 6)")
        .unwrap();
    conn.execute("UPDATE t SET val = 'changed' WHERE id <= 3")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id = 5").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 5);
    let qr = conn.query("SELECT val FROM t WHERE id = 5").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("base".into()));
    assert_eq!(
        count(&mut conn, "SELECT COUNT(*) FROM t WHERE val = 'changed'"),
        0
    );
}

#[test]
fn update_at_multiple_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=5 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v0', 0)"
        ))
        .unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("UPDATE t SET num = 1").unwrap();
    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("UPDATE t SET num = 2").unwrap();
    conn.execute("SAVEPOINT c").unwrap();
    conn.execute("UPDATE t SET num = 3").unwrap();

    conn.execute("ROLLBACK TO c").unwrap();
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 10);
    conn.execute("ROLLBACK TO b").unwrap();
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 5);
    conn.execute("ROLLBACK TO a").unwrap();
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 0);

    conn.execute("COMMIT").unwrap();
}

#[test]
fn pk_change_across_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'original', 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET id = 100 WHERE id = 1").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id, val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("original".into()));
}

#[test]
fn delete_all_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=100 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 0);
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 100);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn update_then_rollback_then_update_different_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=10 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', 0)"
        ))
        .unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET num = 99 WHERE id <= 5").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("UPDATE t SET num = 42 WHERE id > 5").unwrap();
    conn.execute("COMMIT").unwrap();

    let sum = count(&mut conn, "SELECT SUM(num) FROM t");
    assert_eq!(sum, 42 * 5);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 4: DDL inside savepoints
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn create_table_nested_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=5 {
        conn.execute(&format!("SAVEPOINT l{i}")).unwrap();
        conn.execute(&format!("CREATE TABLE tbl_{i} (id INTEGER PRIMARY KEY)"))
            .unwrap();
        conn.execute(&format!("INSERT INTO tbl_{i} (id) VALUES ({i})"))
            .unwrap();
    }
    conn.execute("ROLLBACK TO l3").unwrap();
    conn.execute("COMMIT").unwrap();

    for i in 1..=2 {
        assert_eq!(
            count(&mut conn, &format!("SELECT COUNT(*) FROM tbl_{i}")),
            1
        );
    }
    for i in 3..=5 {
        let err = conn.query(&format!("SELECT * FROM tbl_{i}")).unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}

#[test]
fn drop_table_rollback_restores_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=20 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 20);
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 210);
}

#[test]
fn alter_add_drop_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'x', 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT").unwrap();
    conn.execute("UPDATE t SET extra = 'added'").unwrap();
    conn.execute("ALTER TABLE t DROP COLUMN val").unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("x".into()));
    let err = conn.query("SELECT extra FROM t").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn create_index_rollback_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=10 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v{i}', {i})"
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE INDEX idx_num ON t (num)").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t WHERE num = 5"), 1);
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.execute("DROP INDEX idx_num").unwrap_err();
    assert!(matches!(err, SqlError::IndexNotFound(_)));
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t WHERE num = 5"), 1);
}

#[test]
fn create_view_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1), (2, 'b', 2)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, val FROM t WHERE num > 1")
        .unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM v"), 1);
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let err = conn.query("SELECT * FROM v").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

// ═══════════════════════════════════════════════════════════════════════
// Section 5: Index invariants
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn index_consistent_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE UNIQUE INDEX uq_val ON t (val)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET val = 'x' WHERE id = 1").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (4, 'b', 4)")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    for dup in &["a", "b", "c"] {
        let err = conn
            .execute(&format!(
                "INSERT INTO t (id, val, num) VALUES (99, '{dup}', 99)"
            ))
            .unwrap_err();
        assert!(matches!(err, SqlError::UniqueViolation(_)));
    }
    conn.execute("INSERT INTO t (id, val, num) VALUES (99, 'fresh', 99)")
        .unwrap();
}

#[test]
fn index_many_mutations_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE INDEX idx_num ON t (num)").unwrap();
    for i in 1..=100 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v{i}', {})",
            i % 10
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET num = num + 1000").unwrap();
    conn.execute("DELETE FROM t WHERE id % 2 = 0").unwrap();
    for i in 1000..1020 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'new', 9999)"
        ))
        .unwrap();
    }
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    for bucket in 0..10 {
        let c = count(
            &mut conn,
            &format!("SELECT COUNT(*) FROM t WHERE num = {bucket}"),
        );
        assert_eq!(c, 10);
    }
    assert_eq!(
        count(&mut conn, "SELECT COUNT(*) FROM t WHERE num = 9999"),
        0
    );
}

#[test]
fn multi_column_index_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX uq_ab ON m (a, b)")
        .unwrap();
    for i in 1..=20 {
        conn.execute(&format!(
            "INSERT INTO m (id, a, b) VALUES ({i}, {}, {})",
            i % 4,
            i % 5
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE m SET a = a + 100, b = b + 100")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    // All original (a,b) combos must still collide with a uniqueness probe.
    for i in 1..=20 {
        let a = i % 4;
        let b = i % 5;
        let _err = conn
            .execute(&format!(
                "INSERT INTO m (id, a, b) VALUES ({}, {}, {})",
                1000 + i,
                a,
                b
            ))
            .unwrap_err();
    }
}

#[test]
fn unique_index_transient_violation_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE UNIQUE INDEX uq_val ON t (val)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for attempt in 0..5 {
        conn.execute("SAVEPOINT try").unwrap();
        let _ = conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({}, 'a', {})",
            attempt + 10,
            attempt
        ));
        conn.execute("ROLLBACK TO try").unwrap();
        conn.execute("RELEASE try").unwrap();
    }
    conn.execute("INSERT INTO t (id, val, num) VALUES (99, 'unique', 99)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn index_on_nullable_column_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE n (id INTEGER PRIMARY KEY, nullable_col TEXT)")
        .unwrap();
    // Non-unique index so NULLs and a later multi-row update don't collide.
    conn.execute("CREATE INDEX idx_nc ON n (nullable_col)")
        .unwrap();
    conn.execute("INSERT INTO n (id, nullable_col) VALUES (1, NULL), (2, NULL), (3, 'a')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE n SET nullable_col = 'set' WHERE nullable_col IS NULL")
        .unwrap();
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) FROM n WHERE nullable_col IS NULL"
        ),
        2
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Section 6: Foreign-key × savepoint
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn fk_violation_caught_and_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id))")
        .unwrap();
    conn.execute("INSERT INTO p (id) VALUES (1), (2), (3)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    for attempt in 1..=10 {
        conn.execute("SAVEPOINT try").unwrap();
        let target_pid = if attempt % 3 == 0 {
            99
        } else {
            attempt % 3 + 1
        };
        match conn.execute(&format!(
            "INSERT INTO c (id, pid) VALUES ({attempt}, {target_pid})"
        )) {
            Ok(_) => conn.execute("RELEASE try").unwrap(),
            Err(_) => conn.execute("ROLLBACK TO try").unwrap(),
        };
    }
    conn.execute("COMMIT").unwrap();

    let orphans = count(
        &mut conn,
        "SELECT COUNT(*) FROM c WHERE pid NOT IN (SELECT id FROM p)",
    );
    assert_eq!(orphans, 0);
}

#[test]
fn fk_drop_parent_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id))")
        .unwrap();
    conn.execute("INSERT INTO p (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO c (id, pid) VALUES (1, 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    // FK may or may not block DROP; either way rollback must restore both.
    let _ = conn.execute("DROP TABLE c");
    let _ = conn.execute("DROP TABLE p");
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM p"), 1);
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM c"), 1);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 7: Read-your-writes + rollback
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn select_reads_own_writes_before_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'pre', 0)")
        .unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'mid', 1)")
        .unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn select_after_update_visible_then_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET val = 'changed' WHERE id = 1")
        .unwrap();
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("changed".into()));
    conn.execute("ROLLBACK TO sp").unwrap();
    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("a".into()));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn multi_savepoint_visibility_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'lvl0', 0)")
        .unwrap();
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'lvl1', 1)")
        .unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);

    conn.execute("SAVEPOINT b").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'lvl2', 2)")
        .unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 3);

    conn.execute("ROLLBACK TO b").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);

    conn.execute("ROLLBACK TO a").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);

    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn aggregate_reflects_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    for i in 1..=10 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v', {i})"
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET num = num * 10").unwrap();
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 550);
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(count(&mut conn, "SELECT SUM(num) FROM t"), 55);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn join_results_reflect_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO a (id, v) VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO b (id, a_id) VALUES (11, 1), (12, 2)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("INSERT INTO a (id, v) VALUES (3, 30)")
        .unwrap();
    conn.execute("INSERT INTO b (id, a_id) VALUES (13, 3)")
        .unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM a JOIN b ON b.a_id = a.id")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));

    conn.execute("ROLLBACK TO sp").unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM a JOIN b ON b.a_id = a.id")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));

    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Section 8: Error recovery torture
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn recover_from_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE nn (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute("INSERT INTO nn (id, val) VALUES (1, NULL)")
        .unwrap_err();
    assert!(matches!(err, SqlError::NotNullViolation(_)));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO nn (id, val) VALUES (1, 'ok')")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM nn"), 1);
}

#[test]
fn recover_from_type_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn
        .execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 'not-a-number')")
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 42)")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn recover_from_division_by_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn.execute("UPDATE t SET num = num / 0").unwrap_err();
    assert!(matches!(err, SqlError::DivisionByZero));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("UPDATE t SET num = num * 2").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT num FROM t"), 2);
}

#[test]
fn recover_from_column_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    // Row required so UPDATE reaches column evaluation and raises ColumnNotFound.
    conn.execute("INSERT INTO t (id, val, num) VALUES (0, 'seed', 0)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let err = conn.execute("UPDATE t SET ghost = 1").unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 9: Persistence
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn persist_after_complex_savepoint_flow() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup(&mut conn);
        conn.execute("CREATE INDEX idx_num ON t (num)").unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'a', 1)")
            .unwrap();
        conn.execute("SAVEPOINT s1").unwrap();
        conn.execute("INSERT INTO t (id, val, num) VALUES (2, 'b', 2)")
            .unwrap();
        conn.execute("SAVEPOINT s2").unwrap();
        conn.execute("INSERT INTO t (id, val, num) VALUES (3, 'c', 3)")
            .unwrap();
        conn.execute("ROLLBACK TO s2").unwrap();
        conn.execute("INSERT INTO t (id, val, num) VALUES (4, 'd', 4)")
            .unwrap();
        conn.execute("RELEASE s1").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Text("a".into()));
    assert_eq!(qr.rows[1][1], Value::Text("b".into()));
    assert_eq!(qr.rows[2][1], Value::Text("d".into()));
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t WHERE num = 4"), 1);
}

#[test]
fn persist_rollback_of_ddl_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        setup(&mut conn);

        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT sp").unwrap();
        conn.execute("CREATE TABLE secret (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO secret (id) VALUES (1)").unwrap();
        conn.execute("ROLLBACK TO sp").unwrap();
        conn.execute("INSERT INTO t (id, val, num) VALUES (1, 'kept', 1)")
            .unwrap();
        conn.execute("COMMIT").unwrap();
    }

    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT * FROM secret").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn multiple_txns_with_savepoints_persist() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    for batch in 0..5 {
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT sp").unwrap();
        for i in 0..10 {
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({}, 'b{}', {})",
                batch * 10 + i,
                batch,
                i
            ))
            .unwrap();
        }
        if batch % 2 == 0 {
            conn.execute("ROLLBACK TO sp").unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    // Odd batches persist: 1, 3 → 20 rows.
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 20);
}

// ═══════════════════════════════════════════════════════════════════════
// Section 10: Randomized fuzzing (deterministic, seeded)
// ═══════════════════════════════════════════════════════════════════════

/// SplitMix64 — deterministic seeded PRNG for reproducible fuzz.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn in_range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn fuzz_seed_1() {
    fuzz_scenario(1, 200);
}
#[test]
fn fuzz_seed_2() {
    fuzz_scenario(2, 200);
}
#[test]
fn fuzz_seed_3() {
    fuzz_scenario(3, 200);
}
#[test]
fn fuzz_seed_4() {
    fuzz_scenario(4, 200);
}
#[test]
fn fuzz_seed_5() {
    fuzz_scenario(5, 200);
}

fn fuzz_scenario(seed: u64, ops: usize) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    conn.execute("CREATE INDEX idx_num ON t (num)").unwrap();
    conn.execute("CREATE UNIQUE INDEX uq_val ON t (val)")
        .unwrap();

    let mut rng = Rng::new(seed);
    // Invariant checked at each step: PK count == index count.
    let mut sp_counter: u64 = 0;
    let mut stack: Vec<u64> = Vec::new();

    assert_ok(conn.execute("BEGIN").unwrap());

    for i in 0..ops {
        let op = rng.in_range(8);
        match op {
            0 => {
                sp_counter += 1;
                let name = format!("sp{sp_counter}");
                conn.execute(&format!("SAVEPOINT {name}")).unwrap();
                stack.push(sp_counter);
            }
            1 => {
                if let Some(n) = stack.pop() {
                    let _ = conn.execute(&format!("RELEASE sp{n}"));
                }
            }
            2 => {
                if let Some(&n) = stack.last() {
                    let _ = conn.execute(&format!("ROLLBACK TO sp{n}"));
                }
            }
            3 | 4 => {
                let id = 100_000 + i as i64;
                let val = format!("v{id}");
                let num = (rng.next() % 1000) as i64;
                let _ = conn.execute(&format!(
                    "INSERT INTO t (id, val, num) VALUES ({id}, '{val}', {num})"
                ));
            }
            5 => {
                let bound = rng.in_range(100_000_000);
                let _ = conn.execute(&format!("UPDATE t SET num = num + 1 WHERE id = {bound}"));
            }
            6 => {
                let bound = rng.in_range(100_000_000) as i64;
                let _ = conn.execute(&format!("DELETE FROM t WHERE id = {bound}"));
            }
            7 => {
                let pk_count = count(&mut conn, "SELECT COUNT(*) FROM t");
                let idx_count = count(
                    &mut conn,
                    "SELECT COUNT(*) FROM t WHERE num IS NOT NULL OR num IS NULL",
                );
                assert_eq!(
                    pk_count, idx_count,
                    "table/index mismatch at op {i} seed {seed}"
                );
            }
            _ => unreachable!(),
        }
    }

    while let Some(n) = stack.pop() {
        let _ = conn.execute(&format!("RELEASE sp{n}"));
    }
    conn.execute("COMMIT").unwrap();

    let total = count(&mut conn, "SELECT COUNT(*) FROM t");
    let via_index = count(
        &mut conn,
        "SELECT COUNT(*) FROM t WHERE num >= 0 OR num < 0 OR num IS NULL",
    );
    assert_eq!(total, via_index, "final index mismatch seed {seed}");
}

// ═══════════════════════════════════════════════════════════════════════
// Section 11: Stress — many sequential transactions with savepoints
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn stress_100_transactions_with_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    for batch in 0..100 {
        conn.execute("BEGIN").unwrap();
        for i in 0..5 {
            let id = batch * 5 + i;
            conn.execute("SAVEPOINT sp").unwrap();
            conn.execute(&format!(
                "INSERT INTO t (id, val, num) VALUES ({id}, 'v{id}', {id})"
            ))
            .unwrap();
            if i == 2 {
                conn.execute("ROLLBACK TO sp").unwrap();
            } else {
                conn.execute("RELEASE sp").unwrap();
            }
        }
        conn.execute("COMMIT").unwrap();
    }

    // Each batch commits 4 of 5 rows: 100 × 4 = 400.
    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 400);
}

#[test]
fn stress_big_batch_inside_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    for i in 0..5000u32 {
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v{i}', {i})"
        ))
        .unwrap();
    }
    conn.execute("ROLLBACK TO sp").unwrap();
    conn.execute("INSERT INTO t (id, val, num) VALUES (100000, 'final', 100000)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn stress_many_savepoints_rolling_forward() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    for i in 1..=200 {
        conn.execute(&format!("SAVEPOINT sp{i}")).unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, val, num) VALUES ({i}, 'v{i}', {i})"
        ))
        .unwrap();
        if i % 10 == 0 {
            // RELEASE the outermost of the last 10 implicitly peels the rest.
            conn.execute(&format!("RELEASE sp{}", i - 9)).unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();

    assert_eq!(count(&mut conn, "SELECT COUNT(*) FROM t"), 200);
}
