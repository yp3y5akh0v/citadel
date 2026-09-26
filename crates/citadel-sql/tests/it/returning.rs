use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        ExecutionResult::Query(qr) => qr.rows.len() as u64,
        ExecutionResult::Ok => 0,
    }
}

fn setup_users(conn: &Connection) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, count INTEGER)")
        .unwrap();
}

#[test]
fn insert_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING *",
    );
    assert_eq!(qr.columns, vec!["id", "name", "count"]);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(5));
}

#[test]
fn insert_returning_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING id, name",
    );
    assert_eq!(qr.columns, vec!["id", "name"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'alice', 5) RETURNING id + 1, UPPER(name) AS u",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[0][1], Value::Text("ALICE".into()));
}

#[test]
fn insert_returning_multi_row_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 3) RETURNING id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn insert_returning_qualified_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING users.id",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn update_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = 10 WHERE id = 1 RETURNING *",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][2], Value::Integer(10));
}

#[test]
fn update_returning_no_match_yields_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "UPDATE users SET count = 10 WHERE id = 999 RETURNING *",
    );
    assert_eq!(qr.rows.len(), 0);
    assert_eq!(qr.columns, vec!["id", "name", "count"]);
}

#[test]
fn delete_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users WHERE id = 1 RETURNING *");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));

    let after = query(&conn, "SELECT * FROM users");
    assert_eq!(after.rows.len(), 0);
}

#[test]
fn delete_returning_subset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5), (2, 'Bob', 3)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users RETURNING id");
    assert_eq!(qr.columns, vec!["id"]);
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn update_returning_old_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = count + 10 WHERE id = 1 RETURNING old.count AS was, new.count AS now_",
    );
    assert_eq!(qr.columns, vec!["was", "now_"]);
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[0][1], Value::Integer(15));
}

#[test]
fn update_returning_old_star_and_new_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE users SET count = count + 10 WHERE id = 1 RETURNING old.*, new.*",
    );
    assert_eq!(qr.rows[0].len(), 6);
    assert_eq!(qr.rows[0][2], Value::Integer(5));
    assert_eq!(qr.rows[0][5], Value::Integer(15));
}

#[test]
fn delete_returning_old_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM users WHERE id = 1 RETURNING old.*");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_new_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING new.*",
    );
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_old_star_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) RETURNING old.*",
    );
    assert_eq!(qr.rows.len(), 1);
    for v in &qr.rows[0] {
        assert_eq!(*v, Value::Null);
    }
}

#[test]
fn upsert_do_nothing_on_conflict_returning_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Bob', 99) ON CONFLICT (id) DO NOTHING RETURNING *",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn upsert_do_nothing_no_conflict_returning_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'Alice', 5) ON CONFLICT (id) DO NOTHING RETURNING *",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn upsert_do_update_returning_post_update_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'X', 100) ON CONFLICT (id) DO UPDATE SET count = count + 1 RETURNING count",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(6));
}

#[test]
fn upsert_do_update_returning_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 5)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO users VALUES (1, 'X', 100) ON CONFLICT (id) DO UPDATE SET count = count + 1 RETURNING old.count, new.count",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(5));
    assert_eq!(qr.rows[0][1], Value::Integer(6));
}

#[test]
fn returning_aggregate_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let err = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING COUNT(*)")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref msg) if msg.contains("aggregate")));
}

#[test]
fn returning_unknown_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let err = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING does_not_exist")
        .unwrap_err();
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn prepared_insert_returning() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let stmt = conn
        .prepare("INSERT INTO users VALUES ($1, $2, $3) RETURNING id, name")
        .unwrap();
    let result = stmt
        .query_collect(&[
            Value::Integer(7),
            Value::Text("Eve".into()),
            Value::Integer(0),
        ])
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(7));
    assert_eq!(result.rows[0][1], Value::Text("Eve".into()));
}

#[test]
fn execute_does_not_drop_returning_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);

    let result = conn
        .execute("INSERT INTO users VALUES (1, 'A', 5) RETURNING *")
        .unwrap();
    assert_eq!(rows_affected(result), 1);
}

fn setup_fast_lane(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER, note TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b')")
        .unwrap();
}

#[test]
fn prepared_pk_update_returning_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_fast_lane(&conn);

    let stmt = conn
        .prepare("UPDATE t SET c = c + $1 WHERE id = $2 RETURNING c")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for (add, id, expect) in [(5, 1, 15), (5, 1, 20), (7, 2, 27)] {
        let qr = stmt
            .query_collect(&[Value::Integer(add), Value::Integer(id)])
            .unwrap();
        assert_eq!(qr.columns, vec!["c".to_string()]);
        assert_eq!(qr.rows, vec![vec![Value::Integer(expect)]]);
    }
    conn.execute("COMMIT").unwrap();
    let qr = query(&conn, "SELECT c FROM t ORDER BY id");
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(20)], vec![Value::Integer(27)]]
    );
}

#[test]
fn prepared_pk_update_returning_alias_and_non_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_fast_lane(&conn);

    let stmt = conn
        .prepare("UPDATE t SET c = c + 1 WHERE id = $1 RETURNING c AS newc, id, note")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(1)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        qr.columns,
        vec!["newc".to_string(), "id".to_string(), "note".to_string()]
    );
    assert_eq!(
        qr.rows,
        vec![vec![
            Value::Integer(11),
            Value::Integer(1),
            Value::Text("a".into())
        ]]
    );
}

#[test]
fn prepared_multi_target_update_returning_reads_new_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();

    // Multi-target RETURNING must read the patched row bytes.
    let stmt = conn
        .prepare("UPDATE t SET a = $1, b = $2 WHERE id = $3 RETURNING a, b")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt
        .query_collect(&[Value::Integer(100), Value::Integer(200), Value::Integer(1)])
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(100), Value::Integer(200)]]
    );
}

#[test]
fn prepared_pk_update_returning_zero_match_keeps_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_fast_lane(&conn);

    let stmt = conn
        .prepare("UPDATE t SET c = c + 1 WHERE id = $1 RETURNING c")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(999)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(qr.columns, vec!["c".to_string()]);
    assert!(qr.rows.is_empty());
}

#[test]
fn delete_returning_zero_match_preserves_columns_in_every_transaction_route() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE missing_delete (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    conn.execute("INSERT INTO missing_delete VALUES (1, 'keep')")
        .unwrap();

    for explicit in [false, true] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let result = query(
            &conn,
            "DELETE FROM missing_delete WHERE id = 999 RETURNING id AS removed, note",
        );
        assert_eq!(result.columns, vec!["removed", "note"]);
        assert!(result.rows.is_empty());

        let prepared = conn
            .prepare("DELETE FROM missing_delete WHERE id = $1 RETURNING id AS removed, note")
            .unwrap();
        // The missing integer exercises the compiled key lookup; the other
        // values require predicate semantics outside an encoded integer key.
        for value in [Value::Integer(999), Value::Real(1.5), Value::Null] {
            let result = prepared.query_collect(&[value]).unwrap();
            assert_eq!(result.columns, vec!["removed", "note"]);
            assert!(result.rows.is_empty());
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
    assert_eq!(
        query(&conn, "SELECT id FROM missing_delete").rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn prepared_pk_update_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_fast_lane(&conn);

    let stmt = conn
        .prepare("UPDATE t SET c = c + 1 WHERE id = $1 RETURNING *")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(2)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        qr.columns,
        vec!["id".to_string(), "c".to_string(), "note".to_string()]
    );
    assert_eq!(
        qr.rows,
        vec![vec![
            Value::Integer(2),
            Value::Integer(21),
            Value::Text("b".into())
        ]]
    );
}

#[test]
fn prepared_pk_update_returning_old_new_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_fast_lane(&conn);

    let stmt = conn
        .prepare("UPDATE t SET c = c + 1 WHERE id = $1 RETURNING old.c, new.c")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt.query_collect(&[Value::Integer(1)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(10), Value::Integer(11)]]);
}

#[test]
fn prepared_pk_update_returning_stored_generated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 5)").unwrap();

    let stmt = conn
        .prepare("UPDATE t SET a = $1 WHERE id = $2 RETURNING a, g")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt
        .query_collect(&[Value::Integer(9), Value::Integer(1)])
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(9), Value::Integer(18)]]);
}

#[test]
fn prepared_pk_update_returning_virtual_generated_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 5)").unwrap();

    let stmt = conn
        .prepare("UPDATE t SET a = $1 WHERE id = $2 RETURNING g")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let qr = stmt
        .query_collect(&[Value::Integer(9), Value::Integer(1)])
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Integer(18)]]);
}

#[test]
fn autocommit_update_returning_virtual_generated_is_recomputed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 5)").unwrap();

    let result = conn
        .execute("UPDATE t SET a = 9 WHERE id = 1 RETURNING g")
        .unwrap();
    match result {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows, vec![vec![Value::Integer(18)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}
