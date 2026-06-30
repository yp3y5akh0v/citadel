use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, QueryResult, SqlError, Value};

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

#[test]
fn stored_basic_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         sum INTEGER GENERATED ALWAYS AS (a + b) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 3, 4)")
        .unwrap();
    let qr = query(&conn, "SELECT sum FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(7));
}

#[test]
fn stored_concat() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE u (id INTEGER PRIMARY KEY, fn TEXT NOT NULL, ln TEXT NOT NULL, \
         full TEXT GENERATED ALWAYS AS (fn || ' ' || ln) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO u (id, fn, ln) VALUES (1, 'Alice', 'Doe')")
        .unwrap();
    let qr = query(&conn, "SELECT full FROM u");
    assert_eq!(qr.rows[0][0], Value::Text("Alice Doe".into()));
}

#[test]
fn stored_function() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE a (id INTEGER PRIMARY KEY, email TEXT NOT NULL, \
         email_lower TEXT GENERATED ALWAYS AS (LOWER(email)) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO a (id, email) VALUES (1, 'Alice@Example.COM')")
        .unwrap();
    let qr = query(&conn, "SELECT email_lower FROM a");
    assert_eq!(qr.rows[0][0], Value::Text("alice@example.com".into()));
}

#[test]
fn stored_case_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE g (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, \
         grade TEXT GENERATED ALWAYS AS (CASE WHEN score >= 90 THEN 'A' \
         WHEN score >= 60 THEN 'B' ELSE 'F' END) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO g (id, score) VALUES (1, 95), (2, 75), (3, 30)")
        .unwrap();
    let qr = query(&conn, "SELECT id, grade FROM g ORDER BY id");
    assert_eq!(qr.rows[0][1], Value::Text("A".into()));
    assert_eq!(qr.rows[1][1], Value::Text("B".into()));
    assert_eq!(qr.rows[2][1], Value::Text("F".into()));
}

#[test]
fn stored_null_input_yields_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE n (id INTEGER PRIMARY KEY, a INTEGER, \
         doubled INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO n (id, a) VALUES (1, NULL)")
        .unwrap();
    let qr = query(&conn, "SELECT doubled FROM n");
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn stored_not_null_with_null_expr_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE nn (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER NOT NULL GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    let err = conn
        .execute("INSERT INTO nn (id, a) VALUES (1, NULL)")
        .unwrap_err();
    assert!(matches!(err, SqlError::NotNullViolation(_)));
}

#[test]
fn create_index_on_stored_column_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL, \
         el TEXT GENERATED ALWAYS AS (LOWER(email)) STORED)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_el ON u(el)").unwrap();
    conn.execute("INSERT INTO u (id, email) VALUES (1, 'X@Y.COM')")
        .unwrap();
    let qr = query(&conn, "SELECT id FROM u WHERE el = 'x@y.com'");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn virtual_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, cents INTEGER NOT NULL, \
         dollars REAL GENERATED ALWAYS AS (cents / 100.0) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO p (id, cents) VALUES (1, 250)")
        .unwrap();
    let qr = query(&conn, "SELECT dollars FROM p");
    assert_eq!(qr.rows[0][0], Value::Real(2.5));
}

#[test]
fn virtual_recomputes_after_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, cents INTEGER NOT NULL, \
         dollars REAL GENERATED ALWAYS AS (cents / 100.0) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO p (id, cents) VALUES (1, 250)")
        .unwrap();
    conn.execute("UPDATE p SET cents = 999 WHERE id = 1")
        .unwrap();
    let qr = query(&conn, "SELECT dollars FROM p");
    assert_eq!(qr.rows[0][0], Value::Real(9.99));
}

#[test]
fn create_index_on_virtual_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE v (id INTEGER PRIMARY KEY, x INTEGER, \
         d INTEGER GENERATED ALWAYS AS (x * 2) VIRTUAL)",
    )
    .unwrap();
    let err = conn.execute("CREATE INDEX i ON v(d)").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("VIRTUAL")));
}

#[test]
fn update_propagates_to_stored() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE o (id INTEGER PRIMARY KEY, qty INTEGER, price REAL, \
         total REAL GENERATED ALWAYS AS (qty * price) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO o (id, qty, price) VALUES (1, 3, 9.99)")
        .unwrap();
    conn.execute("UPDATE o SET qty = 5 WHERE id = 1").unwrap();
    let qr = query(&conn, "SELECT total FROM o");
    if let Value::Real(v) = qr.rows[0][0] {
        assert!((v - 49.95).abs() < 1e-9);
    } else {
        panic!("expected real");
    }
}

#[test]
fn update_multiple_base_cols_recomputes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE m (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO m (id, a, b) VALUES (1, 1, 2)")
        .unwrap();
    conn.execute("UPDATE m SET a = 10, b = 20 WHERE id = 1")
        .unwrap();
    let qr = query(&conn, "SELECT s FROM m");
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn single_set_recomputes_stored_from_mixed_deps() {
    // Single-target UPDATE fast path: the stored gen depends on the SET column (taken live
    // from partial_row, not re-decoded) AND a non-set column (which must still be decoded).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + c) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a, c) VALUES (1, 1, 100)")
        .unwrap();
    conn.execute("UPDATE t SET a = 5 WHERE id = 1").unwrap();
    let qr = query(&conn, "SELECT a, c, s FROM t");
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(5), Value::Integer(100), Value::Integer(105)]
    );
}

#[test]
fn prepared_txn_update_recomputes_stored_from_assigned_col() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2 + 1) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 3)").unwrap();

    let stmt = conn
        .prepare("UPDATE t SET a = a + $1 WHERE id = $2")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    stmt.execute(&[Value::Integer(4), Value::Integer(1)])
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT a, d FROM t WHERE id = 1");
    assert_eq!(qr.rows[0], vec![Value::Integer(7), Value::Integer(15)]);
}

#[test]
fn insert_into_generated_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    let err = conn
        .execute("INSERT INTO g (id, a, d) VALUES (1, 3, 99)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CannotInsertIntoGeneratedColumn(_)));
}

#[test]
fn update_set_generated_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO g (id, a) VALUES (1, 3)").unwrap();
    let err = conn
        .execute("UPDATE g SET d = 99 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CannotUpdateGeneratedColumn(_)));
}

#[test]
fn default_and_generated_combined_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE x (id INTEGER PRIMARY KEY, a INTEGER, \
             d INTEGER DEFAULT 5 GENERATED ALWAYS AS (a * 2) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("DEFAULT")));
}

#[test]
fn primary_key_and_generated_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE pk (a INTEGER, \
             d INTEGER PRIMARY KEY GENERATED ALWAYS AS (a * 2) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("PRIMARY KEY")));
}

#[test]
fn chained_generated_refs_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, a INTEGER, \
             b INTEGER GENERATED ALWAYS AS (a * 2) STORED, \
             c INTEGER GENERATED ALWAYS AS (b * 2) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::GeneratedColumnReference(_)));
}

#[test]
fn aggregate_in_generated_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE a (id INTEGER PRIMARY KEY, x INTEGER, \
             c INTEGER GENERATED ALWAYS AS (COUNT(x)) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("aggregate")));
}

#[test]
fn random_in_generated_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE r (id INTEGER PRIMARY KEY, \
             v INTEGER GENERATED ALWAYS AS (RANDOM()) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("volatile")));
}

#[test]
fn now_in_generated_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TABLE r (id INTEGER PRIMARY KEY, \
             v TIMESTAMP GENERATED ALWAYS AS (NOW()) STORED)",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("volatile")));
}

#[test]
fn alter_add_virtual_on_populated_table_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5), (2, 10)")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN d INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL")
        .unwrap();
    let qr = query(&conn, "SELECT id, d FROM t ORDER BY id");
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(20));
}

#[test]
fn alter_add_stored_on_populated_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    let err = conn
        .execute("ALTER TABLE t ADD COLUMN d INTEGER GENERATED ALWAYS AS (a * 2) STORED")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("STORED")));
}

#[test]
fn alter_add_stored_on_empty_table_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN d INTEGER GENERATED ALWAYS AS (a * 2) STORED")
        .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 7)").unwrap();
    let qr = query(&conn, "SELECT d FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(14));
}

#[test]
fn schema_persistence_v5_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("p.db");
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"pw")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
             d INTEGER GENERATED ALWAYS AS (a * 3) STORED, \
             v INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        )
        .unwrap();
        conn.execute("INSERT INTO t (id, a) VALUES (1, 4)").unwrap();
    }
    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"pw")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let qr = query(&conn, "SELECT a, d, v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(4));
    assert_eq!(qr.rows[0][1], Value::Integer(12));
    assert_eq!(qr.rows[0][2], Value::Integer(5));
    conn.execute("INSERT INTO t (id, a) VALUES (2, 10)")
        .unwrap();
    let qr2 = query(&conn, "SELECT d, v FROM t WHERE id = 2");
    assert_eq!(qr2.rows[0][0], Value::Integer(30));
    assert_eq!(qr2.rows[0][1], Value::Integer(11));
}

#[test]
fn returning_includes_generated_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE r (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    let qr = query(&conn, "INSERT INTO r (id, a) VALUES (1, 6) RETURNING d");
    assert_eq!(qr.rows[0][0], Value::Integer(12));
}

#[test]
fn identity_column_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("CREATE TABLE i (id INTEGER GENERATED BY DEFAULT AS IDENTITY, x INTEGER)")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("identity")));
}

#[test]
fn bare_as_syntax_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a INTEGER, d INTEGER AS (a * 5) STORED)")
        .unwrap();
    conn.execute("INSERT INTO b (id, a) VALUES (1, 4)").unwrap();
    let qr = query(&conn, "SELECT d FROM b");
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn drop_base_column_with_gen_ref_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    let err = conn.execute("ALTER TABLE t DROP COLUMN a").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(ref m) if m.contains("generated")));
}

#[test]
fn rename_base_column_rewrites_generated_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 3)").unwrap();
    conn.execute("ALTER TABLE t RENAME COLUMN a TO base")
        .unwrap();
    conn.execute("INSERT INTO t (id, base) VALUES (2, 7)")
        .unwrap();
    let qr = query(&conn, "SELECT id, d FROM t ORDER BY id");
    assert_eq!(qr.rows[0][1], Value::Integer(6));
    assert_eq!(qr.rows[1][1], Value::Integer(14));
}

#[test]
fn where_filter_against_virtual() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         d INTEGER GENERATED ALWAYS AS (a * 10) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 1), (2, 5), (3, 10)")
        .unwrap();
    let qr = query(&conn, "SELECT id FROM t WHERE d >= 50 ORDER BY id");
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn group_by_virtual() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
         bucket INTEGER GENERATED ALWAYS AS (a / 10) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 5), (2, 12), (3, 18), (4, 25)")
        .unwrap();
    let qr = query(
        &conn,
        "SELECT bucket, COUNT(*) FROM t GROUP BY bucket ORDER BY bucket",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[2][1], Value::Integer(1));
}

#[test]
fn virtual_add_overflow_errors_not_wraps() {
    // The checked fast virtual evaluator must error on overflow, never wrap (matching
    // generic eval). Covers both the clause-free and filtered-virtual read paths.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         s INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL)",
    )
    .unwrap();
    conn.execute(&format!(
        "INSERT INTO t (id, a, b) VALUES (1, {}, 1)",
        i64::MAX
    ))
    .unwrap();

    let err = conn.query("SELECT s FROM t").unwrap_err();
    assert!(
        matches!(err, SqlError::IntegerOverflow),
        "clause-free: {err:?}"
    );

    let err = conn.query("SELECT id FROM t WHERE s > 0").unwrap_err();
    assert!(
        matches!(err, SqlError::IntegerOverflow),
        "filtered: {err:?}"
    );
}
