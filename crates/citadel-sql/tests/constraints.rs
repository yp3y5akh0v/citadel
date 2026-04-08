use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

// ── DEFAULT: literal defaults (INT, TEXT, REAL, BOOL) ────────────────

#[test]
fn default_literal_int() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT 42)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn default_literal_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT DEFAULT 'unknown')",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("unknown".into()));
}

#[test]
fn default_literal_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, score REAL DEFAULT 3.14)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT score FROM t WHERE id = 1").unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 3.14).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn default_literal_bool() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN DEFAULT TRUE)",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT active FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

// ── DEFAULT NULL on nullable column ──────────────────────────────────

#[test]
fn default_null_on_nullable_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT NULL)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert!(qr.rows[0][0].is_null());
}

// ── Explicit value overrides default ─────────────────────────────────

#[test]
fn default_explicit_value_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT 42)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, 99)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

// ── Explicit NULL does NOT trigger default ───────────────────────────

#[test]
fn default_explicit_null_stores_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT 42)")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, val) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert!(qr.rows[0][0].is_null());
}

// ── DEFAULT with expression ──────────────────────────────────────────

#[test]
fn default_expression_addition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT (1 + 2))",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn default_expression_abs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT (ABS(-5)))",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

// ── Multiple defaults on different columns ───────────────────────────

#[test]
fn default_multiple_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                name TEXT DEFAULT 'anon', \
                score INTEGER DEFAULT 0, \
                factor REAL DEFAULT 1.0\
            )",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn
        .query("SELECT name, score, factor FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("anon".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(0));
    match &qr.rows[0][2] {
        Value::Real(r) => assert!((*r - 1.0).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

// ── NOT NULL + DEFAULT combo ─────────────────────────────────────────

#[test]
fn default_not_null_combo() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                status TEXT NOT NULL DEFAULT 'active'\
            )",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (1)").unwrap(), 1);

    let qr = conn.query("SELECT status FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("active".into()));
}

// ── DEFAULT persistence after close/reopen ───────────────────────────

#[test]
fn default_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT 77)")
            .unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(77));

        conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();
        let qr = conn.query("SELECT val FROM t WHERE id = 2").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(77));
    }
}

// ── Partial INSERT with prepared statements ──────────────────────────

#[test]
fn default_partial_insert_prepared() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                name TEXT DEFAULT 'guest', \
                score INTEGER DEFAULT 100\
            )",
        )
        .unwrap(),
    );

    conn.execute_params(
        "INSERT INTO t (id, name) VALUES ($1, $2)",
        &[Value::Integer(1), Value::Text("alice".into())],
    )
    .unwrap();

    let qr = conn
        .query("SELECT name, score FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(100));
}

// ── DEFAULT on all non-PK columns ───────────────────────────────────

#[test]
fn default_on_all_non_pk_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                a INTEGER DEFAULT 1, \
                b TEXT DEFAULT 'x', \
                c BOOLEAN DEFAULT FALSE\
            )",
        )
        .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO t (id) VALUES (10)").unwrap(), 1);

    let qr = conn.query("SELECT a, b, c FROM t WHERE id = 10").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
    assert_eq!(qr.rows[0][2], Value::Boolean(false));
}

// ── CHECK: column-level pass ─────────────────────────────────────────

#[test]
fn check_column_level_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, age INTEGER CHECK(age >= 0))",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, age) VALUES (1, 25)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT age FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(25));
}

// ── CHECK: column-level fail ─────────────────────────────────────────

#[test]
fn check_column_level_fail() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t (id, age) VALUES (1, -5)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: table-level pass/fail (multi-column) ──────────────────────

#[test]
fn check_table_level_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                start_val INTEGER, \
                end_val INTEGER, \
                CHECK(start_val < end_val)\
            )",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, start_val, end_val) VALUES (1, 10, 20)")
            .unwrap(),
        1,
    );
}

#[test]
fn check_table_level_fail() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            start_val INTEGER, \
            end_val INTEGER, \
            CHECK(start_val < end_val)\
        )",
    )
    .unwrap();

    let err = conn
        .execute("INSERT INTO t (id, start_val, end_val) VALUES (1, 20, 10)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: NULL passes (UNKNOWN semantics) ───────────────────────────

#[test]
fn check_null_passes_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, x INTEGER CHECK(x > 0))")
            .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, x) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = conn.query("SELECT x FROM t WHERE id = 1").unwrap();
    assert!(qr.rows[0][0].is_null());
}

// ── CHECK: on UPDATE pass/fail ───────────────────────────────────────

#[test]
fn check_update_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t (id, age) VALUES (1, 25)")
        .unwrap();

    assert_rows_affected(
        conn.execute("UPDATE t SET age = 30 WHERE id = 1").unwrap(),
        1,
    );

    let qr = conn.query("SELECT age FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn check_update_fail() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t (id, age) VALUES (1, 25)")
        .unwrap();

    let err = conn
        .execute("UPDATE t SET age = -1 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: named constraint ──────────────────────────────────────────

#[test]
fn check_named_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            age INTEGER, \
            CONSTRAINT chk_age CHECK(age >= 0)\
        )",
    )
    .unwrap();

    let err = conn
        .execute("INSERT INTO t (id, age) VALUES (1, -1)")
        .unwrap_err();
    match err {
        SqlError::CheckViolation(msg) => {
            assert!(
                msg.contains("chk_age"),
                "error message should contain constraint name 'chk_age', got: {msg}"
            );
        }
        other => panic!("expected CheckViolation, got {other:?}"),
    }
}

// ── CHECK: multiple CHECKs all enforced ──────────────────────────────

#[test]
fn check_multiple_all_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            a INTEGER CHECK(a > 0), \
            b INTEGER CHECK(b < 100)\
        )",
    )
    .unwrap();

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, a, b) VALUES (1, 5, 50)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO t (id, a, b) VALUES (2, -1, 50)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    let err = conn
        .execute("INSERT INTO t (id, a, b) VALUES (3, 5, 200)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: with function (LENGTH) ────────────────────────────────────

#[test]
fn check_with_length_function() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            name TEXT CHECK(LENGTH(name) > 0)\
        )",
    )
    .unwrap();

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'alice')")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO t (id, name) VALUES (2, '')")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: with boolean logic ────────────────────────────────────────

#[test]
fn check_with_boolean_logic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            active BOOLEAN, \
            balance INTEGER, \
            CHECK(active = TRUE OR balance > 0)\
        )",
    )
    .unwrap();

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, active, balance) VALUES (1, TRUE, 0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO t (id, active, balance) VALUES (2, FALSE, 100)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO t (id, active, balance) VALUES (3, FALSE, 0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── CHECK: persistence after reopen ──────────────────────────────────

#[test]
fn check_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER CHECK(val > 0))",
        )
        .unwrap();
        conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(10));

        let err = conn
            .execute("INSERT INTO t (id, val) VALUES (2, -5)")
            .unwrap_err();
        assert!(matches!(err, SqlError::CheckViolation(..)));
    }
}

// ── CHECK: transaction rollback after violation ──────────────────────

#[test]
fn check_transaction_rollback_after_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER CHECK(val > 0))")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, 10)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    let err = conn
        .execute("INSERT INTO t (id, val) VALUES (2, -1)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
    conn.execute("ROLLBACK").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ── CHECK: subquery rejected at CREATE TABLE time ────────────────────

#[test]
fn check_subquery_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE other (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    let err = conn
        .execute(
            "CREATE TABLE t (\
                id INTEGER NOT NULL PRIMARY KEY, \
                val INTEGER CHECK(val > (SELECT MIN(id) FROM other))\
            )",
        )
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(_) | SqlError::Parse(_)),
        "expected Unsupported or Parse error for CHECK with subquery, got {err:?}"
    );
}

// ── CHECK: referencing multiple columns in single expression ─────────

#[test]
fn check_multi_column_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (\
            id INTEGER NOT NULL PRIMARY KEY, \
            low INTEGER, \
            mid INTEGER, \
            high INTEGER, \
            CHECK(low <= mid AND mid <= high)\
        )",
    )
    .unwrap();

    assert_rows_affected(
        conn.execute("INSERT INTO t (id, low, mid, high) VALUES (1, 1, 5, 10)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO t (id, low, mid, high) VALUES (2, 5, 3, 10)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    let err = conn
        .execute("INSERT INTO t (id, low, mid, high) VALUES (3, 1, 10, 5)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

// ── FOREIGN KEY: insert valid reference ──────────────────────────────

#[test]
fn fk_insert_valid_reference() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
            .unwrap(),
        1,
    );
}

// ── FOREIGN KEY: insert invalid reference ────────────────────────────

#[test]
fn fk_insert_invalid_reference() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    let err = conn
        .execute("INSERT INTO child (id, parent_id) VALUES (1, 999)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

// ── FOREIGN KEY: NULL FK value allowed ───────────────────────────────

#[test]
fn fk_null_value_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    assert_rows_affected(
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, NULL)")
            .unwrap(),
        1,
    );

    let qr = conn
        .query("SELECT parent_id FROM child WHERE id = 1")
        .unwrap();
    assert!(qr.rows[0][0].is_null());
}

// ── FOREIGN KEY: delete parent with no children ──────────────────────

#[test]
fn fk_delete_parent_no_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO parent (id) VALUES (2)").unwrap();

    assert_rows_affected(conn.execute("DELETE FROM parent WHERE id = 2").unwrap(), 1);
}

// ── FOREIGN KEY: delete parent with children ─────────────────────────

#[test]
fn fk_delete_parent_with_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
        .unwrap();

    let err = conn.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

// ── FOREIGN KEY: update parent PK with no children ───────────────────

#[test]
fn fk_update_parent_pk_no_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();

    assert_rows_affected(
        conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
            .unwrap(),
        1,
    );
}

// ── FOREIGN KEY: update parent PK with children ──────────────────────

#[test]
fn fk_update_parent_pk_with_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
        .unwrap();

    let err = conn
        .execute("UPDATE parent SET id = 99 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

// ── FOREIGN KEY: create FK to nonexistent table ──────────────────────

#[test]
fn fk_nonexistent_parent_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let err = conn
        .execute(
            "CREATE TABLE child (\
                id INTEGER NOT NULL PRIMARY KEY, \
                parent_id INTEGER, \
                FOREIGN KEY (parent_id) REFERENCES ghost(id)\
            )",
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            SqlError::TableNotFound(_)
                | SqlError::ForeignKeyViolation(_)
                | SqlError::Unsupported(_)
        ),
        "expected error for FK to nonexistent table, got {err:?}"
    );
}

// ── FOREIGN KEY: FK referencing non-PK/non-UNIQUE column ─────────────

#[test]
fn fk_reference_non_pk_non_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
        .unwrap();

    let err = conn
        .execute(
            "CREATE TABLE child (\
                id INTEGER NOT NULL PRIMARY KEY, \
                pname TEXT, \
                FOREIGN KEY (pname) REFERENCES parent(name)\
            )",
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            SqlError::ForeignKeyViolation(_)
                | SqlError::Unsupported(_)
                | SqlError::ColumnNotFound(_)
        ),
        "expected error for FK referencing non-PK/non-UNIQUE column, got {err:?}"
    );
}

// ── FOREIGN KEY: drop table referenced by FK ─────────────────────────

#[test]
fn fk_drop_referenced_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    let err = conn.execute("DROP TABLE parent").unwrap_err();
    assert!(
        matches!(
            err,
            SqlError::ForeignKeyViolation(_) | SqlError::Unsupported(_)
        ),
        "expected error when dropping table referenced by FK, got {err:?}"
    );
}

// ── FOREIGN KEY: column-level syntax ─────────────────────────────────

#[test]
fn fk_column_level_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO child (id, parent_id) VALUES (2, 999)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

// ── FOREIGN KEY: table-level syntax ──────────────────────────────────

#[test]
fn fk_table_level_syntax() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (\
            id INTEGER NOT NULL PRIMARY KEY, \
            parent_id INTEGER, \
            FOREIGN KEY (parent_id) REFERENCES parent(id)\
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (5)").unwrap();
    assert_rows_affected(
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 5)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO child (id, parent_id) VALUES (2, 6)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

// ── FOREIGN KEY: persistence after reopen ────────────────────────────

#[test]
fn fk_persistence_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE TABLE child (\
                id INTEGER NOT NULL PRIMARY KEY, \
                parent_id INTEGER, \
                FOREIGN KEY (parent_id) REFERENCES parent(id)\
            )",
        )
        .unwrap();
        conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn
            .query("SELECT parent_id FROM child WHERE id = 1")
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(1));

        let err = conn
            .execute("INSERT INTO child (id, parent_id) VALUES (2, 999)")
            .unwrap_err();
        assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

        let err = conn.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
        assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
    }
}
