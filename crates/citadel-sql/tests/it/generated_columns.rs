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

fn execute_generated(
    conn: &Connection<'_>,
    sql: &str,
    params: &[Value],
    prepared: bool,
) -> Result<u64, SqlError> {
    if prepared {
        conn.prepare(sql)?.execute(params)
    } else {
        match conn.execute_params(sql, params)? {
            ExecutionResult::RowsAffected(count) => Ok(count),
            other => panic!("expected affected rows, got {other:?}"),
        }
    }
}

fn assert_generated_overflow(error: SqlError) {
    assert!(matches!(error, SqlError::IntegerOverflow), "got: {error:?}");
}

fn query_generated(
    conn: &Connection<'_>,
    sql: &str,
    prepared: bool,
) -> Result<QueryResult, SqlError> {
    if prepared {
        conn.prepare(sql)?.query_collect(&[])
    } else {
        conn.query(sql)
    }
}

fn generated_query_rows(conn: &Connection<'_>, sql: &str, prepared: bool) -> Vec<Vec<Value>> {
    query_generated(conn, sql, prepared)
        .unwrap_or_else(|error| panic!("prepared={prepared}: {sql}: {error:?}"))
        .rows
}

fn for_each_generated_query_mode(conn: &Connection<'_>, mut run: impl FnMut(bool)) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(sql) = begin {
            conn.execute(sql).unwrap();
        }
        for prepared in [false, true] {
            run(prepared);
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn stored_checked_insert_autocommit_matches_generic_overflow() {
    for (expression, a, b) in [
        ("a + b", i64::MAX, 1),
        ("a + b", i64::MIN, -1),
        ("a * 2", i64::MAX, 0),
        ("a * 2 + 1", i64::MIN, 0),
        ("a * 1 + 1", i64::MAX, 0),
        ("a * 1 + -1", i64::MIN, 0),
        ("a * 2 + -1", i64::MAX / 2 + 1, 0),
        ("a * 2 + 2", i64::MIN / 2 - 1, 0),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
             g INTEGER NOT NULL GENERATED ALWAYS AS ({expression}) STORED)"
        ))
        .unwrap();
        for prepared in [false, true] {
            assert_generated_overflow(
                execute_generated(
                    &conn,
                    "INSERT INTO t (id, a, b) VALUES ($1, $2, $3)",
                    &[Value::Integer(1), Value::Integer(a), Value::Integer(b)],
                    prepared,
                )
                .unwrap_err(),
            );
            assert!(query(&conn, "SELECT * FROM t").rows.is_empty());
        }
    }
}

#[test]
fn stored_checked_insert_templates_add_reject_overflow() {
    for (values, params) in [
        (
            "$1, $2, $3".to_string(),
            vec![
                Value::Integer(1),
                Value::Integer(i64::MAX),
                Value::Integer(1),
            ],
        ),
        (
            "$1, $2, $3".to_string(),
            vec![
                Value::Integer(1),
                Value::Integer(i64::MIN),
                Value::Integer(-1),
            ],
        ),
        (
            "$1, $2, 1".to_string(),
            vec![Value::Integer(1), Value::Integer(i64::MAX)],
        ),
        (
            "$1, 1, $2".to_string(),
            vec![Value::Integer(1), Value::Integer(i64::MAX)],
        ),
        // Ordinary negative literals are unary expressions and exercise the
        // cached fallback instead of the direct generated-value template.
        (
            "$1, -1, $2".to_string(),
            vec![Value::Integer(1), Value::Integer(i64::MIN)],
        ),
        (format!("$1, {}, 1", i64::MAX), vec![Value::Integer(1)]),
        (format!("$1, {}, -1", i64::MIN), vec![Value::Integer(1)]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
             g INTEGER NOT NULL GENERATED ALWAYS AS (a + b) STORED)",
        )
        .unwrap();
        let stmt = conn
            .prepare(&format!("INSERT INTO t (id, a, b) VALUES ({values})"))
            .unwrap();
        conn.execute("BEGIN").unwrap();
        assert_generated_overflow(stmt.execute(&params).unwrap_err());
        assert!(query(&conn, "SELECT * FROM t").rows.is_empty());
        conn.execute("COMMIT").unwrap();
    }
}

#[test]
fn stored_checked_insert_templates_mul_add_reject_intermediate_overflow() {
    for (expression, bad) in [
        ("a * 2", i64::MAX),
        ("a * 2", i64::MIN),
        ("a * 2 + 1", i64::MAX),
        ("a * 1 + 1", i64::MAX),
        ("a * 1 + -1", i64::MIN),
        // The final mathematical answers fit; the multiplication still overflows.
        ("a * 2 + -1", i64::MAX / 2 + 1),
        ("a * 2 + 2", i64::MIN / 2 - 1),
    ] {
        for literal_input in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
                 g INTEGER NOT NULL GENERATED ALWAYS AS ({expression}) STORED)"
            ))
            .unwrap();
            let (sql, params) = if literal_input {
                (
                    format!("INSERT INTO t (id, a) VALUES ($1, {bad})"),
                    vec![Value::Integer(1)],
                )
            } else {
                (
                    "INSERT INTO t (id, a) VALUES ($1, $2)".into(),
                    vec![Value::Integer(1), Value::Integer(bad)],
                )
            };
            let stmt = conn.prepare(&sql).unwrap();
            conn.execute("BEGIN").unwrap();
            assert_generated_overflow(stmt.execute(&params).unwrap_err());
            assert!(query(&conn, "SELECT * FROM t").rows.is_empty());
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn stored_checked_insert_cached_and_uncached_reject_overflow() {
    for (expression, a, b) in [
        ("a + b", i64::MAX, 1),
        ("a + b", i64::MIN, -1),
        ("a * 2", i64::MAX, 0),
        ("a * 2 + 1", i64::MIN, 0),
        ("a * 2 + -1", i64::MAX / 2 + 1, 0),
    ] {
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            // The default excludes direct encoded INSERT templates. Prepared
            // execution uses its cache; ordinary explicit execution is uncached.
            conn.execute(&format!(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                 tail INTEGER DEFAULT 7, g INTEGER GENERATED ALWAYS AS ({expression}) STORED)"
            ))
            .unwrap();
            conn.execute("BEGIN").unwrap();
            assert_generated_overflow(
                execute_generated(
                    &conn,
                    "INSERT INTO t (id, a, b) VALUES ($1, $2, $3)",
                    &[Value::Integer(1), Value::Integer(a), Value::Integer(b)],
                    prepared,
                )
                .unwrap_err(),
            );
            assert!(query(&conn, "SELECT * FROM t").rows.is_empty());
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn stored_checked_insert_late_error_cannot_publish_a_prefix() {
    for prepared in [false, true] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
                 g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2 + 1) STORED)",
            )
            .unwrap();
            conn.execute("INSERT INTO t (id, a) VALUES (0, 2)").unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            assert_generated_overflow(
                execute_generated(
                    &conn,
                    "INSERT INTO t (id, a) VALUES (1, 3), (2, $1)",
                    &[Value::Integer(i64::MAX)],
                    prepared,
                )
                .unwrap_err(),
            );
            if explicit {
                assert!(matches!(
                    conn.execute("COMMIT"),
                    Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                ));
            }
            assert_eq!(
                query(&conn, "SELECT * FROM t").rows,
                vec![vec![
                    Value::Integer(0),
                    Value::Integer(2),
                    Value::Integer(5)
                ]]
            );
        }
    }
}

#[test]
fn stored_checked_update_point_rejects_overflow_before_replacement() {
    for (expression, bad, b) in [
        ("a + b", i64::MAX, 1),
        ("a * 2 + 1", i64::MAX, 0),
        ("a * 2 + -1", i64::MAX / 2 + 1, 0),
    ] {
        for prepared in [false, true] {
            for explicit in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                conn.execute(&format!(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, \
                     g INTEGER NOT NULL GENERATED ALWAYS AS ({expression}) STORED)"
                ))
                .unwrap();
                conn.execute(&format!("INSERT INTO t (id, a, b) VALUES (1, 1, {b})"))
                    .unwrap();
                let before = query(&conn, "SELECT * FROM t").rows;
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                assert_generated_overflow(
                    execute_generated(
                        &conn,
                        "UPDATE t SET a = $1 WHERE id = 1",
                        &[Value::Integer(bad)],
                        prepared,
                    )
                    .unwrap_err(),
                );
                // Point updates build a replacement before publishing it.
                if explicit {
                    conn.execute("COMMIT").unwrap();
                }
                assert_eq!(query(&conn, "SELECT * FROM t").rows, before);
            }
        }
    }
}

#[test]
fn stored_checked_update_range_and_scan_overflow_are_atomic() {
    for (nullable, predicate) in [
        (false, "id >= 1 AND id <= 2"),
        (true, "id >= 1 AND id <= 2"),
        (true, "a >= 0"),
    ] {
        for prepared in [false, true] {
            for explicit in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                let not_null = if nullable { "" } else { "NOT NULL" };
                conn.execute(&format!(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER {not_null}, \
                     g INTEGER {not_null} GENERATED ALWAYS AS (a * 2 + 1) STORED)"
                ))
                .unwrap();
                let outside = if nullable { "NULL" } else { "10" };
                conn.execute(&format!(
                    "INSERT INTO t (id, a) VALUES (1, 0), (2, {}), (3, {outside})",
                    i64::MAX / 2
                ))
                .unwrap();
                let before = query(&conn, "SELECT * FROM t ORDER BY id").rows;
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                assert_generated_overflow(
                    execute_generated(
                        &conn,
                        &format!("UPDATE t SET a = a + 1 WHERE {predicate}"),
                        &[],
                        prepared,
                    )
                    .unwrap_err(),
                );
                if explicit {
                    if nullable {
                        // Collected rows are evaluated before the batch is written.
                        conn.execute("COMMIT").unwrap();
                    } else {
                        // The fixed-width range already patched the first row.
                        assert!(matches!(
                            conn.execute("COMMIT"),
                            Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                        ));
                    }
                }
                assert_eq!(query(&conn, "SELECT * FROM t ORDER BY id").rows, before);
            }
        }
    }
}

#[test]
fn stored_checked_update_savepoint_recovers_after_generated_overflow() {
    for prepared in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
             g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2 + 1) STORED)",
        )
        .unwrap();
        conn.execute(&format!(
            "INSERT INTO t (id, a) VALUES (1, 0), (2, {})",
            i64::MAX / 2
        ))
        .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT before_update").unwrap();
        assert_generated_overflow(
            execute_generated(
                &conn,
                "UPDATE t SET a = a + 1 WHERE id >= 1 AND id <= 2",
                &[],
                prepared,
            )
            .unwrap_err(),
        );
        assert!(matches!(
            conn.query("SELECT 1"),
            Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
        ));
        conn.execute("ROLLBACK TO before_update").unwrap();
        assert_eq!(
            execute_generated(&conn, "UPDATE t SET a = 5 WHERE id = 1", &[], prepared).unwrap(),
            1
        );
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            query(&conn, "SELECT * FROM t ORDER BY id").rows,
            vec![
                vec![Value::Integer(1), Value::Integer(5), Value::Integer(11)],
                vec![
                    Value::Integer(2),
                    Value::Integer(i64::MAX / 2),
                    Value::Integer(i64::MAX)
                ],
            ]
        );
    }
}

#[test]
fn stored_checked_integer_boundaries_and_null_real_fallbacks() {
    for real in [false, true] {
        for prepared in [false, true] {
            for explicit in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                let data_type = if real { "REAL" } else { "INTEGER" };
                conn.execute(&format!(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, a {data_type}, b {data_type}, \
                     g {data_type} GENERATED ALWAYS AS (a + b) STORED, \
                     h {data_type} GENERATED ALWAYS AS (a * 2 + 1) STORED)"
                ))
                .unwrap();
                let inputs = if real {
                    vec![
                        (Value::Null, Value::Real(3.0)),
                        (Value::Real(1.5), Value::Real(2.25)),
                        (Value::Real(-1.5), Value::Real(2.25)),
                    ]
                } else {
                    vec![
                        (Value::Null, Value::Integer(3)),
                        (
                            Value::Integer(i64::MAX / 2),
                            Value::Integer(i64::MAX / 2 + 1),
                        ),
                        (Value::Integer(i64::MIN / 2), Value::Integer(i64::MIN / 2)),
                    ]
                };
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                for (index, (a, b)) in inputs.into_iter().enumerate() {
                    assert_eq!(
                        execute_generated(
                            &conn,
                            "INSERT INTO t (id, a, b) VALUES ($1, $2, $3)",
                            &[Value::Integer(index as i64 + 1), a, b],
                            prepared,
                        )
                        .unwrap(),
                        1
                    );
                }
                assert_eq!(
                    execute_generated(
                        &conn,
                        "INSERT INTO t (id, a, b) VALUES ($1, 10, 20)",
                        &[Value::Integer(4)],
                        prepared
                    )
                    .unwrap(),
                    1
                );
                assert_eq!(
                    execute_generated(&conn, "UPDATE t SET a = a + 1 WHERE id = 4", &[], prepared)
                        .unwrap(),
                    1
                );
                if explicit {
                    conn.execute("COMMIT").unwrap();
                }
                let expected = if real {
                    vec![
                        vec![Value::Null, Value::Null],
                        vec![Value::Real(3.75), Value::Real(4.0)],
                        vec![Value::Real(0.75), Value::Real(-2.0)],
                        vec![Value::Real(31.0), Value::Real(23.0)],
                    ]
                } else {
                    vec![
                        vec![Value::Null, Value::Null],
                        vec![Value::Integer(i64::MAX), Value::Integer(i64::MAX)],
                        vec![Value::Integer(i64::MIN), Value::Integer(i64::MIN + 1)],
                        vec![Value::Integer(31), Value::Integer(23)],
                    ]
                };
                assert_eq!(
                    query(&conn, "SELECT g, h FROM t ORDER BY id").rows,
                    expected
                );
            }
        }
    }
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

#[test]
fn stream_aggregates_over_virtual_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let qr = conn.query("SELECT SUM(g) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(120));
    let qr = conn
        .query("SELECT MIN(g), MAX(g), COUNT(g) FROM t")
        .unwrap();
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(20), Value::Integer(60), Value::Integer(3)]
    );
}

#[test]
fn group_by_over_virtual_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 10), (2, 10), (3, 30)")
        .unwrap();

    let mut rows = conn
        .query("SELECT g, COUNT(*) FROM t GROUP BY g")
        .unwrap()
        .rows;
    rows.sort_by(|left, right| left[0].cmp(&right[0]));
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(20), Value::Integer(2)],
            vec![Value::Integer(60), Value::Integer(1)],
        ]
    );
}

#[test]
fn stream_group_by_min_over_virtual_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 10), (2, 10), (3, 30)")
        .unwrap();

    let mut rows = conn
        .query("SELECT a, MIN(g) FROM t GROUP BY a")
        .unwrap()
        .rows;
    rows.sort_by(|left, right| left[0].cmp(&right[0]));
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(10), Value::Integer(20)],
            vec![Value::Integer(30), Value::Integer(60)],
        ]
    );
}

#[test]
fn order_by_virtual_column_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 30), (2, 10), (3, 20)")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM t ORDER BY g DESC LIMIT 2")
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn virtual_select_base_columns_do_not_evaluate_unused_virtuals() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute(&format!(
        "INSERT INTO t (id, a) VALUES (1, 1), (2, {})",
        i64::MAX
    ))
    .unwrap();

    let all = vec![
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(i64::MAX)],
    ];
    let last = vec![all[1].clone()];
    for_each_generated_query_mode(&conn, |prepared| {
        for (sql, expected) in [
            ("SELECT id, a FROM t ORDER BY id", &all),
            ("SELECT id, a FROM t WHERE id = 2", &last),
            ("SELECT id, a FROM t WHERE id >= 2 ORDER BY id", &last),
            ("SELECT id, a FROM t WHERE a > 1 ORDER BY a", &last),
            ("SELECT id, a FROM t ORDER BY a DESC LIMIT 1", &last),
            ("SELECT DISTINCT id, a FROM t ORDER BY id", &all),
        ] {
            assert_eq!(
                generated_query_rows(&conn, sql, prepared),
                *expected,
                "prepared={prepared}: {sql}"
            );
        }
    });
}

#[test]
fn virtual_select_required_columns_still_report_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute(&format!(
        "INSERT INTO t (id, a) VALUES (1, 1), (2, {})",
        i64::MAX
    ))
    .unwrap();

    for_each_generated_query_mode(&conn, |prepared| {
        for sql in [
            "SELECT g FROM t WHERE id = 1",
            "SELECT g FROM t WHERE a = 1 ORDER BY id",
        ] {
            assert_eq!(
                generated_query_rows(&conn, sql, prepared),
                vec![vec![Value::Integer(2)]],
                "prepared={prepared}: {sql}"
            );
        }
        for sql in [
            "SELECT g FROM t WHERE id = 2",
            "SELECT * FROM t WHERE id = 2",
            "SELECT g FROM t ORDER BY id",
            "SELECT id FROM t WHERE g > 0",
            "SELECT id FROM t ORDER BY g",
            "SELECT SUM(g) FROM t",
            "SELECT g, COUNT(*) FROM t GROUP BY g",
        ] {
            let error = query_generated(&conn, sql, prepared).unwrap_err();
            assert!(
                matches!(error, SqlError::IntegerOverflow),
                "prepared={prepared}: {sql}: {error:?}"
            );
        }
        assert_eq!(
            generated_query_rows(&conn, "SELECT g FROM t WHERE id = 1", prepared),
            vec![vec![Value::Integer(2)]]
        );
    });
}

#[test]
fn virtual_select_only_evaluates_referenced_virtual_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         safe INTEGER GENERATED ALWAYS AS (a / 2) VIRTUAL, \
         overflowing INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute(&format!(
        "INSERT INTO t (id, a) VALUES (1, 1), (2, {})",
        i64::MAX
    ))
    .unwrap();

    let max_half = Value::Integer(i64::MAX / 2);
    for_each_generated_query_mode(&conn, |prepared| {
        for sql in [
            "SELECT safe FROM t WHERE id = 2",
            "SELECT safe FROM t WHERE safe > 0 ORDER BY id",
            "SELECT safe FROM t ORDER BY safe DESC LIMIT 1",
            "SELECT safe AS value FROM t ORDER BY value DESC LIMIT 1",
            "SELECT safe FROM t ORDER BY 1 DESC LIMIT 1",
            "SELECT MAX(safe) FROM t",
        ] {
            assert_eq!(
                generated_query_rows(&conn, sql, prepared),
                vec![vec![max_half.clone()]],
                "prepared={prepared}: {sql}"
            );
        }
        assert_eq!(
            generated_query_rows(&conn, "SELECT safe FROM t ORDER BY id", prepared),
            vec![vec![Value::Integer(0)], vec![max_half.clone()]]
        );
        assert_eq!(
            generated_query_rows(
                &conn,
                "SELECT safe, COUNT(*) FROM t GROUP BY safe ORDER BY safe",
                prepared,
            ),
            vec![
                vec![Value::Integer(0), Value::Integer(1)],
                vec![max_half.clone(), Value::Integer(1)],
            ]
        );
        assert_eq!(
            generated_query_rows(
                &conn,
                "SELECT safe, ROW_NUMBER() OVER (ORDER BY safe) FROM t ORDER BY safe",
                prepared,
            ),
            vec![
                vec![Value::Integer(0), Value::Integer(1)],
                vec![max_half.clone(), Value::Integer(2)],
            ]
        );
        assert_generated_overflow(
            query_generated(&conn, "SELECT overflowing FROM t WHERE id = 2", prepared).unwrap_err(),
        );
    });
}

#[test]
fn virtual_select_generic_filter_materializes_requested_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute(&format!(
        "INSERT INTO t (id, a) VALUES (1, 1), (2, {})",
        i64::MAX
    ))
    .unwrap();
    for_each_generated_query_mode(&conn, |prepared| {
        for sql in [
            "SELECT g FROM t WHERE COALESCE(a, 0) = 1",
            "SELECT g FROM t WHERE COALESCE(a, 0) = 1 ORDER BY id",
        ] {
            assert_eq!(
                generated_query_rows(&conn, sql, prepared),
                vec![vec![Value::Integer(2)]],
                "prepared={prepared}: {sql}"
            );
        }
    });
}

#[test]
fn virtual_select_constant_join_does_not_materialize_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("CREATE TABLE tiny (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO tiny VALUES (1), (2)").unwrap();
    conn.execute(&format!("INSERT INTO t (id, a) VALUES (1, {})", i64::MAX))
        .unwrap();
    for_each_generated_query_mode(&conn, |prepared| {
        assert_eq!(
            generated_query_rows(&conn, "SELECT 1 FROM t CROSS JOIN tiny", prepared),
            vec![vec![Value::Integer(1)], vec![Value::Integer(1)]]
        );
    });
}

#[test]
fn virtual_select_index_and_prefix_scans_skip_unused_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (tenant INTEGER, id INTEGER, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL, PRIMARY KEY (tenant, id))",
    )
    .unwrap();
    conn.execute("CREATE INDEX a_index ON t (a)").unwrap();
    conn.execute(&format!(
        "INSERT INTO t (tenant, id, a) VALUES (1, 1, {}), (2, 1, 1)",
        i64::MAX
    ))
    .unwrap();
    for_each_generated_query_mode(&conn, |prepared| {
        for sql in [
            "SELECT a FROM t WHERE tenant = 1 ORDER BY id",
            "SELECT a FROM t WHERE a = 9223372036854775807",
        ] {
            assert_eq!(
                generated_query_rows(&conn, sql, prepared),
                vec![vec![Value::Integer(i64::MAX)]],
                "prepared={prepared}: {sql}"
            );
        }
    });
}

#[test]
fn virtual_select_defaults_apply_only_to_requested_missing_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, \
         g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, a) VALUES (1, 1)").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (9223372036854775807 + 1)")
        .unwrap();
    conn.execute("INSERT INTO t (id, a, b) VALUES (2, 1, NULL)")
        .unwrap();
    for_each_generated_query_mode(&conn, |prepared| {
        for id in [1, 2] {
            let sql = format!("SELECT id FROM t WHERE id = {id}");
            assert_eq!(
                generated_query_rows(&conn, &sql, prepared),
                vec![vec![Value::Integer(id)]],
                "prepared={prepared}: {sql}"
            );
        }
        assert_eq!(
            generated_query_rows(&conn, "SELECT b FROM t WHERE id = 2", prepared),
            vec![vec![Value::Null]]
        );
        assert!(generated_query_rows(&conn, "SELECT b FROM t WHERE id = 999", prepared).is_empty());
        assert_generated_overflow(
            query_generated(&conn, "SELECT b FROM t WHERE id = 1", prepared).unwrap_err(),
        );
    });
}
