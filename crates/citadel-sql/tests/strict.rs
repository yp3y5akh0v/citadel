use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn create_memory_db() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn assert_ok(r: ExecutionResult) {
    assert!(matches!(
        r,
        ExecutionResult::Ok | ExecutionResult::RowsAffected(_)
    ));
}

#[test]
fn strict_table_accepts_exact_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
            .unwrap(),
    );
    assert_ok(conn.execute("INSERT INTO t VALUES (1, 42)").unwrap());
    let qr = conn.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn strict_table_rejects_text_to_integer_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 'xyz')").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_rejects_lossy_leading_zeros() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (1, '000123')")
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_accepts_lossless_text_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '42')").unwrap();
    let qr = conn.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn strict_table_rejects_real_to_integer_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 5.5)").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_table_accepts_real_to_integer_whole() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn strict_table_accepts_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn strict_table_update_rejects_lossy() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let err = conn
        .execute("UPDATE t SET n = 'xyz' WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn non_strict_table_truncates_real_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.5)").unwrap();
    let qr = conn.query("SELECT n FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn strict_table_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1, 'xyz')").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_numeric_boundaries_reject_lossy_writes_without_changing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    for prepared in [false, true] {
        for explicit in [false, true] {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "CREATE TABLE boundary (id INTEGER PRIMARY KEY, n INTEGER, r REAL) STRICT",
            )
            .unwrap();
            conn.execute("INSERT INTO boundary VALUES (1, 42, 0.5)")
                .unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            for sql in [
                "INSERT INTO boundary VALUES (2, $1, 0.5)",
                "UPDATE boundary SET n = $1 WHERE id = 1",
                "UPDATE boundary SET id = $1 WHERE id = 1",
            ] {
                let params = [Value::Real(-(i64::MIN as f64))];
                let result = if prepared {
                    conn.prepare(sql).unwrap().execute(&params).map(|_| ())
                } else {
                    conn.execute_params(sql, &params).map(|_| ())
                };
                let err =
                    result.expect_err(&format!("{sql}; prepared={prepared}; explicit={explicit}"));
                assert!(
                    matches!(err, SqlError::TypeMismatch { .. }),
                    "{sql}: {err:?}"
                );
                assert_eq!(
                    conn.query("SELECT id, n, r FROM boundary").unwrap().rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Integer(42),
                        Value::Real(0.5)
                    ]]
                );
            }
            if explicit {
                conn.execute("ROLLBACK").unwrap();
            }
            conn.execute("DROP TABLE boundary").unwrap();
        }
    }
}

#[test]
fn strict_numeric_boundaries_round_trip_exact_large_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE boundary (id INTEGER PRIMARY KEY, n INTEGER, r REAL) STRICT")
        .unwrap();
    let insert = conn
        .prepare("INSERT INTO boundary VALUES ($1, $2, $3)")
        .unwrap();
    for (id, integer) in [i64::MIN, i64::MAX - 1023, 1i64 << 60]
        .into_iter()
        .enumerate()
    {
        insert
            .execute(&[
                Value::Integer(id as i64),
                Value::Real(integer as f64),
                Value::Integer(integer),
            ])
            .unwrap();
    }
    assert_eq!(
        conn.query("SELECT n, r FROM boundary ORDER BY id")
            .unwrap()
            .rows,
        [i64::MIN, i64::MAX - 1023, 1i64 << 60]
            .into_iter()
            .map(|integer| vec![Value::Integer(integer), Value::Real(integer as f64)])
            .collect::<Vec<_>>()
    );
    let err = conn
        .execute_params(
            "UPDATE boundary SET r = $1 WHERE id = 0",
            &[Value::Integer(i64::MAX)],
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn strict_updates_validate_every_scan_and_patch_lane() {
    for nullable in [false, true] {
        for predicate in ["id = 2", "id >= 1 AND id <= 3", "n = 2"] {
            for prepared in [false, true] {
                for explicit in [false, true] {
                    let db = create_memory_db();
                    let conn = Connection::open(&db).unwrap();
                    conn.execute(&format!(
                        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER{}) STRICT",
                        if nullable { "" } else { " NOT NULL" }
                    ))
                    .unwrap();
                    conn.execute("INSERT INTO t VALUES (1, 2), (2, 2), (3, 2)")
                        .unwrap();
                    if explicit {
                        conn.execute("BEGIN").unwrap();
                    }
                    let sql = format!("UPDATE t SET n = $1 WHERE {predicate}");
                    let error = if prepared {
                        conn.prepare(&sql)
                            .unwrap()
                            .execute(&[Value::Real(1.5)])
                            .unwrap_err()
                    } else {
                        conn.execute_params(&sql, &[Value::Real(1.5)]).unwrap_err()
                    };
                    assert!(
                        matches!(error, SqlError::TypeMismatch { .. }),
                        "{sql}; nullable={nullable}, prepared={prepared}, \
                         explicit={explicit}: {error:?}"
                    );
                    if explicit {
                        conn.execute("ROLLBACK").unwrap();
                    }
                    assert_eq!(
                        conn.query("SELECT id, n FROM t ORDER BY id").unwrap().rows,
                        (1..=3)
                            .map(|id| vec![Value::Integer(id), Value::Integer(2)])
                            .collect::<Vec<_>>()
                    );
                    let expected = if predicate == "id = 2" { 1 } else { 3 };
                    assert!(matches!(
                        conn.execute_params(&sql, &[Value::Real(3.0)]).unwrap(),
                        ExecutionResult::RowsAffected(count) if count == expected
                    ));
                }
            }
        }
    }
}

#[test]
fn strict_generated_updates_reject_lossy_values_without_changing_source() {
    for indexed in [false, true] {
        for prepared in [false, true] {
            for explicit in [false, true] {
                let db = create_memory_db();
                let conn = Connection::open(&db).unwrap();
                conn.execute(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL NOT NULL, \
                     n INTEGER GENERATED ALWAYS AS (r) STORED NOT NULL) STRICT",
                )
                .unwrap();
                conn.execute("INSERT INTO t (id, r) VALUES (1, 2.0), (2, 2.0)")
                    .unwrap();
                if indexed {
                    conn.execute("CREATE INDEX t_n ON t (n)").unwrap();
                }
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                let sql = "UPDATE t SET r = 1.5 WHERE id >= 1 AND id <= 2";
                let error = if prepared {
                    conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
                } else {
                    conn.execute(sql).unwrap_err()
                };
                assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
                if explicit {
                    conn.execute("ROLLBACK").unwrap();
                }
                assert_eq!(
                    conn.query("SELECT r, n FROM t ORDER BY id").unwrap().rows,
                    vec![vec![Value::Real(2.0), Value::Integer(2)]; 2]
                );
            }
        }
    }
}

#[test]
fn strict_update_late_failure_cannot_commit_a_prefix() {
    for explicit in [false, true] {
        for prepared in [false, true] {
            let db = create_memory_db();
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL) STRICT")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 2), (2, 2)").unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let sql = "UPDATE t SET n = CASE WHEN id = 1 THEN 3.0 ELSE 1.5 END \
                       WHERE id >= 1 AND id <= 2";
            let error = if prepared {
                conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
            } else {
                conn.execute(sql).unwrap_err()
            };
            assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
            if explicit {
                assert!(conn.execute("COMMIT").is_err());
                if conn.in_transaction() {
                    conn.execute("ROLLBACK").unwrap();
                }
            }
            assert_eq!(
                conn.query("SELECT n FROM t ORDER BY id").unwrap().rows,
                vec![vec![Value::Integer(2)]; 2]
            );
        }
    }
}

#[test]
fn non_strict_update_fast_paths_keep_permissive_coercion() {
    let db = create_memory_db();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (2, 2)").unwrap();
    for explicit in [false, true] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        conn.prepare("UPDATE t SET n = $1 WHERE id >= 1 AND id <= 2")
            .unwrap()
            .execute(&[Value::Real(1.5)])
            .unwrap();
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        let rows = conn.query("SELECT n FROM t").unwrap().rows;
        assert_eq!(rows, vec![vec![Value::Integer(1)]; 2]);
        assert!(rows.iter().all(|row| matches!(row[0], Value::Integer(1))));
    }
}
