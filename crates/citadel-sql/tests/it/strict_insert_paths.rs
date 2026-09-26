use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"strict-insert-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn execute(
    conn: &Connection<'_>,
    sql: &str,
    params: &[Value],
    prepared: bool,
) -> Result<(), SqlError> {
    if prepared {
        conn.prepare(sql)?.execute(params).map(|_| ())
    } else {
        conn.execute_params(sql, params).map(|_| ())
    }
}

fn reject_insert(ddl: &str, sql: &str, params: &[Value]) {
    for prepared in [false, true] {
        for explicit in [false, true] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(ddl).unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let error = execute(&conn, sql, params, prepared)
                .expect_err(&format!("{sql}; prepared={prepared}, explicit={explicit}"));
            assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
            if explicit {
                conn.execute("ROLLBACK").unwrap();
            }
            assert_eq!(
                conn.query("SELECT COUNT(*) FROM t").unwrap().rows,
                vec![vec![Value::Integer(0)]]
            );
        }
    }
}

#[test]
fn strict_insert_values_and_select_reject_lossy_conversion() {
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT";
    for sql in [
        "INSERT INTO t VALUES ($1, 1.5)",
        "INSERT INTO t VALUES ($1, 1.0 + 0.5)",
        "INSERT INTO t VALUES ($1, $2)",
        "INSERT INTO t SELECT $1, 1.5",
    ] {
        let params = if sql.contains("$2") {
            vec![Value::Integer(1), Value::Real(1.5)]
        } else {
            vec![Value::Integer(1)]
        };
        reject_insert(ddl, sql, &params);
    }
}

#[test]
fn strict_insert_default_rejects_lossy_conversion() {
    reject_insert(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER DEFAULT 1.5) STRICT",
        "INSERT INTO t (id) VALUES ($1)",
        &[Value::Integer(1)],
    );
}

#[test]
fn strict_insert_stored_generated_rejects_lossy_conversion() {
    reject_insert(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL, \
         n INTEGER GENERATED ALWAYS AS (r) STORED) STRICT",
        "INSERT INTO t (id, r) VALUES ($1, 1.5)",
        &[Value::Integer(1)],
    );
}

#[test]
fn strict_upsert_rejects_lossy_assignments_and_generated_values() {
    for generated in [false, true] {
        for indexed in [false, true] {
            for prepared in [false, true] {
                for explicit in [false, true] {
                    let db = database();
                    let conn = Connection::open(&db).unwrap();
                    if generated {
                        conn.execute(
                            "CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL, \
                             n INTEGER GENERATED ALWAYS AS (r) STORED) STRICT",
                        )
                        .unwrap();
                        conn.execute("INSERT INTO t (id, r) VALUES (1, 2.0)")
                            .unwrap();
                    } else {
                        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
                            .unwrap();
                        conn.execute("INSERT INTO t VALUES (1, 2)").unwrap();
                    }
                    if indexed {
                        conn.execute("CREATE INDEX t_n ON t (n)").unwrap();
                    }
                    if explicit {
                        conn.execute("BEGIN").unwrap();
                    }
                    let sql = if generated {
                        "INSERT INTO t (id, r) VALUES ($1, 2.0) \
                         ON CONFLICT (id) DO UPDATE SET r = 1.5"
                    } else {
                        "INSERT INTO t VALUES ($1, 2) \
                         ON CONFLICT (id) DO UPDATE SET n = 1.5"
                    };
                    let error =
                        execute(&conn, sql, &[Value::Integer(1)], prepared).expect_err(&format!(
                            "generated={generated}, indexed={indexed}, \
                             prepared={prepared}, explicit={explicit}"
                        ));
                    assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
                    if explicit {
                        conn.execute("ROLLBACK").unwrap();
                    }
                    assert_eq!(
                        conn.query("SELECT id, n FROM t").unwrap().rows,
                        vec![vec![Value::Integer(1), Value::Integer(2)]]
                    );
                }
            }
        }
    }
}

#[test]
fn strict_insert_late_failure_cannot_commit_a_prefix() {
    for prepared in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER) STRICT")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        let error = execute(
            &conn,
            "INSERT INTO t VALUES (1, 2), (2, 1.5)",
            &[],
            prepared,
        )
        .unwrap_err();
        assert!(matches!(error, SqlError::TypeMismatch { .. }));
        assert!(conn.execute("COMMIT").is_err());
        if conn.in_transaction() {
            conn.execute("ROLLBACK").unwrap();
        }
        assert!(conn.query("SELECT * FROM t").unwrap().rows.is_empty());
    }
}

#[test]
fn prepared_insert_lossless_fallback_preserves_integer_storage() {
    for strict in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL){}",
            if strict { " STRICT" } else { "" }
        ))
        .unwrap();
        conn.execute("BEGIN").unwrap();
        let insert = conn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
        insert
            .execute(&[Value::Real(1.0), Value::Real(2.0)])
            .unwrap();
        let text_result = insert.execute(&[Value::Integer(2), Value::Text("3".into())]);
        if strict {
            text_result.unwrap();
        } else {
            assert!(matches!(text_result, Err(SqlError::TypeMismatch { .. })));
            insert
                .execute(&[Value::Integer(2), Value::Real(3.0)])
                .unwrap();
        }
        insert
            .execute(&[Value::Integer(3), Value::Integer(4)])
            .unwrap();
        conn.prepare("INSERT INTO t VALUES ($1, 5.0)")
            .unwrap()
            .execute(&[Value::Integer(4)])
            .unwrap();
        conn.execute("COMMIT").unwrap();
        let rows = conn.query("SELECT id, n FROM t ORDER BY id").unwrap().rows;
        assert_eq!(
            rows,
            (1..=4)
                .map(|id| vec![Value::Integer(id), Value::Integer(id + 1)])
                .collect::<Vec<_>>()
        );
        assert!(rows
            .iter()
            .flatten()
            .all(|v| matches!(v, Value::Integer(_))));
    }
}

#[test]
fn prepared_insert_literals_keep_non_strict_coercion_and_not_null_checks() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.prepare("INSERT INTO t VALUES ($1, 1.5)")
        .unwrap()
        .execute(&[Value::Integer(1)])
        .unwrap();
    let error = conn
        .prepare("INSERT INTO t VALUES ($1, NULL)")
        .unwrap()
        .execute(&[Value::Integer(2)])
        .unwrap_err();
    assert!(matches!(error, SqlError::NotNullViolation(_)), "{error:?}");
    conn.execute("COMMIT").unwrap();
    let rows = conn.query("SELECT n FROM t").unwrap().rows;
    assert!(matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Integer(1)])));
}

#[test]
fn strict_virtual_generated_coercion_matches_full_and_partial_decoding() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, r REAL, \
         n INTEGER GENERATED ALWAYS AS (r) VIRTUAL) STRICT",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id, r) VALUES (1, 1.5)")
        .unwrap();
    for sql in [
        "SELECT * FROM t",
        "SELECT n FROM t",
        "SELECT n FROM t WHERE id = 1",
    ] {
        for prepared in [false, true] {
            let result = if prepared {
                conn.prepare(sql).unwrap().query_collect(&[])
            } else {
                conn.query(sql)
            };
            let error = result.expect_err(sql);
            assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
        }
    }
}
