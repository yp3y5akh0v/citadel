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

fn rows_affected(result: ExecutionResult) -> u64 {
    match result {
        ExecutionResult::RowsAffected(n) => n,
        other => panic!("expected RowsAffected, got {other:?}"),
    }
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

fn for_checked_upsert_modes(setup: &[&str], check: impl Fn(&Connection<'_>, bool)) {
    for prepared in [true, false] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            for sql in setup {
                conn.execute(sql).unwrap();
            }
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            check(&conn, prepared);
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
}

fn execute_checked_upsert(
    conn: &Connection<'_>,
    prepared: bool,
    sql: &str,
    params: &[Value],
) -> Result<u64, SqlError> {
    if prepared {
        conn.prepare(sql)
            .expect("UPSERT arithmetic must not fail while preparing")
            .execute(params)
    } else {
        conn.execute_params(sql, params).map(rows_affected)
    }
}

#[test]
fn upsert_checked_excluded_virtual_uses_proposed_value_then_recomputes_new() {
    for unique_conflict in [false, true] {
        for_checked_upsert_modes(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER UNIQUE, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
                "INSERT INTO t (id, k, a) VALUES (1, 10, 1)",
            ],
            |conn, prepared| {
                let (target, proposed_id) = if unique_conflict { ("k", 9) } else { ("id", 1) };
                // No trigger or RETURNING independently requests the proposed virtual.
                for (proposed, predicate, affected, expected) in [
                    (3, "", 1, 6),
                    (4, " WHERE excluded.g = 8", 1, 8),
                    (5, " WHERE excluded.g = 8", 0, 8),
                ] {
                    let sql = format!("INSERT INTO t (id, k, a) VALUES ($1, 10, $2) ON CONFLICT ({target}) DO UPDATE SET a = excluded.g{predicate}");
                    assert_eq!(execute_checked_upsert(conn, prepared, &sql, &[Value::Integer(proposed_id), Value::Integer(proposed)]).unwrap(), affected);
                    assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(10), Value::Integer(expected), Value::Integer(expected * 2)]]);
                }
            },
        );
    }
}

#[test]
fn upsert_checked_excluded_virtual_is_unused_without_an_accepted_conflict() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            assert_eq!(execute_checked_upsert(
                conn, prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = excluded.g WHERE FALSE",
                &[Value::Integer(1), Value::Integer(i64::MAX)],
            ).unwrap(), 0);
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(1), Value::Integer(2)]]);
            // Even a WHERE reference is unused when insertion finds no conflict.
            assert_eq!(execute_checked_upsert(
                conn, prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = excluded.g WHERE excluded.g > 0",
                &[Value::Integer(2), Value::Integer(i64::MAX)],
            ).unwrap(), 1);
            assert_eq!(query(conn, "SELECT id, a FROM t ORDER BY id").rows, vec![
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(i64::MAX)],
            ]);
            assert!(matches!(conn.query("SELECT g FROM t WHERE id = 2"), Err(SqlError::IntegerOverflow)));
        },
    );
}

#[test]
fn upsert_checked_excluded_virtual_required_where_or_set_overflow_preserves_old() {
    for action in ["SET a = excluded.g", "SET a = 2 WHERE excluded.g > 0"] {
        for_checked_upsert_modes(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
                "INSERT INTO t (id, a) VALUES (1, 1)",
            ],
            |conn, prepared| {
                let sql = format!("INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE {action}");
                let error = execute_checked_upsert(conn, prepared, &sql, &[Value::Integer(1), Value::Integer(i64::MAX)]).unwrap_err();
                assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
                assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(1), Value::Integer(2)]]);
            },
        );
    }
}

#[test]
fn upsert_checked_excluded_virtual_late_overflow_cannot_publish_a_prefix() {
    for action in [
        "SET a = excluded.g",
        "SET a = excluded.a WHERE excluded.g > 0",
    ] {
        for prepared in [true, false] {
            for explicit in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)").unwrap();
                conn.execute("INSERT INTO t (id, a) VALUES (1, 1), (2, 2)")
                    .unwrap();
                let before = query(&conn, "SELECT * FROM t ORDER BY id").rows;
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                let sql = format!("INSERT INTO t (id, a) VALUES (1, 3), (2, $1) ON CONFLICT (id) DO UPDATE {action}");
                let error =
                    execute_checked_upsert(&conn, prepared, &sql, &[Value::Integer(i64::MAX)])
                        .unwrap_err();
                assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
                if explicit {
                    assert!(matches!(
                        conn.execute("COMMIT"),
                        Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                    ));
                }
                assert_eq!(query(&conn, "SELECT * FROM t ORDER BY id").rows, before);
            }
        }
    }
}

#[test]
fn upsert_checked_excluded_virtual_preserves_lazy_case_and_coalesce() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            for (assignment, predicate, expected) in [
                ("CASE WHEN excluded.id = 1 THEN 3 ELSE excluded.g END", "CASE WHEN excluded.id = 1 THEN TRUE ELSE excluded.g > 0 END", 3),
                ("COALESCE(4, excluded.g)", "COALESCE(TRUE, excluded.g > 0)", 4),
            ] {
                let sql = format!("INSERT INTO t (id, a) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET a = {assignment} WHERE {predicate}");
                assert_eq!(execute_checked_upsert(conn, prepared, &sql, &[Value::Integer(i64::MAX)]).unwrap(), 1);
                assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(expected), Value::Integer(expected * 2)]]);
            }
            // The same references must fail when their lazy branch is actually visited.
            for action in [
                "SET a = CASE WHEN FALSE THEN 3 ELSE excluded.g END",
                "SET a = COALESCE(NULL, excluded.g)",
                "SET a = 5 WHERE CASE WHEN FALSE THEN TRUE ELSE excluded.g > 0 END",
                "SET a = 5 WHERE COALESCE(NULL, excluded.g > 0)",
            ] {
                let sql = format!("INSERT INTO t (id, a) VALUES (1, $1) ON CONFLICT (id) DO UPDATE {action}");
                let error = execute_checked_upsert(conn, prepared, &sql, &[Value::Integer(i64::MAX)]).unwrap_err();
                assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
                assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(4), Value::Integer(8)]]);
            }
        },
    );
}

#[test]
fn upsert_checked_excluded_virtual_does_not_evaluate_unrelated_virtual() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, sink INTEGER, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL, unused INTEGER GENERATED ALWAYS AS ((a - 1) * 9223372036854775807) VIRTUAL)",
            "INSERT INTO t (id, a, sink) VALUES (1, 1, 0)",
        ],
        |conn, prepared| {
            // Proposed a=3 overflows unused, but both virtuals of the accepted NEW row are safe.
            assert_eq!(execute_checked_upsert(
                conn, prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET sink = excluded.g WHERE excluded.g = 6",
                &[Value::Integer(1), Value::Integer(3)],
            ).unwrap(), 1);
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(1), Value::Integer(6), Value::Integer(2), Value::Integer(0)]]);
        },
    );
}

#[test]
fn upsert_checked_excluded_virtual_applies_declared_type_and_null_coercion() {
    for strict in [false, true] {
        let ddl = format!("CREATE TABLE t (id INTEGER PRIMARY KEY, a REAL, sink REAL, g INTEGER GENERATED ALWAYS AS (a / 2.0) VIRTUAL){}", if strict { " STRICT" } else { "" });
        for_checked_upsert_modes(
            &[&ddl, "INSERT INTO t (id, a, sink) VALUES (1, 4.0, 9.0)"],
            |conn, prepared| {
                let sql = "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET sink = excluded.g";
                let fractional = execute_checked_upsert(
                    conn,
                    prepared,
                    sql,
                    &[Value::Integer(1), Value::Real(3.0)],
                );
                let expected = if strict {
                    let error = fractional.unwrap_err();
                    assert!(
                        matches!(error, SqlError::TypeMismatch { .. }),
                        "got {error:?}"
                    );
                    Value::Real(9.0)
                } else {
                    assert_eq!(fractional.unwrap(), 1);
                    // INTEGER g truncates 1.5 before assignment coerces it into REAL sink.
                    Value::Real(1.0)
                };
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Real(4.0),
                        expected,
                        Value::Integer(2)
                    ]]
                );
                for (proposed, expected) in [
                    (Value::Real(4.0), Value::Real(2.0)),
                    (Value::Null, Value::Null),
                ] {
                    assert_eq!(
                        execute_checked_upsert(conn, prepared, sql, &[Value::Integer(1), proposed])
                            .unwrap(),
                        1
                    );
                    assert_eq!(
                        query(conn, "SELECT * FROM t").rows,
                        vec![vec![
                            Value::Integer(1),
                            Value::Real(4.0),
                            expected,
                            Value::Integer(2)
                        ]]
                    );
                }
            },
        );
    }
}

#[test]
fn upsert_checked_arithmetic_boundaries_and_two_target_atomicity() {
    for (initial, expression, expected) in [
        (Value::Integer(i64::MAX), "v + 1", None),
        (Value::Integer(i64::MIN), "v - 1", None),
        (
            Value::Integer(-1),
            "v - -9223372036854775808",
            Some(Value::Integer(i64::MAX)),
        ),
        (
            Value::Integer(i64::MIN),
            "v - -9223372036854775808",
            Some(Value::Integer(0)),
        ),
        (Value::Integer(0), "v - -9223372036854775808", None),
        (Value::Null, "v - -9223372036854775808", Some(Value::Null)),
        (
            Value::Integer(i64::MAX - 1),
            "v + 1",
            Some(Value::Integer(i64::MAX)),
        ),
        (
            Value::Integer(i64::MIN + 1),
            "v - 1",
            Some(Value::Integer(i64::MIN)),
        ),
    ] {
        let insert = format!("INSERT INTO t VALUES (1, 5, {initial})");
        for_checked_upsert_modes(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, first INTEGER NOT NULL, v INTEGER)",
                &insert,
            ],
            |conn, prepared| {
                let result = execute_checked_upsert(
                    conn,
                    prepared,
                    &format!("INSERT INTO t VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET first = first + 1, v = {expression}"),
                    &[Value::Integer(1), Value::Integer(99), Value::Integer(0)],
                );
                let (first, value) = if let Some(expected) = &expected {
                    assert_eq!(result.unwrap(), 1);
                    (6, expected.clone())
                } else {
                    let error = result.unwrap_err();
                    assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
                    (5, initial.clone())
                };
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![Value::Integer(1), Value::Integer(first), value]]
                );
            },
        );
    }
}

#[test]
fn upsert_checked_late_overflow_cannot_publish_a_prefix() {
    for prepared in [true, false] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
                .unwrap();
            conn.execute(&format!("INSERT INTO t VALUES (1, 10), (2, {})", i64::MAX))
                .unwrap();
            let before = query(&conn, "SELECT * FROM t ORDER BY id").rows;
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let error = execute_checked_upsert(
                &conn,
                prepared,
                "INSERT INTO t VALUES (1, $1), (2, $1) ON CONFLICT (id) DO UPDATE SET v = v + 1",
                &[Value::Integer(0)],
            )
            .unwrap_err();
            assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
            if explicit {
                assert!(matches!(
                    conn.execute("COMMIT"),
                    Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                ));
            }
            assert_eq!(query(&conn, "SELECT * FROM t ORDER BY id").rows, before);
        }
    }
}

#[test]
fn upsert_checked_generated_duplicate_recomputes_stored_value() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2 + 1) STORED)",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            let sql = "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1";
            for (proposed, a, generated) in [(10, 2, 5), (0, 3, 7)] {
                assert_eq!(execute_checked_upsert(conn, prepared, sql, &[Value::Integer(1), Value::Integer(proposed)]).unwrap(), 1);
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![Value::Integer(1), Value::Integer(a), Value::Integer(generated)]]
                );
            }
        },
    );
}

#[test]
fn upsert_checked_generated_overflow_preserves_all_assigned_columns() {
    let insert = format!(
        "INSERT INTO t (id, first, a) VALUES (1, 5, {})",
        i64::MAX / 2
    );
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, first INTEGER NOT NULL, a INTEGER NOT NULL, g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2 + 1) STORED)",
            &insert,
        ],
        |conn, prepared| {
            let before = query(conn, "SELECT * FROM t").rows;
            // Proposed generated value is safe. Overflow comes from recomputing
            // the accepted duplicate update after the earlier SET target changed.
            let error = execute_checked_upsert(
                conn,
                prepared,
                "INSERT INTO t (id, first, a) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET first = first + 1, a = a + 1",
                &[Value::Integer(1), Value::Integer(99), Value::Integer(0)],
            ).unwrap_err();
            assert!(matches!(error, SqlError::IntegerOverflow), "got {error:?}");
            assert_eq!(query(conn, "SELECT * FROM t").rows, before);
        },
    );
}

#[test]
fn upsert_checked_generated_index_check_and_trigger_parity() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2 + 1) STORED, CHECK (g < 10))",
            "CREATE INDEX t_g ON t (g)",
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, old_g INTEGER, new_g INTEGER)",
            "CREATE TRIGGER audit_update AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.a, OLD.g, NEW.g); END",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            assert_eq!(execute_checked_upsert(
                conn,
                prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1",
                &[Value::Integer(1), Value::Integer(0)],
            ).unwrap(), 1);
            assert_eq!(query(conn, "SELECT id FROM t WHERE g = 5").rows, vec![vec![Value::Integer(1)]]);
            assert!(query(conn, "SELECT id FROM t WHERE g = 3").rows.is_empty());
            let error = execute_checked_upsert(
                conn,
                prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 10",
                &[Value::Integer(1), Value::Integer(0)],
            ).unwrap_err();
            assert!(matches!(error, SqlError::CheckViolation(_)), "got {error:?}");
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(2), Value::Integer(5)]]);
            assert_eq!(query(conn, "SELECT * FROM audit").rows, vec![vec![Value::Integer(2), Value::Integer(3), Value::Integer(5)]]);
        },
    );
}

#[test]
fn upsert_checked_missing_default_is_distinct_from_stored_null() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
            "INSERT INTO t VALUES (1, 1), (3, 3)",
            "ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 10",
            "ALTER TABLE t ADD COLUMN tail INTEGER DEFAULT 70",
            "INSERT INTO t VALUES (2, 2, NULL, 99)",
        ],
        |conn, prepared| {
            for id in 1..=3 {
                assert_eq!(execute_checked_upsert(
                    conn,
                    prepared,
                    "INSERT INTO t VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET v = v + 1",
                    &[Value::Integer(id), Value::Integer(99), Value::Integer(0), Value::Integer(0)],
                ).unwrap(), 1);
            }
            assert_eq!(
                query(conn, "SELECT * FROM t ORDER BY id").rows,
                vec![
                    vec![
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(11),
                        Value::Integer(70)
                    ],
                    vec![
                        Value::Integer(2),
                        Value::Integer(2),
                        Value::Null,
                        Value::Integer(99)
                    ],
                    vec![
                        Value::Integer(3),
                        Value::Integer(3),
                        Value::Integer(11),
                        Value::Integer(70)
                    ],
                ]
            );
        },
    );
}

#[test]
fn upsert_checked_intervening_defaults_cross_bitmap_boundary() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER, f INTEGER, g INTEGER)",
            "INSERT INTO t VALUES (1, 1, NULL, 3, 4, 5, 6, 7)",
            "ALTER TABLE t ADD COLUMN middle INTEGER DEFAULT 10",
            "ALTER TABLE t ADD COLUMN last INTEGER DEFAULT 20",
        ],
        |conn, prepared| {
            assert_eq!(execute_checked_upsert(
                conn,
                prepared,
                "INSERT INTO t (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET last = last + 1",
                &[Value::Integer(1)],
            ).unwrap(), 1);
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![
                Value::Integer(1), Value::Integer(1), Value::Null, Value::Integer(3),
                Value::Integer(4), Value::Integer(5), Value::Integer(6), Value::Integer(7),
                Value::Integer(10), Value::Integer(21),
            ]]);
        },
    );
}

#[test]
fn upsert_checked_virtual_generated_returning_and_trigger_use_new_value() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2 + 1) VIRTUAL)",
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, old_g INTEGER, new_g INTEGER)",
            "CREATE TRIGGER audit_update AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.a, OLD.g, NEW.g); END",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            let sql = "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1 RETURNING a, g";
            let params = [Value::Integer(1), Value::Integer(10)];
            let returned = if prepared {
                conn.prepare(sql).unwrap().query_collect(&params).unwrap()
            } else {
                conn.query_params(sql, &params).unwrap()
            };
            assert_eq!(returned.rows, vec![vec![Value::Integer(2), Value::Integer(5)]]);
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(2), Value::Integer(5)]]);
            assert_eq!(query(conn, "SELECT * FROM audit").rows, vec![vec![Value::Integer(2), Value::Integer(3), Value::Integer(5)]]);
        },
    );
}

#[test]
fn upsert_checked_virtual_generated_constraints_reject_invalid_new_values() {
    for not_null in [true, false] {
        let create = if not_null {
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2) VIRTUAL)"
        } else {
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL, CHECK (g > 0))"
        };
        for_checked_upsert_modes(
            &[create, "INSERT INTO t (id, a) VALUES (1, 1)"],
            |conn, prepared| {
                // Both the old row and proposed INSERT have valid generated values.
                // Only the accepted conflict assignment violates the virtual constraint.
                let invalid = if not_null {
                    Value::Null
                } else {
                    Value::Integer(0)
                };
                let error = execute_checked_upsert(
                    conn,
                    prepared,
                    "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = $3",
                    &[Value::Integer(1), Value::Integer(3), invalid],
                )
                .unwrap_err();
                if not_null {
                    assert!(
                        matches!(error, SqlError::NotNullViolation(ref column) if column == "g"),
                        "got {error:?}"
                    );
                } else {
                    assert!(
                        matches!(error, SqlError::CheckViolation(_)),
                        "got {error:?}"
                    );
                }
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(2)
                    ]]
                );
            },
        );
    }
}

#[test]
fn insert_virtual_generated_constraints_match_autocommit_and_prepared_transaction() {
    for not_null in [true, false] {
        for explicit_prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = create_db(dir.path());
            let conn = Connection::open(&db).unwrap();
            let create = if not_null {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, g INTEGER NOT NULL GENERATED ALWAYS AS (a * 2) VIRTUAL)"
            } else {
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL, CHECK (g > 0))"
            };
            conn.execute(create).unwrap();
            if explicit_prepared {
                conn.execute("BEGIN").unwrap();
            }
            let sql = "INSERT INTO t (id, a) VALUES ($1, $2)";
            let prepared = explicit_prepared.then(|| conn.prepare(sql).unwrap());
            let execute = |params: &[Value]| match &prepared {
                Some(stmt) => stmt.execute(params),
                None => conn.execute_params(sql, params).map(rows_affected),
            };
            assert_eq!(execute(&[Value::Integer(1), Value::Integer(1)]).unwrap(), 1);
            let invalid = if not_null {
                Value::Null
            } else {
                Value::Integer(0)
            };
            let error = execute(&[Value::Integer(2), invalid]).unwrap_err();
            if not_null {
                assert!(
                    matches!(error, SqlError::NotNullViolation(ref column) if column == "g"),
                    "got {error:?}"
                );
            } else {
                assert!(
                    matches!(error, SqlError::CheckViolation(_)),
                    "got {error:?}"
                );
            }
            if explicit_prepared {
                // The rejected row performs no write, so the earlier valid INSERT
                // remains committable within the same prepared-statement session.
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                query(&conn, "SELECT * FROM t").rows,
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Integer(2)
                ]]
            );
        }
    }
}

#[test]
fn insert_unused_virtual_error_stays_deferred_with_other_constraints() {
    for extra in [
        "CHECK (a > 0)",
        "safe INTEGER NOT NULL GENERATED ALWAYS AS (a + 0) VIRTUAL",
    ] {
        let create = format!("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, bad INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL, {extra})");
        for_checked_upsert_modes(&[&create], |conn, prepared| {
            assert_eq!(
                execute_checked_upsert(
                    conn,
                    prepared,
                    "INSERT INTO t (id, a) VALUES ($1, $2)",
                    &[Value::Integer(1), Value::Integer(i64::MAX)],
                )
                .unwrap(),
                1
            );
            assert_eq!(
                query(conn, "SELECT COUNT(*) FROM t").rows,
                vec![vec![Value::Integer(1)]]
            );
            assert!(matches!(
                conn.query("SELECT bad FROM t"),
                Err(SqlError::IntegerOverflow)
            ));
        });
    }
}

#[test]
fn insert_virtual_new_value_reaches_before_after_triggers_and_returning() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
            "CREATE TABLE audit (id INTEGER, stage TEXT, g INTEGER, PRIMARY KEY (id, stage))",
            "CREATE TRIGGER audit_before BEFORE INSERT ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, 'before', NEW.g); END",
            "CREATE TRIGGER audit_after AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, 'after', NEW.g); END",
        ],
        |conn, prepared| {
            let sql = "INSERT INTO t (id, a) VALUES ($1, $2) RETURNING g";
            let params = [Value::Integer(1), Value::Integer(3)];
            let returned = if prepared {
                conn.prepare(sql).unwrap().query_collect(&params).unwrap()
            } else {
                conn.query_params(sql, &params).unwrap()
            };
            assert_eq!(returned.rows, vec![vec![Value::Integer(6)]]);
            // Also exercise trigger references when RETURNING is absent.
            assert_eq!(execute_checked_upsert(conn, prepared, "INSERT INTO t (id, a) VALUES ($1, $2)", &[Value::Integer(2), Value::Integer(4)]).unwrap(), 1);
            assert_eq!(query(conn, "SELECT * FROM audit ORDER BY id, stage").rows, vec![
                vec![Value::Integer(1), Value::Text("after".into()), Value::Integer(6)],
                vec![Value::Integer(1), Value::Text("before".into()), Value::Integer(6)],
                vec![Value::Integer(2), Value::Text("after".into()), Value::Integer(8)],
                vec![Value::Integer(2), Value::Text("before".into()), Value::Integer(8)],
            ]);
            assert_eq!(query(conn, "SELECT * FROM t ORDER BY id").rows, vec![
                vec![Value::Integer(1), Value::Integer(3), Value::Integer(6)],
                vec![Value::Integer(2), Value::Integer(4), Value::Integer(8)],
            ]);
        },
    );
}

#[test]
fn insert_virtual_returning_without_triggers_uses_logical_value() {
    for_checked_upsert_modes(
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)"],
        |conn, prepared| {
            let sql = "INSERT INTO t (id, a) VALUES ($1, $2) RETURNING a, g";
            let params = [Value::Integer(1), Value::Integer(3)];
            let returned = if prepared {
                conn.prepare(sql).unwrap().query_collect(&params).unwrap()
            } else {
                conn.query_params(sql, &params).unwrap()
            };
            assert_eq!(returned.rows, vec![vec![Value::Integer(3), Value::Integer(6)]]);
            assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(3), Value::Integer(6)]]);
        },
    );
}

#[test]
fn upsert_checked_returning_virtual_evaluates_only_accepted_new_row() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL)",
            "INSERT INTO t (id, a) VALUES (1, 1)",
        ],
        |conn, prepared| {
            for skip in [false, true] {
                let predicate = if skip { " WHERE FALSE" } else { "" };
                let sql = format!("INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1{predicate} RETURNING g");
                let params = [Value::Integer(1), Value::Integer(i64::MAX)];
                let returned = if prepared {
                    conn.prepare(&sql).unwrap().query_collect(&params).unwrap()
                } else {
                    conn.query_params(&sql, &params).unwrap()
                };
                if skip {
                    assert!(returned.rows.is_empty());
                } else {
                    assert_eq!(returned.rows, vec![vec![Value::Integer(3)]]);
                }
                assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]]);
            }
        },
    );
}

#[test]
fn upsert_checked_after_insert_virtual_trigger_runs_only_for_insert_outcome() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL)",
            "INSERT INTO t (id, a) VALUES (1, 1)",
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, g INTEGER)",
            "CREATE TRIGGER audit_insert AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, NEW.g); END",
        ],
        |conn, prepared| {
            for skip in [false, true] {
                let predicate = if skip { " WHERE FALSE" } else { "" };
                let sql = format!("INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1{predicate}");
                assert_eq!(execute_checked_upsert(conn, prepared, &sql, &[Value::Integer(1), Value::Integer(i64::MAX)]).unwrap(), if skip { 0 } else { 1 });
                assert!(query(conn, "SELECT * FROM audit").rows.is_empty());
                assert_eq!(query(conn, "SELECT * FROM t").rows, vec![vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]]);
            }
            assert_eq!(execute_checked_upsert(
                conn,
                prepared,
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET a = a + 1",
                &[Value::Integer(2), Value::Integer(3)],
            ).unwrap(), 1);
            assert_eq!(query(conn, "SELECT * FROM audit").rows, vec![vec![Value::Integer(2), Value::Integer(4)]]);
            assert_eq!(query(conn, "SELECT * FROM t ORDER BY id").rows, vec![
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                vec![Value::Integer(2), Value::Integer(3), Value::Integer(4)],
            ]);
        },
    );
}

#[test]
fn insert_returning_old_or_base_columns_does_not_evaluate_unused_virtual() {
    for_checked_upsert_modes(
        &["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL)"],
        |conn, prepared| {
            for (id, projection, expected) in [
                (1, "old.g", vec![Value::Null]),
                (2, "old.*", vec![Value::Null, Value::Null, Value::Null]),
                (3, "id", vec![Value::Integer(3)]),
            ] {
                let sql = format!("INSERT INTO t (id, a) VALUES ($1, $2) RETURNING {projection}");
                let params = [Value::Integer(id), Value::Integer(i64::MAX)];
                let returned = if prepared {
                    conn.prepare(&sql).unwrap().query_collect(&params).unwrap()
                } else {
                    conn.query_params(&sql, &params).unwrap()
                };
                assert_eq!(returned.rows, vec![expected]);
                assert_eq!(query(conn, "SELECT COUNT(*) FROM t").rows, vec![vec![Value::Integer(id)]]);
            }
            assert!(matches!(conn.query("SELECT g FROM t"), Err(SqlError::IntegerOverflow)));
        },
    );
}

#[test]
fn insert_late_virtual_returning_or_trigger_error_cannot_publish_rows() {
    for after_trigger in [false, true] {
        for prepared in [true, false] {
            for explicit in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = create_db(dir.path());
                let conn = Connection::open(&db).unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL)").unwrap();
                conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, g INTEGER)")
                    .unwrap();
                if after_trigger {
                    conn.execute("CREATE TRIGGER audit_insert AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, NEW.g); END").unwrap();
                }
                if explicit {
                    conn.execute("BEGIN").unwrap();
                }
                let returning = if after_trigger { "" } else { " RETURNING g" };
                let sql = format!("INSERT INTO t (id, a) VALUES (1, 1), (2, $1){returning}");
                let params = [Value::Integer(i64::MAX)];
                let result = if prepared {
                    conn.prepare(&sql).unwrap().query_collect(&params)
                } else {
                    conn.query_params(&sql, &params)
                };
                assert!(
                    matches!(result, Err(SqlError::IntegerOverflow)),
                    "got {result:?}"
                );
                if explicit {
                    assert!(matches!(
                        conn.execute("COMMIT"),
                        Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                    ));
                }
                // The valid first row and any AFTER INSERT audit prefix must be
                // rolled back together with the row whose late consumer failed.
                assert_eq!(
                    query(&conn, "SELECT COUNT(*) FROM t").rows,
                    vec![vec![Value::Integer(0)]]
                );
                assert!(query(&conn, "SELECT * FROM audit").rows.is_empty());
            }
        }
    }
}

#[test]
fn upsert_checked_parameters_only_in_conflict_set_and_where_are_bound() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
            "INSERT INTO t VALUES (1, 10)",
        ],
        |conn, prepared| {
            // INSERT has no parameters. Both conflict expressions must contribute
            // to the statement's parameter count and receive the current bindings.
            let sql = "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET v = v + $1 WHERE v = $2";
            let statement = prepared.then(|| conn.prepare(sql).unwrap());
            if let Some(statement) = &statement {
                assert_eq!(statement.param_count(), 2);
            }
            for (increment, expected_old, count, expected_new) in [
                (5, 0, 0, 10),
                (5, 10, 1, 15),
                (100, 10, 0, 15),
                (2, 15, 1, 17),
            ] {
                let params = [Value::Integer(increment), Value::Integer(expected_old)];
                let affected = if let Some(statement) = &statement {
                    statement.execute(&params).unwrap()
                } else {
                    rows_affected(conn.execute_params(sql, &params).unwrap())
                };
                assert_eq!(affected, count);
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![Value::Integer(1), Value::Integer(expected_new)]]
                );
            }
        },
    );
}

#[test]
fn upsert_checked_missing_defaults_preserve_generic_error_priority() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE plain (id INTEGER PRIMARY KEY, a INTEGER)",
            "CREATE TABLE indexed (id INTEGER PRIMARY KEY, a INTEGER)",
            "CREATE INDEX indexed_a ON indexed (a)",
            "INSERT INTO plain VALUES (1, 7)",
            "INSERT INTO indexed VALUES (1, 7)",
            "ALTER TABLE plain ADD COLUMN nullv INTEGER NOT NULL DEFAULT NULL",
            "ALTER TABLE indexed ADD COLUMN nullv INTEGER NOT NULL DEFAULT NULL",
            "ALTER TABLE plain ADD COLUMN maxv INTEGER DEFAULT 9223372036854775807",
            "ALTER TABLE indexed ADD COLUMN maxv INTEGER DEFAULT 9223372036854775807",
        ],
        |conn, prepared| {
            for table in ["plain", "indexed"] {
                for overflow in [true, false] {
                    let assignments = if overflow {
                        "nullv = nullv + 1, maxv = maxv + 1"
                    } else {
                        "nullv = nullv + 1"
                    };
                    let error = execute_checked_upsert(
                        conn,
                        prepared,
                        &format!("INSERT INTO {table} VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET {assignments}"),
                        &[Value::Integer(1), Value::Integer(99), Value::Integer(0), Value::Integer(0)],
                    ).unwrap_err();
                    // Generic UPSERT evaluates every RHS before checking assigned
                    // NOT NULL constraints, so the later arithmetic error wins.
                    if overflow {
                        assert!(
                            matches!(error, SqlError::IntegerOverflow),
                            "{table}: {error:?}"
                        );
                    } else {
                        assert!(
                            matches!(error, SqlError::NotNullViolation(ref column) if column == "nullv"),
                            "{table}: {error:?}"
                        );
                    }
                    assert_eq!(
                        query(conn, &format!("SELECT * FROM {table}")).rows,
                        vec![vec![
                            Value::Integer(1),
                            Value::Integer(7),
                            Value::Null,
                            Value::Integer(i64::MAX)
                        ]]
                    );
                }
            }
        },
    );
}

#[test]
fn upsert_checked_cannot_assign_stored_or_virtual_generated_columns() {
    for kind in ["STORED", "VIRTUAL"] {
        let create = format!("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2 + 1) {kind})");
        for_checked_upsert_modes(
            &[&create, "INSERT INTO t (id, a) VALUES (1, 1)"],
            |conn, prepared| {
                assert!(matches!(
                    conn.execute("UPDATE t SET g = g + 1 WHERE id = 1"),
                    Err(SqlError::CannotUpdateGeneratedColumn(_))
                ));
                let sql = "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET g = g + 1";
                let params = [Value::Integer(1), Value::Integer(10)];
                let result = if prepared {
                    conn.prepare(sql).and_then(|stmt| stmt.execute(&params))
                } else {
                    conn.execute_params(sql, &params).map(rows_affected)
                };
                assert!(
                    matches!(result, Err(SqlError::CannotUpdateGeneratedColumn(_))),
                    "got {result:?}"
                );
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Integer(1),
                        Value::Integer(3)
                    ]]
                );
            },
        );
    }
}

#[test]
fn upsert_checked_empty_table_rejects_generated_conflict_assignment() {
    for kind in ["STORED", "VIRTUAL"] {
        let create = format!("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, g INTEGER GENERATED ALWAYS AS (a * 2 + 1) {kind})");
        for_checked_upsert_modes(&[&create], |conn, prepared| {
            let sql =
                "INSERT INTO t (id, a) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET g = g + 1";
            let params = [Value::Integer(1), Value::Integer(10)];
            let result = if prepared {
                conn.prepare(sql).and_then(|stmt| stmt.execute(&params))
            } else {
                conn.execute_params(sql, &params).map(rows_affected)
            };
            assert!(
                matches!(result, Err(SqlError::CannotUpdateGeneratedColumn(_))),
                "the invalid conflict clause must be rejected before the new row is inserted: {result:?}"
            );
            assert!(query(conn, "SELECT * FROM t").rows.is_empty());
        });
    }
}

#[test]
fn upsert_checked_missing_volatile_default_keeps_old_new_capture_consistent() {
    for_checked_upsert_modes(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1), (2)",
            "ALTER TABLE t ADD COLUMN v INTEGER DEFAULT (RANDOM() % 1000000)",
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, old_v INTEGER, new_v INTEGER)",
            "CREATE TRIGGER audit_update AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id, OLD.v, NEW.v); END",
        ],
        |conn, prepared| {
            let sql = "INSERT INTO t (id, v) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = v + 1 RETURNING v";
            for id in 1..=2 {
                let params = [Value::Integer(id), Value::Integer(0)];
                let returned = if prepared {
                    conn.prepare(sql).unwrap().query_collect(&params).unwrap()
                } else {
                    conn.query_params(sql, &params).unwrap()
                };
                let audit = query(conn, &format!("SELECT old_v, new_v FROM audit WHERE id = {id}"));
                let Value::Integer(old) = audit.rows[0][0] else {
                    panic!("missing default was not captured as an integer");
                };
                // This relation holds for every random draw. Bounding the default
                // excludes arithmetic overflow; independent draws need not differ.
                assert!((-999_999..=999_999).contains(&old));
                assert_eq!(audit.rows, vec![vec![Value::Integer(old), Value::Integer(old + 1)]]);
                assert_eq!(returned.rows, vec![vec![Value::Integer(old + 1)]]);
                assert_eq!(query(conn, &format!("SELECT * FROM t WHERE id = {id}")).rows, vec![vec![Value::Integer(id), Value::Integer(old + 1)]]);
            }
        },
    );
}

#[test]
fn upsert_checked_repeated_targets_read_old_values_and_keep_earlier_errors() {
    for (initial, assignments, expected) in [
        (5, "v = v + 1, v = v + 2", Some(7)),
        (i64::MAX, "v = v + 1, v = v - 1", None),
    ] {
        let insert = format!("INSERT INTO t VALUES (1, {initial})");
        for_checked_upsert_modes(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
                &insert,
            ],
            |conn, prepared| {
                let result = execute_checked_upsert(
                    conn,
                    prepared,
                    &format!("INSERT INTO t VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET {assignments}"),
                    &[Value::Integer(1), Value::Integer(0)],
                );
                if expected.is_some() {
                    assert_eq!(result.unwrap(), 1);
                } else {
                    assert!(
                        matches!(result, Err(SqlError::IntegerOverflow)),
                        "got {result:?}"
                    );
                }
                assert_eq!(
                    query(conn, "SELECT * FROM t").rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Integer(expected.unwrap_or(initial))
                    ]]
                );
            },
        );
    }
}

#[test]
fn upsert_checked_cross_type_missing_defaults_match_indexed_semantics() {
    for (default, expected) in [("7.5", Some(8)), ("-0.5", Some(0)), ("'7'", None)] {
        let plain_default = format!("ALTER TABLE plain ADD COLUMN v INTEGER DEFAULT {default}");
        let indexed_default = format!("ALTER TABLE indexed ADD COLUMN v INTEGER DEFAULT {default}");
        for_checked_upsert_modes(
            &[
                "CREATE TABLE plain (id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE TABLE indexed (id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE INDEX indexed_a ON indexed (a)",
                "INSERT INTO plain VALUES (1, 5)",
                "INSERT INTO indexed VALUES (1, 5)",
                &plain_default,
                &indexed_default,
                "ALTER TABLE plain ADD COLUMN tail TEXT DEFAULT 'keep'",
                "ALTER TABLE indexed ADD COLUMN tail TEXT DEFAULT 'keep'",
            ],
            |conn, prepared| {
                let mut observed = Vec::new();
                for table in ["plain", "indexed"] {
                    let result = execute_checked_upsert(
                        conn,
                        prepared,
                        &format!("INSERT INTO {table} VALUES ($1, $2, $3, 'proposed') ON CONFLICT (id) DO UPDATE SET v = v + 1"),
                        &[Value::Integer(1), Value::Integer(99), Value::Integer(0)],
                    );
                    let value = if let Some(expected) = expected {
                        assert_eq!(result.unwrap(), 1);
                        Value::Integer(expected)
                    } else {
                        // A numeric-looking TEXT default is accepted by ALTER,
                        // but the generic arithmetic evaluator rejects TEXT + INTEGER.
                        assert!(
                            matches!(result, Err(SqlError::TypeMismatch { .. })),
                            "got {result:?}"
                        );
                        Value::Text("7".into())
                    };
                    let rows = query(conn, &format!("SELECT * FROM {table}")).rows;
                    assert_eq!(
                        rows,
                        vec![vec![
                            Value::Integer(1),
                            Value::Integer(5),
                            value,
                            Value::Text("keep".into())
                        ]]
                    );
                    if expected.is_some() {
                        assert!(matches!(rows[0][2], Value::Integer(_)));
                    }
                    observed.push(rows);
                }
                assert_eq!(observed[0], observed[1]);
            },
        );
    }
}

#[test]
fn do_nothing_pk_conflict_skips_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'new') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("original".into()));
}

#[test]
fn do_nothing_new_row_inserts_normally() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'hello') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn do_nothing_no_target_works_on_any_unique_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected_pk = rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'b@x') ON CONFLICT DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected_pk, 0);

    let affected_email = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'a@x') ON CONFLICT DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected_email, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_nothing_on_unique_index_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'a@x') ON CONFLICT (email) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_nothing_multi_row_values_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 'skip'), (2, 'b'), (3, 'c') ON CONFLICT (id) DO NOTHING",
        )
        .unwrap(),
    );
    assert_eq!(affected, 2);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn do_nothing_insert_select_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    conn.execute("INSERT INTO dst VALUES (1, 'existing'), (2, 'existing')")
        .unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO dst SELECT id, v FROM src ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM dst WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("existing".into()));
}

#[test]
fn do_nothing_null_in_unique_column_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    let affected = rows_affected(
        conn.execute("INSERT INTO t VALUES (2, NULL) ON CONFLICT (email) DO NOTHING")
            .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn do_nothing_inside_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn.execute("BEGIN").unwrap();
    rows_affected(
        conn.execute("INSERT INTO t VALUES (1, 'skip') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    rows_affected(
        conn.execute("INSERT INTO t VALUES (2, 'b') ON CONFLICT (id) DO NOTHING")
            .unwrap(),
    );
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn do_nothing_inside_savepoint_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    conn.execute("SAVEPOINT s1").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    conn.execute("ROLLBACK TO s1").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn do_update_pk_conflict_sets_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT (id) DO UPDATE SET v = 'new'",
        )
        .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("new".into()));
}

#[test]
fn do_update_pk_conflict_increment_counter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE counters (k TEXT PRIMARY KEY, c INTEGER)")
        .unwrap();

    for _ in 0..5 {
        conn.execute(
            "INSERT INTO counters VALUES ('hits', 1) \
             ON CONFLICT (k) DO UPDATE SET c = c + 1",
        )
        .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM counters WHERE k = 'hits'");
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn do_update_uses_excluded_column_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'proposed') ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("proposed".into()));
}

#[test]
fn do_update_mixed_existing_and_excluded_in_expr() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    conn.execute("INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET c = c + excluded.c")
        .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(15));
}

#[test]
fn do_update_unqualified_col_refers_to_existing_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100, 200)").unwrap();

    conn.execute("INSERT INTO t VALUES (1, 1, 2) ON CONFLICT (id) DO UPDATE SET a = b")
        .unwrap();

    let qr = query(&conn, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(200));
    assert_eq!(qr.rows[0][1], Value::Integer(200));
}

#[test]
fn do_update_where_true_fires() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = 99 WHERE c < 10",
        )
        .unwrap(),
    );
    assert_eq!(affected, 1);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(99));
}

#[test]
fn do_update_where_false_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 50)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = 999 WHERE c < 10",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(50));
}

#[test]
fn do_update_where_null_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, flag INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5, NULL)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 0, NULL) \
             ON CONFLICT (id) DO UPDATE SET c = 100 WHERE flag = 1",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);
}

#[test]
fn do_update_on_unique_index_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x', 0)").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (99, 'a@x', 1) \
         ON CONFLICT (email) DO UPDATE SET hits = hits + 1",
    )
    .unwrap();

    let qr = query(&conn, "SELECT id, hits FROM t WHERE email = 'a@x'");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    let count = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count.rows[0][0], Value::Integer(1));
}

#[test]
fn do_update_rejects_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = NULL")
        .expect_err("NOT NULL should fire after DO UPDATE");
    assert!(matches!(err, SqlError::NotNullViolation(_)));
}

#[test]
fn do_update_rejects_check_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = -1")
        .expect_err("CHECK should fire after DO UPDATE");
    assert!(matches!(err, SqlError::CheckViolation(_)));
}

#[test]
fn do_update_rejects_fk_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = conn
        .execute("INSERT INTO child VALUES (10, 1) ON CONFLICT (id) DO UPDATE SET pid = 999")
        .expect_err("FK should fire after DO UPDATE");
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn do_update_rejects_conflicting_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (2, 'ignored') ON CONFLICT (id) DO UPDATE SET email = 'a@x'")
        .expect_err("updated email collides with existing unique value");
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn do_update_case_insensitive_column_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (ID INTEGER PRIMARY KEY, V TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT (id) DO UPDATE SET v = Excluded.V",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Text("ignored".into()));
}

#[test]
fn do_update_excluded_not_found_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET v = excluded.nope")
        .expect_err("excluded.nope does not exist");
    assert!(matches!(err, SqlError::ColumnNotFound(_)));
}

#[test]
fn do_update_error_rolls_back_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();

    let _ = conn
        .execute(
            "INSERT INTO t VALUES (2, 10), (1, 0) \
             ON CONFLICT (id) DO UPDATE SET c = -1",
        )
        .expect_err("second row's CHECK fires and aborts");

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(5));
}

#[test]
fn on_constraint_named_unique_index_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX t_email_idx ON t (email)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (2, 'a@x') ON CONFLICT ON CONSTRAINT t_email_idx DO NOTHING",
        )
        .unwrap(),
    );
    assert_eq!(affected, 0);

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn on_constraint_rejects_unknown_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT ON CONSTRAINT missing_idx DO NOTHING")
        .expect_err("unknown constraint should error");
    assert!(matches!(err, SqlError::Plan(_)));
}

#[test]
fn multi_row_values_second_row_conflicts_with_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 10), (1, 20) \
         ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
    )
    .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn insert_select_on_conflict_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (k TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE dst (k TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES ('a', 10), ('b', 20)")
        .unwrap();
    conn.execute("INSERT INTO dst VALUES ('a', 1)").unwrap();

    conn.execute(
        "INSERT INTO dst SELECT k, v FROM src \
         ON CONFLICT (k) DO UPDATE SET v = excluded.v",
    )
    .unwrap();

    let qr = query(&conn, "SELECT v FROM dst WHERE k = 'a'");
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    let qr = query(&conn, "SELECT v FROM dst WHERE k = 'b'");
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn rejects_on_conflict_without_target_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT DO UPDATE SET v = 'b'")
        .expect_err("DO UPDATE without target should error");
    assert!(matches!(err, SqlError::Plan(_)));
}

#[test]
fn rejects_conflict_target_not_matching_any_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (1, 'a') ON CONFLICT (name) DO NOTHING")
        .expect_err("should reject target without matching unique constraint");
    match err {
        SqlError::Plan(msg) => assert!(msg.contains("does not match any unique constraint")),
        other => panic!("expected Plan error, got {other:?}"),
    }
}

#[test]
fn prepared_upsert_reused_across_calls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();

    let stmt = conn
        .prepare(
            "INSERT INTO t VALUES ($1, 1) \
             ON CONFLICT (id) DO UPDATE SET c = c + 1",
        )
        .unwrap();

    for _ in 0..3 {
        stmt.execute(&[Value::Integer(1)]).unwrap();
    }
    stmt.execute(&[Value::Integer(2)]).unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    let qr = query(&conn, "SELECT c FROM t WHERE id = 2");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn prepared_upsert_excluded_with_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = conn
        .prepare(
            "INSERT INTO t VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
        )
        .unwrap();

    stmt.execute(&[Value::Integer(1), Value::Integer(5)])
        .unwrap();
    stmt.execute(&[Value::Integer(1), Value::Integer(3)])
        .unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(18));
}

#[test]
fn prepared_upsert_do_nothing_with_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, 'x') ON CONFLICT (id) DO NOTHING")
        .unwrap();

    for i in 0..5 {
        stmt.execute(&[Value::Integer(i % 3)]).unwrap();
    }

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn multi_row_values_mixed_new_and_conflict_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    let affected = rows_affected(
        conn.execute(
            "INSERT INTO t VALUES (1, 5), (2, 20), (3, 30) \
             ON CONFLICT (id) DO UPDATE SET c = c + excluded.c",
        )
        .unwrap(),
    );
    assert_eq!(affected, 3);

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(105));
    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn do_update_counter_fast_path_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE ct (k TEXT PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO ct VALUES ('hot', 0)").unwrap();

    for _ in 0..200 {
        conn.execute("INSERT INTO ct VALUES ('hot', 1) ON CONFLICT (k) DO UPDATE SET c = c + 1")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM ct WHERE k = 'hot'");
    assert_eq!(qr.rows[0][0], Value::Integer(200));
}

#[test]
fn do_update_counter_crosses_varint_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 126)").unwrap();

    for _ in 0..10 {
        conn.execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = c + 1")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(136));
}

#[test]
fn do_update_counter_subtract_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    for _ in 0..30 {
        conn.execute("INSERT INTO t VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET c = c - 2")
            .unwrap();
    }

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows[0][0], Value::Integer(40));
}

#[test]
fn multi_row_error_mid_batch_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER CHECK (c >= 0))")
        .unwrap();

    let _ = conn
        .execute(
            "INSERT INTO t VALUES (1, 10), (2, -1), (3, 30) \
             ON CONFLICT (id) DO NOTHING",
        )
        .expect_err("row 2 violates CHECK");

    let qr = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn prepared_counter_upsert_in_txn_patches_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, 1) ON CONFLICT(id) DO UPDATE SET c = c + 1")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in [1, 2, 1, 1, 2] {
        assert_eq!(stmt.execute(&[Value::Integer(id)]).unwrap(), 1);
    }
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT id, c FROM t ORDER BY id");
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(2)],
        ]
    );
}

#[test]
fn counter_upsert_matches_indexed_table_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // plain = Patch-eligible; indexed = heavy lane. Results must agree.
    conn.execute("CREATE TABLE plain (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE indexed (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX indexed_c ON indexed (c)")
        .unwrap();

    for table in ["plain", "indexed"] {
        let stmt = conn
            .prepare(&format!(
                "INSERT INTO {table} VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET c = c + 5"
            ))
            .unwrap();
        conn.execute("BEGIN").unwrap();
        // Row 2 starts NULL: NULL + 5 stays NULL on both lanes.
        for (id, c) in [
            (1, Value::Integer(10)),
            (2, Value::Null),
            (1, Value::Integer(0)),
            (2, Value::Null),
        ] {
            stmt.execute(&[Value::Integer(id), c]).unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }

    let plain = query(&conn, "SELECT id, c FROM plain ORDER BY id");
    let indexed = query(&conn, "SELECT id, c FROM indexed ORDER BY id");
    assert_eq!(plain.rows, indexed.rows);
    assert_eq!(
        plain.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(15)],
            vec![Value::Integer(2), Value::Null],
        ]
    );
}

#[test]
fn counter_upsert_with_update_trigger_fires() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER NOT NULL PRIMARY KEY, tag TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (NEW.c, 'upd'); END",
    )
    .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, 1) ON CONFLICT(id) DO UPDATE SET c = c + 1")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    stmt.execute(&[Value::Integer(1)]).unwrap();
    stmt.execute(&[Value::Integer(1)]).unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT COUNT(*) FROM audit");
    assert_eq!(qr.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn counter_upsert_savepoint_rollback_restores_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, 1) ON CONFLICT(id) DO UPDATE SET c = c + 1")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    stmt.execute(&[Value::Integer(1)]).unwrap();
    conn.execute("ROLLBACK TO s").unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "SELECT c FROM t WHERE id = 1");
    assert_eq!(qr.rows, vec![vec![Value::Integer(10)]]);
}
