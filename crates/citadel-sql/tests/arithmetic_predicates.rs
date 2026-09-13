use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"arithmetic-predicate-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn in_transactions(conn: &Connection<'_>, check: impl Fn()) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        check();
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

fn assert_rows(conn: &Connection<'_>, sql: &str, expected: &[Vec<Value>]) {
    assert_eq!(conn.query(sql).unwrap().rows, expected, "{sql}");
    let prepared = conn.prepare(sql).unwrap();
    assert_eq!(prepared.query_collect(&[]).unwrap().rows, expected, "{sql}");
    assert_eq!(
        prepared.query(&[]).unwrap().collect().unwrap().rows,
        expected,
        "streamed {sql}"
    );
}

fn assert_predicate(conn: &Connection<'_>, predicate: &str, ids: &[i64]) {
    let expected: Vec<_> = ids.iter().map(|&id| vec![Value::Integer(id)]).collect();
    in_transactions(conn, || {
        for suffix in ["", " ORDER BY id", " ORDER BY id LIMIT 10"] {
            for condition in [
                format!("COALESCE({predicate}, FALSE)"),
                predicate.to_owned(),
            ] {
                assert_rows(
                    conn,
                    &format!("SELECT id FROM items WHERE {condition}{suffix}"),
                    &expected,
                );
            }
        }
        for condition in [
            format!("COALESCE({predicate}, FALSE)"),
            predicate.to_owned(),
        ] {
            assert_rows(
                conn,
                &format!("SELECT COUNT(*) FROM items WHERE {condition}"),
                &[vec![Value::Integer(ids.len() as i64)]],
            );
        }
    });
}

#[test]
fn arithmetic_predicates_preserve_floating_point_rounding() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, x REAL)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 9007199254740992.0)")
        .unwrap();
    assert_predicate(&conn, "x + 1.0 = 9007199254740992.0", &[1]);
    assert_predicate(&conn, "9007199254740992.0 = x + 1.0", &[1]);
}

#[test]
fn arithmetic_predicates_propagate_integer_overflow() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 9223372036854775807)")
        .unwrap();
    in_transactions(&conn, || {
        for projection in ["id", "COUNT(*)"] {
            for suffix in ["", " ORDER BY 1 LIMIT 10"] {
                for predicate in ["COALESCE(x + 1 > 0, FALSE)", "x + 1 > 0", "0 < x + 1"] {
                    let sql = format!("SELECT {projection} FROM items WHERE {predicate}{suffix}");
                    assert!(
                        matches!(conn.query(&sql), Err(SqlError::IntegerOverflow)),
                        "{sql}"
                    );
                    let prepared = conn.prepare(&sql).unwrap();
                    assert!(matches!(
                        prepared.query_collect(&[]),
                        Err(SqlError::IntegerOverflow)
                    ));
                    assert!(matches!(
                        prepared.query(&[]).and_then(|rows| rows.collect()),
                        Err(SqlError::IntegerOverflow)
                    ));
                }
            }
        }
    });
}

#[test]
fn arithmetic_predicates_preserve_calendar_month_clamping() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    conn.execute(
        "INSERT INTO items VALUES (1, TIMESTAMP '2023-01-28 12:00:00'), \
         (2, TIMESTAMP '2023-01-31 12:00:00')",
    )
    .unwrap();
    assert_predicate(
        &conn,
        "ts + INTERVAL '1 month' = TIMESTAMP '2023-02-28 12:00:00'",
        &[1, 2],
    );
}

#[test]
fn arithmetic_predicates_preserve_time_wrapping_across_midnight() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, tm TIME)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, TIME '23:30:00')")
        .unwrap();
    assert_predicate(&conn, "tm + INTERVAL '1 hour' < TIME '01:00:00'", &[1]);
}

#[test]
fn filtered_grouping_preserves_missing_stored_defaults() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, DATE '2023-01-01')")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN g INTEGER DEFAULT 7")
        .unwrap();
    in_transactions(&conn, || {
        for predicate in [
            "id > 0",
            "d + INTERVAL '1 day' = TIMESTAMP '2023-01-02 00:00:00'",
        ] {
            for condition in [
                format!("COALESCE({predicate}, FALSE)"),
                predicate.to_owned(),
            ] {
                assert_rows(
                    &conn,
                    &format!("SELECT g, COUNT(*) FROM items WHERE {condition} GROUP BY g"),
                    &[vec![Value::Integer(7), Value::Integer(1)]],
                );
            }
        }
    });
}

#[test]
fn arithmetic_comparisons_match_the_original_expression_in_both_orders() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, x REAL)")
        .unwrap();
    conn.execute(
        "INSERT INTO items VALUES (1, -9007199254740992.0), (2, -1.0), \
         (3, NULL), (4, 1.0), (5, 9007199254740992.0)",
    )
    .unwrap();
    for arithmetic in ["+", "-"] {
        for comparison in ["=", "!=", "<", "<=", ">", ">="] {
            for predicate in [
                format!("x {arithmetic} 1.0 {comparison} 9007199254740992.0"),
                format!("9007199254740992.0 {comparison} x {arithmetic} 1.0"),
            ] {
                let expected = conn
                    .query(&format!(
                        "SELECT id FROM items WHERE COALESCE({predicate}, FALSE) ORDER BY id"
                    ))
                    .unwrap();
                let ids: Vec<_> = expected
                    .rows
                    .iter()
                    .map(|row| match row[0] {
                        Value::Integer(id) => id,
                        _ => panic!("noninteger fixture id"),
                    })
                    .collect();
                assert_predicate(&conn, &predicate, &ids);
            }
        }
    }
}

#[test]
fn outer_null_comparison_does_not_hide_inner_arithmetic_errors() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1, $2)").unwrap();
    for (value, arithmetic) in [(i64::MAX, "x + 1"), (i64::MIN, "x - 1")] {
        conn.execute("DELETE FROM items").unwrap();
        insert
            .execute(&[Value::Integer(1), Value::Integer(value)])
            .unwrap();
        in_transactions(&conn, || {
            for predicate in [
                format!("{arithmetic} = NULL"),
                format!("NULL = {arithmetic}"),
            ] {
                for condition in [format!("COALESCE({predicate}, FALSE)"), predicate] {
                    for projection in ["id", "COUNT(*)"] {
                        let sql = format!("SELECT {projection} FROM items WHERE {condition}");
                        assert!(
                            matches!(conn.query(&sql), Err(SqlError::IntegerOverflow)),
                            "{sql}"
                        );
                        let prepared = conn.prepare(&sql).unwrap();
                        assert!(matches!(
                            prepared.query(&[]).and_then(|rows| rows.collect()),
                            Err(SqlError::IntegerOverflow)
                        ));
                    }
                }
            }
        });
    }
}

fn assert_group_rows(conn: &Connection<'_>, sql: &str, mut expected: Vec<Vec<Value>>) {
    expected.sort();
    let prepared = conn.prepare(sql).unwrap();
    for result in [
        conn.query(sql).unwrap(),
        prepared.query_collect(&[]).unwrap(),
        prepared.query(&[]).unwrap().collect().unwrap(),
    ] {
        let mut rows = result.rows;
        rows.sort();
        assert_eq!(rows, expected, "{sql}");
    }
}

fn assert_stream_group_plan(conn: &Connection<'_>, sql: &str, expected: bool) {
    let ExecutionResult::Query(plan) = conn.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("EXPLAIN did not return rows");
    };
    assert_eq!(
        plan.rows
            .iter()
            .flatten()
            .any(|value| matches!(value, Value::Text(text) if text.contains("STREAM GROUP BY"))),
        expected,
        "{sql}: {:?}",
        plan.rows
    );
}

#[test]
fn streamed_groups_distinguish_explicit_nulls_and_missing_aggregate_defaults() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, DATE '2023-01-01'), (2, DATE '2023-01-01')")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN g INTEGER DEFAULT 7")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN v INTEGER DEFAULT 5")
        .unwrap();
    conn.execute(
        "INSERT INTO items VALUES (3, DATE '2023-01-01', NULL, NULL), (4, DATE '2023-01-01', 7, 9)",
    )
    .unwrap();
    in_transactions(&conn, || {
        for predicate in [
            "id > 0",
            "d + INTERVAL '1 day' = TIMESTAMP '2023-01-02 00:00:00'",
        ] {
            let sql = format!("SELECT g, COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM items WHERE {predicate} GROUP BY g");
            assert_stream_group_plan(&conn, &sql, true);
            let expected = vec![
                vec![
                    Value::Integer(7),
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Integer(19),
                    Value::Real(19.0 / 3.0),
                    Value::Integer(5),
                    Value::Integer(9),
                ],
                vec![
                    Value::Null,
                    Value::Integer(1),
                    Value::Integer(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
            ];
            assert_group_rows(&conn, &sql, expected.clone());
            assert_group_rows(
                &conn,
                &sql.replace(predicate, &format!("COALESCE({predicate}, FALSE)")),
                expected,
            );
        }
    });
}

#[test]
fn grouped_noninteger_primary_key_aggregates_keep_generic_execution() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, g INTEGER, d DATE)")
        .unwrap();
    conn.execute(
        "INSERT INTO items VALUES ('a', 7, DATE '2023-01-01'), ('b', 7, DATE '2023-01-01')",
    )
    .unwrap();
    in_transactions(&conn, || {
        let sql = "SELECT g, COUNT(id), MIN(id), MAX(id) FROM items WHERE d + INTERVAL '1 day' = TIMESTAMP '2023-01-02 00:00:00' GROUP BY g";
        assert_stream_group_plan(&conn, sql, false);
        assert_group_rows(
            &conn,
            sql,
            vec![vec![
                Value::Integer(7),
                Value::Integer(2),
                Value::Text("a".into()),
                Value::Text("b".into()),
            ]],
        );
    });
}

#[test]
fn streamed_groups_do_not_pre_evaluate_unsafe_or_unreferenced_defaults() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (table, default) in [("volatile_items", "RANDOM()"), ("failing_items", "(1 / 0)")] {
        conn.execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"))
            .unwrap();
        conn.execute(&format!("INSERT INTO {table} VALUES (1)"))
            .unwrap();
        conn.execute(&format!(
            "ALTER TABLE {table} ADD COLUMN g INTEGER DEFAULT {default}"
        ))
        .unwrap();
        let sql = format!("SELECT g, COUNT(*) FROM {table} WHERE id > 0 GROUP BY g");
        assert_stream_group_plan(&conn, &sql, false);
        if table == "failing_items" {
            assert!(matches!(conn.query(&sql), Err(SqlError::DivisionByZero)));
        }
        conn.execute(&format!("DELETE FROM {table}")).unwrap();
        conn.execute(&format!("INSERT INTO {table} VALUES (1, 7), (2, 7)"))
            .unwrap();
        in_transactions(&conn, || {
            assert_stream_group_plan(&conn, &sql, false);
            assert_group_rows(
                &conn,
                &sql,
                vec![vec![Value::Integer(7), Value::Integer(2)]],
            );
            let aggregate = format!("SELECT id, SUM(g) FROM {table} WHERE id > 0 GROUP BY id");
            assert_stream_group_plan(&conn, &aggregate, false);
            assert_group_rows(
                &conn,
                &aggregate,
                vec![
                    vec![Value::Integer(1), Value::Integer(7)],
                    vec![Value::Integer(2), Value::Integer(7)],
                ],
            );
        });
    }
    conn.execute("CREATE TABLE unreferenced (id INTEGER PRIMARY KEY, g INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO unreferenced VALUES (1, 7)")
        .unwrap();
    conn.execute("ALTER TABLE unreferenced ADD COLUMN unused INTEGER DEFAULT (1 / 0)")
        .unwrap();
    let sql = "SELECT g, COUNT(*) FROM unreferenced WHERE id > 0 GROUP BY g";
    assert_stream_group_plan(&conn, sql, true);
    assert_group_rows(&conn, sql, vec![vec![Value::Integer(7), Value::Integer(1)]]);
}

#[test]
fn streamed_aggregates_reject_legacy_mixed_numeric_and_interval_storage() {
    for (missing_id, stored_id) in [(1, 2), (2, 1)] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, g INTEGER, d DATE)")
            .unwrap();
        conn.execute(&format!(
            "INSERT INTO items VALUES ({missing_id}, 7, DATE '2023-01-01')"
        ))
        .unwrap();
        conn.execute("ALTER TABLE items ADD COLUMN v INTERVAL DEFAULT 1")
            .unwrap();
        // Preserve the old mixed-storage error contract explicitly. Missing
        // defaults now coerce INTEGER 1 to INTERVAL just as INSERT does; old
        // databases may still contain a physically stored INTEGER in this slot.
        let mut legacy = conn
            .query(&format!("SELECT g,d FROM items WHERE id={missing_id}"))
            .unwrap()
            .rows
            .remove(0);
        legacy.push(Value::Integer(1));
        let mut wtx = db.begin_write().unwrap();
        wtx.table_insert(
            b"items",
            &citadel_sql::encoding::encode_composite_key(&[Value::Integer(missing_id)]),
            &citadel_sql::encoding::encode_row(&legacy),
        )
        .unwrap();
        wtx.commit().unwrap();
        conn.execute(&format!(
            "INSERT INTO items VALUES ({stored_id}, 7, DATE '2023-01-01', INTERVAL '1 day')"
        ))
        .unwrap();
        in_transactions(&conn, || {
            for operation in ["SUM", "AVG"] {
                for (projection, group_by) in [
                    (format!("g, {operation}(v)"), " GROUP BY g"),
                    (format!("{operation}(v)"), ""),
                ] {
                    for filter in [
                        "",
                        " WHERE id > 0",
                        " WHERE d + INTERVAL '1 day' = TIMESTAMP '2023-01-02 00:00:00'",
                    ] {
                        let sql = format!("SELECT {projection} FROM items{filter}{group_by}");
                        let generic = format!("{sql} HAVING COUNT(*) > 0");
                        let error = conn.query(&generic).unwrap_err();
                        assert!(matches!(error, SqlError::TypeMismatch { .. }), "{generic}");
                        let expected = error.to_string();
                        assert_eq!(conn.query(&sql).unwrap_err().to_string(), expected, "{sql}");
                        let prepared = conn.prepare(&sql).unwrap();
                        assert_eq!(
                            prepared.query_collect(&[]).unwrap_err().to_string(),
                            expected,
                            "{sql}"
                        );
                        assert_eq!(
                            prepared
                                .query(&[])
                                .and_then(|rows| rows.collect())
                                .unwrap_err()
                                .to_string(),
                            expected,
                            "{sql}"
                        );
                    }
                }
            }
        });
    }
}
