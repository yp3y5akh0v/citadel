use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, DataType, ExecutionResult, SqlError, Value};

const MAX_ROWS: &str = "9223372036854775807";

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"topk-scan-test")
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

fn assert_query(conn: &Connection<'_>, sql: &str, expected: &[Vec<Value>]) {
    let result = conn.query(sql).unwrap();
    assert_eq!(result.columns, ["id", "rank"], "{sql}");
    assert_eq!(result.rows, expected, "{sql}");
    let prepared = conn.prepare(sql).unwrap();
    for _ in 0..2 {
        let result = prepared.query_collect(&[]).unwrap();
        assert_eq!(result.columns, ["id", "rank"], "prepared {sql}");
        assert_eq!(result.rows, expected, "prepared {sql}");
    }
}

fn seed_small_tables(conn: &Connection<'_>) {
    for table in ["items", "empty_items"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, rank INTEGER)"
        ))
        .unwrap();
    }
    conn.execute("INSERT INTO items VALUES (1, 9), (2, 1)")
        .unwrap();
}

#[test]
fn topk_materializes_only_referenced_virtual_columns() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            rank INTEGER,
            a INTEGER,
            g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL,
            h INTEGER GENERATED ALWAYS AS (rank + 1) VIRTUAL
        )",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO items (id, rank, a) VALUES
         (1, 2, 9223372036854775807), (2, 1, 9223372036854775807)",
    )
    .unwrap();

    in_transactions(&conn, || {
        for (order, id, rank) in [("id", 1, 2), ("items.rank", 2, 1)] {
            assert_query(
                &conn,
                &format!("SELECT id, rank FROM items ORDER BY {order} LIMIT 1"),
                &[vec![Value::Integer(id), Value::Integer(rank)]],
            );
            assert_query(
                &conn,
                &format!("SELECT id, h AS rank FROM items ORDER BY {order} LIMIT 1"),
                &[vec![Value::Integer(id), Value::Integer(rank + 1)]],
            );
            let sql = format!("SELECT g FROM items ORDER BY {order} LIMIT 1");
            assert!(matches!(conn.query(&sql), Err(SqlError::IntegerOverflow)));
            assert!(matches!(
                conn.prepare(&sql).unwrap().query_collect(&[]),
                Err(SqlError::IntegerOverflow)
            ));
        }
    });
}

#[test]
fn huge_limit_uses_only_rows_present_in_the_scan() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed_small_tables(&conn);
    in_transactions(&conn, || {
        for table in ["items", "empty_items"] {
            for order in ["id ASC", "rank ASC"] {
                let expected = if table == "empty_items" {
                    vec![]
                } else if order == "id ASC" {
                    vec![
                        vec![Value::Integer(1), Value::Integer(9)],
                        vec![Value::Integer(2), Value::Integer(1)],
                    ]
                } else {
                    vec![
                        vec![Value::Integer(2), Value::Integer(1)],
                        vec![Value::Integer(1), Value::Integer(9)],
                    ]
                };
                assert_query(
                    &conn,
                    &format!("SELECT id, rank FROM {table} ORDER BY {order} LIMIT {MAX_ROWS}"),
                    &expected,
                );
            }
        }
    });
}

#[test]
fn huge_offset_and_zero_limit_do_not_reserve_requested_row_counts() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed_small_tables(&conn);
    in_transactions(&conn, || {
        for table in ["items", "empty_items"] {
            for order in ["id ASC", "rank ASC"] {
                for limit in ["0", "1", MAX_ROWS] {
                    assert_query(
                        &conn,
                        &format!(
                            "SELECT id, rank FROM {table} ORDER BY {order} \
                             LIMIT {limit} OFFSET {MAX_ROWS}"
                        ),
                        &[],
                    );
                }
            }
        }
    });
}

fn rows_by_id(rows: &mut [Vec<Value>]) {
    rows.sort_by(|left, right| left[0].cmp(&right[0]));
}

#[test]
fn added_default_sort_keys_match_decoded_rows_in_topk_scans() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (10, 'old-one'), (20, 'old-two')")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN rank INTEGER DEFAULT 7")
        .unwrap();
    conn.execute(
        "INSERT INTO items VALUES (1, 'new-one', 1), (2, 'new-five', 5), \
         (3, 'new-nine', 9), (4, 'explicit-null', NULL)",
    )
    .unwrap();

    in_transactions(&conn, || {
        for (suffix, expected) in [
            ("ASC LIMIT 3", vec![(1, Some(1)), (2, Some(5)), (4, None)]),
            (
                "ASC NULLS LAST LIMIT 4",
                vec![(1, Some(1)), (2, Some(5)), (10, Some(7)), (20, Some(7))],
            ),
            (
                "DESC NULLS LAST LIMIT 3",
                vec![(3, Some(9)), (10, Some(7)), (20, Some(7))],
            ),
            (
                "ASC NULLS LAST LIMIT 2 OFFSET 2",
                vec![(10, Some(7)), (20, Some(7))],
            ),
        ] {
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(id, rank)| {
                    vec![Value::Integer(id), rank.map_or(Value::Null, Value::Integer)]
                })
                .collect();
            let generic = format!("SELECT id, rank FROM items ORDER BY rank + 0 {suffix}");
            let mut reference = conn.query(&generic).unwrap().rows;
            rows_by_id(&mut reference);
            assert_eq!(reference, expected, "generic {suffix}");

            let raw = format!("SELECT id, rank FROM items ORDER BY rank {suffix}");
            let mut actual = conn.query(&raw).unwrap().rows;
            rows_by_id(&mut actual);
            assert_eq!(actual, expected, "raw {suffix}");
            let prepared = conn.prepare(&raw).unwrap();
            for _ in 0..2 {
                let mut actual = prepared.query_collect(&[]).unwrap().rows;
                rows_by_id(&mut actual);
                assert_eq!(actual, expected, "prepared {suffix}");
            }
        }
    });
}

#[test]
fn volatile_sort_defaults_use_decoded_row_ordering() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (table, definition) in [
        ("random_items", "INTEGER DEFAULT RANDOM()"),
        ("clock_items", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
    ] {
        conn.execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"))
            .unwrap();
        conn.execute(&format!("INSERT INTO {table} VALUES (1), (2), (3), (4)"))
            .unwrap();
        conn.execute(&format!("ALTER TABLE {table} ADD COLUMN rank {definition}"))
            .unwrap();
    }

    in_transactions(&conn, || {
        for (table, expected_type) in [
            ("random_items", DataType::Integer),
            ("clock_items", DataType::Timestamp),
        ] {
            for (order, descending) in [("rank ASC", false), ("2 DESC", true)] {
                let sql = format!("SELECT id, rank FROM {table} ORDER BY {order} LIMIT 3");
                let ExecutionResult::Query(plan) = conn.execute(&format!("EXPLAIN {sql}")).unwrap()
                else {
                    panic!("EXPLAIN did not return a query result");
                };
                assert!(
                    !plan.rows.iter().flatten().any(
                        |value| matches!(value, Value::Text(text) if text.contains("TOPK SCAN"))
                    ),
                    "volatile default must be evaluated once per decoded row: {sql}, {plan:?}"
                );

                let check_sorted = |rows: &[Vec<Value>]| {
                    assert_eq!(rows.len(), 3, "{sql}");
                    for row in rows {
                        assert_eq!(row[1].data_type(), expected_type, "{sql}: {row:?}");
                    }
                    assert!(
                        rows.windows(2).all(|pair| {
                            if descending {
                                pair[0][1] >= pair[1][1]
                            } else {
                                pair[0][1] <= pair[1][1]
                            }
                        }),
                        "{sql}: {rows:?}"
                    );
                };
                check_sorted(&conn.query(&sql).unwrap().rows);
                let prepared = conn.prepare(&sql).unwrap();
                for _ in 0..2 {
                    check_sorted(&prepared.query_collect(&[]).unwrap().rows);
                }
            }
        }
    });
}

#[test]
fn filtered_topk_does_not_cache_volatile_or_failing_defaults() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (table, default) in [("volatile_items", "RANDOM()"), ("error_items", "(1 / 0)")] {
        conn.execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"))
            .unwrap();
        conn.execute(&format!("INSERT INTO {table} VALUES (1), (2), (3), (4)"))
            .unwrap();
        conn.execute(&format!(
            "ALTER TABLE {table} ADD COLUMN sample INTEGER DEFAULT {default}"
        ))
        .unwrap();
    }

    in_transactions(&conn, || {
        for predicate in ["sample >= 0", "sample BETWEEN 0 AND 9223372036854775807"] {
            for table in ["volatile_items", "error_items"] {
                let sql =
                    format!("SELECT id, sample FROM {table} WHERE {predicate} ORDER BY id LIMIT 3");
                let ExecutionResult::Query(plan) = conn.execute(&format!("EXPLAIN {sql}")).unwrap()
                else {
                    panic!("EXPLAIN did not return a query result");
                };
                assert!(
                    !plan.rows.iter().flatten().any(
                        |value| matches!(value, Value::Text(text) if text.contains("TOPK SCAN"))
                    ),
                    "unsafe predicate default: {sql}, {plan:?}"
                );
                let check = |result: citadel_sql::Result<citadel_sql::QueryResult>| {
                    if table == "error_items" {
                        assert!(
                            matches!(result, Err(citadel_sql::SqlError::DivisionByZero)),
                            "{sql}: {result:?}"
                        );
                    } else {
                        for row in result.unwrap().rows {
                            assert!(
                                matches!(row[1], Value::Integer(value) if value >= 0),
                                "{sql}: {row:?}"
                            );
                        }
                    }
                };
                check(conn.query(&sql));
                let prepared = conn.prepare(&sql).unwrap();
                check(prepared.query_collect(&[]));
            }
        }
    });
}
