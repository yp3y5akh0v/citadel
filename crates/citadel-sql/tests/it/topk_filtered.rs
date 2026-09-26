use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"topk-filtered-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn seed(conn: &Connection<'_>) {
    conn.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, bucket INTEGER, score INTEGER, name TEXT)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn
        .prepare("INSERT INTO events VALUES ($1,$2,$3,$4)")
        .unwrap();
    for id in 0..48 {
        let score = (id * 17) % 48;
        let name = format!("item-{score:03}{}", " ".repeat((id % 3) as usize));
        insert
            .execute(&[
                Value::Integer(id),
                if id == 47 {
                    Value::Null
                } else {
                    Value::Integer(if id == 0 { 2 } else { id % 5 })
                },
                if id == 0 {
                    Value::Null
                } else {
                    Value::Integer(score)
                },
                if id == 0 {
                    Value::Null
                } else {
                    Value::Text(
                        if id % 2 == 0 {
                            name.to_uppercase()
                        } else {
                            name
                        }
                        .into(),
                    )
                },
            ])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn explain(conn: &Connection<'_>, sql: &str) -> Vec<String> {
    let ExecutionResult::Query(result) = conn.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("EXPLAIN did not return a query result");
    };
    result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(line) => line.to_string(),
            other => panic!("unexpected EXPLAIN value: {other:?}"),
        })
        .collect()
}

fn assert_ordering(
    conn: &Connection<'_>,
    predicate: &str,
    order: &str,
    generic_order: &str,
    topk: bool,
) {
    let sql = |order: &str, limit: &str, offset: usize| {
        format!(
            "SELECT id, score, name FROM events WHERE {predicate} \
             ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        )
    };
    let planned_sql = sql(order, "3", 0);
    let plan = explain(conn, &planned_sql);
    assert_eq!(
        plan.iter().any(|line| line.contains("TOPK SCAN")),
        topk,
        "{planned_sql}: {plan:?}"
    );
    let generic_sql = sql(generic_order, "3", 0);
    let generic_plan = explain(conn, &generic_sql);
    assert!(
        !generic_plan.iter().any(|line| line.contains("TOPK SCAN")),
        "{generic_sql}: {generic_plan:?}"
    );
    for offset in [0, 2] {
        let actual = conn.query(&sql(order, "3", offset)).unwrap();
        let expected = conn.query(&sql(generic_order, "3", offset)).unwrap();
        assert_eq!(actual.columns, expected.columns);
        assert_eq!(
            actual.rows, expected.rows,
            "{predicate}; {order}; offset={offset}"
        );
        let prepared = conn.prepare(&sql(order, "$1", offset)).unwrap();
        let reference = conn.prepare(&sql(generic_order, "$1", offset)).unwrap();
        for limit in [1, 7, 64, 1] {
            let params = [Value::Integer(limit)];
            let actual = prepared.query_collect(&params).unwrap();
            let expected = reference.query_collect(&params).unwrap();
            assert_eq!(actual.columns, expected.columns);
            assert_eq!(
                actual.rows, expected.rows,
                "{predicate}; {order}; limit={limit}; offset={offset}"
            );
        }
    }
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

#[test]
fn filtered_numeric_topk_matches_generic_ordering() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    in_transactions(&conn, || {
        for (predicate, topk) in [
            ("bucket >= 2", true),
            ("bucket BETWEEN 1 AND 3", true),
            ("bucket NOT BETWEEN 1 AND 3", true),
            ("bucket < 0", true),
            ("bucket < -1", false),
            ("bucket + 1 >= 3", true),
        ] {
            for suffix in [
                "ASC NULLS FIRST",
                "ASC NULLS LAST",
                "DESC NULLS FIRST",
                "DESC NULLS LAST",
            ] {
                assert_ordering(
                    &conn,
                    predicate,
                    &format!("score {suffix}"),
                    &format!("score + 0 {suffix}"),
                    topk,
                );
            }
            for direction in ["ASC", "DESC"] {
                assert_ordering(
                    &conn,
                    predicate,
                    &format!("id {direction}"),
                    &format!("id + 0 {direction}"),
                    topk,
                );
            }
        }
    });
}

#[test]
fn filtered_text_topk_matches_generic_collations() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    in_transactions(&conn, || {
        for collation in ["BINARY", "NOCASE", "RTRIM"] {
            for suffix in [
                "ASC NULLS FIRST",
                "ASC NULLS LAST",
                "DESC NULLS FIRST",
                "DESC NULLS LAST",
            ] {
                assert_ordering(
                    &conn,
                    "bucket BETWEEN 1 AND 3",
                    &format!("name COLLATE {collation} {suffix}"),
                    &format!("(name || '') COLLATE {collation} {suffix}"),
                    true,
                );
            }
        }
    });
}

#[test]
fn filtered_topk_preserves_index_and_primary_key_range_plans() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE INDEX by_bucket ON events(bucket)")
        .unwrap();
    for (predicate, expected_plan) in [
        ("bucket = 2", "USING INDEX by_bucket"),
        ("id >= 3", "USING PRIMARY KEY RANGE"),
        ("id >= 3 AND id <= 30", "USING PRIMARY KEY RANGE"),
        ("id BETWEEN 3 AND 30", "USING PRIMARY KEY RANGE"),
    ] {
        let sql = format!("SELECT id FROM events WHERE {predicate} ORDER BY score DESC LIMIT 3");
        let plan = explain(&conn, &sql);
        assert!(
            plan.iter().any(|line| line.contains(expected_plan)),
            "{sql}: {plan:?}"
        );
        in_transactions(&conn, || {
            assert_ordering(
                &conn,
                predicate,
                "score DESC NULLS LAST",
                "score + 0 DESC NULLS LAST",
                false,
            );
        });
    }
    in_transactions(&conn, || {
        assert_ordering(
            &conn,
            "bucket BETWEEN 1 AND 3",
            "score DESC NULLS LAST",
            "score + 0 DESC NULLS LAST",
            true,
        );
    });
}

#[test]
fn filtered_topk_respects_write_savepoint_and_read_snapshot_visibility() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, bucket INTEGER, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO events VALUES (1,2,10),(2,2,20),(3,0,30),(4,2,40)")
        .unwrap();
    let sql = "SELECT id FROM events WHERE bucket = 2 ORDER BY score DESC LIMIT $1";
    let prepared = conn.prepare(sql).unwrap();
    let check = |expected: &[i64]| {
        let result = prepared.query_collect(&[Value::Integer(3)]).unwrap();
        assert_eq!(
            result.rows,
            expected
                .iter()
                .map(|&id| vec![Value::Integer(id)])
                .collect::<Vec<_>>()
        );
    };
    check(&[4, 2, 1]);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO events VALUES (5,2,50)").unwrap();
    check(&[5, 4, 2]);
    conn.execute("SAVEPOINT changed").unwrap();
    conn.execute("UPDATE events SET bucket=0 WHERE id=5")
        .unwrap();
    conn.execute("DELETE FROM events WHERE id=4").unwrap();
    check(&[2, 1]);
    conn.execute("ROLLBACK TO changed").unwrap();
    check(&[5, 4, 2]);
    conn.execute("RELEASE changed").unwrap();
    conn.execute("ROLLBACK").unwrap();
    check(&[4, 2, 1]);

    conn.execute("BEGIN READ ONLY").unwrap();
    check(&[4, 2, 1]);
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("INSERT INTO events VALUES (6,2,60)")
        .unwrap();
    check(&[4, 2, 1]);
    conn.execute("ROLLBACK").unwrap();
    check(&[6, 4, 2]);
}

#[test]
fn unsupported_and_virtual_filters_keep_generic_execution() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    assert_ordering(
        &conn,
        "ABS(bucket) >= 3",
        "score DESC",
        "score + 0 DESC",
        false,
    );
    conn.execute(
        "ALTER TABLE events ADD COLUMN adjusted INTEGER GENERATED ALWAYS AS (bucket + 1) VIRTUAL",
    )
    .unwrap();
    in_transactions(&conn, || {
        assert_ordering(
            &conn,
            "adjusted BETWEEN 2 AND 4",
            "score DESC",
            "score + 0 DESC",
            false,
        );
    });
}
