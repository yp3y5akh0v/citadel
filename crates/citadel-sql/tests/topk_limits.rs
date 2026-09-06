use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

// This value must fail an attempted result-sized reservation with capacity
// overflow, not request a merely large allocation that could exhaust memory.
const MAX_ROWS: &str = "9223372036854775807";

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"topk-limit-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn in_transactions(conn: &Connection<'_>, check: impl Fn(bool)) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        check(begin != Some("BEGIN"));
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

fn assert_plan(conn: &Connection<'_>, sql: &str, expected: &str) {
    let ExecutionResult::Query(plan) = conn.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("EXPLAIN did not return a query result");
    };
    assert!(
        plan.rows
            .iter()
            .flatten()
            .any(|value| matches!(value, Value::Text(line) if line.contains(expected))),
        "{sql}: expected {expected}, got {plan:?}"
    );
}

fn assert_ids(conn: &Connection<'_>, sql: &str, expected: &[i64]) {
    let check = |rows: &[Vec<Value>]| {
        for row in rows {
            assert_eq!(row.len(), 1, "{sql}: {row:?}");
        }
        let ids: Vec<_> = rows
            .iter()
            .map(|row| match row[0] {
                Value::Integer(id) => id,
                ref other => panic!("{sql}: expected integer id, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, expected, "{sql}");
    };
    check(&conn.query(sql).unwrap().rows);
    let prepared = conn.prepare(sql).unwrap();
    for _ in 0..2 {
        check(&prepared.query_collect(&[]).unwrap().rows);
    }
}

fn seed_ann(conn: &Connection<'_>) {
    for table in ["vectors", "empty_vectors"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, category INTEGER, v VECTOR(3))"
        ))
        .unwrap();
        conn.execute(&format!(
            "CREATE INDEX {table}_ann ON {table} USING ann (v) \
             WITH (metric = 'l2', filters = 'category')"
        ))
        .unwrap();
    }
    conn.execute(
        "INSERT INTO vectors VALUES (1, 1, '[3,0,0]'::VECTOR(3)), \
         (2, 1, '[1,0,0]'::VECTOR(3)), (3, 2, '[2,0,0]'::VECTOR(3))",
    )
    .unwrap();
    assert_ids(conn, &ann_query("vectors", "", "LIMIT 1"), &[2]);
    assert!(
        conn.ann_cache_status("vectors", "v").unwrap().is_some(),
        "the tiny fixture must build and use an ANN index, not an exact fallback"
    );
}

fn ann_query(table: &str, predicate: &str, suffix: &str) -> String {
    format!(
        "SELECT id FROM {table} {predicate} \
         ORDER BY v <-> '[0,0,0]'::VECTOR(3) {suffix}"
    )
}

#[test]
fn ann_huge_limit_retains_only_actual_index_survivors() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed_ann(&conn);
    in_transactions(&conn, |read_path| {
        for (predicate, expected) in [
            ("", &[2, 3, 1][..]),
            ("WHERE category = 1", &[2, 1][..]),
            ("WHERE category = 1 AND id <> 1", &[2][..]),
        ] {
            let sql = ann_query("vectors", predicate, &format!("LIMIT {MAX_ROWS}"));
            if read_path {
                assert_plan(&conn, &sql, "ANN TOP-K (approximate index)");
            }
            assert_ids(&conn, &sql, expected);
        }
        let sql = ann_query("empty_vectors", "", &format!("LIMIT {MAX_ROWS}"));
        if read_path {
            assert_plan(&conn, &sql, "ANN TOP-K (approximate index)");
        }
        assert_ids(&conn, &sql, &[]);
    });
}

#[test]
fn ann_huge_offset_and_zero_limit_do_not_reserve_requested_counts() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed_ann(&conn);
    in_transactions(&conn, |read_path| {
        for table in ["vectors", "empty_vectors"] {
            for limit in ["1", MAX_ROWS, "0"] {
                let sql = ann_query(table, "", &format!("LIMIT {limit} OFFSET {MAX_ROWS}"));
                if read_path && limit != "0" {
                    assert_plan(&conn, &sql, "ANN TOP-K (approximate index)");
                }
                assert_ids(&conn, &sql, &[]);
            }
        }
        let sql = ann_query("vectors", "", &format!("LIMIT {MAX_ROWS} OFFSET 1"));
        if read_path {
            assert_plan(&conn, &sql, "ANN TOP-K (approximate index)");
        }
        assert_ids(&conn, &sql, &[3, 1]);
    });
}
