use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"stream-group-order")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn assert_plan(conn: &Connection<'_>, sql: &str, streaming: bool) {
    let ExecutionResult::Query(plan) = conn.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("EXPLAIN did not return rows");
    };
    assert_eq!(
        plan.rows
            .iter()
            .flatten()
            .any(|value| matches!(value, Value::Text(text) if text.contains("STREAM GROUP BY"))),
        streaming,
        "{sql}: {:?}",
        plan.rows
    );
}

fn assert_differential(conn: &Connection<'_>, sql: &str, streaming: bool) -> QueryResult {
    // Integer/NULL g + 0 has the same groups, but expression keys cannot use
    // StreamGroupByPlan. This is an independent materialized execution oracle.
    let generic = sql.replace("GROUP BY g", "GROUP BY g + 0");
    assert_ne!(generic, sql);
    assert_plan(conn, sql, streaming);
    assert_plan(conn, &generic, false);
    let expected = conn.query(&generic).unwrap();
    let prepared = conn.prepare(sql).unwrap();
    for actual in [
        conn.query(sql).unwrap(),
        prepared.query_collect(&[]).unwrap(),
        prepared.query(&[]).unwrap().collect().unwrap(),
    ] {
        assert_eq!(actual.columns, expected.columns, "{sql}");
        assert_eq!(actual.rows, expected.rows, "{sql}");
    }
    expected
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

fn seed(conn: &Connection<'_>) {
    conn.execute(
        "CREATE TABLE facts (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, label TEXT COLLATE NOCASE)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO facts VALUES
         (1, NULL, NULL, 'Z'), (2, NULL, 5, 'a'),
         (3, 1, 10, 'Beta'), (4, 1, -2, 'beta'),
         (5, 2, 8, 'Alpha'), (6, 2, NULL, 'alpha'),
         (7, 3, 3, 'delta'), (8, 3, 5, 'Delta'),
         (9, 4, NULL, 'omega')",
    )
    .unwrap();
}

#[test]
fn ordered_groups_bind_outputs_and_preserve_nulls_directions_and_stable_ties() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    in_transactions(&conn, || {
        let result = assert_differential(
            &conn,
            "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM facts GROUP BY g ORDER BY total DESC NULLS LAST",
            true,
        );
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(8)],
                vec![Value::Integer(2), Value::Integer(2), Value::Integer(8)],
                vec![Value::Integer(3), Value::Integer(2), Value::Integer(8)],
                vec![Value::Null, Value::Integer(2), Value::Integer(5)],
                vec![Value::Integer(4), Value::Integer(1), Value::Null],
            ]
        );
        for sql in [
            "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM facts WHERE id >= 1 GROUP BY g ORDER BY g",
            "SELECT g AS key, SUM(v) AS total FROM facts GROUP BY g ORDER BY key DESC NULLS FIRST",
            "SELECT g AS key, SUM(v) AS total FROM facts GROUP BY g ORDER BY facts.g DESC",
            "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM facts GROUP BY g ORDER BY 3 DESC NULLS FIRST, 1 DESC NULLS LAST",
            "SELECT g AS key, SUM(v) AS g FROM facts GROUP BY g ORDER BY g",
            "SELECT SUM(v) AS total FROM facts GROUP BY g ORDER BY total",
            "SELECT g, MIN(label) AS first FROM facts GROUP BY g ORDER BY first, g DESC",
            "SELECT g, MIN(label) AS first FROM facts GROUP BY g ORDER BY first",
        ] {
            assert_differential(&conn, sql, true);
        }
    });
    conn.execute("UPDATE facts SET label = 'BETA' WHERE g = 3")
        .unwrap();
    assert_differential(
        &conn,
        "SELECT g, MIN(label) AS first FROM facts GROUP BY g ORDER BY first, g DESC",
        true,
    );
}

#[test]
fn ordered_groups_handle_empty_input_added_defaults_and_pending_writes() {
    let db = database();
    // Exercise the shared cancellable sorting path as well as the no-token
    // path used by the other fixture, without scheduler-dependent cancellation.
    db.set_cancel(Some(CancelToken::new()));
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1), (2)").unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN g INTEGER DEFAULT 7")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN v INTEGER DEFAULT 5")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (3, NULL, NULL), (4, 7, 9)")
        .unwrap();
    let sql = "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM items GROUP BY g ORDER BY g";
    in_transactions(&conn, || {
        assert_eq!(
            assert_differential(&conn, sql, true).rows,
            vec![
                vec![Value::Null, Value::Integer(1), Value::Null],
                vec![Value::Integer(7), Value::Integer(3), Value::Integer(19)],
            ]
        );
        let empty = assert_differential(
            &conn,
            "SELECT g, SUM(v) AS total FROM items WHERE id < 0 GROUP BY g ORDER BY 2, 1",
            true,
        );
        assert!(empty.rows.is_empty());
        assert_eq!(empty.columns, ["g", "total"]);
    });
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT pending").unwrap();
    conn.execute("INSERT INTO items VALUES (5, 2, 10)").unwrap();
    assert_eq!(assert_differential(&conn, sql, true).rows.len(), 3);
    conn.execute("ROLLBACK TO pending").unwrap();
    assert_eq!(assert_differential(&conn, sql, true).rows.len(), 2);
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn ordered_group_guards_retain_generic_semantics() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    in_transactions(&conn, || {
        for sql in [
            "SELECT g, SUM(v) AS total FROM facts GROUP BY g HAVING COUNT(*) > 1 ORDER BY g",
            "SELECT g, SUM(v) AS total FROM facts GROUP BY g ORDER BY g LIMIT 2",
            "SELECT g, SUM(v) AS total FROM facts GROUP BY g ORDER BY g OFFSET 2",
            "SELECT DISTINCT SUM(v) AS total FROM facts GROUP BY g ORDER BY total",
            "SELECT SUM(v) AS total FROM facts GROUP BY g ORDER BY g",
            "SELECT g, SUM(v) AS total FROM facts GROUP BY g ORDER BY SUM(v)",
            "SELECT g, MIN(label) AS first FROM facts GROUP BY g ORDER BY MIN(label) COLLATE NOCASE, g",
            "SELECT g, SUM(DISTINCT v) AS total FROM facts GROUP BY g ORDER BY g",
        ] {
            assert_differential(&conn, sql, false);
        }
    });
    for sql in [
        "SELECT g FROM facts GROUP BY g ORDER BY 2",
        "SELECT g FROM facts WHERE id < 0 GROUP BY g ORDER BY 2",
        "SELECT g AS x, SUM(v) AS x FROM facts GROUP BY g ORDER BY x",
    ] {
        assert!(conn.query(sql).is_err(), "{sql}");
    }
}
