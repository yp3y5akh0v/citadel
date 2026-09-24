use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn with_connection(check: impl FnOnce(&Connection<'_>)) {
    let db = DatabaseBuilder::new("")
        .passphrase(b"function-modifier-regression")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE q (id INTEGER PRIMARY KEY, v INTEGER, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO q VALUES (1,2,'b'), (2,2,'a'), (3,NULL,'c')")
        .unwrap();
    check(&conn);
}

fn rejects(sql: &str) {
    with_connection(|conn| {
        let result = conn.query(sql);
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
        assert!(
            matches!(conn.prepare(sql), Err(SqlError::Unsupported(_))),
            "{sql}"
        );
    });
}

#[test]
fn aggregate_filter_is_not_silently_discarded() {
    rejects("SELECT COUNT(*) FILTER (WHERE v IS NULL) FROM q");
}

#[test]
fn aggregate_ordering_is_not_silently_discarded() {
    rejects("SELECT STRING_AGG(label, ',' ORDER BY label) FROM q");
    rejects("SELECT ARRAY_AGG(label ORDER BY label) FROM q");
}

#[test]
fn aggregate_within_group_is_not_silently_discarded() {
    rejects("SELECT STRING_AGG(label, ',') WITHIN GROUP (ORDER BY label) FROM q");
}

#[test]
fn window_distinct_is_not_silently_discarded() {
    rejects("SELECT COUNT(DISTINCT v) OVER () FROM q");
}

#[test]
fn inherited_window_is_not_silently_discarded() {
    rejects("SELECT SUM(v) OVER (missing_window ORDER BY id) FROM q");
}

#[test]
fn rejected_nested_aggregate_keeps_explicit_transaction_usable() {
    with_connection(|conn| {
        conn.execute("BEGIN").unwrap();
        let before = conn.query("SELECT * FROM q ORDER BY id").unwrap().rows;
        let result = conn
            .execute("UPDATE q SET v=(SELECT COUNT(*) FILTER (WHERE v IS NULL) FROM q) WHERE id=1");
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{result:?}"
        );
        assert_eq!(
            conn.query("SELECT * FROM q ORDER BY id").unwrap().rows,
            before
        );
        conn.execute("UPDATE q SET v=7 WHERE id=1").unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT v FROM q WHERE id=1").unwrap().rows,
            vec![vec![Value::Integer(7)]]
        );
    });
}

#[test]
fn ordinary_distinct_aggregates_and_windows_keep_their_results() {
    with_connection(|conn| {
        assert_eq!(
            conn.query("SELECT COUNT(DISTINCT v), COUNT(*) FROM q")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(1), Value::Integer(3)]]
        );
        assert_eq!(
            conn.query("SELECT COUNT(v) OVER () FROM q ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(2)]; 3]
        );
    });
}
