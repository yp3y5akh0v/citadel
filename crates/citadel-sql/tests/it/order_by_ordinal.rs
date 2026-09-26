use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn database(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("ordinal.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(result) => result,
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn first_ints(conn: &Connection, sql: &str) -> Vec<i64> {
    query(conn, sql)
        .rows
        .into_iter()
        .map(|row| match row.first() {
            Some(Value::Integer(value)) => *value,
            other => panic!("expected integer first column for {sql}, got {other:?}"),
        })
        .collect()
}

fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES \
         (1,50,'B'),(2,10,'a'),(3,40,'C'),(4,20,'d'),(5,30,'E')",
    )
    .unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (6,15),(7,35)").unwrap();
}

#[test]
fn ordinals_sort_projected_values_in_full_sort_and_topk_lanes() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    seeded(&conn);

    assert_eq!(
        first_ints(&conn, "SELECT id, n FROM t ORDER BY 2"),
        vec![2, 4, 5, 3, 1]
    );
    assert_eq!(
        first_ints(&conn, "SELECT id, -n FROM t ORDER BY 2"),
        vec![1, 3, 5, 4, 2],
        "the ordinal names the computed output, not source column 2"
    );
    assert_eq!(
        first_ints(&conn, "SELECT id, n FROM t ORDER BY +2 DESC LIMIT 2"),
        vec![1, 3],
        "unary-plus integer ordinals must reach the LIMIT/TopK lane"
    );
    assert_eq!(
        first_ints(&conn, "SELECT id, n FROM t ORDER BY 2 LIMIT 2 OFFSET 1"),
        vec![4, 5]
    );
    assert_eq!(
        first_ints(&conn, "SELECT id, s FROM t ORDER BY 2"),
        vec![2, 1, 3, 4, 5],
        "an ordinal keeps the projected column's NOCASE collation"
    );

    for sql in [
        "EXPLAIN SELECT id, s FROM t ORDER BY 2 LIMIT 1",
        "EXPLAIN SELECT * FROM t ORDER BY 2 LIMIT 1",
    ] {
        let explain = query(&conn, sql);
        assert!(
            explain.rows.iter().any(|row| {
                row.iter()
                    .any(|value| matches!(value, Value::Text(text) if text.contains("TOPK SCAN")))
            }),
            "an ordinal naming a stored projected column must retain the fused TopK scan: {sql}"
        );
    }
}

#[test]
fn ordinals_sort_distinct_aggregate_and_compound_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    seeded(&conn);

    assert_eq!(
        first_ints(
            &conn,
            "SELECT DISTINCT n % 30 FROM t ORDER BY 1 DESC LIMIT 2"
        ),
        vec![20, 10]
    );

    let aggregate = query(
        &conn,
        "SELECT id % 2 AS parity, COUNT(*) AS c \
         FROM t GROUP BY 1 ORDER BY 2 DESC, 1",
    );
    assert_eq!(
        aggregate.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(0), Value::Integer(2)],
        ]
    );

    assert_eq!(
        first_ints(
            &conn,
            "SELECT id, n FROM t UNION ALL SELECT id, n FROM u ORDER BY +2"
        ),
        vec![2, 6, 4, 5, 7, 3, 1]
    );
}

#[test]
fn integer_ordinals_are_validated_against_the_expanded_output() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    seeded(&conn);

    assert_eq!(
        first_ints(&conn, "SELECT *, -n AS neg FROM t ORDER BY 4 LIMIT 2"),
        vec![1, 3],
        "validation and lookup happen after `*` expands"
    );

    for sql in [
        "SELECT id, n FROM t ORDER BY 0",
        "SELECT id, n FROM t ORDER BY -1",
        "SELECT id, n FROM t ORDER BY 3",
        "SELECT id, n FROM t ORDER BY +3 LIMIT 1",
        "SELECT DISTINCT n FROM t ORDER BY 2",
        "SELECT COUNT(*) FROM t ORDER BY 2",
        "SELECT id FROM t UNION ALL SELECT id FROM t ORDER BY 2",
        "SELECT 1 ORDER BY 2",
    ] {
        match conn.execute(sql) {
            Err(SqlError::InvalidValue(message)) => assert!(
                message.starts_with("ORDER BY position ") && message.ends_with(" out of range"),
                "unexpected ordinal error for {sql}: {message}"
            ),
            other => panic!("expected an out-of-range ordinal error for {sql}, got {other:?}"),
        }
    }

    assert_eq!(
        first_ints(&conn, "SELECT id FROM t ORDER BY 1.0"),
        vec![1, 2, 3, 4, 5],
        "a real literal is an ordinary constant, not an ordinal"
    );
}

#[test]
fn integer_inside_window_order_by_remains_a_constant() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE w (n INTEGER, id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO w VALUES (30,1),(10,2),(20,3)")
        .unwrap();

    let result = query(
        &conn,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY 1) AS rn FROM w ORDER BY id",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(3)],
        ],
        "window ORDER BY 1 is a literal constant, not a top-level output ordinal"
    );
}

#[test]
fn no_from_select_applies_where_order_limit_and_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();

    assert!(query(&conn, "SELECT 1 WHERE FALSE").rows.is_empty());
    assert!(query(&conn, "SELECT 1 ORDER BY 1 LIMIT 0").rows.is_empty());
    assert!(query(&conn, "SELECT 1 OFFSET 1").rows.is_empty());
    assert_eq!(
        query(&conn, "SELECT DISTINCT 1 ORDER BY 1 LIMIT 1").rows,
        vec![vec![Value::Integer(1)]]
    );
}
