use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(result) => result.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn seed(conn: &Connection) {
    conn.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO source VALUES (1,'A')").unwrap();
}

#[test]
fn recursive_union_deduplicates_under_the_anchor_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    seed(&conn);

    assert_eq!(
        rows(
            &conn,
            "WITH RECURSIVE r(s) AS ( \
                 SELECT s FROM source WHERE id = 1 \
                 UNION \
                 SELECT LOWER(s) FROM r \
             ) SELECT s FROM r",
        ),
        vec![vec![Value::Text("A".into())]],
        "UNION must reject recursive 'a' as equal to anchor 'A' under NOCASE"
    );
}

#[test]
fn recursive_working_schema_preserves_the_anchor_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    seed(&conn);

    assert_eq!(
        rows(
            &conn,
            "WITH RECURSIVE r(s,n) AS ( \
                 SELECT s, 0 FROM source WHERE id = 1 \
                 UNION ALL \
                 SELECT s, n + 1 FROM r WHERE s = 'a' AND n < 1 \
             ) SELECT s, n FROM r ORDER BY n",
        ),
        vec![
            vec![Value::Text("A".into()), Value::Integer(0)],
            vec![Value::Text("A".into()), Value::Integer(1)],
        ],
        "the recursive self-reference must compare its NOCASE column as NOCASE"
    );
}
