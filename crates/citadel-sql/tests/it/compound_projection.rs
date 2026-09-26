use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn database(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("compound-projection.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn cached_compound_expands_star_without_dropping_adjacent_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();

    let after = match conn
        .execute("SELECT *, id AS extra FROM t UNION ALL SELECT *, id AS extra FROM t")
        .unwrap()
    {
        ExecutionResult::Query(result) => result,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(after.columns, vec!["id", "n", "extra"]);
    assert_eq!(
        after.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(20), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(10), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(20), Value::Integer(2)],
        ]
    );

    let before = match conn
        .execute("SELECT id AS extra, * FROM t UNION ALL SELECT id AS extra, * FROM t")
        .unwrap()
    {
        ExecutionResult::Query(result) => result,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(before.columns, vec!["extra", "id", "n"]);
    assert_eq!(
        before.rows[0],
        vec![Value::Integer(1), Value::Integer(1), Value::Integer(10)]
    );
}

#[test]
fn cached_compound_does_not_mask_width_mismatch_after_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,10)").unwrap();

    for sql in [
        "SELECT * FROM t UNION ALL SELECT *, id FROM t",
        "SELECT *, id FROM t UNION ALL SELECT * FROM t",
    ] {
        assert!(
            matches!(
                conn.execute(sql),
                Err(SqlError::CompoundColumnCountMismatch { left: 2, right: 3 })
                    | Err(SqlError::CompoundColumnCountMismatch { left: 3, right: 2 })
            ),
            "branch widths must be checked after star expansion: {sql}"
        );
    }
}

#[test]
fn compound_order_by_rejects_an_ambiguous_output_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,'b'),(2,'a')")
        .unwrap();

    assert!(matches!(
        conn.execute(
            "SELECT id AS x, s AS x FROM t \
             UNION ALL SELECT id, s FROM t ORDER BY x"
        ),
        Err(SqlError::AmbiguousColumn(name)) if name == "x"
    ));
}
