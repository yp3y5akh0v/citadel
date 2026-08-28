//! `SELECT DISTINCT ... ORDER BY` sorts after projecting, so ORDER BY expressions naming
//! discarded source columns resolved to `Value::Null`. Keys are now taken before projection.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn ints(conn: &Connection, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn texts(conn: &Connection, sql: &str) -> Vec<String> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Text(s)) => s.to_string(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// `g` repeats so DISTINCT has something to remove; `n` is unique per row and out of scan
/// order so a dropped sort is visible rather than accidentally right.
fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1,'a',50),(2,'b',10),(3,'a',40),(4,'b',20),(5,'c',30)")
        .unwrap();
}

#[test]
fn distinct_sorts_by_an_output_alias() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(&conn, "SELECT DISTINCT n AS v FROM t ORDER BY v DESC"),
        vec![50, 40, 30, 20, 10]
    );
    assert_eq!(
        ints(&conn, "SELECT DISTINCT n * 2 AS d FROM t ORDER BY d DESC"),
        vec![100, 80, 60, 40, 20],
        "an alias over an expression has no other way to be named"
    );
}

/// The mirror case, which needs no alias in the ORDER BY at all: the projection renames the
/// column, so the source name it sorts by is absent from the output.
#[test]
fn distinct_sorts_by_a_column_the_projection_renamed() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        texts(&conn, "SELECT DISTINCT g AS grp FROM t ORDER BY g DESC"),
        vec!["c", "b", "a"]
    );
}

/// A dropped sort under LIMIT does not merely reorder the answer, it returns different rows.
#[test]
fn distinct_under_a_limit_keeps_the_right_rows() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ints(
            &conn,
            "SELECT DISTINCT n AS v FROM t ORDER BY v DESC LIMIT 2"
        ),
        vec![50, 40]
    );
}

/// Each surviving row is ordered by the key of the row it deduplicated to, which is at least
/// a defined answer.
#[test]
fn distinct_orders_by_a_column_outside_the_select_list() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        texts(&conn, "SELECT DISTINCT g FROM t ORDER BY n DESC"),
        vec!["a", "c", "b"],
        "first rows kept are a=50, b=10, c=30, so descending by n is a, c, b"
    );
}

/// The sort keys come from the source rows now, so the collation has to be looked up there
/// too - reading it from the output columns would silently fall back to binary ordering.
///
/// 'B' and 'a' are the discriminator: binary puts every uppercase letter before every
/// lowercase one, so it would answer B, a.
#[test]
fn distinct_keeps_the_source_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'B'),(2,'a'),(3,'b')")
        .unwrap();

    assert_eq!(
        texts(&conn, "SELECT DISTINCT s AS v FROM c ORDER BY v"),
        vec!["a", "B"],
        "'b' folds onto 'B', and NOCASE orders a before B"
    );
}
