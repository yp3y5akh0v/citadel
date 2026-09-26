//! `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`: `=` that treats NULL as a value, and the
//! only comparison here that never yields NULL. Parsed but unconverted before this.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    rows(conn, sql)
        .iter()
        .map(|r| match r.first() {
            Some(Value::Integer(n)) => *n,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

/// Every combination of present and absent on both sides.
fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE n (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
        .unwrap();
    conn.execute(
        "INSERT INTO n VALUES (1,'x','x'),(2,'x','y'),(3,NULL,NULL),(4,NULL,'x'),(5,'x',NULL)",
    )
    .unwrap();
}

#[test]
fn null_is_a_value_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM n WHERE a IS NOT DISTINCT FROM b"),
        vec![1, 3],
        "two NULLs are not distinct; plain = would drop row 3"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM n WHERE a IS DISTINCT FROM b"),
        vec![2, 4, 5],
        "a NULL beside a value is distinct; plain <> would drop rows 4 and 5"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM n WHERE a = b"),
        vec![1],
        "the contrast: = is unknown wherever a NULL is involved"
    );
}

/// The defining property, and the one a three-valued mistake breaks: the two forms partition
/// the table, because neither ever evaluates to NULL.
#[test]
fn the_two_forms_partition_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let mut both = ids(&conn, "SELECT id FROM n WHERE a IS NOT DISTINCT FROM b");
    both.extend(ids(&conn, "SELECT id FROM n WHERE a IS DISTINCT FROM b"));
    both.sort_unstable();
    assert_eq!(both, ids(&conn, "SELECT id FROM n"));
}

/// Used as a value rather than a predicate, it yields true or false and never NULL.
#[test]
fn it_yields_a_boolean_even_where_equality_yields_null() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let out = rows(&conn, "SELECT a IS NOT DISTINCT FROM b AS same FROM n");
    assert_eq!(
        out.iter().filter(|r| r[0].is_null()).count(),
        0,
        "no row is unknown"
    );
    assert_eq!(
        out.iter().filter(|r| r[0] == Value::Boolean(true)).count(),
        2,
        "rows 1 and 3 agree"
    );
}

/// A literal NULL on the right is the readable spelling of `IS NULL`, and has to behave the
/// same way.
#[test]
fn a_null_literal_operand_works() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM n WHERE a IS NOT DISTINCT FROM NULL"),
        ids(&conn, "SELECT id FROM n WHERE a IS NULL")
    );
}

/// It is a comparison, so a column collation reaches it as it reaches `=`.
#[test]
fn it_collates_like_equality() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'b'),(2,'A'),(3,'a'),(4,NULL)")
        .unwrap();

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IS NOT DISTINCT FROM 'A'"),
        ids(&conn, "SELECT id FROM c WHERE s = 'A'"),
        "the same rows equality matches"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IS NOT DISTINCT FROM 'A'"),
        vec![2, 3]
    );
}

#[test]
fn it_uses_the_same_interval_equality_as_equals() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT INTERVAL '1 month' = INTERVAL '30 days', \
             INTERVAL '1 month' IS NOT DISTINCT FROM INTERVAL '30 days'",
        ),
        vec![vec![Value::Boolean(true), Value::Boolean(true)]]
    );
}

#[test]
fn aggregate_evaluation_preserves_the_operand_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'a'),(2,'A')")
        .unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT s IS NOT DISTINCT FROM 'A', COUNT(*) FROM c GROUP BY s",
        ),
        vec![vec![Value::Boolean(true), Value::Integer(2)]]
    );
}

#[test]
fn it_can_wrap_a_window_function() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT ROW_NUMBER() OVER (ORDER BY id) IS DISTINCT FROM 1 FROM t ORDER BY id",
        ),
        vec![
            vec![Value::Boolean(false)],
            vec![Value::Boolean(true)],
            vec![Value::Boolean(true)],
        ]
    );
}

#[test]
fn lateral_binding_reaches_both_operands() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1),(2),(3),(4)")
        .unwrap();
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO e VALUES (1),(3)").unwrap();

    assert_eq!(
        ids(
            &conn,
            "SELECT t.id FROM t, LATERAL (\
                 SELECT e.id FROM e WHERE e.id IS NOT DISTINCT FROM t.id\
             ) p ORDER BY t.id",
        ),
        vec![1, 3]
    );
}

/// A null-safe join key is the usual reason to reach for this. It also exercises the column
/// collection the join projection depends on: an operand whose columns go uncollected drops
/// out of the projected row.
#[test]
fn it_works_as_a_join_condition() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE l (id INTEGER PRIMARY KEY, k TEXT)")
        .unwrap();
    conn.execute("INSERT INTO l VALUES (1,'x'),(2,NULL)")
        .unwrap();
    conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, k TEXT)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (10,'x'),(11,NULL)")
        .unwrap();

    let matched = rows(
        &conn,
        "SELECT l.id, r.id FROM l JOIN r ON l.k IS NOT DISTINCT FROM r.k",
    );
    assert_eq!(
        matched,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(11)],
        ],
        "the NULL keys join to each other, which a plain = never does"
    );
}

/// A subquery operand has to be materialized before evaluation, like every other comparison.
#[test]
fn a_subquery_operand_is_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM n WHERE a IS NOT DISTINCT FROM (SELECT b FROM n WHERE id = 1)"
        ),
        vec![1, 2, 5],
        "rows whose a is 'x', which is row 1's b"
    );
}

/// The write paths take their own route to the predicate.
#[test]
fn it_works_in_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, a TEXT, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO w VALUES (1,'x',0),(2,NULL,0),(3,'y',0)")
        .unwrap();

    conn.execute("UPDATE w SET v = 9 WHERE a IS NOT DISTINCT FROM NULL")
        .unwrap();
    assert_eq!(ids(&conn, "SELECT id FROM w WHERE v = 9"), vec![2]);

    conn.execute("DELETE FROM w WHERE a IS DISTINCT FROM NULL")
        .unwrap();
    assert_eq!(
        ids(&conn, "SELECT id FROM w"),
        vec![2],
        "only the NULL row is not distinct from NULL"
    );
}
