//! A qualified column must name a source of its query or of a query around
//! it. An aliased table is visible only by its alias.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{executor, parser, schema::SchemaManager, Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"qualified-columns")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

/// t: (1, 10), (2, 20); u: (1, 100).
fn setup(conn: &Connection<'_>) {
    for sql in [
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER)",
        "CREATE TABLE u (id INTEGER NOT NULL PRIMARY KEY, b INTEGER)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
        "INSERT INTO u VALUES (1, 100)",
    ] {
        conn.execute(sql).unwrap();
    }
}

fn ints(rows: &[&[i64]]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| row.iter().map(|&value| Value::Integer(value)).collect())
        .collect()
}

#[test]
fn a_qualifier_that_names_no_source_is_rejected_on_every_path() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "SELECT nope.a FROM t",
        "SELECT t.a FROM t AS x",
        "SELECT id FROM t WHERE nope.a = 1",
        "SELECT id FROM t ORDER BY nope.id",
        "SELECT a, COUNT(*) FROM t GROUP BY nope.a",
        "SELECT (SELECT nope.b FROM u) FROM t",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = nope.a)",
        "SELECT t.a FROM t JOIN u AS x ON t.id = x.id WHERE u.b > 0",
        "UPDATE t SET a = nope.a",
        "DELETE FROM t WHERE nope.a = 1",
        "INSERT INTO t SELECT nope.id, 1 FROM u",
        "CREATE VIEW v AS SELECT nope.a FROM t",
    ] {
        for (path, result) in [
            ("execute", conn.execute(sql).map(drop)),
            ("prepare", conn.prepare(sql).map(drop)),
            ("batch", conn.execute_batch(sql).map(drop)),
        ] {
            assert!(
                matches!(&result, Err(SqlError::ColumnNotFound(name)) if name.contains('.')),
                "{path}: {sql}: {result:?}"
            );
        }
    }
    assert_eq!(
        conn.query("SELECT id, a FROM t ORDER BY id").unwrap().rows,
        ints(&[&[1, 10], &[2, 20]])
    );
}

#[test]
fn qualifiers_of_every_visible_source_resolve() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (sql, expected) in [
        ("SELECT t.a FROM t ORDER BY 1", ints(&[&[10], &[20]])),
        ("SELECT x.a FROM t AS x ORDER BY 1", ints(&[&[10], &[20]])),
        (
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
            ints(&[&[1]]),
        ),
        (
            "SELECT d.a FROM (SELECT a FROM t) AS d ORDER BY 1",
            ints(&[&[10], &[20]]),
        ),
        (
            "WITH c AS (SELECT id, a FROM t) SELECT c.a FROM c WHERE c.id = 2",
            ints(&[&[20]]),
        ),
        (
            "SELECT t.a, u.b FROM t JOIN u ON t.id = u.id",
            ints(&[&[10, 100]]),
        ),
        (
            "SELECT tables.table_name IS NOT NULL FROM information_schema.tables \
             WHERE table_name = 't'",
            vec![vec![Value::Boolean(true)]],
        ),
    ] {
        assert_eq!(conn.query(sql).unwrap().rows, expected, "{sql}");
    }
    assert_eq!(
        conn.query("UPDATE t SET a = t.a + 1 WHERE t.id = 1 RETURNING old.a, new.a")
            .unwrap()
            .rows,
        ints(&[&[10, 11]])
    );
    assert_eq!(
        conn.query(
            "INSERT INTO t VALUES (2, 5) ON CONFLICT (id) DO UPDATE \
             SET a = t.a + excluded.a RETURNING t.a"
        )
        .unwrap()
        .rows,
        ints(&[&[25]])
    );
    conn.execute("CREATE TABLE log (id INTEGER NOT NULL PRIMARY KEY, a INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER logged AFTER UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (NEW.id, NEW.a - OLD.a); END",
    )
    .unwrap();
    conn.execute("UPDATE t SET a = a + 3 WHERE id = 1").unwrap();
    assert_eq!(
        conn.query("SELECT id, a FROM log").unwrap().rows,
        ints(&[&[1, 3]])
    );
}

#[test]
fn a_view_stored_before_the_check_keeps_reading() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    // Defined through the executor directly, as a database written before the
    // check could hold it.
    let mut schema = SchemaManager::load(&db).unwrap();
    let mut txn = db.begin_write().unwrap();
    executor::execute_in_txn(
        &mut txn,
        &mut schema,
        &parser::parse_sql("CREATE VIEW legacy AS SELECT t.a FROM t AS x").unwrap(),
        &[],
    )
    .unwrap();
    txn.commit().unwrap();
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT a FROM legacy ORDER BY a").unwrap().rows,
        ints(&[&[10], &[20]])
    );
}
