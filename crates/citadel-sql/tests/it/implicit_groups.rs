//! Without GROUP BY, HAVING or an aggregate in ORDER BY makes all of a query's
//! rows one group, as an aggregate in the select list does.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"implicit-groups")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    for sql in [
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, amount INTEGER)",
        "INSERT INTO sales VALUES (1, 10), (2, 20), (3, 5)",
        "CREATE TABLE empty (id INTEGER NOT NULL PRIMARY KEY)",
    ] {
        conn.execute(sql).unwrap();
    }
}

fn rows(conn: &Connection<'_>, sql: &str) -> Vec<Vec<Value>> {
    conn.query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

fn one() -> Vec<Vec<Value>> {
    vec![vec![Value::Integer(1)]]
}

#[test]
fn having_without_group_by_filters_the_one_group() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (sql, expected) in [
        ("SELECT 1 FROM sales HAVING COUNT(*) > 0", one()),
        ("SELECT 1 FROM sales HAVING COUNT(*) > 5", vec![]),
        ("SELECT 1 FROM sales HAVING MIN(amount) > 0", one()),
        // No rows are still one group.
        ("SELECT 1 FROM empty HAVING COUNT(*) = 0", one()),
        (
            "WITH c AS (SELECT amount FROM sales WHERE id > 0) \
             SELECT 1 FROM c HAVING COUNT(*) > 5",
            vec![],
        ),
        (
            "SELECT 1 FROM (SELECT amount FROM sales) AS d HAVING SUM(d.amount) = 35",
            one(),
        ),
        (
            "SELECT COUNT(*), ROW_NUMBER() OVER () FROM sales HAVING COUNT(*) > 5",
            vec![],
        ),
    ] {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
    let sql = "SELECT 1 FROM sales HAVING COUNT(*) > 5";
    assert!(conn
        .prepare(sql)
        .unwrap()
        .query_collect(&[])
        .unwrap()
        .rows
        .is_empty());
    conn.execute("BEGIN").unwrap();
    assert!(rows(&conn, sql).is_empty());
    conn.execute("COMMIT").unwrap();
}

#[test]
fn an_aggregate_in_order_by_groups_the_query() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(rows(&conn, "SELECT 1 FROM sales ORDER BY COUNT(*)"), one());
}
