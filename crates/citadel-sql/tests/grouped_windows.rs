//! Window functions run after GROUP BY and HAVING: over one row per group,
//! and over the one row an ungrouped aggregate query returns.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"grouped-windows")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    for sql in [
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, region TEXT, dept TEXT, \
         amount INTEGER)",
        "INSERT INTO sales VALUES (1, 'east', 'a', 10), (2, 'east', 'b', 20), \
         (3, 'east', 'a', NULL), (4, 'west', 'a', 5), (5, 'west', 'b', 7), \
         (6, 'north', 'b', NULL)",
    ] {
        conn.execute(sql).unwrap();
    }
}

fn int(value: i64) -> Value {
    Value::Integer(value)
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn rows(conn: &Connection<'_>, sql: &str) -> Vec<Vec<Value>> {
    conn.query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

#[test]
fn windows_run_over_one_row_per_group() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (sql, expected) in [
        (
            "SELECT region, COUNT(*), ROW_NUMBER() OVER (ORDER BY region) FROM sales \
             GROUP BY region ORDER BY region",
            vec![
                vec![text("east"), int(3), int(1)],
                vec![text("north"), int(1), int(2)],
                vec![text("west"), int(2), int(3)],
            ],
        ),
        (
            "SELECT region, ROW_NUMBER() OVER (ORDER BY region) FROM sales \
             GROUP BY region ORDER BY region",
            vec![
                vec![text("east"), int(1)],
                vec![text("north"), int(2)],
                vec![text("west"), int(3)],
            ],
        ),
        (
            "SELECT region, SUM(COUNT(*)) OVER () FROM sales GROUP BY region ORDER BY region",
            vec![
                vec![text("east"), int(6)],
                vec![text("north"), int(6)],
                vec![text("west"), int(6)],
            ],
        ),
        (
            "SELECT region, RANK() OVER (ORDER BY COUNT(*) DESC) FROM sales \
             GROUP BY region ORDER BY region",
            vec![
                vec![text("east"), int(1)],
                vec![text("north"), int(3)],
                vec![text("west"), int(2)],
            ],
        ),
        (
            "SELECT dept, region, SUM(amount), SUM(SUM(amount)) OVER (PARTITION BY dept) \
             FROM sales GROUP BY dept, region ORDER BY dept, region",
            vec![
                vec![text("a"), text("east"), int(10), int(15)],
                vec![text("a"), text("west"), int(5), int(15)],
                vec![text("b"), text("east"), int(20), int(27)],
                vec![text("b"), text("north"), Value::Null, int(27)],
                vec![text("b"), text("west"), int(7), int(27)],
            ],
        ),
        (
            "SELECT region, SUM(COUNT(*) FILTER (WHERE amount > 6)) OVER () FROM sales \
             GROUP BY region ORDER BY region",
            vec![
                vec![text("east"), int(3)],
                vec![text("north"), int(3)],
                vec![text("west"), int(3)],
            ],
        ),
    ] {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn having_removes_groups_before_windows_see_them() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        rows(
            &conn,
            "SELECT region, ROW_NUMBER() OVER (ORDER BY region) FROM sales \
             GROUP BY region HAVING COUNT(*) > 1 ORDER BY region"
        ),
        vec![vec![text("east"), int(1)], vec![text("west"), int(2)]]
    );
    // HAVING may name an output column, as it may without windows.
    assert_eq!(
        rows(
            &conn,
            "SELECT region, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY region) FROM sales \
             GROUP BY region HAVING c > 1 ORDER BY region"
        ),
        vec![
            vec![text("east"), int(3), int(1)],
            vec![text("west"), int(2), int(2)],
        ]
    );
}

#[test]
fn an_ungrouped_aggregate_query_is_one_row_for_its_windows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (sql, expected) in [
        (
            "SELECT COUNT(*), COUNT(*) OVER () FROM sales",
            vec![int(6), int(1)],
        ),
        (
            "SELECT COUNT(*), ROW_NUMBER() OVER () FROM sales WHERE amount > 100",
            vec![int(0), int(1)],
        ),
        ("SELECT SUM(COUNT(*)) OVER () FROM sales", vec![int(6)]),
    ] {
        assert_eq!(rows(&conn, sql), vec![expected], "{sql}");
    }
}

#[test]
fn ordering_distinct_and_limits_apply_after_windows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        rows(
            &conn,
            "SELECT region, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, region) \
             FROM sales GROUP BY region ORDER BY c DESC LIMIT 2"
        ),
        vec![
            vec![text("east"), int(3), int(1)],
            vec![text("west"), int(2), int(2)],
        ]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT DISTINCT COUNT(*) OVER () FROM sales GROUP BY region"
        ),
        vec![vec![int(3)]]
    );
}

#[test]
fn outputs_keep_the_names_the_query_wrote() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let written = conn
        .query(
            "SELECT region, COUNT(*), SUM(COUNT(*)) OVER () AS total FROM sales \
             GROUP BY region",
        )
        .unwrap();
    assert_eq!(written.columns, ["region", "COUNT(*)", "total"]);
}

#[test]
fn every_execution_path_groups_before_windows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let sql = "SELECT region, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY region) AS rn \
               FROM sales GROUP BY region ORDER BY region";
    let expected = vec![
        vec![text("east"), int(3), int(1)],
        vec![text("north"), int(1), int(2)],
        vec![text("west"), int(2), int(3)],
    ];
    assert_eq!(rows(&conn, sql), expected);
    assert_eq!(
        conn.prepare(sql).unwrap().query_collect(&[]).unwrap().rows,
        expected
    );
    conn.execute(&format!("CREATE VIEW ranked AS {sql}"))
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT region, c, rn FROM ranked ORDER BY region"),
        expected
    );
    conn.execute("BEGIN").unwrap();
    assert_eq!(rows(&conn, sql), expected);
    conn.execute("COMMIT").unwrap();
}

#[test]
fn derived_tables_and_ctes_group_before_windows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let expected = vec![
        vec![text("east"), int(3), int(1)],
        vec![text("north"), int(1), int(3)],
        vec![text("west"), int(2), int(2)],
    ];
    for sql in [
        "SELECT d.region, COUNT(*), RANK() OVER (ORDER BY COUNT(*) DESC) \
         FROM (SELECT region, amount FROM sales WHERE id > 0) AS d \
         GROUP BY d.region ORDER BY 1",
        "WITH d AS (SELECT region, amount FROM sales WHERE id > 0) \
         SELECT d.region, COUNT(*), RANK() OVER (ORDER BY COUNT(*) DESC) FROM d \
         GROUP BY d.region ORDER BY 1",
    ] {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
    assert_eq!(
        rows(
            &conn,
            "SELECT MIN(d.amount), SUM(COUNT(*)) OVER () FROM (SELECT amount FROM sales) AS d"
        ),
        vec![vec![int(5), int(6)]]
    );
}

#[test]
fn a_grouped_window_query_it_cannot_run_is_an_error() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "SELECT *, ROW_NUMBER() OVER () FROM sales GROUP BY region",
        "SELECT region, ROW_NUMBER() OVER () AS rn FROM sales GROUP BY rn",
    ] {
        let result = conn.query(sql);
        assert!(
            matches!(result, Err(SqlError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}
