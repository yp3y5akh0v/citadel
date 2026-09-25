//! `aggregate(...) FILTER (WHERE condition)` aggregates only the rows the
//! condition holds for; NULL counts as false.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"aggregate-filter")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

/// `kind` compares without case, so 'B' and 'b' are one kind.
fn setup(conn: &Connection<'_>) {
    for sql in [
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, region TEXT, amount INTEGER, \
         kind TEXT COLLATE NOCASE, note TEXT)",
        "INSERT INTO sales VALUES (1, 'east', 10, 'a', 'x'), (2, 'east', 20, 'B', NULL), \
         (3, 'east', NULL, 'b', 'y'), (4, 'west', 5, 'A', 'x'), (5, 'west', 7, NULL, 'z'), \
         (6, 'north', NULL, 'c', NULL)",
        "CREATE TABLE regions (name TEXT NOT NULL PRIMARY KEY, threshold INTEGER)",
        "INSERT INTO regions VALUES ('east', 15), ('west', 6), ('south', 0)",
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

fn json(value: &str) -> Value {
    Value::Json(value.into())
}

fn one_row(conn: &Connection<'_>, sql: &str) -> Vec<Value> {
    let mut rows = conn.query(sql).unwrap().rows;
    assert_eq!(rows.len(), 1, "{sql}");
    rows.remove(0)
}

#[test]
fn each_aggregate_reads_only_the_rows_its_filter_holds_for() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (aggregate, expected) in [
        ("COUNT(*) FILTER (WHERE amount > 6)", int(3)),
        ("COUNT(amount) FILTER (WHERE region = 'east')", int(2)),
        ("SUM(amount) FILTER (WHERE region = 'east')", int(30)),
        (
            "AVG(amount) FILTER (WHERE region = 'west')",
            Value::Real(6.0),
        ),
        ("MIN(amount) FILTER (WHERE region <> 'east')", int(5)),
        ("MAX(amount) FILTER (WHERE region = 'east')", int(20)),
        // The argument's collation still decides equality and order.
        (
            "COUNT(DISTINCT kind) FILTER (WHERE region = 'east')",
            int(2),
        ),
        ("MIN(kind) FILTER (WHERE region = 'east')", text("a")),
        // JSON_AGG keeps a passing row's NULL, and only passing rows.
        (
            "JSON_AGG(note) FILTER (WHERE region = 'east')",
            json("[\"x\",null,\"y\"]"),
        ),
        (
            "JSON_AGG(DISTINCT note) FILTER (WHERE region <> 'west')",
            json("[\"x\",null,\"y\"]"),
        ),
        (
            "JSON_OBJECT_AGG(note, amount) FILTER (WHERE region = 'west')",
            json("{\"x\":5,\"z\":7}"),
        ),
    ] {
        let sql = format!("SELECT {aggregate} FROM sales");
        assert_eq!(one_row(&conn, &sql), vec![expected], "{sql}");
    }
}

#[test]
fn a_condition_that_holds_for_no_row_gives_each_aggregate_its_empty_value() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        one_row(
            &conn,
            "SELECT COUNT(*) FILTER (WHERE false), COUNT(amount) FILTER (WHERE false), \
             SUM(amount) FILTER (WHERE false), AVG(amount) FILTER (WHERE false), \
             MIN(amount) FILTER (WHERE false) FROM sales"
        ),
        vec![int(0), int(0), Value::Null, Value::Null, Value::Null]
    );
    // amount = amount is NULL, not true, for a NULL amount.
    assert_eq!(
        one_row(
            &conn,
            "SELECT COUNT(*) FILTER (WHERE amount = amount) FROM sales"
        ),
        vec![int(4)]
    );
}

#[test]
fn groups_having_and_ordering_see_filtered_aggregates() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        conn.query(
            "SELECT region, COUNT(*) FILTER (WHERE amount > 6), \
             SUM(amount) FILTER (WHERE kind = 'b') FROM sales GROUP BY region \
             HAVING COUNT(*) FILTER (WHERE note IS NOT NULL) > 0 ORDER BY region"
        )
        .unwrap()
        .rows,
        vec![
            vec![text("east"), int(2), int(20)],
            vec![text("west"), int(1), Value::Null],
        ]
    );
    assert_eq!(
        conn.query(
            "SELECT region FROM sales GROUP BY region \
             ORDER BY COUNT(*) FILTER (WHERE amount IS NULL) DESC, region"
        )
        .unwrap()
        .rows,
        vec![vec![text("east")], vec![text("north")], vec![text("west")]]
    );
}

#[test]
fn a_filter_reads_columns_nothing_else_in_the_query_reads() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        one_row(
            &conn,
            "SELECT SUM(amount) FILTER (WHERE note = 'x') FROM sales"
        ),
        vec![int(15)]
    );
    assert_eq!(
        one_row(
            &conn,
            "SELECT COUNT(*) FILTER (WHERE kind = 'a') FROM sales WHERE amount < 8"
        ),
        vec![int(1)]
    );
    // A table with a column default decodes only the columns a query reads.
    conn.execute(
        "CREATE TABLE tagged (id INTEGER NOT NULL PRIMARY KEY, amount INTEGER, \
         tag TEXT DEFAULT 'none')",
    )
    .unwrap();
    conn.execute("INSERT INTO tagged VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'x')")
        .unwrap();
    assert_eq!(
        one_row(
            &conn,
            "SELECT SUM(amount) FILTER (WHERE tag = 'x') FROM tagged"
        ),
        vec![int(40)]
    );
    // So does a join.
    assert_eq!(
        one_row(
            &conn,
            "SELECT SUM(sales.amount) FILTER (WHERE regions.threshold > 10) \
             FROM sales JOIN regions ON regions.name = sales.region"
        ),
        vec![int(30)]
    );
}

#[test]
fn a_filter_may_hold_a_subquery_and_read_the_outer_row() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for (aggregate, expected) in [
        (
            "COUNT(*) FILTER (WHERE region IN (SELECT name FROM regions WHERE threshold < 10))",
            int(2),
        ),
        (
            "COUNT(*) FILTER (WHERE EXISTS \
             (SELECT 1 FROM regions WHERE regions.name = sales.region AND threshold > 10))",
            int(3),
        ),
        (
            "SUM(amount) FILTER (WHERE amount > (SELECT threshold FROM regions \
             WHERE regions.name = sales.region))",
            int(27),
        ),
    ] {
        let sql = format!("SELECT {aggregate} FROM sales");
        assert_eq!(one_row(&conn, &sql), vec![expected], "{sql}");
    }
    for sql in [
        // The filter reads the outer row.
        "SELECT r.name, (SELECT COUNT(*) FILTER (WHERE sales.amount > r.threshold) \
         FROM sales WHERE sales.region = r.name) FROM regions AS r ORDER BY r.name",
        // Only its own rows; the same values through the hashed lookup.
        "SELECT r.name, (SELECT COUNT(*) FILTER (WHERE sales.amount > 6 \
         AND sales.amount <> 10) FROM sales WHERE sales.region = r.name) \
         FROM regions AS r ORDER BY r.name",
    ] {
        assert_eq!(
            conn.query(sql).unwrap().rows,
            vec![
                vec![text("east"), int(1)],
                vec![text("south"), int(0)],
                vec![text("west"), int(1)],
            ],
            "{sql}"
        );
    }
}

#[test]
fn every_execution_path_applies_the_filter() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let sql = "SELECT region, SUM(amount) FILTER (WHERE amount > 6) AS big FROM sales \
               GROUP BY region ORDER BY region";
    let expected = vec![
        vec![text("east"), int(30)],
        vec![text("north"), Value::Null],
        vec![text("west"), int(7)],
    ];
    assert_eq!(conn.query(sql).unwrap().rows, expected);
    let prepared = conn.prepare(sql).unwrap();
    assert_eq!(prepared.query_collect(&[]).unwrap().rows, expected);
    conn.execute(&format!("CREATE VIEW big_sales AS {sql}"))
        .unwrap();
    assert_eq!(
        conn.query("SELECT region, big FROM big_sales ORDER BY region")
            .unwrap()
            .rows,
        expected
    );
    conn.execute("BEGIN").unwrap();
    assert_eq!(conn.query(sql).unwrap().rows, expected);
    conn.execute(
        "UPDATE regions SET threshold = (SELECT COUNT(*) FILTER (WHERE sales.amount > 6) \
         FROM sales WHERE sales.region = regions.name)",
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT name, threshold FROM regions ORDER BY name")
            .unwrap()
            .rows,
        vec![
            vec![text("east"), int(2)],
            vec![text("south"), int(0)],
            vec![text("west"), int(1)],
        ]
    );
}

#[test]
fn a_filter_binds_parameters_and_resolves_names_like_any_clause() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let prepared = conn
        .prepare("SELECT COUNT(*) FILTER (WHERE amount > $1) FROM sales")
        .unwrap();
    assert_eq!(
        prepared.query_collect(&[int(6)]).unwrap().rows,
        vec![vec![int(3)]]
    );
    let unknown = conn.query("SELECT COUNT(*) FILTER (WHERE nope.amount > 6) FROM sales");
    assert!(
        matches!(&unknown, Err(SqlError::ColumnNotFound(name)) if name == "nope.amount"),
        "{unknown:?}"
    );
    conn.execute("CREATE TABLE refunds (id INTEGER NOT NULL PRIMARY KEY, amount INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO refunds VALUES (1, 3), (2, 4)")
        .unwrap();
    let ambiguous = conn.query(
        "SELECT COUNT(*) FILTER (WHERE amount > 3) FROM sales JOIN refunds \
         ON refunds.id = sales.id",
    );
    assert!(
        matches!(&ambiguous, Err(SqlError::AmbiguousColumn(name)) if name == "amount"),
        "{ambiguous:?}"
    );
    // An UPDATE target's alias reaches a filter inside a SET subquery.
    conn.execute(
        "UPDATE regions AS r SET threshold = (SELECT COUNT(*) FILTER \
         (WHERE sales.amount > r.threshold) FROM sales WHERE sales.region = r.name)",
    )
    .unwrap();
    assert_eq!(
        conn.query("SELECT name, threshold FROM regions ORDER BY name")
            .unwrap()
            .rows,
        vec![
            vec![text("east"), int(1)],
            vec![text("south"), int(0)],
            vec![text("west"), int(1)],
        ]
    );
}

#[test]
fn laterals_windows_and_plans_carry_the_filter() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert_eq!(
        conn.query(
            "SELECT r.name, d.c FROM regions AS r, LATERAL (SELECT COUNT(*) FILTER \
             (WHERE sales.amount > r.threshold) AS c FROM sales \
             WHERE sales.region = r.name) AS d ORDER BY r.name"
        )
        .unwrap()
        .rows,
        vec![
            vec![text("east"), int(1)],
            vec![text("south"), int(0)],
            vec![text("west"), int(1)],
        ]
    );
    assert_eq!(
        conn.query("SELECT COUNT(*) FILTER (WHERE amount > 6), ROW_NUMBER() OVER () FROM sales")
            .unwrap()
            .rows,
        vec![vec![int(3), int(1)]]
    );
    let plan = conn
        .query(
            "EXPLAIN SELECT COUNT(*) FILTER (WHERE region IN (SELECT name FROM regions)) \
             FROM sales",
        )
        .unwrap();
    assert!(
        plan.rows.contains(&vec![text("SUBQUERY")]),
        "{:?}",
        plan.rows
    );
}

#[test]
fn a_materialized_view_keeps_its_filter_and_refuses_a_volatile_one() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute(
        "CREATE MATERIALIZED VIEW big AS SELECT COUNT(*) FILTER (WHERE amount > 6) AS n \
         FROM sales",
    )
    .unwrap();
    conn.execute("INSERT INTO sales VALUES (7, 'west', 50, 'd', NULL), (8, 'west', 1, 'd', NULL)")
        .unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW big").unwrap();
    assert_eq!(
        conn.query("SELECT n FROM big").unwrap().rows,
        vec![vec![int(4)]]
    );
    let volatile = conn.execute(
        "CREATE MATERIALIZED VIEW lucky AS SELECT COUNT(*) FILTER (WHERE RANDOM() > 0) \
         FROM sales",
    );
    assert!(
        matches!(&volatile, Err(SqlError::Unsupported(message)) if message.contains("random")),
        "{volatile:?}"
    );
}

#[test]
fn a_filtered_aggregate_is_named_by_its_filter() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let result = conn
        .query(
            "SELECT COUNT(*) FILTER (WHERE amount > 6), \
             SUM(amount) FILTER (WHERE amount > 6) AS big FROM sales",
        )
        .unwrap();
    assert_eq!(
        result.columns,
        ["COUNT(*) FILTER (WHERE amount > 6)", "big"]
    );
}

#[test]
fn a_filter_that_cannot_apply_is_an_error() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for sql in [
        "SELECT UPPER(note) FILTER (WHERE amount > 6) FROM sales",
        "SELECT SUM(amount) FILTER (WHERE COUNT(*) > 1) FROM sales",
        "SELECT SUM(amount) FILTER (WHERE ROW_NUMBER() OVER () > 1) FROM sales",
        "SELECT SUM(amount) FILTER (WHERE amount > 6) OVER () FROM sales",
    ] {
        for (path, result) in [
            ("query", conn.query(sql).map(drop)),
            ("prepare", conn.prepare(sql).map(drop)),
        ] {
            assert!(
                matches!(result, Err(SqlError::Unsupported(_))),
                "{path}: {sql}: {result:?}"
            );
        }
    }
}
