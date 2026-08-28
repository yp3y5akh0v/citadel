use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query_result(result: ExecutionResult) -> QueryResult {
    match result {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn explain_lines(conn: &Connection<'_>, sql: &str) -> Vec<String> {
    let qr = query_result(conn.execute(sql).unwrap());
    assert_eq!(qr.columns, vec!["plan"]);
    qr.rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.to_string(),
            other => panic!("expected Text, got {other:?}"),
        })
        .collect()
}

fn setup_schema(conn: &Connection<'_>) {
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, email TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name_age ON users (name, age)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_email ON users (email)")
        .unwrap();
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL)",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_user_id ON orders (user_id)")
        .unwrap();
}

#[test]
fn explain_returns_plan_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let qr = query_result(conn.execute("EXPLAIN SELECT * FROM users").unwrap());
    assert_eq!(qr.columns, vec!["plan"]);
}

#[test]
fn explain_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users");
    // The count is the table's exact size, so an empty table reads as 0 rather
    // than being omitted: "no rows" and "size unknown" are different answers.
    assert_eq!(lines, vec!["SCAN TABLE users rows=0"]);
}

/// A full scan reports what it will actually read.
#[test]
fn explain_seq_scan_reports_the_table_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    for n in 0..7 {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age) VALUES ({n}, 'u{n}', {n})"
        ))
        .unwrap();
    }

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users");

    assert_eq!(lines, vec!["SCAN TABLE users rows=7"]);
}

/// End to end: EXPLAIN must name the strategy that actually runs.
///
/// The other assertions here use `contains`, and the label line embeds the scan
/// line, so they pass either way; this asserts the label itself.
#[test]
fn explain_names_the_fused_strategy_that_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    for n in 0..20 {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age) VALUES ({n}, 'u{n}', {n})"
        ))
        .unwrap();
    }

    let count = explain_lines(&conn, "EXPLAIN SELECT COUNT(*) FROM users");
    assert!(
        count.iter().any(|l| l.contains("COUNT(*) FROM CATALOG")),
        "a bare COUNT(*) is answered from the catalog, and EXPLAIN must say so: {count:?}"
    );

    let topk = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users ORDER BY age DESC LIMIT 5",
    );
    assert!(
        topk.iter().any(|l| l.contains("TOPK SCAN")),
        "ORDER BY with a LIMIT runs as one fused pass: {topk:?}"
    );

    // A fused strategy is one node, so the steps it subsumes are not also
    // listed as if they ran separately.
    assert!(
        !topk.iter().any(|l| l.as_str() == "SORT"),
        "the fused top-k does not sort as a separate step: {topk:?}"
    );
}

fn seeded(conn: &Connection<'_>, n: i64) {
    for k in 0..n {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age) VALUES ({k}, 'u{k}', {k})"
        ))
        .unwrap();
    }
}

/// ANALYZE used to be refused outright at the parser.
#[test]
fn explain_analyze_runs_and_reports_measured_time_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 12);

    let lines = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users");

    assert!(
        lines[0].contains("actual time="),
        "ANALYZE must report a measured time: {lines:?}"
    );
    assert!(
        lines[0].contains("emitted=12"),
        "ANALYZE must report the rows the query actually returned: {lines:?}"
    );
}

/// A fused strategy is ONE node with one time, per the wording this feature
/// commits to. It must not print a time per plan line, because only one
/// duration was measured and the rest would be invented.
#[test]
fn explain_analyze_times_one_node_not_every_line() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 12);

    let lines = explain_lines(
        &conn,
        "EXPLAIN ANALYZE SELECT * FROM users WHERE age > 3 ORDER BY age LIMIT 4",
    );

    let timed = lines.iter().filter(|l| l.contains("actual time=")).count();
    assert_eq!(timed, 1, "exactly one node is measured: {lines:?}");
    assert!(
        lines[0].contains("emitted=4"),
        "the LIMIT is what the query returned: {lines:?}"
    );
}

/// `scanned` is the half that shows selectivity. A filter that keeps one
/// row out of many must report reading the many, not the one - otherwise the
/// number is just `emitted` under a second name.
#[test]
fn explain_analyze_reports_rows_scanned_not_just_returned() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 200);

    let lines = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users WHERE age = 7");

    assert!(lines[0].contains("emitted=1"), "one row matches: {lines:?}");
    let scanned = lines[0]
        .split("scanned=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("no scanned= reported: {lines:?}"));
    assert!(
        scanned >= 200,
        "a full-table filter reads every row; scanned={scanned} in {lines:?}"
    );
}

/// The counter must not leak between statements: a second ANALYZE reports its
/// own reads, not the running total since the database opened.
#[test]
fn explain_analyze_scanned_is_per_statement_not_cumulative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 50);

    let sql = "EXPLAIN ANALYZE SELECT * FROM users WHERE age = 7";
    let first = explain_lines(&conn, sql);
    let second = explain_lines(&conn, sql);

    let scanned_of = |lines: &[String]| -> u64 {
        lines[0]
            .split("scanned=")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("no scanned=: {lines:?}"))
    };

    assert_eq!(
        scanned_of(&first),
        scanned_of(&second),
        "the second run reported a running total rather than its own reads"
    );
}

#[test]
fn explain_analyze_scan_count_matches_an_enclosing_local_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 200);

    let enclosing = db.measure_scans();
    let lines = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users WHERE age = 7");
    let reported = lines[0]
        .split("scanned=")
        .nth(1)
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("no scanned= reported: {lines:?}"));

    assert_eq!(
        reported,
        enclosing.rows_scanned(),
        "the nested EXPLAIN span did not count the same operation-local scans"
    );
}

/// A scan stopped early by a LIMIT reports the rows it actually read, which is
/// fewer than the table holds - the counter follows the scan, not the schema.
#[test]
fn explain_analyze_scanned_follows_the_scan_not_the_table_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 500);

    let lines = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users LIMIT 3");

    let scanned = lines[0]
        .split("scanned=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("no scanned=: {lines:?}"));
    assert!(
        scanned < 500,
        "a LIMIT stops the scan early; scanned={scanned} of 500 in {lines:?}"
    );
    assert!(lines[0].contains("emitted=3"), "{lines:?}");
}

/// Plain EXPLAIN describes without executing, so it carries no measurements at
/// all - and must not acquire any now that ANALYZE exists.
#[test]
fn plain_explain_reports_no_measurements() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 5);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users");

    assert!(
        !lines.iter().any(|l| l.contains("actual time=")),
        "plain EXPLAIN measured something it never ran: {lines:?}"
    );
}

/// EXPLAIN ANALYZE always executes: `compile` declines an EXPLAIN, so it never
/// consults the result cache and a second run measures a second execution
/// rather than replaying the first.
#[test]
fn explain_analyze_measures_every_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 8);

    let first = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users");
    let second = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users");

    for lines in [&first, &second] {
        assert!(lines[0].contains("actual time="), "{lines:?}");
        assert!(lines[0].contains("emitted=8"), "{lines:?}");
        assert!(
            !lines.iter().any(|l| l.contains("CACHE HIT")),
            "an EXPLAIN never reaches the result cache: {lines:?}"
        );
    }
}

/// ANALYZE of a write actually performs the write, as PostgreSQL's does. The
/// point of asserting it is that the behaviour is deliberate and visible rather
/// than a surprise discovered in production.
#[test]
fn explain_analyze_of_a_write_really_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 4);

    let lines = explain_lines(
        &conn,
        "EXPLAIN ANALYZE UPDATE users SET name = 'renamed' WHERE id = 1",
    );
    assert!(lines[0].contains("actual time="), "{lines:?}");

    let after = query_result(
        conn.execute("SELECT COUNT(*) FROM users WHERE name = 'renamed'")
            .unwrap(),
    );
    assert_eq!(after.rows[0][0], Value::Integer(1));
}

#[test]
fn parenthesized_explain_analyze_executes_and_measures() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 3);

    let lines = explain_lines(
        &conn,
        "EXPLAIN (ANALYZE TRUE) UPDATE users SET name = 'changed' WHERE id = 1",
    );

    assert!(lines[0].contains("actual time="), "{lines:?}");
    let changed = query_result(
        conn.execute("SELECT COUNT(*) FROM users WHERE name = 'changed'")
            .unwrap(),
    );
    assert_eq!(changed.rows[0][0], Value::Integer(1));
}

#[test]
fn explain_analyze_validates_the_plan_before_executing_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("BEGIN").unwrap();
    let error = conn
        .execute("EXPLAIN ANALYZE CREATE TABLE ghost (id INTEGER PRIMARY KEY)")
        .expect_err("CREATE TABLE has no supported EXPLAIN plan");
    assert!(matches!(error, citadel_sql::SqlError::Unsupported(_)));
    conn.execute("COMMIT")
        .expect("an unsupported plan must not poison an untouched transaction");

    assert!(conn.execute("SELECT * FROM ghost").is_err());
}

#[test]
fn explain_analyze_describes_the_input_before_a_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 3);

    let lines = explain_lines(&conn, "EXPLAIN ANALYZE DELETE FROM users WHERE id = 1");

    assert!(
        lines[0].contains("of 3 rows"),
        "the plan must describe the three-row input, not the two-row result: {lines:?}"
    );
}

#[test]
fn explain_counts_a_temp_tables_backing_storage() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO tmp VALUES (1), (2), (3)")
        .unwrap();

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM tmp");

    assert_eq!(lines, vec!["SCAN TABLE tmp rows=3"]);
}

#[test]
fn explain_analyze_does_not_claim_zero_scans_for_a_returned_point_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    seeded(&conn, 3);

    let lines = explain_lines(&conn, "EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1");

    assert!(lines[0].contains("emitted=1"), "{lines:?}");
    assert!(
        !lines[0].contains("scanned=0"),
        "a successful point read examined data but the lane has no scan counter: {lines:?}"
    );
}

/// A plain scan has no fused strategy to name, so it keeps its ordinary lines.
#[test]
fn explain_leaves_an_unfused_query_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users");

    assert_eq!(lines, vec!["SCAN TABLE users rows=0"]);
}

/// A search reports the size it is searching within, phrased so it cannot be
/// read as "this returns that many rows".
#[test]
fn explain_search_says_what_the_count_refers_to() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    for n in 0..7 {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age) VALUES ({n}, 'u{n}', {n})"
        ))
        .unwrap();
    }

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE id = 3");

    assert!(
        lines[0].contains("of 7 rows"),
        "a search line must not read as a result-size estimate: {lines:?}"
    );
    assert!(
        !lines[0].contains("rows=7"),
        "a search line must not claim it returns the whole table: {lines:?}"
    );
}

#[test]
fn explain_seq_scan_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE age > 30");
    assert!(lines.iter().any(|l| l.starts_with("SCAN TABLE users")));
    assert!(lines.contains(&"FILTER".to_string()));
}

#[test]
fn explain_pk_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE id = 5");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    assert!(lines[0].contains("id = 5"));
}

#[test]
fn explain_index_scan_equality() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE name = 'Alice'");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
    assert!(lines[0].contains("name = ?"));
}

#[test]
fn explain_unique_index_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users WHERE email = 'alice@test.com'",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX idx_email"));
}

#[test]
fn explain_composite_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users WHERE name = 'Alice' AND age = 30",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("USING INDEX idx_name_age"));
    assert!(lines[0].contains("name = ?"));
    assert!(lines[0].contains("age = ?"));
}

#[test]
fn explain_index_range_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE name > 'M'");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
    assert!(lines[0].contains("name > ?"));
}

#[test]
fn explain_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.iter().any(|l| l.contains("SCAN TABLE users AS u")));
    assert!(lines.iter().any(|l| l.contains("SCAN TABLE orders AS o")));
    assert!(lines.contains(&"NESTED LOOP".to_string()));
}

#[test]
fn explain_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.contains(&"LEFT JOIN".to_string()));
}

#[test]
fn explain_right_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
    );
    assert!(lines.contains(&"RIGHT JOIN".to_string()));
}

#[test]
fn explain_cross_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users CROSS JOIN orders");
    assert!(lines.contains(&"CROSS JOIN".to_string()));
}

#[test]
fn explain_multi_way_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER)")
        .unwrap();

    let lines = explain_lines(&conn,
        "EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id");
    assert!(lines.iter().any(|l| l.contains("users")));
    assert!(lines.iter().any(|l| l.contains("orders")));
    assert!(lines.iter().any(|l| l.contains("items")));
}

#[test]
fn explain_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT DISTINCT name FROM users");
    assert!(lines.contains(&"DISTINCT".to_string()));
}

#[test]
fn explain_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users ORDER BY name");
    assert!(lines.contains(&"SORT".to_string()));
}

#[test]
fn explain_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users LIMIT 10");
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

#[test]
fn explain_offset_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users LIMIT 10 OFFSET 5");
    assert!(lines.contains(&"OFFSET 5".to_string()));
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

#[test]
fn explain_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT name, COUNT(*) FROM users GROUP BY name",
    );
    assert!(lines.contains(&"GROUP BY".to_string()));
}

#[test]
fn explain_all_features() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT DISTINCT name FROM users ORDER BY name LIMIT 10 OFFSET 5",
    );
    assert!(lines.iter().any(|l| l.starts_with("SCAN TABLE users")));
    assert!(lines.contains(&"DISTINCT".to_string()));
    assert!(lines.contains(&"SORT".to_string()));
    assert!(lines.contains(&"OFFSET 5".to_string()));
    assert!(lines.contains(&"LIMIT 10".to_string()));
}

#[test]
fn explain_update_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("UPDATE"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
}

#[test]
fn explain_update_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN UPDATE users SET name = 'Bob' WHERE age > 30",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("UPDATE"));
    assert!(lines[0].contains("SCAN TABLE users"));
}

#[test]
fn explain_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN DELETE FROM users WHERE name = 'Alice'");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("DELETE FROM"));
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING INDEX"));
}

#[test]
fn explain_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'Alice')",
    );
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("INSERT INTO users"));
}

#[test]
fn explain_no_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let lines = explain_lines(&conn, "EXPLAIN SELECT 1 + 2");
    assert_eq!(lines, vec!["CONSTANT ROW"]);
}

#[test]
fn explain_explain_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let result = conn.execute("EXPLAIN EXPLAIN SELECT * FROM users");
    assert!(result.is_err());
}

#[test]
fn explain_create_table_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    let result = conn.execute("EXPLAIN CREATE TABLE t (id INTEGER PRIMARY KEY)");
    assert!(result.is_err());
}

#[test]
fn explain_table_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users AS u");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("SCAN TABLE users AS u"));
}

#[test]
fn explain_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    conn.execute("BEGIN").unwrap();
    let lines = explain_lines(&conn, "EXPLAIN SELECT * FROM users WHERE id = 1");
    assert!(lines[0].contains("SEARCH TABLE users"));
    assert!(lines[0].contains("USING PRIMARY KEY"));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn explain_does_not_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("EXPLAIN DELETE FROM users WHERE id = 1")
        .unwrap();

    let qr = query_result(conn.execute("SELECT COUNT(*) FROM users").unwrap());
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn explain_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_schema(&conn);

    let lines = explain_lines(
        &conn,
        "EXPLAIN SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    );
    assert!(lines.contains(&"SUBQUERY".to_string()));
}
