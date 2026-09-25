use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn setup_categories_products(conn: &Connection) {
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE p (id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT, price INTEGER)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO c VALUES (1, 'Books'), (2, 'Toys'), (3, 'Empty')")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (10, 1, 'Rust', 50), (11, 1, 'SQL', 30), (12, 1, 'Go', 40), (13, 2, 'Lego', 100), (14, 2, 'Doll', 25)")
        .unwrap();
}

#[test]
fn lateral_top_n_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, p.name FROM c, LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id ORDER BY price DESC LIMIT 2
         ) p ORDER BY c.id, p.name",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[3][0], Value::Integer(2));
}

#[test]
fn lateral_left_join_preserves_outer_when_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id FROM c LEFT JOIN LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id LIMIT 1
         ) p ON true ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn lateral_cross_join_form() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, p.name FROM c CROSS JOIN LATERAL (
            SELECT name FROM p WHERE p.cat_id = c.id LIMIT 1
         ) p ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn lateral_non_equality_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, budget INTEGER)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, price INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO c VALUES (1, 50), (2, 200)")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (10, 30), (11, 100), (12, 150)")
        .unwrap();

    let qr = query(
        &conn,
        "SELECT c.id, p.id FROM c, LATERAL (
            SELECT id FROM p WHERE p.price < c.budget
         ) p ORDER BY c.id, p.id",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn non_lateral_derived_table_in_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT sub.cat_id, sub.cnt FROM (
            SELECT cat_id, COUNT(*) AS cnt FROM p GROUP BY cat_id
         ) sub ORDER BY sub.cat_id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
}

#[test]
fn non_lateral_derived_table_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let qr = query(
        &conn,
        "SELECT c.id, sub.cnt FROM c INNER JOIN (
            SELECT cat_id, COUNT(*) AS cnt FROM p GROUP BY cat_id
         ) sub ON c.id = sub.cat_id ORDER BY c.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn lateral_right_join_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let result = conn.execute(
        "SELECT * FROM c RIGHT JOIN LATERAL (SELECT name FROM p WHERE p.cat_id = c.id) p ON true",
    );
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}

#[test]
fn lateral_full_outer_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_categories_products(&conn);

    let result = conn.execute(
        "SELECT * FROM c FULL OUTER JOIN LATERAL (SELECT name FROM p WHERE p.cat_id = c.id) p ON true",
    );
    assert!(matches!(result, Err(SqlError::Unsupported(_))));
}

fn int(value: i64) -> Value {
    Value::Integer(value)
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

/// `setup_categories_products` plus tables whose columns share names with
/// them, so a reference resolved against the wrong source reads a value.
fn setup_lateral_scopes(conn: &Connection) {
    setup_categories_products(conn);
    for sql in [
        "ALTER TABLE c ADD COLUMN budget INTEGER",
        "UPDATE c SET budget = CASE id WHEN 1 THEN 50 WHEN 2 THEN 200 ELSE 0 END",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
        "INSERT INTO t VALUES (1, 5), (2, -5)",
        "CREATE TABLE u (id INTEGER PRIMARY KEY, k INTEGER, a INTEGER)",
        "INSERT INTO u VALUES (10, 1, -100), (20, 2, 100)",
    ] {
        conn.execute(sql).unwrap();
    }
}

/// Runs each query in autocommit and inside a transaction.
fn assert_rows_each_way(conn: &Connection, cases: &[(&str, Vec<Vec<Value>>)]) {
    for transaction in [false, true] {
        if transaction {
            assert_ok(conn.execute("BEGIN").unwrap());
        }
        for (sql, expected) in cases {
            assert_eq!(
                &query(conn, sql).rows,
                expected,
                "{sql} (transaction: {transaction})"
            );
        }
        if transaction {
            assert_ok(conn.execute("COMMIT").unwrap());
        }
    }
}

#[test]
fn lateral_reads_the_outer_row_wherever_it_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_lateral_scopes(&conn);
    let first_u = vec![vec![int(1), int(10)]];
    assert_rows_each_way(
        &conn,
        &[
            (
                "SELECT t.id, d.uid FROM t, LATERAL (SELECT u.id AS uid FROM u \
                 WHERE u.k = t.id AND CASE WHEN t.a > 0 THEN true ELSE false END) AS d",
                first_u.clone(),
            ),
            (
                "SELECT t.id, d.uid FROM t, LATERAL (SELECT u.id AS uid FROM u \
                 WHERE u.k = t.id AND COALESCE(t.a, 0) > 0) AS d",
                first_u.clone(),
            ),
            (
                "SELECT t.id, d.uid FROM t, LATERAL (SELECT u.id AS uid FROM u \
                 WHERE u.k = t.id AND EXISTS (SELECT 1 FROM t AS o WHERE o.id = t.id AND t.a > 0)) AS d",
                first_u,
            ),
            (
                "SELECT t.id, d.x FROM t, LATERAL (SELECT CASE WHEN u.k = t.id THEN t.a + 1 END AS x \
                 FROM u WHERE u.k = t.id) AS d ORDER BY 1",
                vec![vec![int(1), int(6)], vec![int(2), int(-4)]],
            ),
            (
                "SELECT c.id, d.id FROM c, LATERAL (SELECT id FROM p WHERE price < budget) AS d \
                 ORDER BY 1, 2",
                [(1, 11), (1, 12), (1, 14), (2, 10), (2, 11), (2, 12), (2, 13), (2, 14)]
                    .map(|(c, p)| vec![int(c), int(p)])
                    .to_vec(),
            ),
            (
                "SELECT c.id, d.name FROM c, LATERAL (SELECT name FROM p WHERE p.cat_id = c.id \
                 ORDER BY price DESC LIMIT 3 - c.id) AS d ORDER BY 1, 2",
                vec![
                    vec![int(1), text("Go")],
                    vec![int(1), text("Rust")],
                    vec![int(2), text("Lego")],
                ],
            ),
            (
                "SELECT c.id, d.n FROM c, LATERAL (SELECT p.cat_id, COUNT(*) AS n FROM p \
                 GROUP BY p.cat_id HAVING p.cat_id = c.id) AS d ORDER BY 1",
                vec![vec![int(1), int(3)], vec![int(2), int(2)]],
            ),
            (
                "SELECT c.id, d.name FROM c, LATERAL (SELECT p.name AS name FROM p \
                 JOIN c AS c2 ON c2.id = p.cat_id AND c2.id = c.id) AS d ORDER BY 1, 2",
                vec![
                    vec![int(1), text("Go")],
                    vec![int(1), text("Rust")],
                    vec![int(1), text("SQL")],
                    vec![int(2), text("Doll")],
                    vec![int(2), text("Lego")],
                ],
            ),
        ],
    );
}

#[test]
fn lateral_sources_hide_outer_sources_of_the_same_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_lateral_scopes(&conn);
    let second_t = vec![vec![int(1), int(-5)], vec![int(2), int(-5)]];
    assert_rows_each_way(
        &conn,
        &[
            (
                "SELECT t.id, d.x FROM t, LATERAL (SELECT t.a AS x FROM t WHERE t.id = 2) AS d \
                 ORDER BY 1",
                second_t.clone(),
            ),
            (
                "SELECT t.id, d.x FROM t, LATERAL (SELECT t.a AS x FROM t \
                 WHERE t.id = t.id AND t.id = 2) AS d ORDER BY 1",
                second_t,
            ),
            (
                "SELECT t.id, d.x FROM t, LATERAL (SELECT t.a AS x FROM u AS t WHERE t.k = 1) AS d \
                 ORDER BY 1",
                vec![vec![int(1), int(-100)], vec![int(2), int(-100)]],
            ),
        ],
    );
}

#[test]
fn lateral_rows_follow_offset_projection_and_ordering_per_outer_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_lateral_scopes(&conn);
    let top_prices = vec![vec![int(1), int(50)], vec![int(2), int(100)]];
    assert_rows_each_way(
        &conn,
        &[
            (
                "SELECT c.id, d.name FROM c, LATERAL (SELECT name FROM p WHERE p.cat_id = c.id \
                 ORDER BY price DESC LIMIT 1 OFFSET 1) AS d ORDER BY 1",
                vec![vec![int(1), text("Go")], vec![int(2), text("Doll")]],
            ),
            (
                "SELECT * FROM c, LATERAL (SELECT price * 2 AS dbl FROM p WHERE p.cat_id = c.id) AS d \
                 WHERE c.id = 2 ORDER BY 4",
                vec![
                    vec![int(2), text("Toys"), int(200), int(50)],
                    vec![int(2), text("Toys"), int(200), int(200)],
                ],
            ),
            (
                "SELECT c.id, d.price FROM c, LATERAL (SELECT price FROM p WHERE p.cat_id = c.id \
                 ORDER BY 1 DESC LIMIT 1) AS d ORDER BY 1",
                top_prices.clone(),
            ),
            (
                "SELECT c.id, d.pr FROM c, LATERAL (SELECT price AS pr FROM p WHERE p.cat_id = c.id \
                 ORDER BY pr DESC LIMIT 1) AS d ORDER BY 1",
                top_prices,
            ),
        ],
    );
    let qr = query(
        &conn,
        "SELECT * FROM c, LATERAL (SELECT price * 2 AS dbl FROM p WHERE p.cat_id = c.id) AS d",
    );
    assert_eq!(qr.columns.len(), 4, "{:?}", qr.columns);
}

#[test]
fn lateral_correlation_compares_as_equals_does() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)",
        "INSERT INTO n VALUES (1, 'A')",
        "CREATE TABLE m (id INTEGER PRIMARY KEY, tag TEXT)",
        "INSERT INTO m VALUES (7, 'a')",
        "CREATE TABLE days (id INTEGER PRIMARY KEY, d DATE)",
        "INSERT INTO days VALUES (1, '2024-01-01')",
        "CREATE TABLE moments (id INTEGER PRIMARY KEY, t TIMESTAMP)",
        "INSERT INTO moments VALUES (5, '2024-01-01 00:00:00')",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_rows_each_way(
        &conn,
        &[
            // `=` takes the collation of its left column.
            (
                "SELECT n.id, d.id FROM n, LATERAL (SELECT m.id FROM m WHERE n.name = m.tag) AS d",
                vec![vec![int(1), int(7)]],
            ),
            (
                "SELECT n.id, d.id FROM n, LATERAL (SELECT m.id FROM m WHERE m.tag = n.name) AS d",
                vec![],
            ),
            // A DATE equals the TIMESTAMP at its midnight.
            (
                "SELECT days.id, d.id FROM days, \
                 LATERAL (SELECT moments.id FROM moments WHERE moments.t = days.d) AS d",
                vec![vec![int(1), int(5)]],
            ),
            // A column a LATERAL item projects keeps its collation.
            (
                "SELECT n.id, d.nm FROM n, \
                 LATERAL (SELECT n2.name AS nm FROM n AS n2 WHERE n2.id = n.id) AS d \
                 WHERE d.nm = 'a'",
                vec![vec![int(1), text("A")]],
            ),
        ],
    );
}

#[test]
fn lateral_items_of_other_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_lateral_scopes(&conn);
    assert_rows_each_way(
        &conn,
        &[
            (
                "SELECT c.id, d.n FROM c, LATERAL (SELECT COUNT(*) AS n FROM p) AS d ORDER BY 1",
                vec![
                    vec![int(1), int(5)],
                    vec![int(2), int(5)],
                    vec![int(3), int(5)],
                ],
            ),
            (
                "SELECT c.id, d.name FROM c LEFT JOIN LATERAL (SELECT name, price FROM p \
                 WHERE p.cat_id = c.id) AS d ON d.price > 40 ORDER BY 1, 2",
                vec![
                    vec![int(1), text("Rust")],
                    vec![int(2), text("Lego")],
                    vec![int(3), Value::Null],
                ],
            ),
            (
                "SELECT c.id, d.name, e.x FROM c, LATERAL (SELECT name, price FROM p \
                 WHERE p.cat_id = c.id ORDER BY price DESC LIMIT 1) AS d, \
                 LATERAL (SELECT d.price * 2 AS x) AS e ORDER BY 1",
                vec![
                    vec![int(1), text("Rust"), int(100)],
                    vec![int(2), text("Lego"), int(200)],
                ],
            ),
            (
                "SELECT c.id, d.v FROM c, LATERAL (SELECT c.budget AS v UNION ALL SELECT c.id) AS d \
                 ORDER BY 1, 2",
                [(1, 1), (1, 50), (2, 2), (2, 200), (3, 0), (3, 3)]
                    .map(|(c, v)| vec![int(c), int(v)])
                    .to_vec(),
            ),
        ],
    );
}
