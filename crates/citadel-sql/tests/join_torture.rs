use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"join-torture")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"join-torture")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn query(conn: &mut Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn exec(conn: &mut Connection, sql: &str) {
    conn.execute(sql).unwrap();
}

fn count(conn: &mut Connection, sql: &str) -> i64 {
    let qr = query(conn, sql);
    match &qr.rows[0][0] {
        Value::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

#[test]
fn left_join_all_rows_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(20));
}

#[test]
fn left_join_no_rows_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 99), (20, 98)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[1], Value::Null);
    }
}

#[test]
fn left_join_mixed_matched_unmatched() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 1, 100), (11, 1, 200), (20, 3, 300)",
    );

    let qr = query(
        &mut c,
        "SELECT a.id, a.name, b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id, b.id",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Integer(1),
            Value::Text("x".into()),
            Value::Integer(100)
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![
            Value::Integer(1),
            Value::Text("x".into()),
            Value::Integer(200)
        ]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Integer(2), Value::Text("y".into()), Value::Null]
    );
    assert_eq!(
        qr.rows[3],
        vec![
            Value::Integer(3),
            Value::Text("z".into()),
            Value::Integer(300)
        ]
    );
}

#[test]
fn left_join_where_is_null_finds_unmatched() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, uid INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    );
    exec(&mut c, "INSERT INTO orders VALUES (10, 1), (11, 2)");

    let qr = query(
        &mut c,
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.uid WHERE o.id IS NULL",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Charlie".into()));
}

#[test]
fn left_join_where_is_not_null_keeps_matched() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 3)");

    let qr = query(
        &mut c,
        "SELECT a.id FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.id IS NOT NULL ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn left_join_empty_right_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Null]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Null]);
}

#[test]
fn left_join_empty_left_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO b VALUES (1), (2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn left_join_one_to_many_null_padding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER NOT NULL, val TEXT NOT NULL, score REAL)");
    exec(&mut c, "INSERT INTO parent VALUES (1, 'P1'), (2, 'P2')");
    exec(
        &mut c,
        "INSERT INTO child VALUES (10, 1, 'c1', 3.15), (11, 1, 'c2', NULL)",
    );

    let qr = query(&mut c,
        "SELECT p.name, ch.val, ch.score FROM parent p LEFT JOIN child ch ON p.id = ch.pid ORDER BY p.id, ch.id"
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Text("P1".into()),
            Value::Text("c1".into()),
            Value::Real(3.15)
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![
            Value::Text("P1".into()),
            Value::Text("c2".into()),
            Value::Null
        ]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("P2".into()), Value::Null, Value::Null]
    );
}

#[test]
fn inner_then_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'X'), (2, 'Y')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2)");
    exec(&mut c, "INSERT INTO d VALUES (100, 10)");

    let qr = query(
        &mut c,
        "SELECT a.name, b.id, d.id \
         FROM a JOIN b ON a.id = b.a_id \
         LEFT JOIN d ON b.id = d.b_id \
         ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Text("X".into()),
            Value::Integer(10),
            Value::Integer(100)
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Y".into()), Value::Integer(20), Value::Null]
    );
}

#[test]
fn left_then_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 3)");
    exec(&mut c, "INSERT INTO d VALUES (100, 10), (200, 20)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id \
         FROM a LEFT JOIN b ON a.id = b.a_id \
         JOIN d ON b.id = d.b_id \
         ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn left_then_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");
    exec(&mut c, "INSERT INTO d VALUES (100, 10)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id \
         FROM a LEFT JOIN b ON a.id = b.a_id \
         LEFT JOIN d ON b.id = d.b_id \
         ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Null, Value::Null]
    );
}

#[test]
fn cross_then_inner_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, pair_sum INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10), (20)");
    exec(&mut c, "INSERT INTO d VALUES (1, 11), (2, 22)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id \
         FROM a CROSS JOIN b \
         JOIN d ON a.id + b.id = d.pair_sum \
         ORDER BY d.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(1)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Integer(20), Value::Integer(2)]
    );
}

#[test]
fn four_way_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, t1_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t3 (id INTEGER NOT NULL PRIMARY KEY, t2_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t4 (id INTEGER NOT NULL PRIMARY KEY, t3_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1)");
    exec(&mut c, "INSERT INTO t2 VALUES (10, 1)");
    exec(&mut c, "INSERT INTO t3 VALUES (100, 10)");
    exec(&mut c, "INSERT INTO t4 VALUES (1000, 100)");

    let qr = query(
        &mut c,
        "SELECT t1.id, t2.id, t3.id, t4.id \
         FROM t1 JOIN t2 ON t1.id = t2.t1_id \
         JOIN t3 ON t2.id = t3.t2_id \
         JOIN t4 ON t3.id = t4.t3_id",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Integer(1),
            Value::Integer(10),
            Value::Integer(100),
            Value::Integer(1000),
        ]
    );
}

#[test]
fn four_way_join_partial_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, t1_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t3 (id INTEGER NOT NULL PRIMARY KEY, t2_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t4 (id INTEGER NOT NULL PRIMARY KEY, t3_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1), (2)");
    exec(&mut c, "INSERT INTO t2 VALUES (10, 1), (20, 2)");
    exec(&mut c, "INSERT INTO t3 VALUES (100, 10), (200, 20)");
    exec(&mut c, "INSERT INTO t4 VALUES (1000, 100)");

    let qr = query(
        &mut c,
        "SELECT t1.id, t4.id \
         FROM t1 JOIN t2 ON t1.id = t2.t1_id \
         JOIN t3 ON t2.id = t3.t2_id \
         JOIN t4 ON t3.id = t4.t3_id",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn self_join_hierarchy_two_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, mgr INTEGER)",
    );
    exec(
        &mut c,
        "INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dir', 2), (4, 'Dev', 3)",
    );

    let qr = query(
        &mut c,
        "SELECT e.name, m.name \
         FROM emp e JOIN emp m ON e.mgr = m.id \
         ORDER BY e.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("VP".into()), Value::Text("CEO".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Dir".into()), Value::Text("VP".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Dev".into()), Value::Text("Dir".into())]
    );
}

#[test]
fn self_join_left_includes_root() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, mgr INTEGER)",
    );
    exec(
        &mut c,
        "INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Dev', 1)",
    );

    let qr = query(
        &mut c,
        "SELECT e.name, m.name \
         FROM emp e LEFT JOIN emp m ON e.mgr = m.id \
         ORDER BY e.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Text("CEO".into()), Value::Null]);
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("VP".into()), Value::Text("CEO".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Dev".into()), Value::Text("CEO".into())]
    );
}

#[test]
fn self_join_all_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')",
    );

    let qr = query(&mut c,
        "SELECT a.name, b.name FROM items a CROSS JOIN items b WHERE a.id < b.id ORDER BY a.id, b.id"
    );
    assert_eq!(qr.rows.len(), 6);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("A".into()), Value::Text("B".into())]
    );
    assert_eq!(
        qr.rows[5],
        vec![Value::Text("C".into()), Value::Text("D".into())]
    );
}

#[test]
fn self_join_triangle() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY)",
    );
    exec(&mut c, "INSERT INTO items VALUES (1), (2), (3), (4)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, c.id \
         FROM items a CROSS JOIN items b CROSS JOIN items c \
         WHERE a.id < b.id AND b.id < c.id \
         ORDER BY a.id, b.id, c.id",
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
    );
    assert_eq!(
        qr.rows[3],
        vec![Value::Integer(2), Value::Integer(3), Value::Integer(4)]
    );
}

#[test]
fn join_count_star_vs_count_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'X'), (2, 'Y'), (3, 'Z')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 1)");

    let qr = query(
        &mut c,
        "SELECT a.name, COUNT(*), COUNT(b.id) \
         FROM a LEFT JOIN b ON a.id = b.a_id \
         GROUP BY a.name ORDER BY a.name",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Text("X".into()),
            Value::Integer(2),
            Value::Integer(2)
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![
            Value::Text("Y".into()),
            Value::Integer(1),
            Value::Integer(0)
        ]
    );
    assert_eq!(
        qr.rows[2],
        vec![
            Value::Text("Z".into()),
            Value::Integer(1),
            Value::Integer(0)
        ]
    );
}

#[test]
fn left_join_sum_null_padding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 'X'), (2, 'Y')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 100), (11, 1, 200)");

    let qr = query(&mut c,
        "SELECT a.name, SUM(b.val) FROM a LEFT JOIN b ON a.id = b.a_id GROUP BY a.name ORDER BY a.name"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("X".into()), Value::Integer(300)]
    );
    assert_eq!(qr.rows[1], vec![Value::Text("Y".into()), Value::Null]);
}

#[test]
fn left_join_avg_ignores_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, score REAL NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 4.0), (11, 1, 6.0)");

    let qr = query(
        &mut c,
        "SELECT a.id, AVG(b.score) FROM a LEFT JOIN b ON a.id = b.a_id GROUP BY a.id ORDER BY a.id",
    );
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Real(5.0)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Null]);
}

#[test]
fn left_join_min_max_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 5), (11, 1, 15)");

    let qr = query(
        &mut c,
        "SELECT a.id, MIN(b.val), MAX(b.val) \
         FROM a LEFT JOIN b ON a.id = b.a_id GROUP BY a.id ORDER BY a.id",
    );
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(5), Value::Integer(15)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Null, Value::Null]
    );
}

#[test]
fn join_having_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, uid INTEGER NOT NULL, amt REAL NOT NULL)");
    exec(
        &mut c,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    );
    exec(
        &mut c,
        "INSERT INTO orders VALUES (10, 1, 10.0), (11, 1, 20.0), (12, 2, 5.0)",
    );

    let qr = query(
        &mut c,
        "SELECT u.name, SUM(o.amt) AS total \
         FROM users u JOIN orders o ON u.id = o.uid \
         GROUP BY u.name HAVING total > 10.0",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(qr.rows[0][1], Value::Real(30.0));
}

#[test]
fn join_aggregate_no_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(&mut c, "INSERT INTO b VALUES (100, 1), (200, 3)");

    let qr = query(
        &mut c,
        "SELECT SUM(a.val), COUNT(*) FROM a JOIN b ON a.id = b.a_id",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(40));
    assert_eq!(qr.rows[0][1], Value::Integer(2));
}

#[test]
fn join_read_your_writes_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );

    exec(&mut c, "BEGIN");
    exec(&mut c, "INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(&mut c, "SELECT a.name, b.id FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Alice".into()));

    exec(&mut c, "INSERT INTO b VALUES (20, 2)");
    let qr = query(
        &mut c,
        "SELECT a.name, b.id FROM a JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    exec(&mut c, "COMMIT");
}

#[test]
fn join_rollback_discards_joined_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");

    exec(&mut c, "BEGIN");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");
    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(n, 1);
    exec(&mut c, "ROLLBACK");

    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(n, 0);
}

#[test]
fn join_after_update_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 100)");
    exec(&mut c, "INSERT INTO b VALUES (100, 'old'), (200, 'new')");

    exec(&mut c, "BEGIN");
    exec(&mut c, "UPDATE a SET ref_id = 200 WHERE id = 1");

    let qr = query(&mut c, "SELECT b.name FROM a JOIN b ON a.ref_id = b.id");
    assert_eq!(qr.rows[0][0], Value::Text("new".into()));
    exec(&mut c, "COMMIT");
}

#[test]
fn join_after_delete_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2)");

    exec(&mut c, "BEGIN");
    exec(&mut c, "DELETE FROM b WHERE a_id = 2");

    let qr = query(&mut c, "SELECT a.id FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    exec(&mut c, "ROLLBACK");

    let qr = query(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn join_create_tables_in_txn_then_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "BEGIN");
    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.rows.len(), 1);
    exec(&mut c, "COMMIT");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn left_join_in_txn_with_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");

    exec(&mut c, "BEGIN");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Null);

    exec(&mut c, "ROLLBACK");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Null);
    assert_eq!(qr.rows[1][1], Value::Null);
}

#[test]
fn qualified_columns_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE T1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE T2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO T1 VALUES (1, 'hello')");
    exec(&mut c, "INSERT INTO T2 VALUES (10, 1)");

    let qr = query(&mut c, "SELECT T1.VAL FROM T1 JOIN T2 ON T1.ID = T2.REF_ID");
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn alias_hides_table_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 'hello')");
    exec(&mut c, "INSERT INTO t2 VALUES (10, 1)");

    let qr = query(
        &mut c,
        "SELECT x.val FROM t1 x JOIN t2 y ON x.id = y.ref_id",
    );
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn ambiguous_column_in_where_after_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 10)");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 20)");

    let qr = query(
        &mut c,
        "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id WHERE val > 5",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn ambiguous_column_in_order_by_after_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 10)");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 20)");

    let qr = query(
        &mut c,
        "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY val",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(10), Value::Integer(20)]);
}

#[test]
fn qualified_disambiguation_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 10), (2, 20)");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 100), (2, 200)");

    let qr = query(
        &mut c,
        "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > 15",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(20), Value::Integer(200)]);
}

#[test]
fn qualified_disambiguation_in_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 20), (2, 10)");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 200), (2, 100)");

    let qr = query(
        &mut c,
        "SELECT t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.val",
    );
    assert_eq!(qr.rows[0], vec![Value::Integer(10), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(20), Value::Integer(200)]);
}

#[test]
fn three_tables_same_column_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t3 (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 10)");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 20)");
    exec(&mut c, "INSERT INTO t3 VALUES (1, 30)");

    let qr = query(
        &mut c,
        "SELECT t1.val, t2.val, t3.val \
         FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id",
    );
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]
    );
}

#[test]
fn on_clause_with_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 10), (2, 20)");
    exec(&mut c, "INSERT INTO b VALUES (100, 5), (200, 10)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a JOIN b ON a.val = b.val * 2 ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(200)]);
}

#[test]
fn on_clause_with_or() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, x INTEGER NOT NULL, y INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, z INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 10, 20), (2, 30, 40)");
    exec(&mut c, "INSERT INTO b VALUES (100, 10), (200, 40)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a JOIN b ON a.x = b.z OR a.y = b.z ORDER BY a.id, b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(200)]);
}

#[test]
fn on_clause_with_not_equal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (1), (2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a JOIN b ON a.id <> b.id ORDER BY a.id, b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(2)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(1)]);
}

#[test]
fn on_clause_with_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE ranges (id INTEGER NOT NULL PRIMARY KEY, lo INTEGER NOT NULL, hi INTEGER NOT NULL)");
    exec(
        &mut c,
        "CREATE TABLE points (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO ranges VALUES (1, 0, 10), (2, 10, 20)");
    exec(
        &mut c,
        "INSERT INTO points VALUES (100, 5), (200, 15), (300, 25)",
    );

    let qr = query(&mut c,
        "SELECT r.id, p.val FROM ranges r JOIN points p ON p.val >= r.lo AND p.val < r.hi ORDER BY r.id, p.val"
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(5)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(15)]);
}

#[test]
fn on_clause_boolean_literal_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10), (20)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a JOIN b ON TRUE ORDER BY a.id, b.id",
    );
    assert_eq!(qr.rows.len(), 4);
}

#[test]
fn on_clause_boolean_literal_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10)");

    let qr = query(&mut c, "SELECT a.id FROM a JOIN b ON FALSE");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_integer_equals_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, val REAL NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 42)");
    exec(&mut c, "INSERT INTO b VALUES (10, 42.0), (20, 42.5)");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a JOIN b ON a.val = b.val");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
}

#[test]
fn join_on_text_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, code TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, code TEXT NOT NULL, desc TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "INSERT INTO a VALUES (1, 'US'), (2, 'GB'), (3, 'FR')",
    );
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 'US', 'United States'), (20, 'FR', 'France')",
    );

    let qr = query(
        &mut c,
        "SELECT a.id, b.desc FROM a JOIN b ON a.code = b.code ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("United States".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(3), Value::Text("France".into())]
    );
}

#[test]
fn join_on_boolean_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL, label TEXT NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, TRUE), (2, FALSE)");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, TRUE, 'on'), (20, FALSE, 'off')",
    );

    let qr = query(
        &mut c,
        "SELECT a.id, b.label FROM a JOIN b ON a.active = b.active ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("on".into())]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Text("off".into())]
    );
}

#[test]
fn distinct_eliminates_join_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, tag TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'X'), (2, 'X'), (3, 'Y')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2), (30, 3)");

    let qr = query(
        &mut c,
        "SELECT DISTINCT a.tag FROM a JOIN b ON a.id = b.a_id ORDER BY a.tag",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("X".into()));
    assert_eq!(qr.rows[1][0], Value::Text("Y".into()));
}

#[test]
fn distinct_on_left_join_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val TEXT NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 'match')");

    let qr = query(
        &mut c,
        "SELECT DISTINCT b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY b.val",
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn order_by_inner_table_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, rank INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 1, 3), (20, 2, 1), (30, 1, 2)",
    );

    let qr = query(
        &mut c,
        "SELECT a.name, b.rank FROM a JOIN b ON a.id = b.a_id ORDER BY b.rank",
    );
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
    assert_eq!(qr.rows[2][1], Value::Integer(3));
}

#[test]
fn order_by_multi_table_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 'B'), (2, 'A')");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 1, 2), (20, 1, 1), (30, 2, 1)",
    );

    let qr = query(
        &mut c,
        "SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id ORDER BY a.name, b.val",
    );
    assert_eq!(qr.rows[0], vec![Value::Text("A".into()), Value::Integer(1)]);
    assert_eq!(qr.rows[1], vec![Value::Text("B".into()), Value::Integer(1)]);
    assert_eq!(qr.rows[2], vec![Value::Text("B".into()), Value::Integer(2)]);
}

#[test]
fn order_by_desc_on_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 100), (20, 2, 200)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.val FROM a JOIN b ON a.id = b.a_id ORDER BY b.val DESC",
    );
    assert_eq!(qr.rows[0][1], Value::Integer(200));
    assert_eq!(qr.rows[1][1], Value::Integer(100));
}

#[test]
fn join_limit_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(&mut c, "SELECT * FROM a JOIN b ON a.id = b.a_id LIMIT 0");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_offset_beyond_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(
        &mut c,
        "SELECT * FROM a JOIN b ON a.id = b.a_id LIMIT 10 OFFSET 100",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_limit_less_than_total() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10), (20), (30)");

    let qr = query(
        &mut c,
        "SELECT * FROM a CROSS JOIN b ORDER BY a.id, b.id LIMIT 5",
    );
    assert_eq!(qr.rows.len(), 5);
}

#[test]
fn join_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        exec(
            &mut c,
            "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
        );
        exec(&mut c, "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, uid INTEGER NOT NULL, amt REAL NOT NULL)");
        exec(&mut c, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
        exec(
            &mut c,
            "INSERT INTO orders VALUES (10, 1, 99.0), (20, 2, 50.0)",
        );
    }
    {
        let db = open_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        let qr = query(
            &mut c,
            "SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid ORDER BY u.id",
        );
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(
            qr.rows[0],
            vec![Value::Text("Alice".into()), Value::Real(99.0)]
        );
        assert_eq!(
            qr.rows[1],
            vec![Value::Text("Bob".into()), Value::Real(50.0)]
        );
    }
}

#[test]
fn left_join_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
        exec(
            &mut c,
            "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
        );
        exec(&mut c, "INSERT INTO a VALUES (1), (2)");
        exec(&mut c, "INSERT INTO b VALUES (10, 1)");
    }
    {
        let db = open_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        let qr = query(
            &mut c,
            "SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
        );
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.rows[0][1], Value::Integer(10));
        assert_eq!(qr.rows[1][1], Value::Null);
    }
}

#[test]
fn committed_txn_join_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        exec(&mut c, "BEGIN");
        exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
        exec(
            &mut c,
            "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
        );
        exec(&mut c, "INSERT INTO a VALUES (1)");
        exec(&mut c, "INSERT INTO b VALUES (10, 1)");
        exec(&mut c, "COMMIT");
    }
    {
        let db = open_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
        assert_eq!(n, 1);
    }
}

#[test]
fn rolled_back_txn_join_not_persisted() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
        exec(
            &mut c,
            "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
        );
        exec(&mut c, "INSERT INTO a VALUES (1)");

        exec(&mut c, "BEGIN");
        exec(&mut c, "INSERT INTO b VALUES (10, 1)");
        exec(&mut c, "ROLLBACK");
    }
    {
        let db = open_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
        assert_eq!(n, 0);
    }
}

#[test]
fn join_single_row_each() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (1)");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(1)]);
}

#[test]
fn join_both_tables_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");

    let qr = query(&mut c, "SELECT * FROM a JOIN b ON a.id = b.id");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn cross_join_both_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");

    let qr = query(&mut c, "SELECT * FROM a CROSS JOIN b");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn left_join_both_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");

    let qr = query(&mut c, "SELECT * FROM a LEFT JOIN b ON a.id = b.id");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_all_nulls_in_join_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "INSERT INTO a VALUES (1, NULL), (2, NULL), (3, NULL)",
    );
    exec(&mut c, "INSERT INTO b VALUES (1), (2)");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a JOIN b ON a.ref_id = b.id");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn left_join_all_nulls_in_join_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1, NULL), (2, NULL)");
    exec(&mut c, "INSERT INTO b VALUES (100)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.ref_id = b.id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][1], Value::Null);
    assert_eq!(qr.rows[1][1], Value::Null);
}

#[test]
fn cross_join_1x1() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (2)");

    let qr = query(&mut c, "SELECT a.id, b.id FROM a CROSS JOIN b");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(2)]);
}

#[test]
fn cross_join_with_where_reduces() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (1), (2), (3)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a CROSS JOIN b WHERE a.id = b.id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[0], row[1]);
    }
}

#[test]
fn cross_join_three_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10)");
    exec(&mut c, "INSERT INTO d VALUES (100)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id FROM a CROSS JOIN b CROSS JOIN d ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Integer(10), Value::Integer(100)]
    );
}

#[test]
fn join_after_bulk_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );

    for i in 0..50 {
        exec(&mut c, &format!("INSERT INTO a VALUES ({i})"));
        exec(
            &mut c,
            &format!("INSERT INTO b VALUES ({}, {})", i + 1000, i),
        );
    }

    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(n, 50);
}

#[test]
fn join_after_update_changing_join_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1, 10)");
    exec(&mut c, "INSERT INTO b VALUES (10), (20)");

    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.ref_id = b.id");
    assert_eq!(n, 1);

    exec(&mut c, "UPDATE a SET ref_id = 20 WHERE id = 1");

    let qr = query(&mut c, "SELECT b.id FROM a JOIN b ON a.ref_id = b.id");
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn join_after_delete_from_inner_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2), (30, 3)");

    exec(&mut c, "DELETE FROM b WHERE a_id = 2");

    let qr = query(
        &mut c,
        "SELECT a.id FROM a JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn join_after_delete_from_outer_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2), (3)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2), (30, 3)");

    exec(&mut c, "DELETE FROM a WHERE id = 2");

    let qr = query(
        &mut c,
        "SELECT a.id FROM a JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn select_star_column_order_matches_table_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, x TEXT NOT NULL, y INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, p REAL NOT NULL, q BOOLEAN NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'hi', 42)");
    exec(&mut c, "INSERT INTO b VALUES (1, 3.15, TRUE)");

    let qr = query(&mut c, "SELECT * FROM a JOIN b ON a.id = b.id");
    assert_eq!(qr.columns.len(), 6);
    assert_eq!(qr.rows[0].len(), 6);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("hi".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(42));
    assert_eq!(qr.rows[0][3], Value::Integer(1));
    assert_eq!(qr.rows[0][4], Value::Real(3.15));
    assert_eq!(qr.rows[0][5], Value::Boolean(true));
}

#[test]
fn select_star_three_tables_column_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, a TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, b TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE t3 (id INTEGER NOT NULL PRIMARY KEY, c TEXT NOT NULL)",
    );
    exec(&mut c, "INSERT INTO t1 VALUES (1, 'A')");
    exec(&mut c, "INSERT INTO t2 VALUES (1, 'B')");
    exec(&mut c, "INSERT INTO t3 VALUES (1, 'C')");

    let qr = query(
        &mut c,
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id",
    );
    assert_eq!(qr.columns.len(), 6);
    assert_eq!(qr.rows[0][1], Value::Text("A".into()));
    assert_eq!(qr.rows[0][3], Value::Text("B".into()));
    assert_eq!(qr.rows[0][5], Value::Text("C".into()));
}

#[test]
fn left_join_group_by_having_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE cats (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE toys (id INTEGER NOT NULL PRIMARY KEY, cat_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "INSERT INTO cats VALUES (1, 'Whiskers'), (2, 'Felix'), (3, 'Garfield')",
    );
    exec(
        &mut c,
        "INSERT INTO toys VALUES (10, 1), (11, 1), (12, 1), (20, 2)",
    );

    let qr = query(
        &mut c,
        "SELECT c.name, COUNT(t.id) AS toy_count \
         FROM cats c LEFT JOIN toys t ON c.id = t.cat_id \
         GROUP BY c.name \
         HAVING toy_count >= 2 \
         ORDER BY c.name",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("Whiskers".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(3));
}

#[test]
fn stress_many_to_many_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE students (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE courses (id INTEGER NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE enrollment (id INTEGER NOT NULL PRIMARY KEY, sid INTEGER NOT NULL, cid INTEGER NOT NULL)");

    for i in 0..20 {
        exec(
            &mut c,
            &format!("INSERT INTO students VALUES ({i}, 'student_{i}')"),
        );
    }
    for i in 0..5 {
        exec(
            &mut c,
            &format!("INSERT INTO courses VALUES ({i}, 'course_{i}')"),
        );
    }
    let mut eid = 0;
    for s in 0..20 {
        for course in 0..3 {
            let cid = (s + course) % 5;
            exec(
                &mut c,
                &format!("INSERT INTO enrollment VALUES ({eid}, {s}, {cid})"),
            );
            eid += 1;
        }
    }
    let n = count(
        &mut c,
        "SELECT COUNT(*) FROM students s \
         JOIN enrollment e ON s.id = e.sid \
         JOIN courses co ON e.cid = co.id",
    );
    assert_eq!(n, 60);

    let qr = query(
        &mut c,
        "SELECT s.name, COUNT(*) AS num_courses \
         FROM students s JOIN enrollment e ON s.id = e.sid \
         GROUP BY s.name \
         ORDER BY s.name \
         LIMIT 3",
    );
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[1], Value::Integer(3));
    }
}

#[test]
fn stress_left_join_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)",
    );
    exec(
        &mut c,
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER NOT NULL)",
    );

    for i in 0..50 {
        exec(&mut c, &format!("INSERT INTO parent VALUES ({i})"));
    }
    for i in 0..50 {
        if i % 2 == 0 {
            exec(
                &mut c,
                &format!("INSERT INTO child VALUES ({}, {})", i + 1000, i),
            );
        }
    }

    let qr = query(
        &mut c,
        "SELECT p.id, child.id FROM parent p LEFT JOIN child ON p.id = child.pid ORDER BY p.id",
    );
    assert_eq!(qr.rows.len(), 50);

    for row in &qr.rows {
        let pid = match &row[0] {
            Value::Integer(n) => *n,
            _ => panic!(),
        };
        if pid % 2 == 0 {
            assert_eq!(row[1], Value::Integer(pid + 1000));
        } else {
            assert_eq!(row[1], Value::Null);
        }
    }
}

#[test]
fn stress_self_cross_join_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)");
    for i in 0..10 {
        exec(&mut c, &format!("INSERT INTO t VALUES ({i})"));
    }

    let n = count(&mut c, "SELECT COUNT(*) FROM t a CROSS JOIN t b");
    assert_eq!(n, 100);

    let n = count(
        &mut c,
        "SELECT COUNT(*) FROM t a CROSS JOIN t b WHERE a.id < b.id",
    );
    assert_eq!(n, 45);
}

#[test]
fn stress_sequential_transactions_with_joins() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );

    for i in 0..20 {
        exec(&mut c, "BEGIN");
        exec(&mut c, &format!("INSERT INTO a VALUES ({i})"));
        exec(
            &mut c,
            &format!("INSERT INTO b VALUES ({}, {})", i + 100, i),
        );
        exec(&mut c, "COMMIT");
    }

    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(n, 20);
}

#[test]
fn stress_alternating_commit_rollback_with_join_verification() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );

    let mut committed = 0i64;
    for i in 0..30 {
        exec(&mut c, "BEGIN");
        exec(&mut c, &format!("INSERT INTO a VALUES ({i})"));
        exec(
            &mut c,
            &format!("INSERT INTO b VALUES ({}, {})", i + 1000, i),
        );
        if i % 2 == 0 {
            exec(&mut c, "COMMIT");
            committed += 1;
        } else {
            exec(&mut c, "ROLLBACK");
        }
    }

    let n = count(&mut c, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(n, committed);
}

#[test]
fn inner_join_then_cross_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");
    exec(&mut c, "INSERT INTO d VALUES (100), (200)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id \
         FROM a JOIN b ON a.id = b.a_id \
         CROSS JOIN d ORDER BY d.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(200)]
    );
}

#[test]
fn left_join_then_cross_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");
    exec(&mut c, "INSERT INTO d VALUES (100)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id \
         FROM a LEFT JOIN b ON a.id = b.a_id \
         CROSS JOIN d ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Integer(2), Value::Null, Value::Integer(100)]
    );
}

#[test]
fn select_only_outer_table_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1)");

    let qr = query(&mut c, "SELECT a.id, a.name FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.columns.len(), 2);
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Text("Alice".into())]
    );
}

#[test]
fn select_only_inner_table_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, data TEXT NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1, 'payload')");

    let qr = query(&mut c, "SELECT b.data FROM a JOIN b ON a.id = b.a_id");
    assert_eq!(qr.columns.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("payload".into()));
}

#[test]
fn select_expression_across_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, mul INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 10)");
    exec(&mut c, "INSERT INTO b VALUES (100, 1, 3)");

    let qr = query(
        &mut c,
        "SELECT a.val * b.mul FROM a JOIN b ON a.id = b.a_id",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

#[test]
fn join_column_not_found_in_on() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (1)");

    let qr = query(&mut c, "SELECT * FROM a JOIN b ON a.nonexistent = b.id");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn join_table_not_found_in_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");

    let result = c.execute("SELECT * FROM nonexistent JOIN a ON nonexistent.id = a.id");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[test]
fn join_wrong_qualified_table_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (1)");

    let result = c.execute("SELECT c.id FROM a JOIN b ON a.id = b.id");
    assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
}

#[test]
fn right_join_all_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(20)]);
}

#[test]
fn right_join_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 999), (20, 888)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Null, Value::Integer(10)]);
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);
}

#[test]
fn right_join_mixed_match_unmatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999), (30, 2)");

    let qr = query(
        &mut c,
        "SELECT a.name, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Integer(10)]
    );
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Bob".into()), Value::Integer(30)]
    );
}

#[test]
fn right_join_one_to_many() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 1), (30, 1)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[0], Value::Integer(1));
    }
}

#[test]
fn right_join_empty_left() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO b VALUES (1), (2), (3)");

    let qr = query(
        &mut c,
        "SELECT a.id, a.val, b.id FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 3);
    for row in &qr.rows {
        assert_eq!(row[0], Value::Null);
        assert_eq!(row[1], Value::Null);
    }
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(2));
    assert_eq!(qr.rows[2][2], Value::Integer(3));
}

#[test]
fn right_join_empty_right() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn right_join_both_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn right_join_null_in_on_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, ref_id INTEGER)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY)");
    exec(&mut c, "INSERT INTO a VALUES (1, NULL), (2, 100)");
    exec(&mut c, "INSERT INTO b VALUES (100), (200)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.ref_id = b.id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(2), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(200)]);
}

#[test]
fn right_join_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(&mut c, "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL, val INTEGER NOT NULL)");
    exec(&mut c, "INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 1, 100), (20, 999, 200), (30, 2, 50)",
    );

    let qr = query(
        &mut c,
        "SELECT a.name, b.val FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.val > 80 ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(
        qr.rows[0],
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(200)]);
}

#[test]
fn right_join_with_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE dept (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, dept_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales')",
    );
    exec(
        &mut c,
        "INSERT INTO emp VALUES (10, 1), (20, 1), (30, 1), (40, 999)",
    );

    let qr = query(&mut c,
        "SELECT dept.name, COUNT(*) FROM dept RIGHT JOIN emp ON dept.id = emp.dept_id GROUP BY dept.name ORDER BY dept.name"
    );
    assert_eq!(qr.rows.len(), 2);
    let null_row = qr.rows.iter().find(|r| r[0] == Value::Null).unwrap();
    assert_eq!(null_row[1], Value::Integer(1));
    let eng_row = qr
        .rows
        .iter()
        .find(|r| r[0] == Value::Text("Engineering".into()))
        .unwrap();
    assert_eq!(eng_row[1], Value::Integer(3));
}

#[test]
fn right_join_is_null_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999), (30, 888)");

    let qr = query(
        &mut c,
        "SELECT b.id FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.id IS NULL ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(20));
    assert_eq!(qr.rows[1][0], Value::Integer(30));
}

#[test]
fn right_join_select_star_column_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, x TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, y TEXT NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'hello')");
    exec(&mut c, "INSERT INTO b VALUES (1, 'world'), (2, 'orphan')");

    let qr = query(
        &mut c,
        "SELECT * FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.columns.len(), 4);
    assert_eq!(
        qr.rows[0],
        vec![
            Value::Integer(1),
            Value::Text("hello".into()),
            Value::Integer(1),
            Value::Text("world".into())
        ]
    );
    assert_eq!(
        qr.rows[1],
        vec![
            Value::Null,
            Value::Null,
            Value::Integer(2),
            Value::Text("orphan".into())
        ]
    );
}

#[test]
fn right_join_self_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, mgr_id INTEGER)",
    );
    exec(
        &mut c,
        "INSERT INTO emp VALUES (1, 'Boss', NULL), (2, 'Alice', 1), (3, 'Bob', 1)",
    );

    let qr = query(
        &mut c,
        "SELECT m.name, e.name FROM emp m RIGHT JOIN emp e ON m.id = e.mgr_id ORDER BY e.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Null, Value::Text("Boss".into())]);
    assert_eq!(
        qr.rows[1],
        vec![Value::Text("Boss".into()), Value::Text("Alice".into())]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Text("Boss".into()), Value::Text("Bob".into())]
    );
}

#[test]
fn right_join_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 1), (30, 999)");

    let qr = query(
        &mut c,
        "SELECT DISTINCT a.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY a.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Null);
    assert_eq!(qr.rows[1][0], Value::Integer(1));
}

#[test]
fn right_join_order_by_limit_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(
        &mut c,
        "INSERT INTO b VALUES (10, 1), (20, 999), (30, 888), (40, 777)",
    );

    let qr = query(
        &mut c,
        "SELECT b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id LIMIT 2 OFFSET 1",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(20));
    assert_eq!(qr.rows[1][0], Value::Integer(30));
}

#[test]
fn right_join_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "BEGIN");
    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999)");

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
    assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);

    exec(&mut c, "COMMIT");
}

#[test]
fn right_join_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
        exec(
            &mut c,
            "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
        );
        exec(&mut c, "INSERT INTO a VALUES (1)");
        exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999)");
    }
    {
        let db = open_db(dir.path());
        let mut c = Connection::open(&db).unwrap();
        let qr = query(
            &mut c,
            "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
        );
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(10)]);
        assert_eq!(qr.rows[1], vec![Value::Null, Value::Integer(20)]);
    }
}

#[test]
fn right_join_vs_left_join_equivalence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999)");

    let right_qr = query(
        &mut c,
        "SELECT a.name, b.id FROM a RIGHT JOIN b ON a.id = b.a_id ORDER BY b.id",
    );

    let left_qr = query(
        &mut c,
        "SELECT a.name, b.id FROM b LEFT JOIN a ON a.id = b.a_id ORDER BY b.id",
    );

    assert_eq!(right_qr.rows.len(), left_qr.rows.len());
    for i in 0..right_qr.rows.len() {
        assert_eq!(right_qr.rows[i][1], left_qr.rows[i][1]);
    }
}

#[test]
fn inner_right_left_multi_way() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE d (id INTEGER NOT NULL PRIMARY KEY, b_id INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1), (2)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 999)");
    exec(
        &mut c,
        "INSERT INTO d VALUES (100, 10), (200, 20), (300, 777)",
    );

    let qr = query(
        &mut c,
        "SELECT a.id, b.id, d.id FROM a \
         INNER JOIN b ON a.id = b.a_id \
         RIGHT JOIN d ON b.id = d.b_id \
         ORDER BY d.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(
        qr.rows[0],
        vec![Value::Integer(1), Value::Integer(10), Value::Integer(100)]
    );
    assert_eq!(
        qr.rows[1],
        vec![Value::Null, Value::Null, Value::Integer(200)]
    );
    assert_eq!(
        qr.rows[2],
        vec![Value::Null, Value::Null, Value::Integer(300)]
    );
}

#[test]
fn right_join_complex_on() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, x INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, y INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)");
    exec(
        &mut c,
        "INSERT INTO b VALUES (100, 10), (200, 20), (300, 50)",
    );

    let qr = query(
        &mut c,
        "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.x = b.y AND a.x > 5 ORDER BY b.id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(100)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(2), Value::Integer(200)]);
    assert_eq!(qr.rows[2], vec![Value::Null, Value::Integer(300)]);
}

#[test]
fn right_join_many_to_many() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(
        &mut c,
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, g INTEGER NOT NULL)",
    );
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, g INTEGER NOT NULL)",
    );
    exec(&mut c, "INSERT INTO a VALUES (1, 1), (2, 1)");
    exec(&mut c, "INSERT INTO b VALUES (10, 1), (20, 1), (30, 2)");

    let n = count(&mut c, "SELECT COUNT(*) FROM a RIGHT JOIN b ON a.g = b.g");
    assert_eq!(n, 5);
}

#[test]
fn right_join_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut c = Connection::open(&db).unwrap();

    exec(&mut c, "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY)");
    exec(
        &mut c,
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER NOT NULL)",
    );

    for i in 0..50 {
        c.execute(&format!("INSERT INTO a VALUES ({i})")).unwrap();
    }
    for i in 0..200 {
        let a_id = if i % 4 == 0 { 999 } else { i % 50 };
        c.execute(&format!("INSERT INTO b VALUES ({i}, {a_id})"))
            .unwrap();
    }

    let matched = count(
        &mut c,
        "SELECT COUNT(*) FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.id IS NOT NULL",
    );
    let unmatched = count(
        &mut c,
        "SELECT COUNT(*) FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.id IS NULL",
    );
    assert_eq!(matched + unmatched, 200);
    assert_eq!(unmatched, 50);
}
