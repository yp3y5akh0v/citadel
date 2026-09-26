use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn exec(conn: &Connection, sql: &str) {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Ok | ExecutionResult::RowsAffected(_) | ExecutionResult::Query(_) => {}
    }
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn count(conn: &Connection, sql: &str) -> i64 {
    let qr = query(conn, sql);
    match qr.rows[0][0] {
        Value::Integer(n) => n,
        _ => panic!("expected Integer count"),
    }
}

#[test]
fn lateral_top_n_per_group_at_scale() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let c = Connection::open(&db).unwrap();
    exec(&c, "CREATE TABLE cat (id INTEGER PRIMARY KEY)");
    exec(
        &c,
        "CREATE TABLE prod (id INTEGER PRIMARY KEY, cat_id INTEGER, price INTEGER)",
    );
    exec(&c, "BEGIN");
    for i in 0..50 {
        c.execute(&format!("INSERT INTO cat VALUES ({i})")).unwrap();
    }
    for i in 0..500 {
        c.execute(&format!(
            "INSERT INTO prod VALUES ({i}, {}, {})",
            i % 50,
            i * 7
        ))
        .unwrap();
    }
    exec(&c, "COMMIT");

    let total = count(
        &c,
        "SELECT COUNT(*) FROM (
            SELECT cat.id, p.id AS pid FROM cat, LATERAL (
                SELECT id FROM prod WHERE prod.cat_id = cat.id ORDER BY price DESC LIMIT 3
            ) p
         ) sub",
    );
    assert_eq!(total, 50 * 3);
}

#[test]
fn lateral_left_join_keeps_empty_groups() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let c = Connection::open(&db).unwrap();
    exec(&c, "CREATE TABLE cat (id INTEGER PRIMARY KEY)");
    exec(
        &c,
        "CREATE TABLE prod (id INTEGER PRIMARY KEY, cat_id INTEGER)",
    );
    exec(&c, "BEGIN");
    for i in 0..20 {
        c.execute(&format!("INSERT INTO cat VALUES ({i})")).unwrap();
    }
    for i in 0..30 {
        c.execute(&format!("INSERT INTO prod VALUES ({i}, {})", i % 10))
            .unwrap();
    }
    exec(&c, "COMMIT");

    let total = count(
        &c,
        "SELECT COUNT(*) FROM (
            SELECT cat.id FROM cat LEFT JOIN LATERAL (
                SELECT id FROM prod WHERE prod.cat_id = cat.id LIMIT 1
            ) p ON true
         ) sub",
    );
    assert_eq!(total, 20);
}

#[test]
fn lateral_inside_savepoint_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let c = Connection::open(&db).unwrap();
    exec(&c, "CREATE TABLE cat (id INTEGER PRIMARY KEY)");
    exec(
        &c,
        "CREATE TABLE prod (id INTEGER PRIMARY KEY, cat_id INTEGER)",
    );
    for i in 1..=3 {
        exec(&c, &format!("INSERT INTO cat VALUES ({i})"));
        exec(&c, &format!("INSERT INTO prod VALUES ({i}, {i})"));
    }

    let baseline = count(
        &c,
        "SELECT COUNT(*) FROM (SELECT cat.id FROM cat, LATERAL (SELECT id FROM prod WHERE prod.cat_id = cat.id) p) sub",
    );
    exec(&c, "BEGIN");
    exec(&c, "SAVEPOINT sp");
    exec(&c, "INSERT INTO prod VALUES (99, 1)");
    exec(&c, "INSERT INTO prod VALUES (100, 1)");
    let mid = count(
        &c,
        "SELECT COUNT(*) FROM (SELECT cat.id FROM cat, LATERAL (SELECT id FROM prod WHERE prod.cat_id = cat.id) p) sub",
    );
    assert_eq!(mid, baseline + 2);
    exec(&c, "ROLLBACK TO sp");
    exec(&c, "COMMIT");
    let after = count(
        &c,
        "SELECT COUNT(*) FROM (SELECT cat.id FROM cat, LATERAL (SELECT id FROM prod WHERE prod.cat_id = cat.id) p) sub",
    );
    assert_eq!(after, baseline);
}

#[test]
fn lateral_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let c = Connection::open(&db).unwrap();
        exec(&c, "CREATE TABLE cat (id INTEGER PRIMARY KEY)");
        exec(
            &c,
            "CREATE TABLE prod (id INTEGER PRIMARY KEY, cat_id INTEGER)",
        );
        for i in 1..=5 {
            exec(&c, &format!("INSERT INTO cat VALUES ({i})"));
            exec(&c, &format!("INSERT INTO prod VALUES ({i}, {i})"));
        }
    }
    let db = open_db(dir.path());
    let c = Connection::open(&db).unwrap();
    let total = count(
        &c,
        "SELECT COUNT(*) FROM (SELECT cat.id FROM cat, LATERAL (SELECT id FROM prod WHERE prod.cat_id = cat.id) p) sub",
    );
    assert_eq!(total, 5);
}

#[test]
fn non_lateral_derived_table_aggregate_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let c = Connection::open(&db).unwrap();
    exec(&c, "CREATE TABLE cat (id INTEGER PRIMARY KEY, name TEXT)");
    exec(
        &c,
        "CREATE TABLE prod (id INTEGER PRIMARY KEY, cat_id INTEGER)",
    );
    exec(&c, "BEGIN");
    for i in 1..=10 {
        c.execute(&format!("INSERT INTO cat VALUES ({i}, 'cat_{i}')"))
            .unwrap();
    }
    for i in 0..100 {
        c.execute(&format!("INSERT INTO prod VALUES ({i}, {})", (i % 10) + 1))
            .unwrap();
    }
    exec(&c, "COMMIT");

    let qr = query(
        &c,
        "SELECT cat.id, sub.cnt FROM cat INNER JOIN (
            SELECT cat_id, COUNT(*) AS cnt FROM prod GROUP BY cat_id
         ) sub ON cat.id = sub.cat_id ORDER BY cat.id",
    );
    assert_eq!(qr.rows.len(), 10);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer((i + 1) as i64));
        assert_eq!(row[1], Value::Integer(10));
    }
}
