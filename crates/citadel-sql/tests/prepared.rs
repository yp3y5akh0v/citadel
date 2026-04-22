use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, PreparedStatement, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn setup_users(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
        .unwrap();
    for (id, name, age) in [(1, "Alice", 30), (2, "Bob", 25), (3, "Carol", 35)] {
        conn.execute(&format!(
            "INSERT INTO users (id, name, age) VALUES ({id}, '{name}', {age})"
        ))
        .unwrap();
    }
}

fn expect_query(stmt: &PreparedStatement<'_, '_>, params: &[Value]) -> Vec<Vec<Value>> {
    stmt.query_collect(params).unwrap().rows
}

#[test]
fn prepare_select_happy_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT name FROM users WHERE id = $1")
        .unwrap();
    assert_eq!(s.param_count(), 1);
    let rows = expect_query(&s, &[Value::Integer(2)]);
    assert_eq!(rows, vec![vec![Value::Text("Bob".into())]]);
}

#[test]
fn prepare_select_reused() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id FROM users WHERE id = $1").unwrap();
    for id in 1..=3 {
        let rows = expect_query(&s, &[Value::Integer(id)]);
        assert_eq!(rows, vec![vec![Value::Integer(id)]]);
    }
}

#[test]
fn prepare_insert_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    assert_eq!(s.param_count(), 3);
    let res = s
        .execute(&[
            Value::Integer(4),
            Value::Text("Dave".into()),
            Value::Integer(40),
        ])
        .unwrap();
    assert_eq!(res, 1);

    let sel = conn
        .prepare("SELECT name FROM users WHERE id = $1")
        .unwrap();
    let rows = expect_query(&sel, &[Value::Integer(4)]);
    assert_eq!(rows, vec![vec![Value::Text("Dave".into())]]);
}

#[test]
fn prepare_update_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("UPDATE users SET age = age + $1 WHERE id = $2")
        .unwrap();
    let res = s.execute(&[Value::Integer(10), Value::Integer(1)]).unwrap();
    assert_eq!(res, 1);
    let sel = conn.prepare("SELECT age FROM users WHERE id = $1").unwrap();
    let rows = expect_query(&sel, &[Value::Integer(1)]);
    assert_eq!(rows, vec![vec![Value::Integer(40)]]);
}

#[test]
fn prepare_delete_execute() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("DELETE FROM users WHERE id = $1").unwrap();
    let res = s.execute(&[Value::Integer(2)]).unwrap();
    assert_eq!(res, 1);
}

#[test]
fn prepare_rejects_invalid_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert!(conn.prepare("SELECT * FORM users").is_err());
}

#[test]
fn prepare_param_count_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT * FROM users WHERE id = $1").unwrap();
    assert!(s.query(&[]).is_err());
    assert!(s.query(&[Value::Integer(1), Value::Integer(2)]).is_err());
}

#[test]
fn prepare_null_param_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    let s = conn.prepare("SELECT id FROM t WHERE v = $1").unwrap();
    let rows = expect_query(&s, &[Value::Null]);
    assert!(rows.is_empty());
}

#[test]
fn prepare_text_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id FROM users WHERE name = $1")
        .unwrap();
    let rows = expect_query(&s, &[Value::Text("Bob".into())]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn prepare_real_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1.5), (2, 2.5)")
        .unwrap();
    let s = conn.prepare("SELECT id FROM t WHERE v = $1").unwrap();
    let rows = expect_query(&s, &[Value::Real(2.5)]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn prepare_blob_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    let ins = conn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    ins.execute(&[Value::Integer(1), Value::Blob(vec![0xDE, 0xAD])])
        .unwrap();
    let sel = conn.prepare("SELECT v FROM t WHERE id = $1").unwrap();
    let rows = expect_query(&sel, &[Value::Integer(1)]);
    assert_eq!(rows, vec![vec![Value::Blob(vec![0xDE, 0xAD])]]);
}

#[test]
fn prepare_same_position_twice() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id FROM users WHERE id = $1 OR age = $1")
        .unwrap();
    let rows = expect_query(&s, &[Value::Integer(30)]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn prepare_in_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    for i in 10..13 {
        s.execute(&[
            Value::Integer(i),
            Value::Text(format!("u{i}").into()),
            Value::Integer(20 + i),
        ])
        .unwrap();
    }
    let cnt = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(cnt.rows[0][0], Value::Integer(6));
}

#[test]
fn prepare_fast_eval_param_add() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("UPDATE users SET age = age + $1").unwrap();
    s.execute(&[Value::Integer(1)]).unwrap();
    s.execute(&[Value::Integer(2)]).unwrap();
    let r = conn.query("SELECT age FROM users ORDER BY id").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(33));
    assert_eq!(r.rows[1][0], Value::Integer(28));
    assert_eq!(r.rows[2][0], Value::Integer(38));
}

#[test]
fn prepare_fast_eval_param_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("UPDATE users SET age = $1").unwrap();
    s.execute(&[Value::Integer(99)]).unwrap();
    let r = conn.query("SELECT DISTINCT age FROM users").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Integer(99)]]);
}

#[test]
fn prepare_schema_change_invalidates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let s = conn.prepare("SELECT * FROM t WHERE id = $1").unwrap();
    expect_query(&s, &[Value::Integer(1)]);

    conn.execute("ALTER TABLE t ADD COLUMN b INTEGER").unwrap();
    let rows = expect_query(&s, &[Value::Integer(1)]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::Integer(10), Value::Null]]
    );
}

#[test]
fn prepare_inside_explicit_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let ins = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    ins.execute(&[
        Value::Integer(4),
        Value::Text("Dave".into()),
        Value::Integer(40),
    ])
    .unwrap();
    ins.execute(&[
        Value::Integer(5),
        Value::Text("Eve".into()),
        Value::Integer(50),
    ])
    .unwrap();
    conn.execute("COMMIT").unwrap();
    let r = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(5));
}

#[test]
fn prepare_rolled_back_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let ins = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    ins.execute(&[
        Value::Integer(4),
        Value::Text("Dave".into()),
        Value::Integer(40),
    ])
    .unwrap();
    conn.execute("ROLLBACK").unwrap();
    let r = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn prepare_multiple_live_statements() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let sel = conn
        .prepare("SELECT name FROM users WHERE id = $1")
        .unwrap();
    let upd = conn
        .prepare("UPDATE users SET age = $1 WHERE id = $2")
        .unwrap();
    let r = expect_query(&sel, &[Value::Integer(1)]);
    assert_eq!(r, vec![vec![Value::Text("Alice".into())]]);
    upd.execute(&[Value::Integer(100), Value::Integer(1)])
        .unwrap();
    let r = conn.query("SELECT age FROM users WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(100));
}

#[test]
fn prepare_fallback_for_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO b VALUES (1, 1), (2, 2)").unwrap();
    let s = conn
        .prepare("SELECT a.v FROM a JOIN b ON a.id = b.a_id WHERE a.id = $1")
        .unwrap();
    let r = expect_query(&s, &[Value::Integer(2)]);
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

#[test]
fn prepare_fallback_for_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT COUNT(*) FROM users WHERE age > $1")
        .unwrap();
    let r = expect_query(&s, &[Value::Integer(26)]);
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn prepare_empty_result_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT * FROM users WHERE id = $1").unwrap();
    let rows = expect_query(&s, &[Value::Integer(999)]);
    assert!(rows.is_empty());
}

#[test]
fn prepare_zero_index_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert!(conn.prepare("SELECT * FROM users WHERE id = $0").is_err());
}

#[test]
fn prepare_query_with_explain() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("EXPLAIN SELECT * FROM users WHERE id = $1")
        .unwrap();
    let r = s.query_collect(&[Value::Integer(5)]).unwrap();
    assert!(!r.rows.is_empty());
}

#[test]
fn prepare_date_param() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("INSERT INTO events VALUES (1, DATE '2024-01-15'), (2, DATE '2024-06-01')")
        .unwrap();
    let s = conn.prepare("SELECT id FROM events WHERE d = $1").unwrap();
    let rows = expect_query(
        &s,
        &[Value::Date(
            citadel_sql::datetime::ymd_to_days(2024, 6, 1).unwrap(),
        )],
    );
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn prepare_param_in_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id FROM users WHERE age > $1 ORDER BY age LIMIT 10")
        .unwrap();
    let rows = expect_query(&s, &[Value::Integer(20)]);
    assert_eq!(rows.len(), 3);
}

#[test]
fn prepare_sql_accessor() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let sql = "SELECT id FROM users WHERE age > $1";
    let s = conn.prepare(sql).unwrap();
    assert_eq!(s.sql(), sql);
}

#[test]
fn prepare_parameter_count_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT * FROM users WHERE id = $1 AND age > $2")
        .unwrap();
    assert_eq!(s.param_count(), 2);
    assert_eq!(s.parameter_count(), 2);
}

#[test]
fn prepare_column_metadata_select_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT * FROM users").unwrap();
    assert_eq!(s.column_count(), 3);
    assert_eq!(s.column_names(), ["id", "name", "age"]);
    assert_eq!(s.column_name(0), Some("id"));
    assert_eq!(s.column_name(1), Some("name"));
    assert_eq!(s.column_name(2), Some("age"));
    assert_eq!(s.column_name(3), None);
    assert_eq!(s.column_index("age"), Some(2));
    assert_eq!(s.column_index("missing"), None);
}

#[test]
fn prepare_column_metadata_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id, age + 1 AS next_age FROM users")
        .unwrap();
    assert_eq!(s.column_names(), ["id", "next_age"]);
}

#[test]
fn prepare_column_metadata_expression_no_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT age + 1 FROM users").unwrap();
    assert_eq!(s.column_count(), 1);
    assert_eq!(s.column_name(0), Some("age + 1"));
}

#[test]
fn prepare_column_metadata_dml_is_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let i = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    let u = conn.prepare("UPDATE users SET age = $1").unwrap();
    let d = conn.prepare("DELETE FROM users WHERE id = $1").unwrap();
    assert_eq!(i.column_count(), 0);
    assert_eq!(u.column_count(), 0);
    assert_eq!(d.column_count(), 0);
}

#[test]
fn prepare_column_metadata_explain_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("EXPLAIN SELECT * FROM users").unwrap();
    assert_eq!(s.column_count(), 1);
    assert_eq!(s.column_name(0), Some("plan"));
}

#[test]
fn prepare_readonly_flag() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    assert!(conn.prepare("SELECT 1").unwrap().readonly());
    assert!(conn.prepare("EXPLAIN SELECT 1").unwrap().readonly());
    assert!(!conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap()
        .readonly());
    assert!(!conn
        .prepare("UPDATE users SET age = $1")
        .unwrap()
        .readonly());
    assert!(!conn
        .prepare("DELETE FROM users WHERE id = $1")
        .unwrap()
        .readonly());
}

#[test]
fn prepare_is_explain_flag() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    assert!(conn
        .prepare("EXPLAIN SELECT * FROM users")
        .unwrap()
        .is_explain());
    assert!(!conn.prepare("SELECT * FROM users").unwrap().is_explain());
}

#[test]
fn prepare_exists_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id FROM users WHERE id = $1").unwrap();
    assert!(s.exists(&[Value::Integer(1)]).unwrap());
}

#[test]
fn prepare_exists_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id FROM users WHERE id = $1").unwrap();
    assert!(!s.exists(&[Value::Integer(999)]).unwrap());
}

#[test]
fn prepare_execute_returns_rows_affected_u64() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("UPDATE users SET age = age + 1").unwrap();
    let n: u64 = s.execute(&[]).unwrap();
    assert_eq!(n, 3);
}

#[test]
fn prepare_execute_select_returns_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT * FROM users").unwrap();
    assert_eq!(s.execute(&[]).unwrap(), 0);
}

#[test]
fn prepare_query_returns_rows_iterator() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id, name FROM users ORDER BY id")
        .unwrap();
    let mut rows = s.query(&[]).unwrap();
    assert_eq!(rows.column_count(), 2);
    assert_eq!(rows.column_names(), ["id", "name"]);

    {
        let r = rows.next().unwrap().expect("first row");
        assert_eq!(r.get(0), Some(&Value::Integer(1)));
        assert_eq!(r.get(1), Some(&Value::Text("Alice".into())));
        assert_eq!(r.column_count(), 2);
        assert_eq!(r.column_name(0), Some("id"));
    }
    {
        let r = rows.next().unwrap().expect("second row");
        assert_eq!(r.get(0), Some(&Value::Integer(2)));
    }
    {
        let r = rows.next().unwrap().expect("third row");
        assert_eq!(r.get(0), Some(&Value::Integer(3)));
    }
    assert!(rows.next().unwrap().is_none());
}

#[test]
fn prepare_row_get_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id, name, age FROM users").unwrap();
    let mut rows = s.query(&[]).unwrap();
    let r = rows.next().unwrap().unwrap();
    assert_eq!(r.get_by_name("name"), Some(&Value::Text("Alice".into())));
    assert_eq!(r.get_by_name("age"), Some(&Value::Integer(30)));
    assert_eq!(r.get_by_name("missing"), None);
}

#[test]
fn prepare_row_as_slice_and_to_vec() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id, name FROM users WHERE id = $1")
        .unwrap();
    let mut rows = s.query(&[Value::Integer(1)]).unwrap();
    let r = rows.next().unwrap().unwrap();
    assert_eq!(
        r.as_slice(),
        &[Value::Integer(1), Value::Text("Alice".into())]
    );
    assert_eq!(
        r.to_vec(),
        vec![Value::Integer(1), Value::Text("Alice".into())]
    );
}

#[test]
fn prepare_rows_collect_equivalent_to_query_collect() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id FROM users ORDER BY id").unwrap();
    let via_iter = s.query(&[]).unwrap().collect().unwrap();
    let via_collect = s.query_collect(&[]).unwrap();
    assert_eq!(via_iter.columns, via_collect.columns);
    assert_eq!(via_iter.rows, via_collect.rows);
}

#[test]
fn prepare_query_row_first_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT name FROM users WHERE id = $1")
        .unwrap();
    let name: String = s
        .query_row(&[Value::Integer(2)], |row| match row.get(0) {
            Some(Value::Text(t)) => Ok(t.to_string()),
            other => panic!("expected Text, got {other:?}"),
        })
        .unwrap();
    assert_eq!(name, "Bob");
}

#[test]
fn prepare_query_row_no_rows_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT id FROM users WHERE id = $1").unwrap();
    let err = s
        .query_row(&[Value::Integer(9999)], |row| Ok(row.to_vec()))
        .unwrap_err();
    assert!(matches!(err, citadel_sql::SqlError::QueryReturnedNoRows));
}

#[test]
fn prepare_streaming_select_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let ins = conn.prepare("INSERT INTO big VALUES ($1, $2)").unwrap();
    for i in 0..5_000i64 {
        ins.execute(&[Value::Integer(i), Value::Text(format!("v{i}").into())])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let s = conn.prepare("SELECT * FROM big").unwrap();
    let mut rows = s.query(&[]).unwrap();
    let mut count = 0i64;
    while let Some(row) = rows.next().unwrap() {
        assert_eq!(row.column_count(), 2);
        if count == 0 {
            assert_eq!(row.get(0), Some(&Value::Integer(0)));
        }
        count += 1;
    }
    assert_eq!(count, 5_000);
}

#[test]
fn prepare_streaming_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT name FROM users").unwrap();
    let mut rows = s.query(&[]).unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        assert_eq!(row.column_count(), 1);
        match row.get(0).unwrap() {
            Value::Text(t) => names.push(t.to_string()),
            _ => panic!(),
        }
    }
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

#[test]
fn prepare_exists_short_circuits_via_stream() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn.prepare("SELECT * FROM users").unwrap();
    assert!(s.exists(&[]).unwrap());
}

#[test]
fn prepare_non_streaming_falls_back_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("SELECT id FROM users WHERE age > $1 ORDER BY age")
        .unwrap();
    let mut rows = s.query(&[Value::Integer(20)]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn prepare_query_dml_yields_empty_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users(&conn);
    let s = conn
        .prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)")
        .unwrap();
    let mut rows = s
        .query(&[
            Value::Integer(9),
            Value::Text("Inserted".into()),
            Value::Integer(50),
        ])
        .unwrap();
    assert!(rows.next().unwrap().is_none());
    let check = conn.query("SELECT name FROM users WHERE id = 9").unwrap();
    assert_eq!(check.rows[0][0], Value::Text("Inserted".into()));
}
