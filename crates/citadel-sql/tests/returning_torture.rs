use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn query(conn: &Connection, sql: &str) -> QueryResult {
    conn.query(sql).unwrap()
}

#[test]
fn insert_returning_100_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..100 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i, i * 10));
    }
    sql.push_str(" RETURNING id, val");

    let qr = query(&conn, &sql);
    assert_eq!(qr.rows.len(), 100);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Integer(i as i64));
        assert_eq!(row[1], Value::Integer(i as i64 * 10));
    }
}

#[test]
fn delete_returning_100_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 10))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = query(&conn, "DELETE FROM t WHERE val >= 500 RETURNING id, val");
    assert_eq!(qr.rows.len(), 50);
    for row in &qr.rows {
        if let Value::Integer(v) = row[1] {
            assert!(v >= 500);
        }
    }
    let after = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(after.rows[0][0], Value::Integer(50));
}

#[test]
fn update_returning_with_old_new_50_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..50 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let qr = query(
        &conn,
        "UPDATE t SET n = n * 3 RETURNING id, old.n AS was, new.n AS now_",
    );
    assert_eq!(qr.rows.len(), 50);
    for row in &qr.rows {
        if let (Value::Integer(id), Value::Integer(was), Value::Integer(now_)) =
            (&row[0], &row[1], &row[2])
        {
            assert_eq!(*was, *id);
            assert_eq!(*now_, was * 3);
        }
    }
}

#[test]
fn upsert_mixed_batch_returning_correct_old_new_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (2, 100), (4, 400), (3, 300) \
         ON CONFLICT (id) DO UPDATE SET c = c + excluded.c \
         RETURNING id, old.c AS was, new.c AS now_",
    );
    assert_eq!(qr.rows.len(), 3);
    let by_id: std::collections::HashMap<i64, (Value, Value)> = qr
        .rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Integer(n) => *n,
                _ => panic!(),
            };
            (id, (r[1].clone(), r[2].clone()))
        })
        .collect();

    assert_eq!(by_id[&2].0, Value::Integer(20));
    assert_eq!(by_id[&2].1, Value::Integer(120));
    assert_eq!(by_id[&4].0, Value::Null);
    assert_eq!(by_id[&4].1, Value::Integer(400));
    assert_eq!(by_id[&3].0, Value::Integer(30));
    assert_eq!(by_id[&3].1, Value::Integer(330));
}

#[test]
fn upsert_do_update_where_skips_some() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 100), (2, 5), (3, 999) \
         ON CONFLICT (id) DO UPDATE SET c = excluded.c \
         WHERE excluded.c > c \
         RETURNING id, c",
    );
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn returning_arithmetic_and_string_funcs() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (5, '  alice  ') RETURNING id * 2 AS doubled, UPPER(TRIM(name)) AS clean",
    );
    assert_eq!(qr.columns, vec!["doubled", "clean"]);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[0][1], Value::Text("ALICE".into()));
}

#[test]
fn returning_case_when() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 75), (2, 30), (3, 95)")
        .unwrap();

    let qr = query(
        &conn,
        "DELETE FROM t RETURNING id, \
         CASE WHEN score >= 90 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'F' END AS grade",
    );
    let by_id: std::collections::HashMap<i64, String> = qr
        .rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Integer(n) => *n,
                _ => panic!(),
            };
            let g = match &r[1] {
                Value::Text(s) => s.to_string(),
                _ => panic!(),
            };
            (id, g)
        })
        .collect();
    assert_eq!(by_id[&1], "B");
    assert_eq!(by_id[&2], "F");
    assert_eq!(by_id[&3], "A");
}

#[test]
fn returning_coalesce_with_null_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, NULL) RETURNING id, COALESCE(name, 'unknown') AS n",
    );
    assert_eq!(qr.rows[0][1], Value::Text("unknown".into()));
}

#[test]
fn returning_arithmetic_difference_old_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, p INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE t SET p = p * 2 RETURNING id, new.p - old.p AS delta",
    );
    let by_id: std::collections::HashMap<i64, i64> = qr
        .rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Integer(n) => *n,
                _ => panic!(),
            };
            let d = match &r[1] {
                Value::Integer(n) => *n,
                _ => panic!(),
            };
            (id, d)
        })
        .collect();
    assert_eq!(by_id[&1], 100);
    assert_eq!(by_id[&2], 200);
}

#[test]
fn returning_with_default_filled_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'pending', count INTEGER DEFAULT 0)")
        .unwrap();

    let qr = query(&conn, "INSERT INTO t (id) VALUES (1) RETURNING *");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("pending".into()));
    assert_eq!(qr.rows[0][2], Value::Integer(0));
}

#[test]
fn returning_multi_column_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 2, 'hello'), (3, 4, 'world') RETURNING a, b, val",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(2));

    let qr2 = query(
        &conn,
        "UPDATE t SET val = 'changed' WHERE a = 1 AND b = 2 RETURNING old.val, new.val",
    );
    assert_eq!(qr2.rows[0][0], Value::Text("hello".into()));
    assert_eq!(qr2.rows[0][1], Value::Text("changed".into()));
}

#[test]
fn delete_returning_empty_set_with_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let qr = query(&conn, "DELETE FROM t RETURNING id, v");
    assert_eq!(qr.rows.len(), 0);
    assert_eq!(qr.columns, vec!["id", "v"]);
}

#[test]
fn upsert_skipped_returning_no_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'orig'), (2, 'orig')")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 'new'), (2, 'new') ON CONFLICT (id) DO NOTHING RETURNING *",
    );
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn insert_select_returning_star() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let qr = query(&conn, "INSERT INTO dst SELECT id, v FROM src RETURNING *");
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn update_no_op_returns_unchanged_old_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let qr = query(
        &conn,
        "UPDATE t SET v = v WHERE id = 1 RETURNING old.v, new.v",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(42));
    assert_eq!(qr.rows[0][1], Value::Integer(42));
}

#[test]
fn returning_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let qr = query(&conn, "INSERT INTO t VALUES (1, NULL) RETURNING *");
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Null);
}

#[test]
fn returning_boolean_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, active BOOLEAN)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, TRUE), (2, FALSE) RETURNING id, active",
    );
    assert_eq!(qr.rows[0][1], Value::Boolean(true));
    assert_eq!(qr.rows[1][1], Value::Boolean(false));
}

#[test]
fn returning_real_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ratio REAL)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 3.5) RETURNING ratio * 2 AS doubled",
    );
    assert_eq!(qr.rows[0][0], Value::Real(7.0));
}

#[test]
fn prepared_returning_executed_repeatedly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, $2) RETURNING id, v + 1 AS plus_one")
        .unwrap();
    for i in 0..5 {
        let qr = stmt
            .query_collect(&[Value::Integer(i), Value::Integer(i * 100)])
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(i));
        assert_eq!(qr.rows[0][1], Value::Integer(i * 100 + 1));
    }
}

#[test]
fn prepared_update_returning_old_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();

    let stmt = conn
        .prepare("UPDATE t SET c = c + $1 WHERE id = $2 RETURNING old.c, new.c")
        .unwrap();
    let qr1 = stmt
        .query_collect(&[Value::Integer(5), Value::Integer(1)])
        .unwrap();
    assert_eq!(qr1.rows[0][0], Value::Integer(0));
    assert_eq!(qr1.rows[0][1], Value::Integer(5));
    let qr2 = stmt
        .query_collect(&[Value::Integer(7), Value::Integer(1)])
        .unwrap();
    assert_eq!(qr2.rows[0][0], Value::Integer(5));
    assert_eq!(qr2.rows[0][1], Value::Integer(12));
}

#[test]
fn returning_inside_savepoint_then_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    conn.execute("SAVEPOINT sp").unwrap();
    let qr = query(
        &conn,
        "UPDATE t SET v = 999 WHERE id = 1 RETURNING old.v, new.v",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(100));
    assert_eq!(qr.rows[0][1], Value::Integer(999));
    conn.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
    conn.execute("COMMIT").unwrap();

    let after = query(&conn, "SELECT v FROM t WHERE id = 1");
    assert_eq!(after.rows[0][0], Value::Integer(100));
}

#[test]
fn returning_star_after_add_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x'")
        .unwrap();

    let qr = query(&conn, "INSERT INTO t (id, v) VALUES (1, 42) RETURNING *");
    assert_eq!(qr.columns, vec!["id", "v", "extra"]);
    assert_eq!(qr.rows[0][2], Value::Text("x".into()));
}

#[test]
fn returning_star_after_drop_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("ALTER TABLE t DROP COLUMN b").unwrap();

    let qr = query(&conn, "INSERT INTO t VALUES (1, 10) RETURNING *");
    assert_eq!(qr.columns, vec!["id", "a"]);
    assert_eq!(qr.rows[0][1], Value::Integer(10));
}

#[test]
fn returning_with_foreign_key_parent_inserted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (1, 'parent')").unwrap();

    let qr = query(&conn, "INSERT INTO c VALUES (10, 1) RETURNING *");
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
}

#[test]
fn returning_fk_violation_propagates_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))")
        .unwrap();

    let err = conn
        .execute("INSERT INTO c VALUES (1, 999) RETURNING *")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn returning_unique_index_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_email ON t(email)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a@b.com')").unwrap();

    let err = conn
        .execute("INSERT INTO t VALUES (2, 'a@b.com') RETURNING *")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn returning_after_on_constraint_upsert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, n INTEGER)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_email ON t(email)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x@y.com', 0)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (2, 'x@y.com', 5) ON CONFLICT ON CONSTRAINT idx_email \
         DO UPDATE SET n = n + 1 RETURNING id, n",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
}

#[test]
fn returning_in_nested_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    let qr1 = query(&conn, "INSERT INTO t VALUES (1, 100) RETURNING *");
    assert_eq!(qr1.rows.len(), 1);
    conn.execute("SAVEPOINT a").unwrap();
    let qr2 = query(&conn, "INSERT INTO t VALUES (2, 200) RETURNING id");
    assert_eq!(qr2.rows[0][0], Value::Integer(2));
    conn.execute("SAVEPOINT b").unwrap();
    let qr3 = query(&conn, "DELETE FROM t WHERE id = 1 RETURNING *");
    assert_eq!(qr3.rows.len(), 1);
    conn.execute("ROLLBACK TO SAVEPOINT b").unwrap();
    conn.execute("COMMIT").unwrap();

    let final_count = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(final_count.rows[0][0], Value::Integer(2));
}

#[test]
fn upsert_idempotent_dedup_returning() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE seen (h TEXT PRIMARY KEY, ts INTEGER)")
        .unwrap();

    let qr1 = query(
        &conn,
        "INSERT INTO seen VALUES ('abc', 100) ON CONFLICT (h) DO NOTHING RETURNING *",
    );
    assert_eq!(qr1.rows.len(), 1);

    let qr2 = query(
        &conn,
        "INSERT INTO seen VALUES ('abc', 200) ON CONFLICT (h) DO NOTHING RETURNING *",
    );
    assert_eq!(qr2.rows.len(), 0);
}

#[test]
fn returning_column_order_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 'A', 'B', 'C') RETURNING c, a, b",
    );
    assert_eq!(qr.columns, vec!["c", "a", "b"]);
    assert_eq!(qr.rows[0][0], Value::Text("C".into()));
    assert_eq!(qr.rows[0][1], Value::Text("A".into()));
    assert_eq!(qr.rows[0][2], Value::Text("B".into()));
}

#[test]
fn returning_division_by_zero_handling() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (1, 10, 0) RETURNING a / b AS result")
        .unwrap_err();
    let _ = err;
}

#[test]
fn returning_negative_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();

    let qr = query(&conn, "INSERT INTO t VALUES (1, -42) RETURNING -n AS pos");
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn update_returning_with_complex_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE t SET n = n + 100 WHERE n > 15 AND n < 35 RETURNING id, n",
    );
    assert_eq!(qr.rows.len(), 2);
    let by_id: std::collections::HashMap<i64, i64> = qr
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Integer(i), Value::Integer(n)) => (*i, *n),
            _ => panic!(),
        })
        .collect();
    assert_eq!(by_id[&2], 120);
    assert_eq!(by_id[&3], 130);
}

#[test]
fn returning_after_rename_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)")
        .unwrap();
    conn.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 'value') RETURNING new_name",
    );
    assert_eq!(qr.rows[0][0], Value::Text("value".into()));
}

#[test]
fn returning_with_qualified_table_in_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO accounts VALUES (1, 1000) RETURNING accounts.id, accounts.balance / 100 AS hundreds",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(10));
}

#[test]
fn upsert_returning_aggregate_via_separate_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let qr = query(
        &conn,
        "UPDATE t SET v = v * 2 RETURNING new.v - old.v AS delta",
    );
    let total_delta: i64 = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(n) => *n,
            _ => 0,
        })
        .sum();
    assert_eq!(total_delta, 60);
}

#[test]
fn prepared_returning_torture_500_executions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES ($1, $2) RETURNING id")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..500 {
        let qr = stmt
            .query_collect(&[Value::Integer(i), Value::Integer(i * 7)])
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(i));
    }
    conn.execute("COMMIT").unwrap();

    let count = query(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count.rows[0][0], Value::Integer(500));
}

#[test]
fn upsert_returning_correctly_distinguishes_inserted_vs_updated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100), (3, 300)")
        .unwrap();

    let qr = query(
        &conn,
        "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4) \
         ON CONFLICT (id) DO UPDATE SET n = n + excluded.n \
         RETURNING id, old.n, new.n",
    );
    assert_eq!(qr.rows.len(), 4);

    let by_id: std::collections::HashMap<i64, (Value, Value)> = qr
        .rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Integer(n) => *n,
                _ => panic!(),
            };
            (id, (r[1].clone(), r[2].clone()))
        })
        .collect();
    assert_eq!(by_id[&1].0, Value::Integer(100));
    assert_eq!(by_id[&1].1, Value::Integer(101));
    assert_eq!(by_id[&2].0, Value::Null);
    assert_eq!(by_id[&2].1, Value::Integer(2));
    assert_eq!(by_id[&3].0, Value::Integer(300));
    assert_eq!(by_id[&3].1, Value::Integer(303));
    assert_eq!(by_id[&4].0, Value::Null);
    assert_eq!(by_id[&4].1, Value::Integer(4));
}
