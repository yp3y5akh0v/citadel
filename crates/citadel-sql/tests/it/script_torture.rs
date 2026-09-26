use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_rows(result: &ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(*n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: &ExecutionResult) {
    assert!(
        matches!(result, ExecutionResult::Ok),
        "expected Ok, got {result:?}"
    );
}

fn query_int(conn: &Connection, sql: &str) -> i64 {
    let exec = conn.execute_script(sql);
    match &exec.completed[0] {
        ExecutionResult::Query(qr) => match &qr.rows[0][0] {
            Value::Integer(n) => *n,
            other => panic!("expected Integer, got {other:?}"),
        },
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn torture_whitespace_only_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    for ws in ["", "   ", "\n\n\n", "\t\t", " \t\n \r\n "] {
        let exec = conn.execute_script(ws);
        assert!(
            exec.completed.is_empty(),
            "ws {:?} should produce no completed",
            ws
        );
        assert!(
            exec.error.is_some(),
            "ws {:?} should produce parse error",
            ws
        );
    }
}

#[test]
fn torture_semicolons_only() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(";;;");
    assert!(exec.completed.is_empty());
    assert!(exec.error.is_some());
}

#[test]
fn torture_trailing_semicolons() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    for suffix in [";", ";;", ";;;"] {
        let sql = format!("SELECT * FROM t{suffix}");
        let exec = conn.execute_script(&sql);
        assert_eq!(exec.completed.len(), 1, "suffix {suffix:?}");
        assert!(exec.error.is_none(), "suffix {suffix:?}: {:?}", exec.error);
    }
}

#[test]
fn torture_comments_only_script_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("-- nothing\n-- here\n");
    assert!(exec.completed.is_empty());
    assert!(matches!(exec.error, Some(SqlError::Parse(_))));
}

#[test]
fn torture_100_statements() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let mut script = String::new();
    for i in 0..100 {
        script.push_str(&format!("INSERT INTO t VALUES ({i}, {});\n", i * 2));
    }

    let exec = conn.execute_script(&script);
    assert_eq!(exec.completed.len(), 100);
    assert!(exec.error.is_none());
    for r in &exec.completed {
        assert_rows(r, 1);
    }

    let count = query_int(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count, 100);
}

#[test]
fn torture_all_ddl_in_one_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
         CREATE INDEX idx_name ON t (name);
         CREATE UNIQUE INDEX idx_name_age ON t (name, age);
         CREATE VIEW v AS SELECT id, name FROM t;
         ALTER TABLE t ADD COLUMN email TEXT;
         ALTER TABLE t RENAME COLUMN name TO full_name;
         DROP VIEW v;
         DROP INDEX idx_name_age;
         DROP INDEX idx_name;
         DROP TABLE t;",
    );
    assert_eq!(exec.completed.len(), 10);
    assert!(exec.error.is_none());
    for r in &exec.completed {
        assert_ok(r);
    }
}

#[test]
fn torture_mid_script_failure_then_continue_externally() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let exec = conn.execute_script(
        "INSERT INTO t VALUES (1, 10);
         INSERT INTO t VALUES (2, 20);
         INSERT INTO t VALUES (1, 30);
         INSERT INTO t VALUES (3, 40)",
    );
    assert_eq!(exec.completed.len(), 2);
    assert!(exec.error.is_some());

    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(query_int(&conn, "SELECT v FROM t WHERE id = 1"), 10);
    assert_eq!(query_int(&conn, "SELECT v FROM t WHERE id = 2"), 20);

    let retry = conn.execute_script("INSERT INTO t VALUES (3, 40)");
    assert_rows(&retry.completed[0], 1);
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 3);
}

#[test]
fn torture_fail_at_first_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "SELECT * FROM nonexistent_table;
         CREATE TABLE t (id INTEGER PRIMARY KEY)",
    );
    assert_eq!(exec.completed.len(), 0);
    assert!(exec.error.is_some());
}

#[test]
fn torture_fail_at_last_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let exec = conn.execute_script(
        "INSERT INTO t VALUES (1, 10);
         INSERT INTO t VALUES (2, 20);
         INSERT INTO t VALUES (1, 30)",
    );
    assert_eq!(exec.completed.len(), 2);
    assert!(exec.error.is_some());
}

#[test]
fn torture_special_chars_in_strings() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT NOT NULL)");

    let tricky = "INSERT INTO t VALUES (1, 'a;b;c');
                  INSERT INTO t VALUES (2, 'it''s fine');
                  INSERT INTO t VALUES (3, ';;;;;');
                  INSERT INTO t VALUES (4, 'unicode: 日本語');
                  INSERT INTO t VALUES (5, '')";
    let exec = conn.execute_script(tricky);
    assert_eq!(exec.completed.len(), 5, "got: {:?}", exec.error);
    assert!(exec.error.is_none());
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 5);
}

#[test]
fn torture_multi_row_insert_and_multi_statement_together() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
         INSERT INTO t VALUES (4, 40), (5, 50);
         SELECT COUNT(*) FROM t",
    );
    assert_eq!(exec.completed.len(), 4);
    assert_ok(&exec.completed[0]);
    assert_rows(&exec.completed[1], 3);
    assert_rows(&exec.completed[2], 2);
    match &exec.completed[3] {
        ExecutionResult::Query(qr) => match &qr.rows[0][0] {
            Value::Integer(n) => assert_eq!(*n, 5),
            _ => panic!("expected integer 5"),
        },
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_begin_rollback_discards_all() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "BEGIN;
         INSERT INTO t VALUES (1);
         INSERT INTO t VALUES (2);
         INSERT INTO t VALUES (3);
         ROLLBACK",
    );
    assert_eq!(exec.completed.len(), 5);
    assert!(exec.error.is_none());
    assert!(!conn.in_transaction());
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 0);
}

#[test]
fn torture_begin_commit_then_begin_rollback_in_one_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "BEGIN; INSERT INTO t VALUES (1); COMMIT;
         BEGIN; INSERT INTO t VALUES (2); ROLLBACK;
         BEGIN; INSERT INTO t VALUES (3); COMMIT;",
    );
    assert_eq!(exec.completed.len(), 9);
    assert!(exec.error.is_none());
    assert!(!conn.in_transaction());

    // Only rows 1 and 3 should remain (txn 2 was rolled back).
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(query_int(&conn, "SELECT id FROM t ORDER BY id LIMIT 1"), 1);
}

#[test]
fn torture_nested_savepoints_with_rollback_partial() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "BEGIN;
         INSERT INTO t VALUES (1);
         SAVEPOINT sp1;
         INSERT INTO t VALUES (2);
         SAVEPOINT sp2;
         INSERT INTO t VALUES (3);
         ROLLBACK TO sp2;
         INSERT INTO t VALUES (4);
         COMMIT",
    );
    assert_eq!(exec.completed.len(), 9);
    assert!(exec.error.is_none());
    assert!(!conn.in_transaction());

    // Rows 1, 2, 4 remain; row 3 was rolled back to sp2.
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 3);
}

#[test]
fn torture_multi_row_insert_partial_failure_mid_stmt() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "INSERT INTO t VALUES (1);
         INSERT INTO t VALUES (2), (3), (2);
         INSERT INTO t VALUES (99)",
    );
    assert_eq!(exec.completed.len(), 1);
    assert!(exec.error.is_some());

    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn torture_schema_change_visible_to_later_statements() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT);
         INSERT INTO t VALUES (1, 'a@b');
         ALTER TABLE t ADD COLUMN age INTEGER;
         UPDATE t SET age = 30 WHERE id = 1;
         SELECT age FROM t WHERE id = 1",
    );
    assert_eq!(exec.completed.len(), 5);
    assert!(exec.error.is_none());
    match &exec.completed[4] {
        ExecutionResult::Query(qr) => match &qr.rows[0][0] {
            Value::Integer(age) => assert_eq!(*age, 30),
            _ => panic!("expected Integer"),
        },
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_drop_table_mid_script_then_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY);
         INSERT INTO t VALUES (1);
         DROP TABLE t;
         SELECT * FROM t",
    );
    assert_eq!(exec.completed.len(), 3);
    assert!(exec.error.is_some());
}

#[test]
fn torture_view_created_and_used_in_same_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
         CREATE VIEW high AS SELECT id FROM t WHERE v > 15;
         SELECT COUNT(*) FROM high",
    );
    assert_eq!(exec.completed.len(), 4);
    assert!(exec.error.is_none());
    match &exec.completed[3] {
        ExecutionResult::Query(qr) => match &qr.rows[0][0] {
            Value::Integer(n) => assert_eq!(*n, 2),
            _ => panic!("expected Integer"),
        },
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_mixed_case_and_whitespace() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "Create\tTABLE\tt\r\n(id integer PRIMARY KEY);\r\n\r\n\
         INsErT INTO t   VALUES\t\t(1);\n\n\n\
         sElEcT  *  fRoM  t",
    );
    assert_eq!(exec.completed.len(), 3);
    assert!(exec.error.is_none());
}

#[test]
fn torture_state_isolation_across_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let a = conn.execute_script("BEGIN; INSERT INTO t VALUES (1)");
    assert_eq!(a.completed.len(), 2);
    assert!(conn.in_transaction());

    let b = conn.execute_script("INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)");
    assert_eq!(b.completed.len(), 2);
    assert!(conn.in_transaction());

    let c = conn.execute_script("COMMIT");
    assert_eq!(c.completed.len(), 1);
    assert!(!conn.in_transaction());

    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 3);
}

#[test]
fn torture_error_message_mentions_problem() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("SELECT * FROM does_not_exist");
    assert!(exec.completed.is_empty());
    let err = exec.error.unwrap();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("does_not_exist") || msg.to_lowercase().contains("table"),
        "error message should mention the missing table, got: {msg}"
    );
}

#[test]
fn torture_large_single_insert_with_many_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let mut rows = Vec::with_capacity(500);
    for i in 0..500 {
        rows.push(format!("({i}, {})", i * 2));
    }
    let sql = format!("INSERT INTO t VALUES {}", rows.join(", "));
    let exec = conn.execute_script(&sql);
    assert_eq!(exec.completed.len(), 1);
    assert_rows(&exec.completed[0], 500);
}

#[test]
fn torture_select_with_integer_literal() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script("SELECT 1; SELECT 2; SELECT 3");
    assert_eq!(exec.completed.len(), 3);
    assert!(exec.error.is_none());
    for (i, r) in exec.completed.iter().enumerate() {
        match r {
            ExecutionResult::Query(qr) => match &qr.rows[0][0] {
                Value::Integer(n) => assert_eq!(*n as usize, i + 1),
                _ => panic!("expected Integer"),
            },
            _ => panic!("expected Query"),
        }
    }
}

#[test]
fn torture_1000_statements_single_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let mut script = String::new();
    for i in 0..1000 {
        script.push_str(&format!("INSERT INTO t VALUES ({i}, {});", i * 7 % 1000));
    }
    let exec = conn.execute_script(&script);
    assert_eq!(exec.completed.len(), 1000);
    assert!(exec.error.is_none());

    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 1000);
}

#[test]
fn torture_deep_savepoint_nesting() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "BEGIN;
         INSERT INTO t VALUES (0);
         SAVEPOINT s1; INSERT INTO t VALUES (1);
         SAVEPOINT s2; INSERT INTO t VALUES (2);
         SAVEPOINT s3; INSERT INTO t VALUES (3);
         SAVEPOINT s4; INSERT INTO t VALUES (4);
         SAVEPOINT s5; INSERT INTO t VALUES (5);
         SAVEPOINT s6; INSERT INTO t VALUES (6);
         SAVEPOINT s7; INSERT INTO t VALUES (7);
         SAVEPOINT s8; INSERT INTO t VALUES (8);
         ROLLBACK TO s4;
         INSERT INTO t VALUES (99);
         COMMIT",
    );
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);
    assert!(!conn.in_transaction());

    // ROLLBACK TO s4 undoes everything after s4's declaration — including INSERT 4.
    // Surviving: rows 0, 1, 2, 3 (declared before s4) plus 99 (added after rollback) = 5.
    let count = query_int(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count, 5);
}

#[test]
fn torture_complex_etl_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE raw (id INTEGER PRIMARY KEY, name TEXT, age TEXT);
         CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL, tier TEXT NOT NULL);
         INSERT INTO raw VALUES (1, 'Alice', '30'), (2, 'Bob', '45'), (3, 'Carol', '22'), (4, 'Dan', '60');
         BEGIN;
         INSERT INTO users SELECT id, name, CAST(age AS INTEGER), CASE WHEN CAST(age AS INTEGER) >= 50 THEN 'senior' WHEN CAST(age AS INTEGER) >= 30 THEN 'adult' ELSE 'young' END FROM raw;
         DELETE FROM raw;
         COMMIT;
         SELECT tier, COUNT(*) FROM users GROUP BY tier ORDER BY tier",
    );
    assert_eq!(exec.completed.len(), 8);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    match &exec.completed[7] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 3);
            // Groups: adult=2 (Alice 30, Bob 45), senior=1 (Dan 60), young=1 (Carol 22)
            assert_eq!(qr.rows[0][0], Value::Text("adult".into()));
            assert_eq!(qr.rows[0][1], Value::Integer(2));
            assert_eq!(qr.rows[1][0], Value::Text("senior".into()));
            assert_eq!(qr.rows[1][1], Value::Integer(1));
            assert_eq!(qr.rows[2][0], Value::Text("young".into()));
            assert_eq!(qr.rows[2][1], Value::Integer(1));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_mixed_statement_types_with_joins_and_aggregates() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount INTEGER NOT NULL, FOREIGN KEY (user_id) REFERENCES users(id));
         INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
         INSERT INTO orders VALUES (1, 1, 100), (2, 1, 250), (3, 2, 75), (4, 2, 200), (5, 3, 50);
         SELECT u.name, COUNT(o.id), SUM(o.amount) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name ORDER BY u.id;
         UPDATE orders SET amount = amount * 2 WHERE user_id = 1;
         DELETE FROM orders WHERE user_id = 3;
         SELECT u.name, COALESCE(SUM(o.amount), 0) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name ORDER BY u.id",
    );
    assert_eq!(exec.completed.len(), 8);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    // Last SELECT: Alice (100+250)*2 = 700, Bob 75+200 = 275, Carol 0
    match &exec.completed[7] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 3);
            assert_eq!(qr.rows[0][1], Value::Integer(700));
            assert_eq!(qr.rows[1][1], Value::Integer(275));
            assert_eq!(qr.rows[2][1], Value::Integer(0));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_cte_and_window_in_multi_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT NOT NULL, amount INTEGER NOT NULL);
         INSERT INTO sales VALUES (1, 'east', 100), (2, 'east', 200), (3, 'west', 150), (4, 'west', 50), (5, 'east', 300);
         WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT region, total FROM totals ORDER BY total DESC;
         SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rk FROM sales ORDER BY region, amount DESC",
    );
    assert_eq!(exec.completed.len(), 4);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    // CTE query: east=600, west=200
    match &exec.completed[2] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 2);
            assert_eq!(qr.rows[0][0], Value::Text("east".into()));
            assert_eq!(qr.rows[0][1], Value::Integer(600));
            assert_eq!(qr.rows[1][0], Value::Text("west".into()));
            assert_eq!(qr.rows[1][1], Value::Integer(200));
        }
        _ => panic!("expected Query for CTE"),
    }

    // Window function: 3 east rows, 2 west rows. First east row = rank 1 (amount 300).
    match &exec.completed[3] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 5);
            assert_eq!(qr.rows[0][1], Value::Text("east".into()));
            assert_eq!(qr.rows[0][2], Value::Integer(300));
            assert_eq!(qr.rows[0][3], Value::Integer(1));
        }
        _ => panic!("expected Query for window fn"),
    }
}

#[test]
fn torture_schema_migration_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE users_v1 (id INTEGER PRIMARY KEY, full_name TEXT);
         INSERT INTO users_v1 VALUES (1, 'Alice Smith'), (2, 'Bob Jones');

         CREATE TABLE users_v2 (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL);
         INSERT INTO users_v2 (id, first_name, last_name) SELECT id, 'Alice', 'Smith' FROM users_v1 WHERE id = 1;
         INSERT INTO users_v2 (id, first_name, last_name) SELECT id, 'Bob', 'Jones' FROM users_v1 WHERE id = 2;

         DROP TABLE users_v1;

         SELECT id, first_name, last_name FROM users_v2 ORDER BY id",
    );
    assert_eq!(exec.completed.len(), 7);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    match &exec.completed[6] {
        ExecutionResult::Query(qr) => assert_eq!(qr.rows.len(), 2),
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_read_modify_write_pattern_in_txn() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);
         INSERT INTO accounts VALUES (1, 1000), (2, 500)",
    );

    let exec = conn.execute_script(
        "BEGIN;
         UPDATE accounts SET balance = balance - 200 WHERE id = 1;
         UPDATE accounts SET balance = balance + 200 WHERE id = 2;
         SELECT id, balance FROM accounts ORDER BY id;
         COMMIT",
    );
    assert_eq!(exec.completed.len(), 5);
    assert!(exec.error.is_none());

    match &exec.completed[3] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows[0][1], Value::Integer(800));
            assert_eq!(qr.rows[1][1], Value::Integer(700));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_correlated_subquery_in_script() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER NOT NULL, name TEXT NOT NULL, salary INTEGER NOT NULL);
         INSERT INTO dept VALUES (1, 'eng'), (2, 'sales');
         INSERT INTO emp VALUES (1, 1, 'a', 100), (2, 1, 'b', 200), (3, 1, 'c', 150), (4, 2, 'd', 120), (5, 2, 'e', 90);
         SELECT e.name, e.salary FROM emp e WHERE e.salary > (SELECT AVG(salary) FROM emp WHERE dept_id = e.dept_id) ORDER BY e.id",
    );
    assert_eq!(exec.completed.len(), 5);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    // Eng avg = 150, so b (200) qualifies. Sales avg = 105, so d (120) qualifies.
    match &exec.completed[4] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 2);
            assert_eq!(qr.rows[0][0], Value::Text("b".into()));
            assert_eq!(qr.rows[1][0], Value::Text("d".into()));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_mid_txn_error_recovery_with_rollback_to() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");

    let first = conn.execute_script(
        "BEGIN;
         INSERT INTO t VALUES (1, 10);
         SAVEPOINT recover;
         INSERT INTO t VALUES (2, 20);
         INSERT INTO t VALUES (2, 30)",
    );
    assert_eq!(first.completed.len(), 4);
    assert!(first.error.is_some());
    assert!(conn.in_transaction());

    let recover = conn.execute_script(
        "ROLLBACK TO recover;
         INSERT INTO t VALUES (3, 30);
         COMMIT",
    );
    assert_eq!(recover.completed.len(), 3);
    assert!(recover.error.is_none());
    assert!(!conn.in_transaction());

    // Final state: id 1 (from before savepoint) + id 3 (after recovery).
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(query_int(&conn, "SELECT v FROM t WHERE id = 1"), 10);
    assert_eq!(query_int(&conn, "SELECT v FROM t WHERE id = 3"), 30);
}

#[test]
fn torture_many_mixed_statements() {
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    let exec = conn.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
         INSERT INTO t VALUES (1, 10);
         SELECT * FROM t;
         BEGIN;
         INSERT INTO t VALUES (2, 20);
         UPDATE t SET v = 100 WHERE id = 1;
         SELECT v FROM t WHERE id = 1;
         SAVEPOINT mid;
         DELETE FROM t WHERE id = 2;
         SELECT COUNT(*) FROM t;
         ROLLBACK TO mid;
         SELECT COUNT(*) FROM t;
         COMMIT;
         SELECT id, v FROM t ORDER BY id",
    );
    assert_eq!(exec.completed.len(), 14);
    assert!(exec.error.is_none(), "got error: {:?}", exec.error);

    // After COMMIT: id 1 (v=100, updated), id 2 (v=20, kept after rollback to mid).
    match &exec.completed[13] {
        ExecutionResult::Query(qr) => {
            assert_eq!(qr.rows.len(), 2);
            assert_eq!(qr.rows[0][0], Value::Integer(1));
            assert_eq!(qr.rows[0][1], Value::Integer(100));
            assert_eq!(qr.rows[1][0], Value::Integer(2));
            assert_eq!(qr.rows[1][1], Value::Integer(20));
        }
        _ => panic!("expected Query"),
    }
}

#[test]
fn torture_transaction_not_auto_rollback_on_error() {
    // Errors inside BEGIN...COMMIT do not auto-rollback.
    let tmp = tempfile::tempdir().unwrap();
    let db = create_db(tmp.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let exec = conn.execute_script(
        "BEGIN;
         INSERT INTO t VALUES (1);
         INSERT INTO t VALUES (1);
         COMMIT",
    );
    assert_eq!(exec.completed.len(), 2);
    assert!(exec.error.is_some());
    assert!(conn.in_transaction());

    let cleanup = conn.execute_script("ROLLBACK");
    assert_ok(&cleanup.completed[0]);
    assert!(!conn.in_transaction());
    assert_eq!(query_int(&conn, "SELECT COUNT(*) FROM t"), 0);
}
