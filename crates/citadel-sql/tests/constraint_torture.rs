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

fn assert_rows_affected(result: ExecutionResult, expected: u64) {
    match result {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn get_rows(result: ExecutionResult) -> Vec<Vec<Value>> {
    match result {
        ExecutionResult::Query(qr) => qr.rows,
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn bulk_insert_with_defaults_1000_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, name TEXT DEFAULT 'unnamed', score REAL DEFAULT 0.0, active BOOLEAN DEFAULT TRUE)"
    ).unwrap());

    for i in 0..1000 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO items (id) VALUES ({i})"))
                .unwrap(),
            1,
        );
    }

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM items WHERE name = 'unnamed'")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(1000));

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM items WHERE score = 0.0")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(1000));

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM items WHERE active = TRUE")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(1000));
}

#[test]
fn bulk_insert_varying_column_subsets() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER DEFAULT 10, b TEXT DEFAULT 'x', c REAL DEFAULT 1.5, d BOOLEAN DEFAULT FALSE)"
    ).unwrap());

    // Only id
    for i in 0..100 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t (id) VALUES ({i})"))
                .unwrap(),
            1,
        );
    }
    // id + a
    for i in 100..200 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t (id, a) VALUES ({i}, 99)"))
                .unwrap(),
            1,
        );
    }
    // id + b + d
    for i in 200..300 {
        assert_rows_affected(
            conn.execute(&format!(
                "INSERT INTO t (id, b, d) VALUES ({i}, 'custom', TRUE)"
            ))
            .unwrap(),
            1,
        );
    }

    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM t WHERE a = 10").unwrap());
    assert_eq!(rows[0][0], Value::Integer(200)); // 0..100 + 200..300

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM t WHERE b = 'x'")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(200)); // 0..100 + 100..200

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM t WHERE c = 1.5")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(300)); // all
}

#[test]
fn check_division_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE ratios (id INTEGER NOT NULL PRIMARY KEY, num INTEGER NOT NULL, denom INTEGER NOT NULL CHECK(denom != 0))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO ratios VALUES (1, 10, 5)")
            .unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO ratios VALUES (2, 10, 0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn check_null_arithmetic_passes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE vals (id INTEGER NOT NULL PRIMARY KEY, x INTEGER CHECK(x > 0), y INTEGER CHECK(y < 100))"
    ).unwrap());

    // NULL passes both checks
    assert_rows_affected(
        conn.execute("INSERT INTO vals VALUES (1, NULL, NULL)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO vals VALUES (2, NULL, 50)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO vals VALUES (3, 10, NULL)")
            .unwrap(),
        1,
    );
}

#[test]
fn check_boundary_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE bounded (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL CHECK(val >= 0 AND val <= 100))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO bounded VALUES (1, 0)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO bounded VALUES (2, 100)").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO bounded VALUES (3, 50)").unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO bounded VALUES (4, -1)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    let err = conn
        .execute("INSERT INTO bounded VALUES (5, 101)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn fk_stress_100_children_delete_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent(id))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO parent VALUES (1, 'root')")
            .unwrap(),
        1,
    );
    for i in 0..100 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO child VALUES ({i}, 1)"))
                .unwrap(),
            1,
        );
    }

    // Cannot delete parent with 100 children
    let err = conn.execute("DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Delete all children first
    assert_rows_affected(
        conn.execute("DELETE FROM child WHERE parent_id = 1")
            .unwrap(),
        100,
    );

    // Now parent can be deleted
    assert_rows_affected(conn.execute("DELETE FROM parent WHERE id = 1").unwrap(), 1);
}

#[test]
fn fk_update_parent_pk_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE dept (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE emp (id INTEGER NOT NULL PRIMARY KEY, dept_id INTEGER NOT NULL REFERENCES dept(id))"
    ).unwrap());

    for i in 1..=5 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO dept VALUES ({i}, 'dept{i}')"))
                .unwrap(),
            1,
        );
    }
    for i in 1..=50 {
        let dept = ((i - 1) % 5) + 1;
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO emp VALUES ({i}, {dept})"))
                .unwrap(),
            1,
        );
    }

    // Cannot update PK of dept that has employees
    let err = conn
        .execute("UPDATE dept SET id = 100 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Can update dept name (non-PK)
    assert_rows_affected(
        conn.execute("UPDATE dept SET name = 'newname' WHERE id = 1")
            .unwrap(),
        1,
    );
}

#[test]
fn mixed_default_check_fk_on_same_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE categories (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
        )
        .unwrap(),
    );
    assert_rows_affected(
        conn.execute("INSERT INTO categories VALUES (1, 'general')")
            .unwrap(),
        1,
    );

    assert_ok(conn.execute(
        "CREATE TABLE products (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL CHECK(LENGTH(name) > 0), price REAL NOT NULL DEFAULT 9.99 CHECK(price > 0), cat_id INTEGER NOT NULL DEFAULT 1 REFERENCES categories(id))"
    ).unwrap());

    // Insert with all defaults (just id + name)
    assert_rows_affected(
        conn.execute("INSERT INTO products (id, name) VALUES (1, 'Widget')")
            .unwrap(),
        1,
    );
    let rows = get_rows(
        conn.execute("SELECT price, cat_id FROM products WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Real(9.99));
    assert_eq!(rows[0][1], Value::Integer(1));

    // CHECK on name: empty string fails
    let err = conn
        .execute("INSERT INTO products (id, name) VALUES (2, '')")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    // CHECK on price: negative fails
    let err = conn
        .execute("INSERT INTO products (id, name, price) VALUES (3, 'Gadget', -1.0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    // FK: invalid category
    let err = conn
        .execute("INSERT INTO products (id, name, cat_id) VALUES (4, 'Thing', 999)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

#[test]
fn transaction_rollback_preserves_state_after_violations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE accounts (id INTEGER NOT NULL PRIMARY KEY, balance INTEGER NOT NULL CHECK(balance >= 0))"
    ).unwrap());
    assert_rows_affected(
        conn.execute("INSERT INTO accounts VALUES (1, 100)")
            .unwrap(),
        1,
    );

    // Start transaction, do valid insert, then violate CHECK
    assert_ok(conn.execute("BEGIN").unwrap());
    assert_rows_affected(
        conn.execute("INSERT INTO accounts VALUES (2, 50)").unwrap(),
        1,
    );

    let err = conn
        .execute("INSERT INTO accounts VALUES (3, -10)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    assert_ok(conn.execute("ROLLBACK").unwrap());

    // Only original row should exist
    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM accounts").unwrap());
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn partial_insert_five_columns_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE records (id INTEGER NOT NULL PRIMARY KEY, a INTEGER DEFAULT 1, b TEXT DEFAULT 'hello', c REAL DEFAULT 3.14, d BOOLEAN DEFAULT TRUE, e INTEGER DEFAULT 42)"
    ).unwrap());

    // Insert with only id, c, e
    assert_rows_affected(
        conn.execute("INSERT INTO records (id, c, e) VALUES (1, 2.71, 99)")
            .unwrap(),
        1,
    );

    let rows = get_rows(
        conn.execute("SELECT a, b, c, d, e FROM records WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(1));
    if let Value::Text(ref s) = rows[0][1] {
        assert_eq!(s.as_str(), "hello");
    } else {
        panic!("expected Text");
    }
    assert_eq!(rows[0][2], Value::Real(2.71));
    assert_eq!(rows[0][3], Value::Boolean(true));
    assert_eq!(rows[0][4], Value::Integer(99));
}

#[test]
fn update_hitting_check_and_fk_simultaneously() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE teams (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_rows_affected(conn.execute("INSERT INTO teams VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO teams VALUES (2)").unwrap(), 1);

    assert_ok(conn.execute(
        "CREATE TABLE players (id INTEGER NOT NULL PRIMARY KEY, team_id INTEGER NOT NULL REFERENCES teams(id), rating INTEGER NOT NULL CHECK(rating >= 0 AND rating <= 100))"
    ).unwrap());
    assert_rows_affected(
        conn.execute("INSERT INTO players VALUES (1, 1, 50)")
            .unwrap(),
        1,
    );

    // Valid update: change team and rating
    assert_rows_affected(
        conn.execute("UPDATE players SET team_id = 2, rating = 75 WHERE id = 1")
            .unwrap(),
        1,
    );

    // Invalid FK
    let err = conn
        .execute("UPDATE players SET team_id = 999 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Invalid CHECK
    let err = conn
        .execute("UPDATE players SET rating = 150 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn multiple_fks_to_different_parents() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE countries (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE cities (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE offices (id INTEGER NOT NULL PRIMARY KEY, country_id INTEGER NOT NULL REFERENCES countries(id), city_id INTEGER NOT NULL REFERENCES cities(id))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO countries VALUES (1, 'USA')")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO cities VALUES (1, 'NYC')")
            .unwrap(),
        1,
    );

    // Valid: both parents exist
    assert_rows_affected(
        conn.execute("INSERT INTO offices VALUES (1, 1, 1)")
            .unwrap(),
        1,
    );

    // Invalid country
    let err = conn
        .execute("INSERT INTO offices VALUES (2, 99, 1)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Invalid city
    let err = conn
        .execute("INSERT INTO offices VALUES (3, 1, 99)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

#[test]
fn self_referencing_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE employees (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, manager_id INTEGER REFERENCES employees(id))"
    ).unwrap());

    // CEO with no manager
    assert_rows_affected(
        conn.execute("INSERT INTO employees VALUES (1, 'CEO', NULL)")
            .unwrap(),
        1,
    );
    // VP reporting to CEO
    assert_rows_affected(
        conn.execute("INSERT INTO employees VALUES (2, 'VP', 1)")
            .unwrap(),
        1,
    );
    // Manager reporting to VP
    assert_rows_affected(
        conn.execute("INSERT INTO employees VALUES (3, 'Manager', 2)")
            .unwrap(),
        1,
    );

    // Invalid: references non-existent employee
    let err = conn
        .execute("INSERT INTO employees VALUES (4, 'Ghost', 999)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Cannot delete VP with subordinates
    let err = conn
        .execute("DELETE FROM employees WHERE id = 2")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Delete leaf first, then intermediate
    assert_rows_affected(
        conn.execute("DELETE FROM employees WHERE id = 3").unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("DELETE FROM employees WHERE id = 2").unwrap(),
        1,
    );
}

#[test]
fn fk_composite_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE ledger (year INTEGER NOT NULL, month INTEGER NOT NULL, amount REAL, PRIMARY KEY (year, month))"
    ).unwrap());
    assert_ok(conn.execute(
        "CREATE TABLE entries (id INTEGER NOT NULL PRIMARY KEY, year INTEGER NOT NULL, month INTEGER NOT NULL, FOREIGN KEY (year, month) REFERENCES ledger(year, month))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO ledger VALUES (2024, 1, 1000.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO ledger VALUES (2024, 2, 2000.0)")
            .unwrap(),
        1,
    );

    // Valid
    assert_rows_affected(
        conn.execute("INSERT INTO entries VALUES (1, 2024, 1)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO entries VALUES (2, 2024, 2)")
            .unwrap(),
        1,
    );

    // Invalid: (2024, 3) doesn't exist
    let err = conn
        .execute("INSERT INTO entries VALUES (3, 2024, 3)")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Cannot delete referenced ledger entry
    let err = conn
        .execute("DELETE FROM ledger WHERE year = 2024 AND month = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

#[test]
fn check_with_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, val INTEGER, CHECK(COALESCE(val, 0) >= 0))"
    ).unwrap());

    // NULL coalesces to 0, passes >= 0
    assert_rows_affected(
        conn.execute("INSERT INTO items VALUES (1, NULL)").unwrap(),
        1,
    );
    assert_rows_affected(conn.execute("INSERT INTO items VALUES (2, 10)").unwrap(), 1);

    let err = conn
        .execute("INSERT INTO items VALUES (3, -5)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn check_with_case_when() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL, CHECK(CASE WHEN status = 'free' THEN amount = 0.0 ELSE amount > 0.0 END))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (1, 'free', 0.0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO orders VALUES (2, 'paid', 9.99)")
            .unwrap(),
        1,
    );

    // free with non-zero
    let err = conn
        .execute("INSERT INTO orders VALUES (3, 'free', 5.0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    // paid with zero
    let err = conn
        .execute("INSERT INTO orders VALUES (4, 'paid', 0.0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn check_with_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL CHECK(val BETWEEN 1 AND 10))"
    ).unwrap());

    assert_rows_affected(conn.execute("INSERT INTO scores VALUES (1, 1)").unwrap(), 1);
    assert_rows_affected(
        conn.execute("INSERT INTO scores VALUES (2, 10)").unwrap(),
        1,
    );
    assert_rows_affected(conn.execute("INSERT INTO scores VALUES (3, 5)").unwrap(), 1);

    let err = conn
        .execute("INSERT INTO scores VALUES (4, 0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    let err = conn
        .execute("INSERT INTO scores VALUES (5, 11)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn default_with_nested_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute(
            "CREATE TABLE calc (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT (2 * 3 + 1))",
        )
        .unwrap(),
    );

    assert_rows_affected(conn.execute("INSERT INTO calc (id) VALUES (1)").unwrap(), 1);
    let rows = get_rows(conn.execute("SELECT val FROM calc WHERE id = 1").unwrap());
    assert_eq!(rows[0][0], Value::Integer(7));
}

#[test]
fn not_null_default_check_on_same_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE strict (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL DEFAULT 50 CHECK(val >= 0 AND val <= 100))"
    ).unwrap());

    // Omit val → gets default 50, passes NOT NULL, passes CHECK
    assert_rows_affected(
        conn.execute("INSERT INTO strict (id) VALUES (1)").unwrap(),
        1,
    );
    let rows = get_rows(conn.execute("SELECT val FROM strict WHERE id = 1").unwrap());
    assert_eq!(rows[0][0], Value::Integer(50));

    // Explicit 75 → passes
    assert_rows_affected(
        conn.execute("INSERT INTO strict VALUES (2, 75)").unwrap(),
        1,
    );

    // Explicit 150 → fails CHECK
    let err = conn
        .execute("INSERT INTO strict VALUES (3, 150)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn interleaved_insert_delete_with_fk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE tags (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE posts (id INTEGER NOT NULL PRIMARY KEY, tag_id INTEGER NOT NULL REFERENCES tags(id))"
    ).unwrap());

    // Interleave: add parent, add children, remove children, remove parent, repeat
    for batch in 0..10 {
        let tag_id = batch + 1;
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO tags VALUES ({tag_id})"))
                .unwrap(),
            1,
        );

        for j in 0..10 {
            let post_id = batch * 10 + j + 1;
            assert_rows_affected(
                conn.execute(&format!("INSERT INTO posts VALUES ({post_id}, {tag_id})"))
                    .unwrap(),
                1,
            );
        }

        // Delete children
        assert_rows_affected(
            conn.execute(&format!("DELETE FROM posts WHERE tag_id = {tag_id}"))
                .unwrap(),
            10,
        );

        // Now can delete parent
        assert_rows_affected(
            conn.execute(&format!("DELETE FROM tags WHERE id = {tag_id}"))
                .unwrap(),
            1,
        );
    }

    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM tags").unwrap());
    assert_eq!(rows[0][0], Value::Integer(0));
    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM posts").unwrap());
    assert_eq!(rows[0][0], Value::Integer(0));
}

#[test]
fn many_fk_violations_then_valid_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE colors (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, color_id INTEGER NOT NULL REFERENCES colors(id))"
    ).unwrap());

    // 50 FK violations
    for i in 0..50 {
        let err = conn
            .execute(&format!("INSERT INTO items VALUES ({i}, 999)"))
            .unwrap_err();
        assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
    }

    // Verify table still empty
    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM items").unwrap());
    assert_eq!(rows[0][0], Value::Integer(0));

    // Now add parent and valid children
    assert_rows_affected(conn.execute("INSERT INTO colors VALUES (1)").unwrap(), 1);
    for i in 0..50 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO items VALUES ({i}, 1)"))
                .unwrap(),
            1,
        );
    }

    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM items").unwrap());
    assert_eq!(rows[0][0], Value::Integer(50));
}

#[test]
fn check_on_update_multiple_set_clauses() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE ranges (id INTEGER NOT NULL PRIMARY KEY, lo INTEGER NOT NULL, hi INTEGER NOT NULL, CHECK(lo <= hi))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO ranges VALUES (1, 10, 20)")
            .unwrap(),
        1,
    );

    // Valid: swap to wider range
    assert_rows_affected(
        conn.execute("UPDATE ranges SET lo = 5, hi = 25 WHERE id = 1")
            .unwrap(),
        1,
    );

    // Invalid: lo > hi
    let err = conn
        .execute("UPDATE ranges SET lo = 30, hi = 10 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));

    // Verify original values unchanged after failed update
    let rows = get_rows(
        conn.execute("SELECT lo, hi FROM ranges WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(5));
    assert_eq!(rows[0][1], Value::Integer(25));
}

#[test]
fn fk_in_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE parent (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE child (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER NOT NULL REFERENCES parent(id))"
    ).unwrap());

    assert_rows_affected(conn.execute("INSERT INTO parent VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO child VALUES (1, 1)").unwrap(), 1);

    // In transaction: add new parent, add child referencing it, then rollback
    assert_ok(conn.execute("BEGIN").unwrap());
    assert_rows_affected(conn.execute("INSERT INTO parent VALUES (2)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO child VALUES (2, 2)").unwrap(), 1);
    assert_ok(conn.execute("ROLLBACK").unwrap());

    // Only original data survives
    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM parent").unwrap());
    assert_eq!(rows[0][0], Value::Integer(1));
    let rows = get_rows(conn.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn default_does_not_override_explicit_null_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER DEFAULT 42)")
            .unwrap(),
    );

    // 100 rows with explicit NULL
    for i in 0..100 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t (id, val) VALUES ({i}, NULL)"))
                .unwrap(),
            1,
        );
    }

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM t WHERE val IS NULL")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(100));

    // 100 rows with omitted val → gets default
    for i in 100..200 {
        assert_rows_affected(
            conn.execute(&format!("INSERT INTO t (id) VALUES ({i})"))
                .unwrap(),
            1,
        );
    }

    let rows = get_rows(
        conn.execute("SELECT COUNT(*) FROM t WHERE val = 42")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::Integer(100));
}

#[test]
fn check_mixed_types_in_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE mixed (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL, count INTEGER NOT NULL, CHECK(active = TRUE OR count > 0))"
    ).unwrap());

    assert_rows_affected(
        conn.execute("INSERT INTO mixed VALUES (1, TRUE, 0)")
            .unwrap(),
        1,
    );
    assert_rows_affected(
        conn.execute("INSERT INTO mixed VALUES (2, FALSE, 5)")
            .unwrap(),
        1,
    );

    // inactive with zero count → fails
    let err = conn
        .execute("INSERT INTO mixed VALUES (3, FALSE, 0)")
        .unwrap_err();
    assert!(matches!(err, SqlError::CheckViolation(..)));
}

#[test]
fn fk_delete_parent_after_child_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id))"
    ).unwrap());

    assert_rows_affected(conn.execute("INSERT INTO p VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO p VALUES (2)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO c VALUES (1, 1)").unwrap(), 1);

    // Move child to parent 2
    assert_rows_affected(
        conn.execute("UPDATE c SET pid = 2 WHERE id = 1").unwrap(),
        1,
    );

    // Now parent 1 has no children → can delete
    assert_rows_affected(conn.execute("DELETE FROM p WHERE id = 1").unwrap(), 1);

    // Parent 2 has child → cannot delete
    let err = conn.execute("DELETE FROM p WHERE id = 2").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));
}

#[test]
fn fk_child_update_to_invalid_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id))"
    ).unwrap());

    assert_rows_affected(conn.execute("INSERT INTO p VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO c VALUES (1, 1)").unwrap(), 1);

    // Update child FK to non-existent parent
    let err = conn
        .execute("UPDATE c SET pid = 999 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(..)));

    // Verify unchanged
    let rows = get_rows(conn.execute("SELECT pid FROM c WHERE id = 1").unwrap());
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn fk_update_child_to_null_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY)")
            .unwrap(),
    );
    assert_ok(
        conn.execute(
            "CREATE TABLE c (id INTEGER NOT NULL PRIMARY KEY, pid INTEGER REFERENCES p(id))",
        )
        .unwrap(),
    );

    assert_rows_affected(conn.execute("INSERT INTO p VALUES (1)").unwrap(), 1);
    assert_rows_affected(conn.execute("INSERT INTO c VALUES (1, 1)").unwrap(), 1);

    // Set FK to NULL → OK (MATCH SIMPLE)
    assert_rows_affected(
        conn.execute("UPDATE c SET pid = NULL WHERE id = 1")
            .unwrap(),
        1,
    );

    let rows = get_rows(conn.execute("SELECT pid FROM c WHERE id = 1").unwrap());
    assert_eq!(rows[0][0], Value::Null);
}
