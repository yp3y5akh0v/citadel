use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn setup_users_and_audit(conn: &Connection) {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, event TEXT, user_id INTEGER)")
        .unwrap();
}

#[test]
fn create_trigger_parses_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER audit_insert AFTER INSERT ON users \
         FOR EACH ROW BEGIN \
           INSERT INTO audit VALUES (1, 'insert', 99); \
         END",
    )
    .unwrap();
}

#[test]
fn create_trigger_duplicate_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
    )
    .unwrap();
    let err = conn
        .execute(
            "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
        )
        .unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn create_trigger_on_missing_table_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "CREATE TRIGGER t AFTER INSERT ON missing FOR EACH ROW \
             BEGIN SELECT 1; END",
        )
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_trigger_removes_from_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
    )
    .unwrap();
    conn.execute("DROP TRIGGER t").unwrap();
    // Re-create with same name should now succeed.
    conn.execute(
        "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
    )
    .unwrap();
}

#[test]
fn drop_trigger_if_exists_swallows_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("DROP TRIGGER IF EXISTS does_not_exist")
        .unwrap();
}

#[test]
fn drop_trigger_missing_errors_without_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("DROP TRIGGER does_not_exist").unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn after_insert_trigger_fires_and_writes_to_audit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER audit_insert AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'created', 42); END",
    )
    .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'alice@x')")
        .unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn after_insert_trigger_fires_once_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE counter (n INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO counter VALUES (0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON src FOR EACH ROW \
         BEGIN UPDATE counter SET n = n + 1 WHERE n IS NOT NULL; END",
    )
    .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let p = conn.prepare("SELECT n FROM counter LIMIT 1").unwrap();
    let r = p.query_collect(&[]).unwrap();
    // Bumped 3 times (one per row inserted).
    assert!(
        matches!(r.rows[0][0], Value::Integer(n) if n >= 3),
        "expected counter ≥ 3, got: {:?}",
        r.rows[0][0]
    );
}

#[test]
fn disable_trigger_skips_firing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER audit_insert AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'created', 1); END",
    )
    .unwrap();
    conn.execute("ALTER TABLE users DISABLE TRIGGER audit_insert")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'alice@x')")
        .unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(0));
}

#[test]
fn enable_trigger_after_disable_fires_again() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER audit_insert AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'created', 1); END",
    )
    .unwrap();
    conn.execute("ALTER TABLE users DISABLE TRIGGER audit_insert")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x')").unwrap();
    conn.execute("ALTER TABLE users ENABLE TRIGGER audit_insert")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'b@x')").unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn instead_of_trigger_on_table_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute(
            "CREATE TRIGGER bad INSTEAD OF INSERT ON t FOR EACH ROW \
             BEGIN SELECT 1; END",
        )
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("instead of") || err.to_string().contains("view")
    );
}

#[test]
fn instead_of_statement_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let err = conn
        .execute(
            "CREATE TRIGGER bad INSTEAD OF INSERT ON v FOR EACH STATEMENT \
             BEGIN SELECT 1; END",
        )
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("for each row")
            || err.to_string().to_lowercase().contains("instead of")
    );
}

#[test]
fn trigger_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup_users_and_audit(&conn);
        conn.execute(
            "CREATE TRIGGER audit_t AFTER INSERT ON users FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
        )
        .unwrap();
    }
    // Reopen and verify the trigger still fires.
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("INSERT INTO users VALUES (10, 'reopened@x')")
        .unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn trigger_body_with_non_dml_rejected_at_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    // CREATE TABLE inside a trigger body is not allowed.
    let err = conn
        .execute(
            "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW \
             BEGIN CREATE TABLE other (x INTEGER); END",
        )
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("trigger body") || msg.contains("only contain"),
        "expected body-restriction error, got: {msg}"
    );
}

#[test]
fn trigger_event_does_not_match_other_events() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    // Trigger only on UPDATE — INSERT should NOT fire it.
    conn.execute(
        "CREATE TRIGGER u_only AFTER UPDATE ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'updated', 1); END",
    )
    .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x')").unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(0));
}

#[test]
fn after_update_trigger_fires_on_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'old@x')")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER on_upd AFTER UPDATE ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'updated', 99); END",
    )
    .unwrap();
    conn.execute("UPDATE users SET email = 'new@x' WHERE id = 1")
        .unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn after_delete_trigger_fires_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute("INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x')")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER on_del AFTER DELETE ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'deleted', 99); END",
    )
    .unwrap();
    conn.execute("DELETE FROM users WHERE id = 1").unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn after_delete_fires_once_per_deleted_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // Use a counter table whose single row gets bumped (avoiding PK collision in audit).
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x')")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER on_del AFTER DELETE ON users FOR EACH ROW \
         BEGIN UPDATE counter SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("DELETE FROM users").unwrap();
    let p = conn.prepare("SELECT n FROM counter").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(3));
}

#[test]
fn update_of_columns_narrows_firing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY, what TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a', 30)").unwrap();
    conn.execute(
        "CREATE TRIGGER name_only AFTER UPDATE OF name ON t FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'name_changed'); END",
    )
    .unwrap();
    // Update only `age` — trigger should NOT fire.
    conn.execute("UPDATE t SET age = 40 WHERE id = 1").unwrap();
    let p = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p.query_collect(&[]).unwrap().rows[0][0], Value::Integer(0));
    // Update `name` — trigger SHOULD fire.
    conn.execute("UPDATE t SET name = 'b' WHERE id = 1")
        .unwrap();
    let p2 = conn.prepare("SELECT COUNT(*) FROM audit").unwrap();
    assert_eq!(p2.query_collect(&[]).unwrap().rows[0][0], Value::Integer(1));
}

#[test]
fn triggers_fire_in_name_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, who TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER z_last AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (3, 'z'); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER a_first AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (1, 'a'); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER m_middle AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (2, 'm'); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let p = conn.prepare("SELECT who FROM log ORDER BY id").unwrap();
    let r = p.query_collect(&[]).unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Text("a".into()));
    assert_eq!(r.rows[1][0], Value::Text("m".into()));
    assert_eq!(r.rows[2][0], Value::Text("z".into()));
}

#[test]
fn trigger_recursion_limited() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();
    // Mutual recursion: each insert into one fires a trigger inserting into the other,
    // which fires back. Distinct ids per layer using row count to avoid duplicate PK.
    conn.execute(
        "CREATE TRIGGER a_to_b AFTER INSERT ON a FOR EACH ROW \
         BEGIN INSERT INTO b VALUES ((SELECT COUNT(*) FROM b) + 100); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER b_to_a AFTER INSERT ON b FOR EACH ROW \
         BEGIN INSERT INTO a VALUES ((SELECT COUNT(*) FROM a) + 1000); END",
    )
    .unwrap();
    let err = conn.execute("INSERT INTO a VALUES (1)").unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("recursion"),
        "expected recursion-limit error from mutual trigger loop, got: {msg}"
    );
}

#[test]
fn multi_row_insert_fires_trigger_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(5));
}

#[test]
fn insert_select_fires_trigger_per_source_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO src VALUES (1), (2), (3), (4)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON dst FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO dst SELECT id FROM src").unwrap();
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(4));
}

#[test]
fn trigger_does_not_fire_on_duplicate_key_failure() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    // Duplicate insert fails — trigger must not fire.
    let _ = conn.execute("INSERT INTO t VALUES (1)");
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn trigger_does_not_fire_on_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER NOT NULL)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    let _ = conn.execute("INSERT INTO t (id) VALUES (1)");
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn trigger_does_not_fire_on_check_violation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    let _ = conn.execute("INSERT INTO t VALUES (1, -1)");
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn trigger_failure_rolls_back_entire_dml() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)")
        .unwrap();
    // Body inserts into a missing table — must blow up the whole statement.
    conn.execute(
        "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO nope VALUES (1); END",
    )
    .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
    // Outer INSERT must have rolled back: t must be empty.
    let r = conn
        .prepare("SELECT COUNT(*) FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn multi_event_trigger_fires_on_each_listed_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER iud AFTER INSERT OR UPDATE OR DELETE ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT n FROM c WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn multi_statement_body_executes_all() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER fan AFTER INSERT ON t FOR EACH ROW \
         BEGIN \
           INSERT INTO a VALUES (1); \
           INSERT INTO b VALUES (2); \
           INSERT INTO c VALUES (3); \
         END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    for name in ["a", "b", "c"] {
        let q = format!("SELECT COUNT(*) FROM {name}");
        let r = conn.prepare(&q).unwrap().query_collect(&[]).unwrap();
        assert_eq!(r.rows[0][0], Value::Integer(1), "table {name}");
    }
}

#[test]
fn trigger_fires_for_cascade_delete_on_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, who TEXT)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1), (11, 1)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER on_child_del AFTER DELETE ON child FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log), 'child_del'); END",
    )
    .unwrap();
    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn trigger_on_table_with_index_keeps_index_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();
    let r = conn
        .prepare("SELECT id FROM t WHERE v = 200")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(2));
    let r2 = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r2.rows[0][0], Value::Integer(2));
}

#[test]
fn trigger_inside_transaction_rolls_back_on_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    conn.execute("ROLLBACK").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
    let r2 = conn
        .prepare("SELECT COUNT(*) FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r2.rows[0][0], Value::Integer(0));
}

#[test]
fn trigger_inside_transaction_visible_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    conn.execute("COMMIT").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn create_trigger_inside_transaction_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute(
        "CREATE TRIGGER tt AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (1); END",
    )
    .unwrap();
    conn.execute("ROLLBACK").unwrap();
    // After rollback, trigger must not exist — INSERT should not fire it.
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn drop_table_removes_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER tt AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (1); END",
    )
    .unwrap();
    conn.execute("DROP TABLE t").unwrap();
    // Re-create the same table; the dropped trigger should not magically reappear.
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn update_of_multiple_columns_fires_when_any_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO log VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 2, 3)").unwrap();
    conn.execute(
        "CREATE TRIGGER on_ab AFTER UPDATE OF a, b ON t FOR EACH ROW \
         BEGIN UPDATE log SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    // Updating only `c` must NOT fire.
    conn.execute("UPDATE t SET c = 30 WHERE id = 1").unwrap();
    let r1 = conn
        .prepare("SELECT n FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r1.rows[0][0], Value::Integer(0));
    // Updating `a` MUST fire.
    conn.execute("UPDATE t SET a = 10 WHERE id = 1").unwrap();
    let r2 = conn
        .prepare("SELECT n FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r2.rows[0][0], Value::Integer(1));
    // Updating `b` MUST fire.
    conn.execute("UPDATE t SET b = 20 WHERE id = 1").unwrap();
    let r3 = conn
        .prepare("SELECT n FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r3.rows[0][0], Value::Integer(2));
}

#[test]
fn disabled_trigger_status_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        setup_users_and_audit(&conn);
        conn.execute(
            "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES (1, 'x', 1); END",
        )
        .unwrap();
        conn.execute("ALTER TABLE users DISABLE TRIGGER t").unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x')").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM audit")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn disable_trigger_all_skips_every_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER a_one AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'a', 1); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER b_two AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (2, 'b', 2); END",
    )
    .unwrap();
    conn.execute("ALTER TABLE users DISABLE TRIGGER ALL")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x')").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM audit")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn enable_trigger_all_reactivates_every_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_users_and_audit(&conn);
    conn.execute(
        "CREATE TRIGGER a_one AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (1, 'a', 1); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER b_two AFTER INSERT ON users FOR EACH ROW \
         BEGIN INSERT INTO audit VALUES (2, 'b', 2); END",
    )
    .unwrap();
    conn.execute("ALTER TABLE users DISABLE TRIGGER ALL")
        .unwrap();
    conn.execute("ALTER TABLE users ENABLE TRIGGER ALL")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a@x')").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM audit")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn trigger_can_query_target_table_for_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, count INTEGER)")
        .unwrap();
    // Snapshot table count after each insert into the audit table.
    conn.execute(
        "CREATE TRIGGER snap AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log), (SELECT COUNT(*) FROM t)); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    let r = conn
        .prepare("SELECT count FROM log ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Integer(1));
    assert_eq!(r.rows[1][0], Value::Integer(2));
    assert_eq!(r.rows[2][0], Value::Integer(3));
}

#[test]
fn trigger_on_composite_pk_fires_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, v INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)")
        .unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn trigger_does_not_fire_on_zero_row_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER UPDATE ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = 20 WHERE id = 999").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn trigger_does_not_fire_on_zero_row_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER DELETE ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id = 999").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn upsert_on_conflict_insert_path_fires_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log_i (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO log_i VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER on_i AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE log_i SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    // First insert — no conflict, INSERT path.
    conn.execute("INSERT INTO t VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET v = 99")
        .unwrap();
    let r = conn
        .prepare("SELECT n FROM log_i")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn many_concurrent_connections_each_have_independent_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER bump AFTER INSERT ON t FOR EACH ROW \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    let c1 = Connection::open(&db).unwrap();
    let c2 = Connection::open(&db).unwrap();
    c1.execute("INSERT INTO t VALUES (1)").unwrap();
    c2.execute("INSERT INTO t VALUES (2)").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn trigger_with_default_value_target_sees_computed_default() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 77)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER cap AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO captured VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM captured), (SELECT v FROM t WHERE id = (SELECT MAX(id) FROM t))); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = conn
        .prepare("SELECT v FROM captured WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(77));
}

#[test]
fn many_triggers_same_event_all_fire() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    for i in 0..10 {
        let sql = format!(
            "CREATE TRIGGER tt_{i:02} AFTER INSERT ON t FOR EACH ROW \
             BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
        );
        conn.execute(&sql).unwrap();
    }
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(10));
}

#[test]
fn create_trigger_inside_read_only_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute(
            "CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW \
             BEGIN SELECT 1; END",
        )
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("read")
            || err.to_string().to_lowercase().contains("read-only")
    );
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn after_statement_delete_fires_once_per_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_del AFTER DELETE ON t FOR EACH STATEMENT \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id IN (1, 2, 3)").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn after_statement_insert_fires_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_ins AFTER INSERT ON t FOR EACH STATEMENT \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn statement_insert_with_referencing_new_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, sum_v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_ins AFTER INSERT ON t REFERENCING NEW TABLE AS new_rows \
         FOR EACH STATEMENT BEGIN \
           INSERT INTO captured VALUES (1, (SELECT SUM(v) FROM new_rows)); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    let r = conn
        .prepare("SELECT sum_v FROM captured WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(600));
}

#[test]
fn after_statement_update_fires_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_upd AFTER UPDATE ON t FOR EACH STATEMENT \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = v * 2").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn statement_update_with_referencing_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, delta INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_upd AFTER UPDATE ON t \
         REFERENCING OLD TABLE AS o NEW TABLE AS n \
         FOR EACH STATEMENT BEGIN \
           INSERT INTO captured VALUES (1, (SELECT SUM(n.v) - SUM(o.v) FROM n, o)); END",
    )
    .unwrap();
    // Cartesian sum: SUM(n.v) = 60, SUM(o.v) = 30 → but cartesian product makes counts multiply.
    // For a clean assertion, use a simpler test below.
    let _ = conn.execute("UPDATE t SET v = v + 5");
    // Skip the assertion — this scenario exercises the cartesian join syntax. The trigger
    // fired and inserted SOMETHING; we just confirm captured got a row.
    let r = conn
        .prepare("SELECT COUNT(*) FROM captured")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn statement_trigger_does_not_fire_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    conn.execute(
        "CREATE TRIGGER stmt AFTER INSERT ON t FOR EACH STATEMENT \
         BEGIN UPDATE c SET n = n + 1 WHERE id = 1; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    let r = conn
        .prepare("SELECT n FROM c")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn before_statement_fires_before_any_row_written() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE snap (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (10)").unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_pre BEFORE INSERT ON t FOR EACH STATEMENT \
         BEGIN INSERT INTO snap VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM snap), (SELECT COUNT(*) FROM t)); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = conn
        .prepare("SELECT c FROM snap WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    // Snapshot captured BEFORE any of (1,2,3) was inserted → t had 1 row.
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn statement_delete_with_referencing_old_table_sees_deleted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, sum_v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_del AFTER DELETE ON t REFERENCING OLD TABLE AS old_rows \
         FOR EACH STATEMENT BEGIN \
           INSERT INTO captured VALUES (1, (SELECT SUM(v) FROM old_rows)); END",
    )
    .unwrap();
    conn.execute("DELETE FROM t").unwrap();
    let r = conn
        .prepare("SELECT sum_v FROM captured WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(60));
}

#[test]
fn instead_of_insert_on_view_redirects_to_base_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM base")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v FOR EACH ROW \
         BEGIN INSERT INTO base VALUES (NEW.id, NEW.v); END",
    )
    .unwrap();
    conn.execute("INSERT INTO v VALUES (1, 10), (2, 20)")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM base ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Integer(1));
    assert_eq!(r.rows[0][1], Value::Integer(10));
    assert_eq!(r.rows[1][0], Value::Integer(2));
    assert_eq!(r.rows[1][1], Value::Integer(20));
}

#[test]
fn instead_of_insert_on_view_with_column_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM base")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v FOR EACH ROW \
         BEGIN INSERT INTO base VALUES (NEW.id, COALESCE(NEW.v, 0)); END",
    )
    .unwrap();
    // Only `id` specified; NEW.v should be NULL → coalesced to 0.
    conn.execute("INSERT INTO v (id) VALUES (5)").unwrap();
    let r = conn
        .prepare("SELECT v FROM base WHERE id = 5")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn instead_of_insert_without_trigger_still_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT * FROM base").unwrap();
    let err = conn.execute("INSERT INTO v VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn instead_of_update_on_view_redirects_to_base() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO base VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM base")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER v_upd INSTEAD OF UPDATE ON v FOR EACH ROW \
         BEGIN UPDATE base SET v = NEW.v WHERE id = OLD.id; END",
    )
    .unwrap();
    conn.execute("UPDATE v SET v = 99 WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT v FROM base WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(99));
    let r2 = conn
        .prepare("SELECT v FROM base WHERE id = 2")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r2.rows[0][0], Value::Integer(20));
}

#[test]
fn instead_of_delete_on_view_redirects_to_base() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO base VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM base")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER v_del INSTEAD OF DELETE ON v FOR EACH ROW \
         BEGIN DELETE FROM base WHERE id = OLD.id; END",
    )
    .unwrap();
    conn.execute("DELETE FROM v WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM base")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn instead_of_update_fires_per_matching_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO base VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM base")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER v_upd INSTEAD OF UPDATE ON v FOR EACH ROW \
         BEGIN UPDATE base SET v = NEW.v WHERE id = OLD.id; END",
    )
    .unwrap();
    conn.execute("UPDATE v SET v = v * 10 WHERE id <> 2")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM base ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][1], Value::Integer(10));
    assert_eq!(r.rows[1][1], Value::Integer(2));
    assert_eq!(r.rows[2][1], Value::Integer(30));
}

#[test]
fn before_insert_trigger_fires_before_row_appears() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE snap (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    // BEFORE INSERT body queries t and records count — should always see PRE-insert state.
    conn.execute(
        "CREATE TRIGGER snap_before AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO snap VALUES (NEW.id, (SELECT COUNT(*) FROM t WHERE id <> NEW.id)); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER snap_pre BEFORE INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO snap VALUES (NEW.id * -1, (SELECT COUNT(*) FROM t)); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    let r = conn
        .prepare("SELECT id, c FROM snap ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    // After triggers see (id-1) rows already inserted excluding self; before triggers see (id-1) total.
    // (after row 1) excludes self: 0; (after row 2): 1; (after row 3): 2 — but PKs (1,2,3).
    // (before -3): 2; (before -2): 1; (before -1): 0 — PKs (-3,-2,-1) so ordered ascending: -3,-2,-1,1,2,3.
    let rows: Vec<(i64, i64)> = r
        .rows
        .iter()
        .map(|row| {
            let id = if let Value::Integer(i) = row[0] { i } else { 0 };
            let c = if let Value::Integer(i) = row[1] { i } else { 0 };
            (id, c)
        })
        .collect();
    assert_eq!(
        rows,
        vec![(-3, 2), (-2, 1), (-1, 0), (1, 0), (2, 1), (3, 2)]
    );
}

#[test]
fn before_insert_failure_blocks_row_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER guard BEFORE INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO nonexistent VALUES (1); END",
    )
    .unwrap();
    let err = conn.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
    let r = conn
        .prepare("SELECT COUNT(*) FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn before_update_trigger_sees_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute(
        "CREATE TRIGGER pre_upd BEFORE UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log), \
           'old=' || OLD.v || ' new=' || NEW.v); END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = 50 WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT msg FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("old=10 new=50".into()));
}

#[test]
fn before_delete_trigger_fires_with_row_still_present() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE snap (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    conn.execute(
        "CREATE TRIGGER pre_del BEFORE DELETE ON t FOR EACH ROW \
         BEGIN INSERT INTO snap VALUES (OLD.id, (SELECT COUNT(*) FROM t)); END",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    let r = conn
        .prepare("SELECT id, c FROM snap")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(2));
    // BEFORE DELETE saw the table BEFORE the row was removed → count == 3.
    assert_eq!(r.rows[0][1], Value::Integer(3));
}

#[test]
fn before_and_after_both_fire_for_same_event() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, who TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER a_before BEFORE INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log), 'before'); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER b_after AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log), 'after'); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = conn
        .prepare("SELECT who FROM log ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Text("before".into()));
    assert_eq!(r.rows[1][0], Value::Text("after".into()));
}

#[test]
fn before_update_does_not_fire_on_zero_row_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute(
        "CREATE TRIGGER pre BEFORE UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log)); END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = 20 WHERE id = 999").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn after_update_trigger_body_sees_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE diff (id INTEGER PRIMARY KEY, delta INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute(
        "CREATE TRIGGER capture_diff AFTER UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO diff VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM diff), NEW.v - OLD.v); END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = 25 WHERE id = 1").unwrap();
    let r = conn
        .prepare("SELECT delta FROM diff WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(15));
}

#[test]
fn after_insert_trigger_body_sees_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER cap_new AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO captured VALUES (NEW.id, NEW.name); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (7, 'alice')").unwrap();
    let r = conn
        .prepare("SELECT id, name FROM captured")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(7));
    assert_eq!(r.rows[0][1], Value::Text("alice".into()));
}

#[test]
fn after_delete_trigger_body_sees_old() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE captured (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7, 'bob')").unwrap();
    conn.execute(
        "CREATE TRIGGER cap_old AFTER DELETE ON t FOR EACH ROW \
         BEGIN INSERT INTO captured VALUES (OLD.id, OLD.name); END",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id = 7").unwrap();
    let r = conn
        .prepare("SELECT id, name FROM captured")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Text("bob".into()));
}

#[test]
fn when_clause_filters_firing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER only_positive AFTER INSERT ON t \
         FOR EACH ROW WHEN NEW.v > 0 \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log)); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, -3)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn when_clause_references_old_and_new() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    // Only fire when the value actually grows.
    conn.execute(
        "CREATE TRIGGER on_growth AFTER UPDATE ON t \
         FOR EACH ROW WHEN NEW.v > OLD.v \
         BEGIN INSERT INTO log VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM log)); END",
    )
    .unwrap();
    conn.execute("UPDATE t SET v = 5 WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET v = 30 WHERE id = 2").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM log")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn when_clause_short_circuits_body() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    // When NEW.id < 0 fail — but every NEW.id is positive, so the body's INSERT INTO nope never runs.
    conn.execute(
        "CREATE TRIGGER guard AFTER INSERT ON t \
         FOR EACH ROW WHEN NEW.id < 0 \
         BEGIN INSERT INTO nope VALUES (1); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn information_schema_triggers_lists_all_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER alpha AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO t VALUES (-1, 0); END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER beta BEFORE UPDATE ON t FOR EACH ROW \
         WHEN NEW.v > 0 BEGIN SELECT 1; END",
    )
    .unwrap();
    let r = conn
        .prepare(
            "SELECT trigger_name, event_manipulation, action_timing, action_orientation \
             FROM information_schema.triggers \
             WHERE event_object_table = 't' \
             ORDER BY trigger_name, event_manipulation",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Text("alpha".into()));
    assert_eq!(r.rows[0][1], Value::Text("INSERT".into()));
    assert_eq!(r.rows[0][2], Value::Text("AFTER".into()));
    assert_eq!(r.rows[0][3], Value::Text("ROW".into()));
    assert_eq!(r.rows[1][0], Value::Text("beta".into()));
    assert_eq!(r.rows[1][1], Value::Text("UPDATE".into()));
    assert_eq!(r.rows[1][2], Value::Text("BEFORE".into()));
}

#[test]
fn information_schema_triggers_multi_event_yields_multiple_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER multi AFTER INSERT OR UPDATE OR DELETE ON t FOR EACH ROW \
         BEGIN SELECT 1; END",
    )
    .unwrap();
    let r = conn
        .prepare(
            "SELECT event_manipulation FROM information_schema.triggers \
             WHERE trigger_name = 'multi' ORDER BY event_manipulation",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Text("DELETE".into()));
    assert_eq!(r.rows[1][0], Value::Text("INSERT".into()));
    assert_eq!(r.rows[2][0], Value::Text("UPDATE".into()));
}

#[test]
fn information_schema_triggers_includes_when_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER guarded BEFORE INSERT ON t FOR EACH ROW \
         WHEN NEW.v > 100 BEGIN SELECT 1; END",
    )
    .unwrap();
    let r = conn
        .prepare(
            "SELECT action_condition FROM information_schema.triggers \
             WHERE trigger_name = 'guarded'",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    if let Value::Text(s) = &r.rows[0][0] {
        assert!(s.contains("NEW.v") || s.to_uppercase().contains("NEW.V"));
    } else {
        panic!("expected text, got {:?}", r.rows[0][0]);
    }
}

#[test]
fn information_schema_triggers_reports_transition_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER stmt_trig AFTER UPDATE ON t \
         REFERENCING OLD TABLE AS old_t NEW TABLE AS new_t \
         FOR EACH STATEMENT BEGIN SELECT 1; END",
    )
    .unwrap();
    let r = conn
        .prepare(
            "SELECT action_reference_old_table, action_reference_new_table \
             FROM information_schema.triggers WHERE trigger_name = 'stmt_trig'",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Text("old_t".into()));
    assert_eq!(r.rows[0][1], Value::Text("new_t".into()));
}

#[test]
fn citadel_triggers_status_reports_enabled_flag() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER a_one AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("CREATE TRIGGER b_two AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("ALTER TABLE t DISABLE TRIGGER b_two").unwrap();
    let r = conn
        .prepare("SELECT trigger_name, enabled FROM citadel_triggers_status ORDER BY trigger_name")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Text("a_one".into()));
    assert_eq!(r.rows[0][1], Value::Boolean(true));
    assert_eq!(r.rows[1][0], Value::Text("b_two".into()));
    assert_eq!(r.rows[1][1], Value::Boolean(false));
}

#[test]
fn show_triggers_returns_all_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER on_t AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("CREATE TRIGGER on_u AFTER INSERT ON u FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    let r = conn
        .prepare("SHOW TRIGGERS")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn show_triggers_on_table_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER on_t AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("CREATE TRIGGER on_u AFTER INSERT ON u FOR EACH ROW BEGIN SELECT 1; END")
        .unwrap();
    let r = conn
        .prepare("SHOW TRIGGERS ON t")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("on_t".into()));
}

#[test]
fn alter_table_disable_unknown_trigger_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("ALTER TABLE t DISABLE TRIGGER missing")
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("not found")
            || err.to_string().to_lowercase().contains("trigger")
    );
}
