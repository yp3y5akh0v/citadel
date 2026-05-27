use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn immediate_fk_rejects_child_before_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE kids (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id))")
        .unwrap();
    let err = conn.execute("INSERT INTO kids VALUES (1, 99)").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn deferred_fk_allows_child_before_parent_in_same_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE kids (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO kids VALUES (1, 99)").unwrap();
    conn.execute("INSERT INTO parents VALUES (99)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn deferred_fk_violation_aborts_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE kids (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO kids VALUES (1, 99)").unwrap();
    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn deferrable_initially_immediate_checks_now() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE kids (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY IMMEDIATE)",
    )
    .unwrap();
    let err = conn.execute("INSERT INTO kids VALUES (1, 99)").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn schema_v11_round_trips_deferrability_flags() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let db = DatabaseBuilder::new(&db_path)
            .passphrase(b"x")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE TABLE c (\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .unwrap();
    }
    let db = DatabaseBuilder::new(&db_path)
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO p VALUES (5)").unwrap();
    conn.execute("COMMIT").unwrap();
}

#[test]
fn deferred_fk_resolves_when_parent_inserted_then_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE c (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO p VALUES (1)").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 1)").unwrap();
    conn.execute("COMMIT").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(matches!(qr.rows[0][0], citadel_sql::Value::Integer(1)));
}

fn setup_parent_child_deferred(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE c (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
}

#[test]
fn savepoint_partial_rollback_keeps_unrelated_deferred_checks() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_parent_child_deferred(&conn);
    conn.execute("INSERT INTO p VALUES (1)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 1)").unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO c VALUES (20, 99)").unwrap();
    conn.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
    conn.execute("COMMIT").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(matches!(qr.rows[0][0], citadel_sql::Value::Integer(1)));
}

#[test]
fn savepoint_rollback_drops_all_savepoint_deferred_checks() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_parent_child_deferred(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT outer_sp").unwrap();
    conn.execute("INSERT INTO c VALUES (1, 999)").unwrap();
    conn.execute("SAVEPOINT inner_sp").unwrap();
    conn.execute("INSERT INTO c VALUES (2, 998)").unwrap();
    conn.execute("ROLLBACK TO SAVEPOINT outer_sp").unwrap();
    conn.execute("COMMIT").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(matches!(qr.rows[0][0], citadel_sql::Value::Integer(0)));
}

#[test]
fn commit_raises_first_violation_when_multiple_pending() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_parent_child_deferred(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (1, 100)").unwrap();
    conn.execute("INSERT INTO c VALUES (2, 200)").unwrap();
    let err = conn.execute("COMMIT").unwrap_err();
    assert!(matches!(err, SqlError::ForeignKeyViolation(_)));
}

#[test]
fn cascade_delete_with_initially_deferred_fk_fires_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE c (\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES p(id) ON DELETE CASCADE \
            DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    conn.execute("INSERT INTO p VALUES (1)").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 1)").unwrap();
    conn.execute("DELETE FROM p WHERE id = 1").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(
        matches!(qr.rows[0][0], citadel_sql::Value::Integer(0)),
        "cascade fires immediately even with INITIALLY DEFERRED"
    );
}

#[test]
fn rollback_clears_deferred_queue_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_parent_child_deferred(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (1, 999)").unwrap();
    conn.execute("INSERT INTO c VALUES (2, 998)").unwrap();
    conn.execute("ROLLBACK").unwrap();
    conn.execute("INSERT INTO p VALUES (1)").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 1)").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(matches!(qr.rows[0][0], citadel_sql::Value::Integer(1)));
}

#[test]
fn second_txn_after_rollback_starts_with_empty_queue() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_parent_child_deferred(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (1, 999)").unwrap();
    conn.execute("ROLLBACK").unwrap();
    conn.execute("INSERT INTO p VALUES (5)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO c VALUES (10, 5)").unwrap();
    conn.execute("COMMIT").unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM c").unwrap();
    assert!(matches!(qr.rows[0][0], citadel_sql::Value::Integer(1)));
}
