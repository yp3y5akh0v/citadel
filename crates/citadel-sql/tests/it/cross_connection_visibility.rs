//! What one connection sees of another's commits, over the same database.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::Connection;

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("x.cdl"))
        .passphrase(b"pw")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn a_connection_sees_a_table_created_after_it_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());

    let a = Connection::open(&db).unwrap();
    let b = Connection::open(&db).unwrap();

    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    // `b` read its schema before the table existed, so this misses and reloads.
    let rows = b.query("SELECT v FROM t").unwrap();
    assert_eq!(rows.rows.len(), 1);
}

#[test]
fn a_connection_sees_a_column_added_after_it_opened() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    let a = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    let b = Connection::open(&db).unwrap();
    a.execute("ALTER TABLE t ADD COLUMN extra TEXT").unwrap();
    a.execute("INSERT INTO t VALUES (1, 42, 'hello')").unwrap();

    let rows = b.query("SELECT extra FROM t").unwrap();
    assert_eq!(rows.rows.len(), 1);
}

#[test]
fn a_name_that_never_existed_still_reports_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    let c = Connection::open(&db).unwrap();
    assert!(
        c.query("SELECT * FROM never_created").is_err(),
        "the retry must not turn a genuine miss into something else"
    );
}

#[test]
fn the_retry_reports_what_is_missing_from_the_schema_it_reloaded() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());

    let a = Connection::open(&db).unwrap();
    let b = Connection::open(&db).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    // `b` misses on the table, reloads, then misses on the column. Reporting the
    // first would name a table that does exist and hide the column that does not.
    let err = b.query("SELECT nope FROM t").unwrap_err().to_string();
    assert!(
        err.contains("nope"),
        "the retry reported the stale miss instead of its own: {err}"
    );
}

#[test]
fn a_connection_does_see_rows_committed_by_another_into_a_known_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());

    let setup = Connection::open(&db).unwrap();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    drop(setup);

    // Both open after the table exists, so both share the same schema.
    let a = Connection::open(&db).unwrap();
    let b = Connection::open(&db).unwrap();

    a.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let rows = b.query("SELECT v FROM t").unwrap();
    assert_eq!(
        rows.rows.len(),
        1,
        "b did not see a row committed by a into a table both knew about"
    );
}
