//! A schema reload must not lose the connection's TEMP tables.
//!
//! TEMP aliases live only in memory, so any path that swaps in a freshly loaded
//! `SchemaManager` drops them and hides tables whose rows are still on disk.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::Connection;

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.cdl"))
        .passphrase(b"pw")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn a_name_miss_does_not_hide_this_connection_s_temp_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    let c = Connection::open(&db).unwrap();

    c.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    c.execute("INSERT INTO tmp VALUES (1)").unwrap();
    assert_eq!(c.query("SELECT id FROM tmp").unwrap().rows.len(), 1);

    // A miss on an unrelated name reloads the schema behind the scenes.
    assert!(c.query("SELECT * FROM no_such_table").is_err());

    assert_eq!(
        c.query("SELECT id FROM tmp").unwrap().rows.len(),
        1,
        "the reload dropped this connection's TEMP alias"
    );
}

#[test]
fn a_rolled_back_transaction_does_not_hide_temp_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(dir.path());
    let c = Connection::open(&db).unwrap();

    c.execute("CREATE TEMPORARY TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    c.execute("INSERT INTO tmp VALUES (1)").unwrap();

    c.execute("BEGIN").unwrap();
    c.execute("INSERT INTO tmp VALUES (2)").unwrap();
    c.execute("ROLLBACK").unwrap();

    assert_eq!(
        c.query("SELECT id FROM tmp").unwrap().rows.len(),
        1,
        "the rollback reload dropped this connection's TEMP alias"
    );
}
