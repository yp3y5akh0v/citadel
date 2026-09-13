use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database(path: &std::path::Path, create: bool) -> Database {
    let builder = DatabaseBuilder::new(path)
        .passphrase(b"schema-codec-test")
        .argon2_profile(Argon2Profile::Iot);
    if create {
        builder.create().unwrap()
    } else {
        builder.open().unwrap()
    }
}

#[test]
fn schema_default_text_wire_boundary_is_atomic_and_survives_reopen() {
    for explicit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let valid = "x".repeat(65533); // quoted SQL length is exactly u16::MAX
        {
            let db = database(&path, true);
            let conn = Connection::open(&db).unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            conn.execute(&format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT DEFAULT '{valid}')"
            ))
            .unwrap();
            conn.execute("INSERT INTO t(id) VALUES(1)").unwrap();
            let too_long = "x".repeat(65534);
            if explicit {
                conn.execute("SAVEPOINT before_bad_create").unwrap();
            }
            let error = conn
                .execute(&format!(
                    "CREATE TABLE bad(id INTEGER PRIMARY KEY,v TEXT DEFAULT '{too_long}')"
                ))
                .unwrap_err();
            assert!(matches!(error, SqlError::InvalidValue(_)), "{error:?}");
            if explicit {
                conn.execute("ROLLBACK TO before_bad_create").unwrap();
                conn.execute("RELEASE before_bad_create").unwrap();
                conn.execute("SAVEPOINT before_bad_alter").unwrap();
            }
            let error = conn
                .execute(&format!(
                    "ALTER TABLE t ADD COLUMN bad TEXT DEFAULT '{too_long}'"
                ))
                .unwrap_err();
            assert!(matches!(error, SqlError::InvalidValue(_)), "{error:?}");
            if explicit {
                conn.execute("ROLLBACK TO before_bad_alter").unwrap();
                conn.execute("RELEASE before_bad_alter").unwrap();
            }
            conn.execute("CREATE TABLE bad(id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("INSERT INTO bad VALUES(9)").unwrap();
            conn.execute("INSERT INTO t(id) VALUES(2)").unwrap();
            if explicit {
                conn.execute("COMMIT").unwrap();
            }
            assert_eq!(
                conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
                vec![vec![Value::Text(valid.clone().into())]; 2]
            );
        }
        let db = database(&path, false);
        let conn = Connection::open(&db).unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            vec![vec![Value::Text(valid.clone().into())]; 2]
        );
        assert_eq!(
            conn.query("SELECT id FROM bad").unwrap().rows,
            vec![vec![Value::Integer(9)]]
        );
        assert_eq!(
            conn.query("SELECT * FROM t").unwrap().columns,
            vec!["id", "v"]
        );
    }
}

#[test]
fn malformed_catalog_record_returns_error_on_connection_open() {
    for catalog in [b"_schema".as_slice(), b"_views", b"_triggers", b"_matviews"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = database(&path, true);
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(catalog).unwrap();
        let version = if catalog == b"_schema" { 14 } else { 1 };
        wtx.table_insert(catalog, b"bad", &[version]).unwrap();
        wtx.commit().unwrap();
        assert!(
            matches!(Connection::open(&db), Err(SqlError::InvalidValue(_))),
            "catalog {catalog:?}"
        );
        let mut wtx = db.begin_write().unwrap();
        wtx.table_delete(catalog, b"bad").unwrap();
        wtx.commit().unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE healthy(id INTEGER PRIMARY KEY)")
            .unwrap();
    }
}
