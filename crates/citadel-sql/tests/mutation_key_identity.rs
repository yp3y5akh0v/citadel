use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::encoding::encode_composite_key;
use citadel_sql::{Connection, SqlError, TableSchema, Value};

fn database(path: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(path.join("key-identity.db"))
        .passphrase(b"key-identity-tests")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}
fn real_bits(value: &Value) -> u64 {
    match value {
        Value::Real(value) => value.to_bits(),
        other => panic!("expected REAL, got {other:?}"),
    }
}
fn index_keys(db: &citadel::Database) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    db.begin_read()
        .table_for_each(&TableSchema::index_table_name("items", "by_n"), |key, _| {
            keys.push(key.to_vec());
            Ok(())
        })
        .unwrap();
    keys
}

#[test]
fn signed_zero_updates_keep_secondary_keys_and_projected_values_exact() {
    for unique in [false, true] {
        for upsert in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = database(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY,n REAL)")
                .unwrap();
            conn.execute(if unique {
                "CREATE UNIQUE INDEX by_n ON items(n)"
            } else {
                "CREATE INDEX by_n ON items(n)"
            })
            .unwrap();
            conn.execute("INSERT INTO items VALUES (1,0.0)").unwrap();
            conn.execute(if upsert {
                "INSERT INTO items VALUES (1,-0.0) ON CONFLICT(id) DO UPDATE SET n=excluded.n"
            } else {
                "UPDATE items SET n=-0.0 WHERE id=1"
            })
            .unwrap();
            let mut expected = vec![Value::Real(-0.0)];
            if !unique {
                expected.push(Value::Integer(1));
            }
            assert_eq!(index_keys(&db), vec![encode_composite_key(&expected)]);
            assert_eq!(
                real_bits(&conn.query("SELECT n FROM items ORDER BY n").unwrap().rows[0][0]),
                (-0.0_f64).to_bits()
            );
            conn.execute("DELETE FROM items WHERE id=1").unwrap();
            assert!(index_keys(&db).is_empty());
        }
    }
}

#[test]
fn exact_foreign_keys_apply_actions_when_parent_signed_zero_bits_change() {
    for action in ["CASCADE", "RESTRICT", "NO ACTION"] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = database(dir.path());
            let conn = Connection::open(&db).unwrap();
            conn.execute("CREATE TABLE parent (id REAL PRIMARY KEY)")
                .unwrap();
            conn.execute(&format!("CREATE TABLE child (id INTEGER PRIMARY KEY,p REAL REFERENCES parent(id) ON UPDATE {action})")).unwrap();
            conn.execute("INSERT INTO parent VALUES(0.0)").unwrap();
            conn.execute("INSERT INTO child VALUES(1,0.0)").unwrap();
            if explicit {
                conn.execute("BEGIN").unwrap();
            }
            let result = conn
                .prepare("UPDATE parent SET id=$1 WHERE id=$2")
                .unwrap()
                .execute(&[Value::Real(-0.0), Value::Real(0.0)]);
            if action == "CASCADE" {
                result.unwrap();
                assert_eq!(
                    real_bits(&conn.query("SELECT p FROM child").unwrap().rows[0][0]),
                    (-0.0_f64).to_bits()
                );
                conn.execute("DELETE FROM child").unwrap();
            } else {
                assert!(
                    matches!(result, Err(SqlError::ForeignKeyViolation(_))),
                    "{action}: {result:?}"
                );
            }
            if explicit {
                conn.execute("ROLLBACK").unwrap();
            }
            if explicit || action != "CASCADE" {
                assert_eq!(
                    real_bits(&conn.query("SELECT id FROM parent").unwrap().rows[0][0]),
                    0.0_f64.to_bits()
                );
                assert_eq!(
                    real_bits(&conn.query("SELECT p FROM child").unwrap().rows[0][0]),
                    0.0_f64.to_bits()
                );
            }
        }
    }
}

#[test]
fn deferred_reference_checks_follow_signed_zero_primary_key_moves() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child(id REAL PRIMARY KEY,p INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO child VALUES(0.0,99)").unwrap();
    conn.execute("UPDATE child SET id=-0.0").unwrap();
    let error = conn.execute("COMMIT").unwrap_err();
    assert!(
        matches!(error, SqlError::ForeignKeyViolation(_)),
        "{error:?}"
    );
    assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
}

#[test]
fn upsert_primary_key_replacement_preserves_signed_zero_representation() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items(id REAL PRIMARY KEY,n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(0.0,1)").unwrap();
    conn.execute(
        "INSERT INTO items VALUES(0.0,2) ON CONFLICT(id) DO UPDATE SET id=-0.0,n=excluded.n",
    )
    .unwrap();
    let rows = conn.query("SELECT id,n FROM items").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(real_bits(&rows[0][0]), (-0.0_f64).to_bits());
    assert_eq!(rows[0][1], Value::Integer(2));
}
