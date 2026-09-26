use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, TableSchema, Value};

fn database(path: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(path.join("identity.db"))
        .passphrase(b"identity-tests")
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

fn index_entries(db: &citadel::Database, table: &str, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    db.begin_read()
        .table_for_each(&TableSchema::index_table_name(table, name), |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
    entries
}

#[test]
fn unchanged_nan_rows_allow_after_only_update_and_delete_triggers() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = database(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, n REAL, v INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, event TEXT)")
            .unwrap();
        conn.execute("CREATE TRIGGER changed AFTER UPDATE ON items FOR EACH ROW BEGIN INSERT INTO events VALUES (OLD.id * 10, 'update'); END").unwrap();
        conn.execute("CREATE TRIGGER removed AFTER DELETE ON items FOR EACH ROW BEGIN INSERT INTO events VALUES (OLD.id * 10 + 1, 'delete'); END").unwrap();
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        conn.execute_params("INSERT INTO items VALUES (1,$1,0)", &[Value::Real(nan)])
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            assert_eq!(
                conn.prepare("UPDATE items SET v=v+1 WHERE id=$1")
                    .unwrap()
                    .execute(&[Value::Integer(1)])
                    .unwrap(),
                1
            );
        } else {
            conn.execute("UPDATE items SET v=v+1 WHERE id=1").unwrap();
        }
        let row = conn.query("SELECT n,v FROM items").unwrap().rows.remove(0);
        assert_eq!(real_bits(&row[0]), nan.to_bits());
        assert_eq!(row[1], Value::Integer(1));
        if prepared {
            assert_eq!(
                conn.prepare("DELETE FROM items WHERE id=$1")
                    .unwrap()
                    .execute(&[Value::Integer(1)])
                    .unwrap(),
                1
            );
        } else {
            conn.execute("DELETE FROM items WHERE id=1").unwrap();
        }
        assert_eq!(
            conn.query("SELECT event FROM events ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![Value::Text("update".into())],
                vec![Value::Text("delete".into())]
            ]
        );
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            let row = conn.query("SELECT n,v FROM items").unwrap().rows.remove(0);
            assert_eq!(real_bits(&row[0]), nan.to_bits());
            assert_eq!(row[1], Value::Integer(0));
            assert!(conn.query("SELECT id FROM events").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn before_trigger_signed_zero_mutation_is_rejected_and_indexes_roll_back() {
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = database(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, n REAL)")
            .unwrap();
        conn.execute("CREATE INDEX by_n ON items(n)").unwrap();
        conn.execute("INSERT INTO items VALUES (1,0.0)").unwrap();
        conn.execute("CREATE TRIGGER changes_current BEFORE DELETE ON items FOR EACH ROW BEGIN UPDATE items SET n=-0.0 WHERE id=OLD.id; END").unwrap();
        let before = index_entries(&db, "items", "by_n");
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let error = if prepared {
            conn.prepare("DELETE FROM items WHERE id=$1")
                .unwrap()
                .execute(&[Value::Integer(1)])
                .unwrap_err()
        } else {
            conn.execute("DELETE FROM items WHERE id=1").unwrap_err()
        };
        assert!(matches!(error, SqlError::Unsupported(_)), "{error:?}");
        if explicit {
            conn.execute("ROLLBACK").unwrap();
        }
        assert_eq!(
            real_bits(&conn.query("SELECT n FROM items").unwrap().rows[0][0]),
            0.0_f64.to_bits()
        );
        assert_eq!(index_entries(&db, "items", "by_n"), before);
    }
}

#[test]
fn pending_cascade_rows_refresh_signed_zero_before_after_triggers() {
    for (prepared, explicit) in [(false, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = database(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE, n REAL)").unwrap();
        conn.execute("CREATE TABLE grandchild (id INTEGER PRIMARY KEY, c INTEGER REFERENCES child(id) ON DELETE CASCADE)").unwrap();
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, n REAL)")
            .unwrap();
        conn.execute("INSERT INTO parent VALUES(1)").unwrap();
        conn.execute("INSERT INTO child VALUES(10,1,0.0),(20,1,0.0)")
            .unwrap();
        conn.execute("INSERT INTO grandchild VALUES(100,10)")
            .unwrap();
        conn.execute("CREATE TRIGGER mutate_pending BEFORE DELETE ON grandchild FOR EACH ROW BEGIN UPDATE child SET n=-0.0 WHERE id=20; END").unwrap();
        conn.execute("CREATE TRIGGER record_old AFTER DELETE ON child FOR EACH ROW BEGIN INSERT INTO events VALUES(OLD.id,OLD.n); END").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        if prepared {
            conn.prepare("DELETE FROM parent WHERE id=$1")
                .unwrap()
                .execute(&[Value::Integer(1)])
                .unwrap();
        } else {
            conn.execute("DELETE FROM parent WHERE id=1").unwrap();
        }
        let rows = conn
            .query("SELECT id,n FROM events ORDER BY id")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
        assert_eq!(real_bits(&rows[0][1]), 0.0_f64.to_bits());
        assert_eq!(real_bits(&rows[1][1]), (-0.0_f64).to_bits());
        assert!(conn.query("SELECT id FROM child").unwrap().rows.is_empty());
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                conn.query("SELECT id FROM child ORDER BY id")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
            assert!(conn.query("SELECT id FROM events").unwrap().rows.is_empty());
        }
    }
}
