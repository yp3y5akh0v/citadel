use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"virtual-query-paths")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn virtual_queries_see_the_current_writer_catalog() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE pending (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO pending VALUES (1,0)").unwrap();
    assert_eq!(
        conn.query("SELECT table_name FROM information_schema.tables WHERE table_name = 'pending'")
            .unwrap()
            .rows,
        vec![vec![Value::Text("pending".into())]]
    );
    conn.execute("UPDATE pending SET n = (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'pending')")
        .unwrap();
    assert_eq!(
        conn.query("SELECT n FROM pending").unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert!(conn
        .query("SELECT table_name FROM information_schema.tables WHERE table_name = 'pending'")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn virtual_sources_join_in_either_position_on_read_and_write_paths() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE wanted (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO wanted VALUES (1,'wanted')")
        .unwrap();
    for explicit in [false, true] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        for sql in [
            "SELECT v.table_name FROM information_schema.tables v JOIN wanted w ON v.table_name = w.name",
            "SELECT v.table_name FROM wanted w JOIN information_schema.tables v ON v.table_name = w.name",
            "SELECT t.table_name FROM information_schema.tables t JOIN information_schema.columns c ON c.table_name = t.table_name WHERE t.table_name = 'wanted' AND c.column_name = 'id'",
        ] {
            assert_eq!(conn.query(sql).unwrap().rows, vec![vec![Value::Text("wanted".into())]], "{explicit}: {sql}");
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn cte_names_shadow_virtual_sources_on_read_and_write_paths() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for explicit in [false, true] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let sql = "WITH pg_timezone_names AS (SELECT 'local' AS name), keep AS (SELECT 1 AS id) \
                   SELECT p.name FROM pg_timezone_names p JOIN keep k ON k.id = 1";
        assert_eq!(
            conn.query(sql).unwrap().rows,
            vec![vec![Value::Text("local".into())]],
            "{explicit}"
        );
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
}

#[test]
fn stored_relations_shadow_builtin_names_in_each_source_position() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE pg_timezone_names (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO pg_timezone_names VALUES (1,'stored')")
        .unwrap();
    conn.execute("CREATE VIEW timezone_abbrevs AS SELECT id,name FROM pg_timezone_names")
        .unwrap();
    conn.execute("CREATE TABLE wanted (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO wanted VALUES (1)").unwrap();
    for explicit in [false, true] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        for source in ["pg_timezone_names", "timezone_abbrevs"] {
            for sql in [
                format!("SELECT name FROM {source} WHERE id = 1"),
                format!("SELECT s.name FROM {source} s JOIN wanted w ON s.id = w.id"),
                format!("SELECT s.name FROM wanted w JOIN {source} s ON s.id = w.id"),
            ] {
                assert_eq!(
                    conn.query(&sql).unwrap().rows,
                    vec![vec![Value::Text("stored".into())]],
                    "{explicit}: {sql}"
                );
            }
        }
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
    }
}
