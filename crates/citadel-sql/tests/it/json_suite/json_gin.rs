use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn create_gin_index_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
}

#[test]
fn gin_index_accelerates_contains_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100 {
        let role = if i % 10 == 0 { "admin" } else { "member" };
        let payload = format!(r#"{{"id":{i},"role":"{role}"}}"#);
        conn.execute(&format!(
            "INSERT INTO users VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    let ids: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => panic!("expected integer"),
        })
        .collect();
    for id in ids {
        assert_eq!(id % 10, 0);
    }
}

#[test]
fn gin_index_maintained_on_insert_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, CAST('{\"role\":\"member\"}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn gin_index_maintained_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, CAST('{\"role\":\"admin\"}' AS JSONB))")
        .unwrap();
    conn.execute("CREATE INDEX idx_data ON users USING gin (data)")
        .unwrap();
    conn.execute("DELETE FROM users WHERE id = 1").unwrap();
    let qr = conn
        .query("SELECT id FROM users WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn gin_rejects_on_non_jsonb_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let result = conn.execute("CREATE INDEX idx_name ON t USING gin (name)");
    assert!(result.is_err(), "GIN on TEXT column should be rejected");
}

#[test]
fn gin_rejects_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    let result = conn.execute("CREATE UNIQUE INDEX idx_data ON users USING gin (data)");
    assert!(result.is_err(), "UNIQUE GIN should be rejected");
}

#[test]
fn jsonb_path_ops_index_creates_and_filters_contains() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..50 {
        let role = if i % 5 == 0 { "admin" } else { "user" };
        let payload = format!(r#"{{"id":{i},"role":"{role}","tags":["a","b"]}}"#);
        conn.execute(&format!(
            "INSERT INTO docs VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX idx_data ON docs USING gin (data) WITH (ops = 'jsonb_path_ops')")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM docs WHERE data @> CAST('{\"role\":\"admin\"}' AS JSONB)")
        .unwrap();
    assert_eq!(qr.rows.len(), 10);
    for r in &qr.rows {
        match &r[0] {
            Value::Integer(i) => assert_eq!(i % 5, 0),
            _ => panic!("expected integer"),
        }
    }
}

#[test]
fn jsonb_path_ops_matches_jsonb_ops_for_contains() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..40 {
        let payload = format!(r#"{{"k":{i},"nested":{{"v":{}}}}}"#, i * 2);
        conn.execute(&format!(
            "INSERT INTO a VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
        conn.execute(&format!(
            "INSERT INTO b VALUES ({i}, CAST('{payload}' AS JSONB))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX a_idx ON a USING gin (data)")
        .unwrap();
    conn.execute("CREATE INDEX b_idx ON b USING gin (data) WITH (ops = 'jsonb_path_ops')")
        .unwrap();

    let probe = "CAST('{\"nested\":{\"v\":20}}' AS JSONB)";
    let qa = conn
        .query(&format!(
            "SELECT id FROM a WHERE data @> {probe} ORDER BY id"
        ))
        .unwrap();
    let qb = conn
        .query(&format!(
            "SELECT id FROM b WHERE data @> {probe} ORDER BY id"
        ))
        .unwrap();
    assert_eq!(qa.rows, qb.rows);
    assert_eq!(qa.rows.len(), 1);
}

#[test]
fn jsonb_path_ops_rejects_unknown_opclass() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX idx ON t USING gin (data) WITH (ops = 'bogus_ops')")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("bogus_ops"),
        "expected error to mention 'bogus_ops', got: {msg}"
    );
}

#[test]
fn jsonb_contains_scan_materializes_only_missing_columns_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(1)").unwrap();
        conn.execute("ALTER TABLE items ADD COLUMN doc JSONB DEFAULT CAST('{\"x\":1}' AS JSONB)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(2,NULL),(3,CAST('null' AS JSONB)),(4,CAST('{}' AS JSONB)),(5,CAST('{\"x\":2}' AS JSONB)),(6,CAST('{\"x\":1}' AS JSONB))").unwrap();
        let direct = conn
            .prepare("SELECT id FROM items WHERE doc @> $1::jsonb ORDER BY id")
            .unwrap();
        let generic = conn
            .prepare("SELECT id FROM items WHERE COALESCE(doc,NULL) @> $1::jsonb ORDER BY id")
            .unwrap();
        let parameters = [Value::Text("{\"x\":1}".into())];
        let expected = vec![vec![Value::Integer(1)], vec![Value::Integer(6)]];
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            assert_eq!(generic.query_collect(&parameters).unwrap().rows, expected);
            assert_eq!(direct.query_collect(&parameters).unwrap().rows, expected);
            assert_eq!(
                direct.query(&parameters).unwrap().collect().unwrap().rows,
                expected
            );
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
        assert_eq!(
            conn.query_params_bounded(
                "SELECT id FROM items WHERE doc @> $1::jsonb ORDER BY id",
                &parameters,
                &citadel_sql::ReadBudget::new(1024 * 1024, 1024 * 1024),
            )
            .unwrap()
            .rows,
            expected
        );
        assert!(matches!(
            conn.query_params_bounded(
                "SELECT id FROM items WHERE doc @> $1::jsonb ORDER BY id",
                &parameters,
                &citadel_sql::ReadBudget::new(0, 0),
            ),
            Err(citadel_sql::SqlError::Storage(
                citadel::Error::ReadBudgetExceeded { .. }
            ))
        ));
        assert_eq!(
            conn.query("SELECT id FROM items WHERE doc IS NULL ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![Value::Integer(2)]]
        );
    }
    let reopened = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&reopened).unwrap();
    assert_eq!(
        conn.query("SELECT id FROM items WHERE doc @> CAST('{\"x\":1}' AS JSONB) ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(6)]]
    );
}

#[test]
fn jsonb_contains_prepared_scan_preserves_alter_savepoint_and_read_snapshot_views() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(1)").unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN doc JSONB DEFAULT CAST('{\"x\":1}' AS JSONB)")
        .unwrap();
    let prepared = conn
        .prepare("SELECT id FROM items WHERE doc @> $1::jsonb ORDER BY id")
        .unwrap();
    let parameters = [Value::Text("{\"x\":1}".into())];
    let one = vec![vec![Value::Integer(1)]];
    assert_eq!(prepared.query_collect(&parameters).unwrap().rows, one);
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_alter").unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN marker INTEGER DEFAULT 7")
        .unwrap();
    assert_eq!(prepared.query_collect(&parameters).unwrap().rows, one);
    conn.execute("INSERT INTO items VALUES(2,CAST('{\"x\":1}' AS JSONB),9)")
        .unwrap();
    conn.execute("UPDATE items SET doc=CAST('{\"x\":2}' AS JSONB) WHERE id=1")
        .unwrap();
    assert_eq!(
        prepared.query_collect(&parameters).unwrap().rows,
        vec![vec![Value::Integer(2)]]
    );
    conn.execute("ROLLBACK TO before_alter").unwrap();
    assert_eq!(prepared.query_collect(&parameters).unwrap().rows, one);
    assert_eq!(
        conn.query("SELECT * FROM items").unwrap().columns,
        ["id", "doc"]
    );
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN READ ONLY").unwrap();
    assert_eq!(prepared.query_collect(&parameters).unwrap().rows, one);
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("INSERT INTO items VALUES(3,CAST('{\"x\":1}' AS JSONB))")
        .unwrap();
    assert_eq!(prepared.query_collect(&parameters).unwrap().rows, one);
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        prepared.query_collect(&parameters).unwrap().rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn jsonb_contains_missing_defaults_use_current_session_and_hidden_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE times(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO times VALUES(1)").unwrap();
    conn.execute(
        r#"ALTER TABLE times ADD COLUMN doc JSONB DEFAULT JSONB_PATH_QUERY_FIRST_TZ(
        CAST('"2023-08-15T12:34:56+05:30"' AS JSONB), '$.time().string()')"#,
    )
    .unwrap();
    let direct = conn
        .prepare("SELECT id FROM times WHERE doc @> $1::jsonb")
        .unwrap();
    let generic = conn
        .prepare("SELECT id FROM times WHERE COALESCE(doc,NULL) @> $1::jsonb")
        .unwrap();
    let check = |expected: &str| {
        let parameters = [Value::Text(format!("\"{expected}\"").into())];
        let expected_rows = vec![vec![Value::Integer(1)]];
        assert_eq!(
            generic.query_collect(&parameters).unwrap().rows,
            expected_rows
        );
        assert_eq!(
            direct.query_collect(&parameters).unwrap().rows,
            expected_rows
        );
        assert_eq!(
            direct.query(&parameters).unwrap().collect().unwrap().rows,
            expected_rows
        );
    };
    conn.set_session_timezone("UTC").unwrap();
    check("07:04:56");
    conn.set_session_timezone("America/New_York").unwrap();
    check("03:04:56");
    assert!(direct
        .query_collect(&[Value::Text("\"07:04:56\"".into())])
        .unwrap()
        .rows
        .is_empty());
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_zone").unwrap();
    conn.execute("SET LOCAL TIME ZONE '+10:00'").unwrap();
    check("17:04:56");
    conn.execute("ROLLBACK TO before_zone").unwrap();
    check("03:04:56");
    conn.execute("COMMIT").unwrap();
    check("03:04:56");

    conn.execute("CREATE TABLE bindings(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO bindings VALUES(1)").unwrap();
    conn.execute("ALTER TABLE bindings ADD COLUMN doc JSONB DEFAULT CAST($2 AS JSONB)")
        .unwrap();
    let parameterized = conn
        .prepare("SELECT id,$2 AS supplied FROM bindings WHERE doc @> $1::jsonb")
        .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for (probe, value, matches) in [
            ("{\"x\":1}", "{\"x\":1}", true),
            ("{\"x\":1}", "{\"x\":2}", false),
        ] {
            let supplied = Value::Text(value.into());
            let result = parameterized
                .query_collect(&[Value::Text(probe.into()), supplied.clone()])
                .unwrap();
            let expected = if matches {
                vec![vec![Value::Integer(1), supplied]]
            } else {
                vec![]
            };
            assert_eq!(result.rows, expected);
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}
