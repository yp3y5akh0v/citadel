use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"prepared-read-context")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn create_old_row(conn: &Connection<'_>, data_type: &str, default: &str) {
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1)").unwrap();
    conn.execute(&format!(
        "ALTER TABLE items ADD COLUMN d {data_type} DEFAULT ({default})"
    ))
    .unwrap();
    conn.execute(&format!(
        "ALTER TABLE items ADD COLUMN v {data_type} GENERATED ALWAYS AS (d) VIRTUAL"
    ))
    .unwrap();
}

#[test]
fn prepared_collect_and_rows_use_the_session_for_hidden_schema_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "DATE", "CURRENT_DATE");
    let prepared = conn.prepare("SELECT d, v FROM items").unwrap();
    for (zone, next_zone) in [("+14:00", "-12:00"), ("-12:00", "+14:00")] {
        conn.set_session_timezone(zone).unwrap();
        let date = conn.query("SELECT CURRENT_DATE").unwrap().rows[0][0].clone();
        let expected = vec![vec![date.clone(), date]];
        assert_eq!(prepared.query_collect(&[]).unwrap().rows, expected);
        assert_eq!(
            prepared.query(&[]).unwrap().collect().unwrap().rows,
            expected
        );
        let rows = prepared.query(&[]).unwrap();
        // A context-dependent prepared read is evaluated inside the statement
        // scope, before its owned Rows result outlives that scope.
        conn.set_session_timezone(next_zone).unwrap();
        assert_eq!(rows.collect().unwrap().rows, expected);
        assert_eq!(conn.session_timezone(), next_zone);
        let next_date = conn.query("SELECT CURRENT_DATE").unwrap().rows[0][0].clone();
        assert_eq!(
            prepared.query_collect(&[]).unwrap().rows,
            vec![vec![next_date.clone(), next_date]]
        );
    }
}

#[test]
fn prepared_jsonpath_defaults_follow_local_savepoint_and_connection_contexts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(
        &conn,
        "TEXT",
        r#"CAST(JSONB_PATH_QUERY_FIRST_TZ(
        CAST('"2023-08-15T12:34:56+05:30"' AS JSONB), '$.time().string()') AS TEXT)"#,
    );
    let prepared = conn.prepare("SELECT d, v FROM items").unwrap();
    let check = |expected: &str| {
        let value = Value::Text(expected.into());
        let expected = vec![vec![value.clone(), value]];
        assert_eq!(prepared.query_collect(&[]).unwrap().rows, expected);
        assert_eq!(
            prepared.query(&[]).unwrap().collect().unwrap().rows,
            expected
        );
        assert_eq!(
            conn.query("SELECT d, v FROM items ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
    };
    conn.set_session_timezone("America/New_York").unwrap();
    check("\"03:04:56\"");
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_zone").unwrap();
    conn.execute("SET LOCAL TIME ZONE '+10:00'").unwrap();
    check("\"17:04:56\"");
    conn.execute("ROLLBACK TO before_zone").unwrap();
    check("\"03:04:56\"");
    conn.execute("SET LOCAL TIME ZONE '+10:00'").unwrap();
    conn.execute("COMMIT").unwrap();
    check("\"03:04:56\"");
    conn.execute("BEGIN").unwrap();
    conn.execute("SET LOCAL TIME ZONE UTC").unwrap();
    check("\"07:04:56\"");
    conn.execute("ROLLBACK").unwrap();
    check("\"03:04:56\"");

    let utc = Connection::open(&db).unwrap();
    let utc_prepared = utc.prepare("SELECT d, v FROM items").unwrap();
    assert_eq!(
        utc_prepared.query_collect(&[]).unwrap().rows,
        vec![vec![Value::Text("\"07:04:56\"".into()); 2]]
    );
    check("\"03:04:56\"");
}

#[test]
fn prepared_plain_scan_rechecks_context_admission_after_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1)").unwrap();
    let prepared = conn.prepare("SELECT * FROM items").unwrap();
    assert_eq!(
        prepared.query(&[]).unwrap().collect().unwrap().columns,
        ["id"]
    );
    conn.execute("ALTER TABLE items ADD COLUMN d DATE DEFAULT CURRENT_DATE")
        .unwrap();
    for zone in ["+14:00", "-12:00"] {
        conn.set_session_timezone(zone).unwrap();
        let date = conn.query("SELECT CURRENT_DATE").unwrap().rows[0][0].clone();
        let collected = prepared.query_collect(&[]).unwrap();
        let streamed = prepared.query(&[]).unwrap().collect().unwrap();
        assert_eq!(collected.columns, ["id", "d"]);
        assert_eq!(streamed.columns, collected.columns);
        assert_eq!(streamed.rows, collected.rows);
        assert_eq!(collected.rows, vec![vec![Value::Integer(1), date]]);
    }
}
