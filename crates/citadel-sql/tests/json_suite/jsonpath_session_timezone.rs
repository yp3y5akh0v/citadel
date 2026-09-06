use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn scalar(connection: &Connection<'_>, sql: &str) -> Value {
    connection.query(sql).unwrap().rows[0][0].clone()
}

fn shifted_time(connection: &Connection<'_>) -> Value {
    scalar(
        connection,
        "SELECT CAST(JSONB_PATH_QUERY_FIRST_TZ(\
             CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB), \
             '$.time().string()') AS TEXT)",
    )
}

#[test]
fn set_timezone_is_transactional_and_set_local_is_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();

    connection.execute("BEGIN").unwrap();
    connection
        .execute("SET TIME ZONE 'America/New_York'")
        .unwrap();
    assert_eq!(connection.session_timezone(), "America/New_York");
    connection.execute("ROLLBACK").unwrap();
    assert_eq!(connection.session_timezone(), "UTC");

    connection.execute("BEGIN").unwrap();
    connection
        .execute("SET TIMEZONE TO 'America/New_York'")
        .unwrap();
    connection.execute("COMMIT").unwrap();
    assert_eq!(connection.session_timezone(), "America/New_York");

    connection.execute("BEGIN").unwrap();
    connection.execute("SET LOCAL TIME ZONE '+05:00'").unwrap();
    assert_eq!(connection.session_timezone(), "+05:00");
    connection.execute("COMMIT").unwrap();
    assert_eq!(connection.session_timezone(), "America/New_York");

    connection.execute("SET TIME ZONE DEFAULT").unwrap();
    assert_eq!(connection.session_timezone(), "UTC");
    assert!(connection.execute("SET LOCAL TIME ZONE '+01:00'").is_err());
    assert_eq!(connection.session_timezone(), "UTC");
}

#[test]
fn rollback_to_savepoint_restores_effective_and_committed_timezone() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();

    connection.execute("BEGIN").unwrap();
    connection.execute("SET TIME ZONE '+01:00'").unwrap();
    connection.execute("SAVEPOINT before_zone").unwrap();
    connection.execute("SET TIMEZONE = '+02:00'").unwrap();
    connection.execute("SET LOCAL TIMEZONE = '+03:00'").unwrap();
    assert_eq!(connection.session_timezone(), "+03:00");
    connection.execute("ROLLBACK TO before_zone").unwrap();
    assert_eq!(connection.session_timezone(), "+01:00");
    connection.execute("COMMIT").unwrap();
    assert_eq!(connection.session_timezone(), "+01:00");
}

#[test]
fn prepared_jsonpath_uses_the_timezone_at_execution() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    let prepared = connection
        .prepare(
            "SELECT CAST(JSONB_PATH_QUERY_FIRST_TZ(\
                 CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB), \
                 '$.time().string()') AS TEXT)",
        )
        .unwrap();

    connection.execute("SET TIME ZONE '+00:00'").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows[0][0],
        Value::Text("\"07:04:56\"".into())
    );
    connection.execute("SET TIME ZONE '+10:00'").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows[0][0],
        Value::Text("\"17:04:56\"".into())
    );
}

#[test]
fn jsonpath_comparisons_use_the_session_timezone() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    let sql = "SELECT JSONB_PATH_MATCH_TZ(\
        CAST('\"2023-08-15T00:00:00\"' AS JSONB), \
        '$.timestamp() == \"2023-08-15T04:00:00+00:00\".timestamp_tz()')";

    assert_eq!(scalar(&connection, sql), Value::Boolean(false));
    connection
        .execute("SET TIME ZONE 'America/New_York'")
        .unwrap();
    assert_eq!(scalar(&connection, sql), Value::Boolean(true));
}

#[test]
fn connection_scopes_do_not_leak_between_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let utc = Connection::open(&db).unwrap();
    let east = Connection::open(&db).unwrap();
    east.execute("SET TIME ZONE '+10:00'").unwrap();

    assert_eq!(shifted_time(&utc), Value::Text("\"07:04:56\"".into()));
    assert_eq!(shifted_time(&east), Value::Text("\"17:04:56\"".into()));
    assert_eq!(shifted_time(&utc), Value::Text("\"07:04:56\"".into()));
}

#[test]
fn standard_jsonpath_operator_is_not_streamed_without_statement_context() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, happened JSONB)")
        .unwrap();
    connection
        .execute(
            "INSERT INTO events VALUES \
             (1, CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB))",
        )
        .unwrap();
    let prepared = connection
        .prepare(
            "SELECT happened @@ '$.time_tz().string() == \"17:04:56+10:00\"' \
             FROM events WHERE id = 1",
        )
        .unwrap();

    connection.execute("SET TIME ZONE '+00:00'").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows[0][0],
        Value::Boolean(false)
    );
    connection.execute("SET TIME ZONE '+10:00'").unwrap();
    assert_eq!(
        prepared.query_collect(&[]).unwrap().rows[0][0],
        Value::Boolean(true)
    );
}

#[test]
fn recovery_and_read_only_lanes_receive_the_jsonpath_context() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let connection = Connection::open(&db).unwrap();
    connection.execute("SET TIME ZONE '+10:00'").unwrap();
    connection
        .execute("CREATE TABLE events (id INTEGER PRIMARY KEY, happened JSONB, matched BOOLEAN)")
        .unwrap();
    connection
        .execute(
            "INSERT INTO events VALUES \
             (1, CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB), false)",
        )
        .unwrap();
    connection
        .execute_params_uncancelled_recovery(
            "UPDATE events SET matched = \
             happened @@ '$.time_tz().string() == \"17:04:56+10:00\"' WHERE id = 1",
            &[],
        )
        .unwrap();
    assert_eq!(
        scalar(&connection, "SELECT matched FROM events WHERE id = 1"),
        Value::Boolean(true)
    );

    connection.execute("BEGIN READ ONLY").unwrap();
    assert_eq!(
        shifted_time(&connection),
        Value::Text("\"17:04:56\"".into())
    );
    connection.execute("COMMIT").unwrap();
}
