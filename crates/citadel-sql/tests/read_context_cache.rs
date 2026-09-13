use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"read-context-cache")
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
    conn.execute("CREATE TABLE anchors (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO anchors VALUES (1)").unwrap();
    // Explicit projection makes this a materialized view read rather than the
    // separate SELECT-* view fusion path.
    conn.execute("CREATE VIEW contextual_view AS SELECT id, d, v FROM items")
        .unwrap();
}

const READS: [&str; 8] = [
    "SELECT d, v FROM items ORDER BY id",
    "SELECT i.d, i.v FROM items i JOIN anchors a ON i.id = a.id ORDER BY i.id",
    "SELECT i.d, i.v FROM anchors a JOIN items i ON a.id = i.id ORDER BY i.id",
    "SELECT d, v FROM items UNION ALL SELECT d, v FROM items",
    "SELECT i.d, i.v FROM (WITH items AS (SELECT 1 AS id) SELECT id FROM items) a JOIN items i ON a.id=i.id ORDER BY i.id",
    "WITH items AS (SELECT d, v FROM items) SELECT d, v FROM items ORDER BY d",
    "WITH items AS (SELECT 1 AS id) SELECT d, v FROM contextual_view ORDER BY id",
    "WITH RECURSIVE items(d, v) AS (SELECT d, v FROM items UNION ALL SELECT d, v FROM items WHERE 0) SELECT d, v FROM items ORDER BY d",
];

#[test]
fn decoded_join_rows_do_not_retain_scoped_parameter_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "INTEGER", "$1");
    let prepared = conn
        .prepare(
            "SELECT i.d, i.v, $1 AS supplied FROM anchors a \
         JOIN items i ON a.id = i.id ORDER BY i.id",
        )
        .unwrap();
    for parameter in [7, 19, 7] {
        let value = Value::Integer(parameter);
        assert_eq!(
            prepared
                .query_collect(std::slice::from_ref(&value))
                .unwrap()
                .rows,
            vec![vec![value; 3]]
        );
    }
}

#[test]
fn cached_reads_recheck_lazy_default_and_virtual_dependencies_after_timezone_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(&conn, "DATE", "CURRENT_DATE");
    let prepared: Vec<_> = READS.iter().map(|sql| conn.prepare(sql).unwrap()).collect();
    let mut dates = Vec::new();
    for zone in ["+14:00", "-12:00", "+14:00"] {
        conn.set_session_timezone(zone).unwrap();
        let date = conn.query("SELECT CURRENT_DATE").unwrap().rows[0][0].clone();
        dates.push(date.clone());
        for (index, statement) in prepared.iter().enumerate() {
            let count = if index == 3 { 2 } else { 1 };
            for _ in 0..2 {
                assert_eq!(
                    statement.query_collect(&[]).unwrap().rows,
                    vec![vec![date.clone(), date.clone()]; count],
                    "zone {zone}, {}",
                    READS[index]
                );
            }
        }
    }
    // These zones are 26 hours apart, so this cannot depend on a sleep or on
    // which side of midnight the test starts.
    assert_ne!(dates[0], dates[1]);
}

#[test]
fn decoded_join_and_compound_rows_do_not_retain_jsonpath_session_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    create_old_row(
        &conn,
        "TEXT",
        r#"CAST(JSONB_PATH_QUERY_FIRST_TZ(
        CAST('"2023-08-15T12:34:56+05:30"' AS JSONB), '$.time().string()') AS TEXT)"#,
    );
    let prepared: Vec<_> = READS.iter().map(|sql| conn.prepare(sql).unwrap()).collect();
    // The input instant fixes the named-zone DST offset independently of the
    // current date. Both schema expressions materialize from the old short row.
    for (zone, expected) in [
        ("UTC", "\"07:04:56\""),
        ("America/New_York", "\"03:04:56\""),
        ("+10:00", "\"17:04:56\""),
        ("UTC", "\"07:04:56\""),
    ] {
        conn.set_session_timezone(zone).unwrap();
        let value = Value::Text(expected.into());
        for (index, statement) in prepared.iter().enumerate() {
            let count = if index == 3 { 2 } else { 1 };
            assert_eq!(
                statement.query_collect(&[]).unwrap().rows,
                vec![vec![value.clone(), value.clone()]; count],
                "zone {zone}, {}",
                READS[index]
            );
        }
    }
}

#[test]
fn prepared_wildcard_recompiles_hidden_context_dependencies_after_alter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1)").unwrap();
    let prepared = conn.prepare("SELECT * FROM items ORDER BY id").unwrap();
    assert_eq!(prepared.query_collect(&[]).unwrap().columns, ["id"]);
    conn.execute("ALTER TABLE items ADD COLUMN d DATE DEFAULT CURRENT_DATE")
        .unwrap();
    conn.execute("ALTER TABLE items ADD COLUMN v DATE GENERATED ALWAYS AS (d) VIRTUAL")
        .unwrap();
    for zone in ["+14:00", "-12:00"] {
        conn.set_session_timezone(zone).unwrap();
        let date = conn.query("SELECT CURRENT_DATE").unwrap().rows[0][0].clone();
        let result = prepared.query_collect(&[]).unwrap();
        assert_eq!(result.columns, ["id", "d", "v"]);
        assert_eq!(
            result.rows,
            vec![vec![Value::Integer(1), date.clone(), date]]
        );
    }
}
