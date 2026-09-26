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
fn json_exists_simple_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_exists(CAST('{\"a\":1}' AS JSONB), '$.a')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn json_exists_missing_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_exists(CAST('{\"a\":1}' AS JSONB), '$.b')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn json_value_scalar() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_value(CAST('{\"a\":42}' AS JSONB), '$.a')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("42".into()));
}

#[test]
fn json_value_string_unquoted() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_value(CAST('{\"name\":\"alice\"}' AS JSONB), '$.name')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn json_query_returns_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_query(CAST('{\"items\":[1,2,3]}' AS JSONB), '$.items')")
        .unwrap();
    let bytes = match &qr.rows[0][0] {
        Value::Jsonb(b) => b.clone(),
        _ => panic!("expected jsonb"),
    };
    let s = citadel_sql::json::decode_to_text(&bytes).unwrap();
    assert!(s.contains("1"));
    assert!(s.contains("2"));
    assert!(s.contains("3"));
}

#[test]
fn jsonpath_predicate_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT json_exists(CAST('{\"items\":[{\"x\":1},{\"x\":10}]}' AS JSONB), '$.items[*] ? (@.x > 5)')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_question_operator_with_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":10}' AS JSONB) @? '$.x ? (@ > 5)'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn json_table_basic_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT * FROM JSON_TABLE(\
                CAST('[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]' AS JSONB), \
                '$[*]' COLUMNS (\
                    a INT PATH '$.a', \
                    b TEXT PATH '$.b'\
                )\
             ) AS jt",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][1], Value::Text("y".into()));
}

#[test]
fn json_table_for_ordinality_counter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT * FROM JSON_TABLE(\
                CAST('[\"a\",\"b\",\"c\"]' AS JSONB), \
                '$[*]' COLUMNS (\
                    rn FOR ORDINALITY, \
                    v TEXT PATH '$'\
                )\
             ) AS jt",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("a".into()));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[2][1], Value::Text("c".into()));
}

#[test]
fn json_table_nested_path_flattens() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT * FROM JSON_TABLE(\
                CAST('[{\"id\":1,\"tags\":[\"x\",\"y\"]},{\"id\":2,\"tags\":[\"z\"]}]' AS JSONB), \
                '$[*]' COLUMNS (\
                    id INT PATH '$.id', \
                    NESTED PATH '$.tags[*]' COLUMNS (\
                        tag TEXT PATH '$'\
                    )\
                )\
             ) AS jt",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("x".into()));
    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Text("y".into()));
    assert_eq!(qr.rows[2][0], Value::Integer(2));
    assert_eq!(qr.rows[2][1], Value::Text("z".into()));
}

#[test]
fn json_table_null_source_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT * FROM JSON_TABLE(\
                NULL, \
                '$[*]' COLUMNS (\
                    v TEXT PATH '$'\
                )\
             ) AS jt",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn json_table_exists_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT * FROM JSON_TABLE(\
                CAST('[{\"a\":1},{\"b\":2}]' AS JSONB), \
                '$[*]' COLUMNS (\
                    has_a BOOLEAN EXISTS PATH '$.a'\
                )\
             ) AS jt",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
    assert_eq!(qr.rows[1][0], Value::Boolean(false));
}
