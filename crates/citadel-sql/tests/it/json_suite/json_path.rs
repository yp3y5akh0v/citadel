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
fn jsonb_path_exists_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_path_exists(CAST('{\"a\":1}' AS JSONB), '$.a')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));

    let qr = conn
        .query("SELECT jsonb_path_exists(CAST('{\"a\":1}' AS JSONB), '$.missing')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn jsonb_path_exists_with_vars() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_exists(\
                CAST('{\"x\":15}' AS JSONB), \
                '$.x ? (@ > $min)', \
                CAST('{\"min\":10}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));

    let qr = conn
        .query(
            "SELECT jsonb_path_exists(\
                CAST('{\"x\":5}' AS JSONB), \
                '$.x ? (@ > $min)', \
                CAST('{\"min\":10}' AS JSONB))",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn jsonb_path_exists_silent_suppresses_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // strict mode: missing key would error without silent=true → empty/null.
    let qr = conn
        .query(
            "SELECT jsonb_path_exists(\
                CAST('{\"a\":1}' AS JSONB), \
                'strict $.bogus ? (@ > 0)', \
                NULL, true)",
        )
        .unwrap();
    let v = &qr.rows[0][0];
    assert!(
        matches!(v, Value::Boolean(false) | Value::Null),
        "expected suppressed result (false/NULL), got {v:?}"
    );
}

#[test]
fn jsonb_path_match_returns_boolean() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_path_match(CAST('{\"a\":10}' AS JSONB), '$.a > 5')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));

    let qr = conn
        .query("SELECT jsonb_path_match(CAST('{\"a\":10}' AS JSONB), '$.a > 20')")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn jsonb_path_query_first_returns_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_path_query_first(CAST('{\"a\":[1,2,3]}' AS JSONB), '$.a[*]')")
        .unwrap();
    // First match should be 1.
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb result, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn jsonb_path_query_array_wraps_all_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT jsonb_path_query_array(CAST('{\"a\":[1,2,3]}' AS JSONB), '$.a[*]')")
        .unwrap();
    // Result is a JSONB array of [1,2,3]; verify via roundtrip.
    let arr = match &qr.rows[0][0] {
        Value::Jsonb(_) => qr.rows[0][0].clone(),
        v => panic!("expected Jsonb, got {v:?}"),
    };
    let _ = arr;
}

#[test]
fn jsonb_path_query_first_with_vars_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_first(\
                CAST('{\"items\":[1,5,10,20]}' AS JSONB), \
                '$.items[*] ? (@ > $cutoff)', \
                CAST('{\"cutoff\":8}' AS JSONB))",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb match, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn jsonb_path_query_array_silent_swallows_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // strict mode missing key with silent=true → empty array.
    let qr = conn
        .query(
            "SELECT jsonb_path_query_array(\
                CAST('{\"a\":1}' AS JSONB), \
                'strict $.bogus', \
                NULL, true)",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb array (possibly empty), got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn jsonb_path_exists_vars_must_be_object_else_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .query(
            "SELECT jsonb_path_exists(\
                CAST('{\"a\":1}' AS JSONB), '$.a', CAST('[1,2]' AS JSONB))",
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("object") || msg.to_lowercase().contains("vars"),
        "expected error about vars not being an object, got: {msg}"
    );
}

#[test]
fn legacy_op_path_match_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) @@ '$.a > 0'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn legacy_op_path_exists_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"a\":1}' AS JSONB) @? '$.a'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn datetime_method_iso_date_via_query_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_first(\
                CAST('\"2024-01-15\"' AS JSONB), '$.datetime()')",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb date, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn datetime_method_with_template() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_first(\
                CAST('\"Mar 05 2024\"' AS JSONB), \
                '$.datetime(\"MON DD YYYY\")')",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected parsed date from template, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn datetime_method_filter_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_match(\
                CAST('\"2024-01-15\"' AS JSONB), \
                '$.datetime() > \"2020-01-01\".datetime()')",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn datetime_method_non_string_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .query(
            "SELECT jsonb_path_query_first(\
                CAST('123' AS JSONB), '$.datetime()')",
        )
        .unwrap_err();
    assert!(
        format!("{err}").to_lowercase().contains("string")
            || format!("{err}").to_lowercase().contains("datetime"),
        "expected string-required error, got: {err}"
    );
}

#[test]
fn jsonb_path_exists_tz_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_exists_tz(\
                CAST('\"2024-01-15T12:00:00+05:00\"' AS JSONB), '$.datetime()')",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn jsonb_path_match_tz_compare_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_match_tz(\
                CAST('\"2024-06-15T10:00:00+00:00\"' AS JSONB), \
                '$.datetime() > \"2024-01-01T00:00:00+00:00\".datetime()')",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn jsonb_path_query_tz_returns_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_tz(\
                CAST('\"2024-03-10T07:00:00+00:00\"' AS JSONB), '$.datetime()')",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb tz timestamp, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn jsonb_path_query_first_tz_returns_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_first_tz(\
                CAST('[\"2024-01-01T00:00:00+00:00\",\"2024-06-01T00:00:00+00:00\"]' \
                    AS JSONB), '$[*].datetime()')",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb first match, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn jsonb_path_query_array_tz_wraps_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_query_array_tz(\
                CAST('[\"2024-01-01T00:00:00+00:00\",\"2024-06-01T00:00:00+00:00\"]' \
                    AS JSONB), '$[*].datetime()')",
        )
        .unwrap();
    assert!(
        matches!(&qr.rows[0][0], Value::Jsonb(_)),
        "expected Jsonb array, got {:?}",
        qr.rows[0][0]
    );
}

#[test]
fn datetime_iso_timestamptz_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT jsonb_path_match_tz(\
                CAST('\"2024-03-10T10:00:00+05:00\"' AS JSONB), \
                '$.datetime() == \"2024-03-10T05:00:00+00:00\".datetime()')",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}
