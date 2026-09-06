use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn conn() -> (tempfile::TempDir, citadel::Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    (dir, db)
}

fn scalar(c: &Connection<'_>, sql: &str) -> Value {
    let r = c.prepare(sql).unwrap().query_collect(&[]).unwrap();
    r.rows[0][0].clone()
}

#[test]
fn datetime_compare_across_dst_spring_forward() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"2024-03-10T03:30:00-04:00\"' AS JSONB), \
            '$.datetime() > \"2024-03-10T01:30:00-05:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn datetime_compare_across_dst_fall_back_different_instants() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"2024-11-03T01:30:00-04:00\"' AS JSONB), \
            '$.datetime() < \"2024-11-03T01:30:00-05:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn session_tz_default_utc_compare_works() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"2024-06-15T10:00:00+00:00\"' AS JSONB), \
            '$.datetime() > \"2024-06-15T09:00:00+00:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn tz_aware_query_returns_match() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_query_tz(\
            CAST('\"2024-03-10T07:00:00+00:00\"' AS JSONB), '$.datetime()')",
    );
    assert!(matches!(v, Value::Jsonb(_)), "expected Jsonb, got {v:?}");
}

#[test]
fn non_tz_query_on_same_row_also_works() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_query_first(\
            CAST('\"2024-03-10T07:00:00+00:00\"' AS JSONB), '$.datetime()')",
    );
    assert!(matches!(v, Value::Jsonb(_)), "expected Jsonb, got {v:?}");
}

#[test]
fn cannot_convert_without_tz_surfaces_as_error() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let res = c
        .prepare(
            "SELECT jsonb_path_match(\
                CAST('\"2024-06-15T10:00:00\"' AS JSONB), \
                '$.datetime() > \"2024-06-15T05:00:00+00:00\".datetime()')",
        )
        .unwrap()
        .query_collect(&[]);
    let err = res.unwrap_err().to_string().to_lowercase();
    assert!(
        err.contains("convert") || err.contains("tz") || err.contains("time zone"),
        "expected convert-without-tz error, got: {err}"
    );
}

#[test]
fn convert_without_tz_propagates_even_with_silent() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let res = c
        .prepare(
            "SELECT jsonb_path_match(\
                CAST('\"2024-06-15T10:00:00\"' AS JSONB), \
                '$.datetime() > \"2024-06-15T05:00:00+00:00\".datetime()', \
                NULL, true)",
        )
        .unwrap()
        .query_collect(&[]);
    let err = res.unwrap_err().to_string().to_lowercase();
    assert!(
        err.contains("convert") || err.contains("tz") || err.contains("time zone"),
        "DatetimeConvertWithoutTz must propagate even with silent => true, got: {err}"
    );
}

#[test]
fn wide_year_date_compare_against_plain_timestamp() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match(\
            CAST('\"1000000-01-01\"' AS JSONB), \
            '$.datetime() > \"2020-01-01 12:00:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn wide_year_date_compare_against_timestamptz_via_tz_fn() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"1000000-01-01\"' AS JSONB), \
            '$.datetime() > \"2020-01-01T12:00:00+00:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn template_with_tz_directives_compares_correctly() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"2024-03-10 12:30:45+05:30\"' AS JSONB), \
            '$.datetime(\"YYYY-MM-DD HH24:MI:SSTZH:TZM\") > \
             \"2024-01-01T00:00:00+00:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn datetime_returns_plain_string_at_api_boundary() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_query_first(\
            CAST('\"2024-01-15\"' AS JSONB), '$.datetime()')",
    );
    let s = match v {
        Value::Jsonb(b) => String::from_utf8_lossy(&b).to_string(),
        other => panic!("expected Jsonb, got {other:?}"),
    };
    assert!(
        !s.contains("__pg_datetime"),
        "marker must be unwrapped at API boundary, got: {s}"
    );
}

#[test]
fn mixed_tz_offsets_same_instant_compare_equal() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_match_tz(\
            CAST('\"2024-03-10T10:00:00+05:00\"' AS JSONB), \
            '$.datetime() == \"2024-03-10T05:00:00+00:00\".datetime()')",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn datetime_iso_microseconds_round_trip() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let v = scalar(
        &c,
        "SELECT jsonb_path_query_first(\
            CAST('\"2024-01-15 12:30:45.123456\"' AS JSONB), '$.datetime()')",
    );
    assert!(matches!(v, Value::Jsonb(_)));
}

#[test]
fn datetime_garbage_input_errors() {
    let (_d, db) = conn();
    let c = Connection::open(&db).unwrap();
    let res = c
        .prepare(
            "SELECT jsonb_path_query_first(\
                CAST('\"not-a-date\"' AS JSONB), '$.datetime()')",
        )
        .unwrap()
        .query_collect(&[]);
    assert!(res.is_err());
}
