//! Integration tests for SQL DATE / TIME / TIMESTAMP / INTERVAL.
//!
//! Covers storage, literals, CAST, coercion, arithmetic, comparison, functions,
//! index interaction, DEFAULT/CHECK, NULL handling, PG-normalized INTERVAL equality,
//! infinity sentinels, timezone TVFs, and FFI/WASM parity at the SQL layer.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"datetime-test")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"datetime-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn assert_ok(r: ExecutionResult) {
    match r {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn assert_rows(r: ExecutionResult, expected: u64) {
    match r {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, expected),
        other => panic!("expected RowsAffected({expected}), got {other:?}"),
    }
}

fn scalar(conn: &mut Connection<'_>, sql: &str) -> Value {
    let qr = conn.query(sql).unwrap();
    qr.rows[0][0].clone()
}

fn text(conn: &mut Connection<'_>, sql: &str) -> String {
    match scalar(conn, sql) {
        Value::Text(s) => s.to_string(),
        v => panic!("expected TEXT, got {v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Storage mechanics
// ═══════════════════════════════════════════════════════════════════

#[test]
fn create_table_with_all_temporal_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE, t TIME, ts TIMESTAMP, iv INTERVAL)",
        )
        .unwrap(),
    );
    assert_rows(
        conn.execute(
            "INSERT INTO t VALUES (1, DATE '2024-01-15', TIME '12:30:45', TIMESTAMP '2024-01-15 12:30:45', INTERVAL '1 day')",
        )
        .unwrap(),
        1,
    );
    let qr = conn.query("SELECT d, t, ts, iv FROM t").unwrap();
    match &qr.rows[0][0] {
        Value::Date(_) => {}
        v => panic!("expected Date, got {v:?}"),
    }
    match &qr.rows[0][1] {
        Value::Time(_) => {}
        v => panic!("expected Time, got {v:?}"),
    }
    match &qr.rows[0][2] {
        Value::Timestamp(_) => {}
        v => panic!("expected Timestamp, got {v:?}"),
    }
    match &qr.rows[0][3] {
        Value::Interval { .. } => {}
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn insert_null_temporal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE, ts TIMESTAMP)")
            .unwrap(),
    );
    assert_rows(
        conn.execute("INSERT INTO t VALUES (1, NULL, NULL)")
            .unwrap(),
        1,
    );
    let qr = conn.query("SELECT d, ts FROM t").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Null));
    assert!(matches!(qr.rows[0][1], Value::Null));
}

#[test]
fn persist_reopen_temporal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE, ts TIMESTAMP)")
            .unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1, DATE '2024-06-15', TIMESTAMP '2024-06-15 10:00:00')",
        )
        .unwrap();
    }
    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT d, ts FROM t WHERE id = 1").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Date(_)));
    assert!(matches!(qr.rows[0][1], Value::Timestamp(_)));
}

// ═══════════════════════════════════════════════════════════════════
// Literal parsing
// ═══════════════════════════════════════════════════════════════════

#[test]
fn date_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-15'");
    assert_eq!(v.to_string(), "2024-01-15");
}

#[test]
fn time_literal_with_subsec() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TIME '12:30:45.123456'");
    assert_eq!(v.to_string(), "12:30:45.123456");
}

#[test]
fn timestamp_literal_iso() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TIMESTAMP '2024-01-15T12:30:00Z'");
    assert_eq!(v.to_string(), "2024-01-15 12:30:00");
}

#[test]
fn interval_verbose_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT INTERVAL '1 year 2 months 3 days 04:05:06'",
    );
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => {
            assert_eq!(months, 14);
            assert_eq!(days, 3);
            assert_eq!(micros, 4 * 3_600_000_000i64 + 5 * 60_000_000 + 6_000_000);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn interval_iso8601_duration() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT INTERVAL 'P1Y2M3D'");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 14);
            assert_eq!(days, 3);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Arithmetic
// ═══════════════════════════════════════════════════════════════════

#[test]
fn date_plus_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-15' + 10");
    assert_eq!(v.to_string(), "2024-01-25");
}

#[test]
fn date_minus_date_returns_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-25' - DATE '2024-01-15'");
    assert_eq!(v, Value::Integer(10));
}

#[test]
fn date_plus_interval_returns_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-15' + INTERVAL '2 hours'");
    match v {
        Value::Timestamp(_) => assert_eq!(v.to_string(), "2024-01-15 02:00:00"),
        v => panic!("expected Timestamp, got {v:?}"),
    }
}

#[test]
fn timestamp_plus_interval_month_clamp() {
    // Jan 31 + 1 month = Feb 29 in leap year.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month'",
    );
    assert_eq!(v.to_string(), "2024-02-29 00:00:00");
}

#[test]
fn timestamp_minus_timestamp_returns_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-01-02 12:00:00' - TIMESTAMP '2024-01-01 00:00:00'",
    );
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => {
            assert_eq!(months, 0);
            assert_eq!(days, 1);
            assert_eq!(micros, 12 * 3_600_000_000i64);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn interval_plus_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT INTERVAL '1 day' + INTERVAL '2 days'");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 3),
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn date_plus_real_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT DATE '2024-01-15' + 1.5").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

// ═══════════════════════════════════════════════════════════════════
// Comparison / ORDER BY
// ═══════════════════════════════════════════════════════════════════

#[test]
fn date_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-01' < DATE '2024-02-01'");
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn order_by_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TIMESTAMP '2024-03-01 00:00:00'), (2, TIMESTAMP '2024-01-01 00:00:00'), (3, TIMESTAMP '2024-02-01 00:00:00')")
        .unwrap();
    let qr = conn.query("SELECT id FROM t ORDER BY ts").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
    assert_eq!(qr.rows[2][0], Value::Integer(1));
}

#[test]
fn pg_normalized_interval_equality() {
    // PG semantic: INTERVAL '1 month' = INTERVAL '30 days' (30-day month normalization).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT INTERVAL '1 month' = INTERVAL '30 days'");
    assert_eq!(v, Value::Boolean(true));

    let v2 = scalar(&mut conn, "SELECT INTERVAL '24 hours' = INTERVAL '1 day'");
    assert_eq!(v2, Value::Boolean(true));

    let v3 = scalar(&mut conn, "SELECT INTERVAL '25 hours' > INTERVAL '1 day'");
    assert_eq!(v3, Value::Boolean(true));
}

// ═══════════════════════════════════════════════════════════════════
// Functions — current time
// ═══════════════════════════════════════════════════════════════════

#[test]
fn now_returns_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT NOW()");
    assert!(matches!(v, Value::Timestamp(_)));
}

#[test]
fn current_date_returns_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CURRENT_DATE");
    assert!(matches!(v, Value::Date(_)));
}

// ═══════════════════════════════════════════════════════════════════
// Functions — EXTRACT / DATE_TRUNC
// ═══════════════════════════════════════════════════════════════════

#[test]
fn extract_year_from_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT EXTRACT(YEAR FROM DATE '2024-06-15')");
    assert_eq!(v, Value::Integer(2024));
}

#[test]
fn extract_hour_from_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 13:45:00')",
    );
    assert_eq!(v, Value::Integer(13));
}

#[test]
fn extract_epoch_from_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 2024-01-01 = 19723 days * 86400 = 1704067200.
    let v = scalar(&mut conn, "SELECT EXTRACT(EPOCH FROM DATE '2024-01-01')");
    assert_eq!(v, Value::Integer(1704067200));
}

#[test]
fn date_trunc_month() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT DATE_TRUNC('month', TIMESTAMP '2024-03-15 12:30:45')",
    );
    assert_eq!(v.to_string(), "2024-03-01 00:00:00");
}

#[test]
fn date_trunc_week_monday() {
    // 2024-01-07 is a Sunday; trunc('week') returns previous Monday = 2024-01-01.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE_TRUNC('week', DATE '2024-01-07')");
    assert_eq!(v.to_string(), "2024-01-01");
}

// ═══════════════════════════════════════════════════════════════════
// Functions — MAKE_*
// ═══════════════════════════════════════════════════════════════════

#[test]
fn make_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT MAKE_DATE(2024, 6, 15)");
    assert_eq!(v.to_string(), "2024-06-15");
}

#[test]
fn make_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT MAKE_INTERVAL(1, 2, 0, 3)");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 14); // 1 year + 2 months
            assert_eq!(days, 3);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Functions — JUSTIFY / AGE
// ═══════════════════════════════════════════════════════════════════

#[test]
fn justify_days_normalizes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT JUSTIFY_DAYS(INTERVAL '65 days')");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 2);
            assert_eq!(days, 5);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn age_symbolic_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT AGE(TIMESTAMP '2024-04-10 00:00:00', TIMESTAMP '2024-01-01 00:00:00')",
    );
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => {
            assert_eq!(months, 3);
            assert_eq!(days, 9);
            assert_eq!(micros, 0);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Functions — SQLite-compat
// ═══════════════════════════════════════════════════════════════════

#[test]
fn strftime_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let s = text(
        &mut conn,
        "SELECT STRFTIME('%Y-%m', TIMESTAMP '2024-03-15 12:00:00')",
    );
    assert_eq!(s, "2024-03");
}

#[test]
fn unixepoch_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT UNIXEPOCH(TIMESTAMP '2024-01-01 00:00:00')",
    );
    assert_eq!(v, Value::Integer(1704067200));
}

#[test]
fn julianday_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 2000-01-01 12:00:00 UTC == Julian day 2451545.0 exactly.
    let v = scalar(
        &mut conn,
        "SELECT JULIANDAY(TIMESTAMP '2000-01-01 12:00:00')",
    );
    if let Value::Real(j) = v {
        assert!((j - 2451545.0).abs() < 1e-6);
    } else {
        panic!("expected Real, got {v:?}");
    }
}

// ═══════════════════════════════════════════════════════════════════
// Index interaction
// ═══════════════════════════════════════════════════════════════════

#[test]
fn index_on_date_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("CREATE INDEX idx_d ON e (d)").unwrap();
    for i in 1..=50 {
        let sql = format!(
            "INSERT INTO e VALUES ({i}, DATE '2024-01-{:02}')",
            (i % 28) + 1
        );
        conn.execute(&sql).unwrap();
    }
    let qr = conn
        .query("SELECT COUNT(*) FROM e WHERE d BETWEEN DATE '2024-01-10' AND DATE '2024-01-15'")
        .unwrap();
    if let Value::Integer(n) = qr.rows[0][0] {
        assert!(n > 0);
    } else {
        panic!("expected Integer count");
    }
}

#[test]
fn unique_index_on_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_ts ON e (ts)")
        .unwrap();
    conn.execute("INSERT INTO e VALUES (1, TIMESTAMP '2024-01-01 00:00:00')")
        .unwrap();
    let err = conn
        .execute("INSERT INTO e VALUES (2, TIMESTAMP '2024-01-01 00:00:00')")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

// ═══════════════════════════════════════════════════════════════════
// Infinity sentinels
// ═══════════════════════════════════════════════════════════════════

#[test]
fn infinity_timestamp_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TIMESTAMP 'infinity'");
    assert_eq!(v, Value::Timestamp(i64::MAX));
    let v2 = scalar(&mut conn, "SELECT TIMESTAMP '-infinity'");
    assert_eq!(v2, Value::Timestamp(i64::MIN));
}

#[test]
fn isfinite_on_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let a = scalar(&mut conn, "SELECT ISFINITE(TIMESTAMP 'infinity')");
    assert_eq!(a, Value::Boolean(false));
    let b = scalar(
        &mut conn,
        "SELECT ISFINITE(TIMESTAMP '2024-01-01 00:00:00')",
    );
    assert_eq!(b, Value::Boolean(true));
}

#[test]
fn infinity_compares_greater() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP 'infinity' > TIMESTAMP '5000-01-01 00:00:00'",
    );
    assert_eq!(v, Value::Boolean(true));
}

// ═══════════════════════════════════════════════════════════════════
// CAST
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cast_text_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CAST('2024-06-15' AS DATE)");
    assert_eq!(v.to_string(), "2024-06-15");
}

#[test]
fn cast_integer_to_timestamp_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CAST(1704067200 AS TIMESTAMP)");
    assert_eq!(v.to_string(), "2024-01-01 00:00:00");
}

#[test]
fn cast_timestamp_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT CAST(TIMESTAMP '2024-06-15 14:00:00' AS DATE)",
    );
    assert_eq!(v.to_string(), "2024-06-15");
}

// ═══════════════════════════════════════════════════════════════════
// NULL handling
// ═══════════════════════════════════════════════════════════════════

#[test]
fn null_arithmetic_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2024-01-01' + NULL");
    assert!(matches!(v, Value::Null));
}

#[test]
fn extract_null_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT EXTRACT(YEAR FROM NULL)");
    assert!(matches!(v, Value::Null));
}

// ═══════════════════════════════════════════════════════════════════
// BC dates
// ═══════════════════════════════════════════════════════════════════

#[test]
fn bc_date_parses_and_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '0001-01-01 BC'");
    assert_eq!(v.to_string(), "0001-01-01 BC");
}

#[test]
fn bc_date_before_ad() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '0001-01-01 BC' < DATE '0001-01-01'");
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn year_0_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT DATE '0000-01-01'").unwrap_err();
    assert!(matches!(err, SqlError::InvalidDateLiteral(_)));
}

// ═══════════════════════════════════════════════════════════════════
// DEFAULT
// ═══════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════
// Timezone TVFs
// ═══════════════════════════════════════════════════════════════════

#[test]
fn timezone_names_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM timezone_names()").unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => assert!(*n > 100, "expected >100 IANA zones, got {n}"),
        v => panic!("expected Integer count, got {v:?}"),
    }
}

#[test]
fn timezone_names_has_utc() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT name FROM timezone_names() WHERE name = 'UTC'")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn timezone_abbrevs_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT COUNT(*) FROM timezone_abbrevs()")
        .unwrap();
    match &qr.rows[0][0] {
        Value::Integer(n) => assert!(*n > 0, "expected >0 abbrevs, got {n}"),
        v => panic!("expected Integer count, got {v:?}"),
    }
}

#[test]
fn current_timestamp_stable_within_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    let t1 = scalar(&mut conn, "SELECT CURRENT_TIMESTAMP");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t2 = scalar(&mut conn, "SELECT CURRENT_TIMESTAMP");
    conn.execute("COMMIT").unwrap();
    assert_eq!(t1, t2, "CURRENT_TIMESTAMP should be stable within a txn");
}

#[test]
fn clock_timestamp_advances_within_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    let t1 = scalar(&mut conn, "SELECT CLOCK_TIMESTAMP()");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t2 = scalar(&mut conn, "SELECT CLOCK_TIMESTAMP()");
    conn.execute("COMMIT").unwrap();
    // CLOCK_TIMESTAMP reads fresh each call, so t2 > t1 (PG semantic).
    match (t1, t2) {
        (Value::Timestamp(a), Value::Timestamp(b)) => {
            assert!(
                b > a,
                "CLOCK_TIMESTAMP should advance within a txn ({a} vs {b})"
            );
        }
        other => panic!("expected Timestamps, got {other:?}"),
    }
}

#[test]
fn set_time_zone_valid_zone() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(conn.execute("SET TIME ZONE 'America/New_York'").unwrap());
    assert_ok(conn.execute("SET TIME ZONE '+05:00'").unwrap());
    assert_ok(conn.execute("SET TIME ZONE 'UTC'").unwrap());
}

#[test]
fn set_time_zone_rejects_posix_shorthand() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SET TIME ZONE 'UTC+5'").unwrap_err();
    assert!(matches!(err, SqlError::InvalidTimezone(_)));
}

#[test]
fn sum_interval_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES (1, INTERVAL '1 day'), (2, INTERVAL '2 days'), (3, INTERVAL '3 hours')",
    )
    .unwrap();
    let v = scalar(&mut conn, "SELECT SUM(iv) FROM t");
    match v {
        Value::Interval { days, micros, .. } => {
            assert_eq!(days, 3);
            assert_eq!(micros, 3 * 3_600_000_000);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn avg_interval_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, INTERVAL '2 days'), (2, INTERVAL '4 days')")
        .unwrap();
    let v = scalar(&mut conn, "SELECT AVG(iv) FROM t");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 3),
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn default_current_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let qr = conn.query("SELECT created_at FROM t WHERE id = 1").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Timestamp(_)));
}
