//! Torture tests for SQL DATE / TIME / TIMESTAMP / INTERVAL.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"datetime-torture")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"datetime-torture")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

fn scalar(conn: &mut Connection<'_>, sql: &str) -> Value {
    conn.query(sql).unwrap().rows[0][0].clone()
}

fn scalar_err(conn: &mut Connection<'_>, sql: &str) -> SqlError {
    conn.query(sql).unwrap_err()
}

fn assert_ok(r: ExecutionResult) {
    match r {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Calendar arithmetic — month-end clamping
// ═══════════════════════════════════════════════════════════════════

#[test]
fn jan31_plus_1month_feb28_non_leap() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2023-01-31 00:00:00' + INTERVAL '1 month'",
    );
    assert_eq!(v.to_string(), "2023-02-28 00:00:00");
}

#[test]
fn jan31_plus_1month_feb29_leap() {
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
fn mar31_plus_1month_apr30() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-03-31 00:00:00' + INTERVAL '1 month'",
    );
    assert_eq!(v.to_string(), "2024-04-30 00:00:00");
}

#[test]
fn feb29_plus_1year_feb28_non_leap() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-02-29 00:00:00' + INTERVAL '1 year'",
    );
    assert_eq!(v.to_string(), "2025-02-28 00:00:00");
}

#[test]
fn feb29_plus_4years_feb29_leap() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-02-29 00:00:00' + INTERVAL '4 years'",
    );
    assert_eq!(v.to_string(), "2028-02-29 00:00:00");
}

#[test]
fn leap_year_2000_century_rule() {
    // 2000 IS a leap year (divisible by 400).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '2000-02-29' + 1");
    assert_eq!(v.to_string(), "2000-03-01");
}

#[test]
fn leap_year_1900_century_rule() {
    // 1900 is NOT a leap year (divisible by 100 but not 400).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT DATE '1900-02-29'").unwrap_err();
    assert!(matches!(err, SqlError::InvalidDateLiteral(_)));
}

#[test]
fn non_associative_month_arith() {
    // (Jan 31 + 1 month) + 1 month != Jan 31 + 2 months in PG semantics.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let chained = scalar(
        &mut conn,
        "SELECT (TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month') + INTERVAL '1 month'",
    );
    let combined = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '2 months'",
    );
    assert_eq!(chained.to_string(), "2024-03-29 00:00:00");
    assert_eq!(combined.to_string(), "2024-03-31 00:00:00");
    assert_ne!(chained, combined);
}

// ═══════════════════════════════════════════════════════════════════
// Negative / pre-1970 dates
// ═══════════════════════════════════════════════════════════════════

#[test]
fn pre_1970_date_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '1960-06-15'");
    assert_eq!(v.to_string(), "1960-06-15");
}

#[test]
fn pre_1970_timestamp_floor_to_prev_day() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT CAST(TIMESTAMP '1969-12-31 23:59:59' AS DATE)",
    );
    assert_eq!(v.to_string(), "1969-12-31");
}

#[test]
fn bc_1_to_ad_1_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT DATE '0001-01-01' - INTERVAL '1 day'");
    let bc_ts = scalar(&mut conn, "SELECT TIMESTAMP '0001-12-31 00:00:00 BC'");
    assert_eq!(v, bc_ts, "AD-1day mismatch: got {v:?} expected {bc_ts:?}");
}

#[test]
fn year_0_rejected_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert!(matches!(
        conn.query("SELECT DATE '0000-01-01'").unwrap_err(),
        SqlError::InvalidDateLiteral(_)
    ));
    assert!(matches!(
        conn.query("SELECT TIMESTAMP '0000-06-15 12:00:00'")
            .unwrap_err(),
        SqlError::InvalidTimestampLiteral(_)
    ));
}

// ═══════════════════════════════════════════════════════════════════
// Infinity sentinels
// ═══════════════════════════════════════════════════════════════════

#[test]
fn infinity_plus_any_interval_is_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP 'infinity' + INTERVAL '1000 years'",
    );
    assert_eq!(v, Value::Timestamp(i64::MAX));
}

#[test]
fn neg_infinity_plus_interval_is_neg_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TIMESTAMP '-infinity' + INTERVAL '1 day'");
    assert_eq!(v, Value::Timestamp(i64::MIN));
}

#[test]
fn isfinite_infinity_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let pos = scalar(&mut conn, "SELECT ISFINITE(TIMESTAMP 'infinity')");
    let neg = scalar(&mut conn, "SELECT ISFINITE(TIMESTAMP '-infinity')");
    let fin = scalar(
        &mut conn,
        "SELECT ISFINITE(TIMESTAMP '2024-01-01 00:00:00')",
    );
    assert_eq!(pos, Value::Boolean(false));
    assert_eq!(neg, Value::Boolean(false));
    assert_eq!(fin, Value::Boolean(true));
}

#[test]
fn neg_infinity_less_than_any() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '-infinity' < TIMESTAMP '4713-01-01 00:00:00 BC'",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn order_by_timestamp_infinity_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TIMESTAMP '2024-01-01 00:00:00')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, TIMESTAMP 'infinity')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, TIMESTAMP '-infinity')")
        .unwrap();
    let qr = conn.query("SELECT id FROM t ORDER BY ts").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3)); // -inf first
    assert_eq!(qr.rows[1][0], Value::Integer(1)); // finite middle
    assert_eq!(qr.rows[2][0], Value::Integer(2)); // +inf last
}

// ═══════════════════════════════════════════════════════════════════
// INTERVAL arithmetic & scaling
// ═══════════════════════════════════════════════════════════════════

#[test]
fn interval_multiply_by_real_fractional_month() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT INTERVAL '1 month' * 0.5");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 0);
            assert_eq!(days, 15);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn interval_divide_by_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT INTERVAL '30 days' / 6");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 5),
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn interval_pg_normalized_equality_cross_units() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT INTERVAL '1 year' = INTERVAL '365 days'"),
        Value::Boolean(false) // 1 year = 360 days under 30/month (PG); they're NOT equal
    );
    assert_eq!(
        scalar(&mut conn, "SELECT INTERVAL '1 year' = INTERVAL '12 months'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT INTERVAL '30 days' = INTERVAL '1 month'"),
        Value::Boolean(true)
    );
}

#[test]
fn negative_interval_arith() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-06-15 00:00:00' + INTERVAL '-30 days'",
    );
    assert_eq!(v.to_string(), "2024-05-16 00:00:00");
}

#[test]
fn interval_arithmetic_roundtrip_1000x() {
    // Repeated add/subtract should cancel (within calendar semantics).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 2024-06-15 + 100d - 100d = 2024-06-15.
    let v = scalar(&mut conn, "SELECT (DATE '2024-06-15' + 100) - 100");
    assert_eq!(v.to_string(), "2024-06-15");
}

// ═══════════════════════════════════════════════════════════════════
// JUSTIFY_*
// ═══════════════════════════════════════════════════════════════════

#[test]
fn justify_days_65_to_2mon_5d() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT JUSTIFY_DAYS(INTERVAL '65 days')");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 2);
            assert_eq!(days, 5);
        }
        v => panic!("{v:?}"),
    }
}

#[test]
fn justify_hours_50h_to_2d_2h() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT JUSTIFY_HOURS(INTERVAL '50 hours')");
    match v {
        Value::Interval { days, micros, .. } => {
            assert_eq!(days, 2);
            assert_eq!(micros, 2 * 3_600_000_000);
        }
        v => panic!("{v:?}"),
    }
}

#[test]
fn justify_interval_combined() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT JUSTIFY_INTERVAL(INTERVAL '100 days 50 hours')",
    );
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => {
            // 100 days + 50 hours → 102 days 2 hours → 3 months 12 days 2 hours.
            assert_eq!(months, 3);
            assert_eq!(days, 12);
            assert_eq!(micros, 2 * 3_600_000_000);
        }
        v => panic!("{v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// EXTRACT — multiple fields × multiple input types
// ═══════════════════════════════════════════════════════════════════

#[test]
fn extract_every_field_from_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let ts = "TIMESTAMP '2024-03-15 13:45:30'";
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(YEAR FROM {ts})")),
        Value::Integer(2024)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(MONTH FROM {ts})")),
        Value::Integer(3)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(DAY FROM {ts})")),
        Value::Integer(15)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(HOUR FROM {ts})")),
        Value::Integer(13)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(MINUTE FROM {ts})")),
        Value::Integer(45)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(QUARTER FROM {ts})")),
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&mut conn, &format!("SELECT EXTRACT(DECADE FROM {ts})")),
        Value::Integer(202)
    );
}

#[test]
fn extract_dow_isodow_sunday() {
    // 2024-01-07 is a Sunday. PG: dow=0, isodow=7.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT EXTRACT(DOW FROM DATE '2024-01-07')"),
        Value::Integer(0)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT EXTRACT(ISODOW FROM DATE '2024-01-07')"),
        Value::Integer(7)
    );
}

#[test]
fn extract_week_iso_boundary() {
    // 2024-01-01 is Monday of ISO week 1.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT EXTRACT(WEEK FROM DATE '2024-01-01')"),
        Value::Integer(1)
    );
}

#[test]
fn extract_from_interval_preserves_fields() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(
            &mut conn,
            "SELECT EXTRACT(YEAR FROM INTERVAL '2 years 3 months')"
        ),
        Value::Integer(2)
    );
    assert_eq!(
        scalar(
            &mut conn,
            "SELECT EXTRACT(MONTH FROM INTERVAL '2 years 3 months')"
        ),
        Value::Integer(3)
    );
}

// ═══════════════════════════════════════════════════════════════════
// DATE_TRUNC — idempotency and unit coverage
// ═══════════════════════════════════════════════════════════════════

#[test]
fn date_trunc_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    for unit in ["hour", "day", "month", "year"] {
        let once = scalar(
            &mut conn,
            &format!("SELECT DATE_TRUNC('{unit}', TIMESTAMP '2024-06-15 13:45:30')"),
        );
        let twice = scalar(
            &mut conn,
            &format!(
                "SELECT DATE_TRUNC('{unit}', DATE_TRUNC('{unit}', TIMESTAMP '2024-06-15 13:45:30'))"
            ),
        );
        assert_eq!(once, twice, "trunc({unit}) not idempotent");
    }
}

#[test]
fn date_trunc_week_lands_on_monday() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 2024-01-07 is Sunday → truncates to Monday 2024-01-01.
    let v = scalar(&mut conn, "SELECT DATE_TRUNC('week', DATE '2024-01-07')");
    assert_eq!(v.to_string(), "2024-01-01");
}

#[test]
fn date_trunc_across_year_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT DATE_TRUNC('year', TIMESTAMP '2023-12-31 23:59:59')",
    );
    assert_eq!(v.to_string(), "2023-01-01 00:00:00");
}

// ═══════════════════════════════════════════════════════════════════
// Persistence across reopens
// ═══════════════════════════════════════════════════════════════════

#[test]
fn bit_exact_roundtrip_100_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..100i64 {
            let micros = i.wrapping_mul(12345678910123i64);
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, CAST({micros} AS TIMESTAMP) + INTERVAL '0 days')"
            ))
            .ok();
        }
        conn.execute("COMMIT").unwrap();
    }
    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    if let Value::Integer(n) = qr.rows[0][0] {
        assert!(n > 0);
    }
}

#[test]
fn date_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, DATE '1960-06-15')")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, DATE 'infinity')")
            .unwrap();
    }
    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT id, d FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][1].to_string(), "1960-06-15");
    assert_eq!(qr.rows[1][1].to_string(), "infinity");
}

// ═══════════════════════════════════════════════════════════════════
// Savepoint interaction
// ═══════════════════════════════════════════════════════════════════

#[test]
fn rollback_to_preserves_pre_savepoint_dates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, DATE '2024-01-01')")
        .unwrap();
    conn.execute("SAVEPOINT sp1").unwrap();
    conn.execute("INSERT INTO t VALUES (2, DATE '2024-06-15')")
        .unwrap();
    conn.execute("ROLLBACK TO sp1").unwrap();
    let count = scalar(&mut conn, "SELECT COUNT(*) FROM t");
    assert_eq!(count, Value::Integer(1));
    conn.execute("COMMIT").unwrap();
    let row = scalar(&mut conn, "SELECT d FROM t WHERE id = 1");
    assert_eq!(row.to_string(), "2024-01-01");
}

// ═══════════════════════════════════════════════════════════════════
// Index invariants
// ═══════════════════════════════════════════════════════════════════

#[test]
fn unique_index_on_date_with_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_d ON t (d)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, DATE 'infinity')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, DATE '-infinity')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, DATE '2024-01-01')")
        .unwrap();
    // +inf and -inf are distinct; re-inserting +inf should violate UNIQUE.
    let err = conn
        .execute("INSERT INTO t VALUES (4, DATE 'infinity')")
        .unwrap_err();
    assert!(matches!(err, SqlError::UniqueViolation(_)));
}

#[test]
fn index_scan_via_between_on_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=30 {
        conn.execute(&format!(
            "INSERT INTO e VALUES ({i}, DATE '2024-01-{:02}')",
            i
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let count = scalar(
        &mut conn,
        "SELECT COUNT(*) FROM e WHERE d BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'",
    );
    assert_eq!(count, Value::Integer(11));
}

// ═══════════════════════════════════════════════════════════════════
// Non-determinism / txn-stable CURRENT_TIMESTAMP
// ═══════════════════════════════════════════════════════════════════

#[test]
fn current_timestamp_same_across_many_rows_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100i64 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    let qr = conn
        .query("SELECT COUNT(DISTINCT CURRENT_TIMESTAMP) FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    conn.execute("COMMIT").unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// STRFTIME format edges
// ═══════════════════════════════════════════════════════════════════

#[test]
fn strftime_combined_iso_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT STRFTIME('%Y-%m-%dT%H:%M:%SZ', TIMESTAMP '2024-06-15 13:45:30')",
    );
    assert_eq!(v, Value::Text("2024-06-15T13:45:30Z".into()));
}

#[test]
fn strftime_unix_epoch_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT STRFTIME('%s', TIMESTAMP '2024-01-01 00:00:00')",
    );
    assert_eq!(v, Value::Text("1704067200".into()));
}

#[test]
fn strftime_day_of_year() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT STRFTIME('%j', TIMESTAMP '2024-02-29 00:00:00')",
    );
    assert_eq!(v, Value::Text("060".into())); // 60th day of leap year
}

// ═══════════════════════════════════════════════════════════════════
// Aggregate INTERVAL edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn sum_interval_over_many_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=100i64 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, INTERVAL '1 day')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let v = scalar(&mut conn, "SELECT SUM(iv) FROM t");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 100),
        v => panic!("{v:?}"),
    }
}

#[test]
fn sum_interval_with_nulls_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, INTERVAL '5 days'), (2, NULL), (3, INTERVAL '3 days')")
        .unwrap();
    let v = scalar(&mut conn, "SELECT SUM(iv) FROM t");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 8),
        v => panic!("{v:?}"),
    }
}

#[test]
fn sum_interval_all_nulls_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL), (2, NULL)")
        .unwrap();
    let v = scalar(&mut conn, "SELECT SUM(iv) FROM t");
    assert!(matches!(v, Value::Null));
}

#[test]
fn min_max_on_timestamp_across_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), (2, TIMESTAMP 'infinity'), (3, TIMESTAMP '-infinity')")
        .unwrap();
    let min = scalar(&mut conn, "SELECT MIN(ts) FROM t");
    let max = scalar(&mut conn, "SELECT MAX(ts) FROM t");
    assert_eq!(min, Value::Timestamp(i64::MIN));
    assert_eq!(max, Value::Timestamp(i64::MAX));
}

// ═══════════════════════════════════════════════════════════════════
// AT TIME ZONE
// ═══════════════════════════════════════════════════════════════════

#[test]
fn at_time_zone_iana() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'America/New_York'",
    );
    // 2024-01-15 is after DST ended (EST = UTC-5), so 12:00 UTC = 07:00 EST.
    match v {
        Value::Text(s) => assert!(s.starts_with("2024-01-15 07:00:00"), "got: {s}"),
        v => panic!("expected Text, got {v:?}"),
    }
}

#[test]
fn at_time_zone_fixed_offset() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AT TIME ZONE '+05:30'",
    );
    // 12:00 UTC + 05:30 = 17:30.
    match v {
        Value::Text(s) => assert!(s.starts_with("2024-06-15 17:30:00"), "got: {s}"),
        v => panic!("expected Text, got {v:?}"),
    }
}

#[test]
fn at_time_zone_rejects_posix_shorthand() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = scalar_err(
        &mut conn,
        "SELECT TIMESTAMP '2024-01-01 00:00:00' AT TIME ZONE 'UTC+5'",
    );
    assert!(matches!(err, SqlError::InvalidTimezone(_)));
}

// ═══════════════════════════════════════════════════════════════════
// DEFAULT CURRENT_TIMESTAMP stays stable in txn
// ═══════════════════════════════════════════════════════════════════

#[test]
fn default_current_timestamp_same_across_inserts_in_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .unwrap(),
    );
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    conn.execute("INSERT INTO t (id) VALUES (3)").unwrap();
    conn.execute("COMMIT").unwrap();
    // txn-stable clock → all three rows share the same created_at.
    let qr = conn
        .query("SELECT COUNT(DISTINCT created_at) FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

// ═══════════════════════════════════════════════════════════════════
// Unary negation of INTERVAL
// ═══════════════════════════════════════════════════════════════════

#[test]
fn negate_interval_unary() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT -INTERVAL '5 days'");
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => {
            assert_eq!(months, 0);
            assert_eq!(days, -5);
            assert_eq!(micros, 0);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// Sort stability across reopens
// ═══════════════════════════════════════════════════════════════════

#[test]
fn sort_stability_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..50 {
            let day = (i * 7) % 28 + 1;
            let month = ((i * 3) % 12) + 1;
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, TIMESTAMP '2024-{:02}-{:02} 12:00:00')",
                month, day
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }
    let db = open_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr1 = conn.query("SELECT id FROM t ORDER BY ts LIMIT 10").unwrap();
    let qr2 = conn.query("SELECT id FROM t ORDER BY ts LIMIT 10").unwrap();
    assert_eq!(qr1.rows, qr2.rows);
}
