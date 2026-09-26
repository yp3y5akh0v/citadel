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

fn scalar(conn: &Connection<'_>, sql: &str) -> Value {
    let qr = conn.query(sql).unwrap();
    qr.rows[0][0].clone()
}

fn text(conn: &Connection<'_>, sql: &str) -> String {
    match scalar(conn, sql) {
        Value::Text(s) => s.to_string(),
        v => panic!("expected TEXT, got {v:?}"),
    }
}

#[test]
fn create_table_with_all_temporal_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
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
    let conn = Connection::open(&db).unwrap();
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
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DATE, ts TIMESTAMP)")
            .unwrap();
        conn.execute(
            "INSERT INTO t VALUES (1, DATE '2024-06-15', TIMESTAMP '2024-06-15 10:00:00')",
        )
        .unwrap();
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn.query("SELECT d, ts FROM t WHERE id = 1").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Date(_)));
    assert!(matches!(qr.rows[0][1], Value::Timestamp(_)));
}

#[test]
fn date_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-15'");
    assert_eq!(v.to_string(), "2024-01-15");
}

#[test]
fn time_literal_with_subsec() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT TIME '12:30:45.123456'");
    assert_eq!(v.to_string(), "12:30:45.123456");
}

#[test]
fn timestamp_literal_iso() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT TIMESTAMP '2024-01-15T12:30:00Z'");
    assert_eq!(v.to_string(), "2024-01-15 12:30:00");
}

#[test]
fn interval_verbose_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT INTERVAL '1 year 2 months 3 days 04:05:06'");
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
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT INTERVAL 'P1Y2M3D'");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 14);
            assert_eq!(days, 3);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn interval_fractions_spill_and_hours_stay_whole() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for (sql, expected) in [
        ("SELECT INTERVAL '1.5 days'", "1 day 12:00:00"),
        ("SELECT INTERVAL '1.5' DAY", "1 day 12:00:00"),
        (
            "SELECT CAST('1.75 months' AS INTERVAL)",
            "1 mon 22 days 12:00:00",
        ),
        ("SELECT INTERVAL '1.5 years'", "1 year 6 mons"),
        ("SELECT INTERVAL '3 hours' * 100", "300:00:00"),
        ("SELECT CAST(INTERVAL '300 hours' AS TEXT)", "300:00:00"),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    match conn.query("SELECT INTERVAL '153722867281 minutes'") {
        Err(SqlError::InvalidIntervalLiteral(message)) => {
            assert!(message.starts_with("field value out of range"), "{message}")
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn each_interval_field_moves_a_timestamp_with_its_own_sign() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // PostgreSQL 17's answers: the months first (the day kept inside the
    // month), then the days, then the time.
    for (sql, expected) in [
        (
            "SELECT TIMESTAMP '2024-01-31 12:00:00' + INTERVAL '1 month -1 day'",
            "2024-02-28 12:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-01-31 12:00:00' + INTERVAL '-1 month 1 day'",
            "2024-01-01 12:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-01-31 12:00:00' + INTERVAL '1 month -1 hour'",
            "2024-02-29 11:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-01-15 12:00:00' + INTERVAL '2 months -3 days'",
            "2024-03-12 12:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-01-15 12:00:00' - INTERVAL '2 months -3 days'",
            "2023-11-18 12:00:00",
        ),
        (
            "SELECT DATE '2024-01-31' + INTERVAL '1 month -1 day'",
            "2024-02-28 00:00:00",
        ),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    // TIME wraps around the day even when the sum leaves i64.
    let wrapped = (36_000_000_000i128 + i128::from(i64::MAX)) % 86_400_000_000;
    assert_eq!(
        scalar(
            &conn,
            "SELECT TIME '10:00:00' + INTERVAL '9223372036854775807 microseconds'"
        ),
        Value::Time(wrapped as i64)
    );
}

#[test]
fn interval_arithmetic_and_averages_cascade_fractions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE spans (id INTEGER PRIMARY KEY, g INTERVAL)",
        "INSERT INTO spans VALUES (1, '1 day 02:00:00'), (2, '-3 hours'), (3, '1 mon 15 days'), \
         (4, '00:00:00.000001'), (5, NULL)",
        "CREATE TABLE wide (id INTEGER PRIMARY KEY, g INTERVAL)",
        "INSERT INTO wide VALUES (1, '2000000000 days'), (2, '2000000000 days')",
    ] {
        conn.execute(sql).unwrap();
    }
    for transaction in [false, true] {
        if transaction {
            assert_ok(conn.execute("BEGIN").unwrap());
        }
        for (sql, expected) in [
            ("SELECT INTERVAL '10 days' / 4", "2 days 12:00:00"),
            ("SELECT INTERVAL '1 mon 15 days' / 2", "22 days 12:00:00"),
            ("SELECT INTERVAL '1 day 02:00:00' / 2.0", "13:00:00"),
            ("SELECT INTERVAL '1 month' * 1.5", "1 mon 15 days"),
            (
                "SELECT justify_interval(INTERVAL '1 mon -1 hour')",
                "29 days 23:00:00",
            ),
            ("SELECT justify_hours(INTERVAL '1 day -1 hour')", "23:00:00"),
            ("SELECT justify_days(INTERVAL '1 mon -5 days')", "25 days"),
            ("SELECT AVG(g) FROM spans", "11 days 11:45:00"),
            (
                "SELECT AVG(x.g) FROM spans AS x JOIN spans AS y ON y.id = x.id",
                "11 days 11:45:00",
            ),
            ("SELECT SUM(g) FROM spans", "1 mon 16 days -00:59:59.999999"),
        ] {
            assert_eq!(
                scalar(&conn, sql).to_string(),
                expected,
                "{sql} (transaction: {transaction})"
            );
        }
        for sql in [
            "SELECT SUM(g) FROM wide",
            "SELECT SUM(x.g) FROM wide AS x JOIN wide AS y ON y.id = x.id",
            "SELECT INTERVAL '2000000000 days' * 2",
            "SELECT INTERVAL '2000000000 days' + INTERVAL '2000000000 days'",
            "SELECT DATE '2024-01-01' - INTERVAL '-178956970 years -8 months'",
            "SELECT -INTERVAL '-178956970 years -8 months'",
        ] {
            match conn.query(sql) {
                Err(SqlError::InvalidValue(message)) => {
                    assert_eq!(message, "interval out of range", "{sql}")
                }
                other => panic!("{sql} (transaction: {transaction}): {other:?}"),
            }
        }
        assert!(matches!(
            conn.query("SELECT INTERVAL '1 day' / 0"),
            Err(SqlError::DivisionByZero)
        ));
        if transaction {
            assert_ok(conn.execute("COMMIT").unwrap());
        }
    }
}

#[test]
fn date_plus_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-15' + 10");
    assert_eq!(v.to_string(), "2024-01-25");
}

#[test]
fn date_minus_date_returns_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-25' - DATE '2024-01-15'");
    assert_eq!(v, Value::Integer(10));
}

#[test]
fn date_plus_interval_returns_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-15' + INTERVAL '2 hours'");
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
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
        "SELECT TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1 month'",
    );
    assert_eq!(v.to_string(), "2024-02-29 00:00:00");
}

#[test]
fn timestamp_minus_timestamp_returns_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
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
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT INTERVAL '1 day' + INTERVAL '2 days'");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 3),
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn date_plus_real_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT DATE '2024-01-15' + 1.5").unwrap_err();
    assert!(matches!(err, SqlError::TypeMismatch { .. }));
}

#[test]
fn date_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-01' < DATE '2024-02-01'");
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn order_by_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
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
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT INTERVAL '1 month' = INTERVAL '30 days'");
    assert_eq!(v, Value::Boolean(true));

    let v2 = scalar(&conn, "SELECT INTERVAL '24 hours' = INTERVAL '1 day'");
    assert_eq!(v2, Value::Boolean(true));

    let v3 = scalar(&conn, "SELECT INTERVAL '25 hours' > INTERVAL '1 day'");
    assert_eq!(v3, Value::Boolean(true));
}

#[test]
fn intervals_compare_with_text_by_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTERVAL)")
            .unwrap(),
    );
    assert_rows(
        conn.execute(
            "INSERT INTO s VALUES (1, INTERVAL '1 month'), (2, INTERVAL '30 days'), \
             (3, INTERVAL '31 days')",
        )
        .unwrap(),
        3,
    );
    // Text beside an interval reads as an interval and compares by length.
    assert_eq!(
        conn.query(
            "SELECT INTERVAL '1 month' < '31 days', INTERVAL '1 month' = '30 days', \
             '30 days' = INTERVAL '1 month', '31 days' > INTERVAL '1 month', \
             INTERVAL '1 month' <> '720 hours'"
        )
        .unwrap()
        .rows,
        vec![[true, true, true, true, false].map(Value::Boolean).to_vec()]
    );
    for (predicate, expected) in [
        ("v = '30 days'", &[1, 2][..]),
        ("v > '29 days'", &[1, 2, 3]),
        ("v < '31 days'", &[1, 2]),
        ("v IN ('30 days')", &[1, 2]),
        ("v BETWEEN '30 days' AND '30 days'", &[1, 2]),
        ("v NOT IN ('720 hours')", &[3]),
    ] {
        let sql = format!("SELECT id FROM s WHERE {predicate} ORDER BY id");
        assert_eq!(
            conn.query(&sql).unwrap().rows,
            expected
                .iter()
                .map(|&id| vec![Value::Integer(id)])
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
}

#[test]
fn intervals_sort_by_normalized_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE spans (id INTEGER PRIMARY KEY, v INTERVAL)")
            .unwrap(),
    );
    assert_rows(
        conn.execute(
            "INSERT INTO spans VALUES (1, INTERVAL '1 month'), (2, INTERVAL '31 days'), \
             (3, INTERVAL '29 days'), (4, INTERVAL '-1 month 45 days'), \
             (5, INTERVAL '2 days -50 hours'), (6, NULL), (7, INTERVAL '30 days')",
        )
        .unwrap(),
        7,
    );
    const HOUR: i64 = 3_600_000_000;
    let interval = |months, days, micros| Value::Interval {
        months,
        days,
        micros,
    };
    let rows = |sql: &str| conn.query(sql).unwrap().rows;
    let integers = |values: &[i64]| -> Vec<Vec<Value>> {
        values.iter().map(|&v| vec![Value::Integer(v)]).collect()
    };
    // A month counts 30 days: '1 month' and '30 days' tie, so id orders them.
    assert_eq!(
        rows("SELECT id FROM spans ORDER BY v, id"),
        integers(&[6, 5, 4, 3, 1, 7, 2])
    );
    assert_eq!(
        rows("SELECT id FROM spans ORDER BY v DESC, id"),
        integers(&[2, 1, 7, 3, 4, 5, 6])
    );
    assert_eq!(
        rows("SELECT id FROM spans ORDER BY v DESC, id LIMIT 3"),
        integers(&[2, 1, 7])
    );
    // A single sort key, over rows without a tie.
    assert_eq!(
        rows("SELECT id FROM spans WHERE id <> 7 ORDER BY v"),
        integers(&[6, 5, 4, 3, 1, 2])
    );
    assert_eq!(
        rows("SELECT id FROM spans WHERE id <> 7 ORDER BY v DESC"),
        integers(&[2, 1, 3, 4, 5, 6])
    );
    assert_eq!(
        rows("SELECT id FROM spans WHERE id <> 7 ORDER BY v DESC LIMIT 2"),
        integers(&[2, 1])
    );
    assert_eq!(
        rows("SELECT id FROM spans WHERE id <> 7 ORDER BY v COLLATE NOCASE"),
        integers(&[6, 5, 4, 3, 1, 2])
    );
    assert_eq!(
        rows("SELECT MIN(v), MAX(v) FROM spans"),
        vec![vec![interval(0, 2, -50 * HOUR), interval(0, 31, 0)]]
    );
    assert_eq!(
        rows("SELECT id <= 3 AS g, MIN(v), MAX(v) FROM spans GROUP BY id <= 3 ORDER BY g"),
        vec![
            vec![
                Value::Boolean(false),
                interval(0, 2, -50 * HOUR),
                interval(0, 30, 0)
            ],
            vec![Value::Boolean(true), interval(0, 29, 0), interval(0, 31, 0)],
        ]
    );
    assert_eq!(
        rows("SELECT RANK() OVER (ORDER BY v) FROM spans ORDER BY id"),
        integers(&[5, 7, 4, 3, 2, 1, 5])
    );
    assert_eq!(
        rows("SELECT COUNT(*) OVER (PARTITION BY v) FROM spans ORDER BY id"),
        integers(&[2, 1, 1, 1, 1, 1, 2])
    );
    assert_eq!(
        rows("SELECT MAX(v) OVER () FROM spans ORDER BY id LIMIT 1"),
        vec![vec![interval(0, 31, 0)]]
    );
    assert_eq!(
        rows(
            "SELECT MAX(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
             MIN(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM spans ORDER BY id"
        ),
        vec![
            vec![interval(1, 0, 0), interval(1, 0, 0)],
            vec![interval(0, 31, 0), interval(1, 0, 0)],
            vec![interval(0, 31, 0), interval(0, 29, 0)],
            vec![interval(0, 29, 0), interval(-1, 45, 0)],
            vec![interval(-1, 45, 0), interval(0, 2, -50 * HOUR)],
            vec![interval(0, 2, -50 * HOUR), interval(0, 2, -50 * HOUR)],
            vec![interval(0, 30, 0), interval(0, 30, 0)],
        ]
    );
}

#[test]
fn interval_arrays_compare_and_group_by_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE s (id INTEGER PRIMARY KEY, v INTERVAL)",
        "INSERT INTO s VALUES (1, INTERVAL '1 month'), (2, INTERVAL '30 days'), \
         (3, INTERVAL '31 days'), (4, INTERVAL '29 days')",
        "CREATE MATERIALIZED VIEW mv AS SELECT id, ARRAY[v] AS k FROM s",
    ] {
        conn.execute(sql).unwrap();
    }
    let rows = |sql: &str| conn.query(sql).unwrap().rows;
    let integers = |values: &[i64]| -> Vec<Vec<Value>> {
        values.iter().map(|&v| vec![Value::Integer(v)]).collect()
    };
    // Array elements compare as intervals do: ids 1 and 2 hold one length.
    assert_eq!(
        rows(
            "SELECT ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days'], \
             ARRAY[INTERVAL '1 month'] < ARRAY[INTERVAL '31 days'], \
             ARRAY[INTERVAL '1 month', INTERVAL '1 day'] > ARRAY[INTERVAL '30 days']"
        ),
        vec![[true, true, true].map(Value::Boolean).to_vec()]
    );
    for sql in [
        "SELECT COUNT(DISTINCT ARRAY[v]) FROM s",
        "SELECT COUNT(*) FROM (SELECT DISTINCT ARRAY[v] AS x FROM s) d",
        "SELECT COUNT(*) FROM (SELECT ARRAY[v] AS x FROM s GROUP BY ARRAY[v]) d",
        "SELECT COUNT(*) FROM (SELECT ARRAY[v] FROM s UNION SELECT ARRAY[v] FROM s) u",
    ] {
        assert_eq!(rows(sql), integers(&[3]), "{sql}");
    }
    assert_eq!(
        rows("SELECT id FROM s ORDER BY ARRAY[v], id"),
        integers(&[4, 1, 2, 3])
    );
    assert_eq!(
        rows(
            "WITH a AS (SELECT id, ARRAY[v] AS k FROM s) \
             SELECT a.id, b.id FROM a JOIN a AS b ON a.k = b.k ORDER BY 1, 2"
        ),
        [(1, 1), (1, 2), (2, 1), (2, 2), (3, 3), (4, 4)]
            .map(|(a, b)| vec![Value::Integer(a), Value::Integer(b)])
            .to_vec()
    );
    assert_eq!(
        rows(
            "SELECT id FROM s WHERE ARRAY[v] IN (SELECT ARRAY[v] FROM s WHERE id = 2) ORDER BY id"
        ),
        integers(&[1, 2])
    );
    // A stored array column: the top-k sort and the scan filter read it raw.
    assert_eq!(
        rows("SELECT id FROM mv ORDER BY k DESC LIMIT 1"),
        integers(&[3])
    );
    assert_eq!(
        rows("SELECT id FROM mv WHERE k = ARRAY[INTERVAL '30 days'] ORDER BY id"),
        integers(&[1, 2])
    );
}

#[test]
fn interval_array_keys_match_by_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE s (id INTEGER PRIMARY KEY, v INTERVAL)",
        "INSERT INTO s VALUES (1, INTERVAL '1 month'), (2, INTERVAL '30 days'), \
         (3, INTERVAL '31 days')",
        "CREATE MATERIALIZED VIEW by_key AS SELECT ARRAY[v] AS k, id FROM s",
        "CREATE MATERIALIZED VIEW by_id AS SELECT id, ARRAY[v] AS k FROM s",
        "CREATE INDEX by_id_k ON by_id (k)",
    ] {
        conn.execute(sql).unwrap();
    }
    let interval = |months, days| Value::Interval {
        months,
        days,
        micros: 0,
    };
    let ids = |sql: &str, element: Value| -> Vec<Value> {
        let bound = Value::Array(std::sync::Arc::new(vec![element]));
        conn.query_params(sql, &[bound])
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    // Keys hold the fields: a key range from '1 month' skips '31 days', one up to
    // '31 days' skips '1 month'.
    for view in ["by_key", "by_id"] {
        let query = |op: &str| format!("SELECT id FROM {view} WHERE k {op} $1 ORDER BY id");
        assert_eq!(
            ids(&query("="), interval(0, 30)),
            [Value::Integer(1), Value::Integer(2)]
        );
        assert_eq!(ids(&query(">"), interval(1, 0)), [Value::Integer(3)]);
        assert_eq!(
            ids(&query("<"), interval(0, 31)),
            [Value::Integer(1), Value::Integer(2)]
        );
    }
}

#[test]
fn intervals_group_by_normalized_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE g (id INTEGER PRIMARY KEY, v INTERVAL)")
            .unwrap(),
    );
    assert_rows(
        conn.execute(
            "INSERT INTO g VALUES (1, INTERVAL '1 month'), (2, INTERVAL '30 days'), \
             (3, INTERVAL '720 hours'), (4, INTERVAL '31 days'), (5, NULL), \
             (6, INTERVAL '2147483647 months 30 days'), \
             (7, INTERVAL '2147483646 months 60 days'), \
             (8, INTERVAL '-2147483648 months -30 days'), \
             (9, INTERVAL '-2147483647 months -60 days')",
        )
        .unwrap(),
        9,
    );
    // A month counts 30 days: ids 1 to 3 share one length, as do 6 and 7, and 8 and 9.
    for (sql, expected) in [
        ("SELECT COUNT(DISTINCT v) FROM g", 4),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT v FROM g) d", 5),
        (
            "SELECT COUNT(*) FROM (SELECT DISTINCT v FROM g WHERE id > 0) d",
            5,
        ),
        ("SELECT COUNT(*) FROM (SELECT v FROM g GROUP BY v) d", 5),
        (
            "SELECT COUNT(*) FROM (SELECT v FROM g UNION SELECT v FROM g) u",
            5,
        ),
        (
            "SELECT COUNT(*) FROM (SELECT v FROM g WHERE id IN (1, 4) \
             INTERSECT SELECT v FROM g WHERE id IN (2, 6)) x",
            1,
        ),
        (
            "SELECT COUNT(*) FROM (SELECT v FROM g WHERE id IN (2, 4) \
             EXCEPT SELECT v FROM g WHERE id = 3) x",
            1,
        ),
        (
            "SELECT COUNT(*) FROM (SELECT v FROM g WHERE id IN (6, 8) \
             EXCEPT SELECT v FROM g WHERE id IN (7, 9)) x",
            0,
        ),
    ] {
        assert_eq!(scalar(&conn, sql), Value::Integer(expected), "{sql}");
    }
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM g GROUP BY v ORDER BY 1")
            .unwrap()
            .rows,
        [1, 1, 2, 2, 3].map(|n| vec![Value::Integer(n)])
    );
}

#[test]
fn interval_keys_sort_by_length_under_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE spans (v INTERVAL PRIMARY KEY, k INTEGER)",
        "CREATE INDEX spans_k_v ON spans (k, v)",
        "INSERT INTO spans VALUES (INTERVAL '31 days', 1), (INTERVAL '1 month', 1), \
         (INTERVAL '29 days', 1), (INTERVAL '2 months', 1)",
        "CREATE MATERIALIZED VIEW span_lists AS SELECT ARRAY[v] AS vs, k FROM spans",
    ] {
        conn.execute(sql).unwrap();
    }
    let days = |days| Value::Interval {
        months: 0,
        days,
        micros: 0,
    };
    let month = Value::Interval {
        months: 1,
        days: 0,
        micros: 0,
    };
    let array = |element: Value| vec![Value::Array(std::sync::Arc::new(vec![element]))];
    let rows = |sql: &str| conn.query(sql).unwrap().rows;
    // Keys hold months before days, so key order puts '31 days' ahead of '1 month'.
    let first_two = vec![vec![days(29)], vec![month.clone()]];
    assert_eq!(rows("SELECT v FROM spans ORDER BY v LIMIT 2"), first_two);
    assert_eq!(
        rows("SELECT v FROM spans WHERE k = 1 ORDER BY v LIMIT 2"),
        first_two
    );
    assert_eq!(
        rows("SELECT v FROM spans ORDER BY v LIMIT 2 OFFSET 1"),
        vec![vec![month.clone()], vec![days(31)]]
    );
    assert_eq!(
        rows("SELECT vs FROM span_lists ORDER BY vs LIMIT 2"),
        vec![array(days(29)), array(month.clone())]
    );
    assert_eq!(
        rows("SELECT vs FROM span_lists ORDER BY vs LIMIT 2 OFFSET 1"),
        vec![array(month), array(days(31))]
    );
}

#[test]
fn now_returns_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT NOW()");
    assert!(matches!(v, Value::Timestamp(_)));
}

#[test]
fn current_date_returns_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT CURRENT_DATE");
    assert!(matches!(v, Value::Date(_)));
}

#[test]
fn extract_year_from_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT EXTRACT(YEAR FROM DATE '2024-06-15')");
    assert_eq!(v, Value::Integer(2024));
}

#[test]
fn extract_hour_from_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
        "SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 13:45:00')",
    );
    assert_eq!(v, Value::Integer(13));
}

#[test]
fn extract_epoch_from_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // 2024-01-01 = 19723 days * 86400 = 1704067200.
    let v = scalar(&conn, "SELECT EXTRACT(EPOCH FROM DATE '2024-01-01')");
    assert_eq!(v, Value::Integer(1704067200));
}

#[test]
fn date_trunc_month() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
        "SELECT DATE_TRUNC('month', TIMESTAMP '2024-03-15 12:30:45')",
    );
    assert_eq!(v.to_string(), "2024-03-01 00:00:00");
}

#[test]
fn date_trunc_week_monday() {
    // 2024-01-07 is a Sunday; trunc('week') returns previous Monday = 2024-01-01.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE_TRUNC('week', DATE '2024-01-07')");
    assert_eq!(v.to_string(), "2024-01-01");
}

#[test]
fn make_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT MAKE_DATE(2024, 6, 15)");
    assert_eq!(v.to_string(), "2024-06-15");
}

#[test]
fn make_interval() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT MAKE_INTERVAL(1, 2, 0, 3)");
    match v {
        Value::Interval { months, days, .. } => {
            assert_eq!(months, 14); // 1 year + 2 months
            assert_eq!(days, 3);
        }
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn justify_days_normalizes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT JUSTIFY_DAYS(INTERVAL '65 days')");
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
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
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

#[test]
fn strftime_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let s = text(
        &conn,
        "SELECT STRFTIME('%Y-%m', TIMESTAMP '2024-03-15 12:00:00')",
    );
    assert_eq!(s, "2024-03");
}

#[test]
fn unixepoch_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT UNIXEPOCH(TIMESTAMP '2024-01-01 00:00:00')");
    assert_eq!(v, Value::Integer(1704067200));
}

#[test]
fn julianday_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // 2000-01-01 12:00:00 UTC == Julian day 2451545.0 exactly.
    let v = scalar(&conn, "SELECT JULIANDAY(TIMESTAMP '2000-01-01 12:00:00')");
    if let Value::Real(j) = v {
        assert!((j - 2451545.0).abs() < 1e-6);
    } else {
        panic!("expected Real, got {v:?}");
    }
}

#[test]
fn index_on_date_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
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
    let conn = Connection::open(&db).unwrap();
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

#[test]
fn infinity_timestamp_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT TIMESTAMP 'infinity'");
    assert_eq!(v, Value::Timestamp(i64::MAX));
    let v2 = scalar(&conn, "SELECT TIMESTAMP '-infinity'");
    assert_eq!(v2, Value::Timestamp(i64::MIN));
}

#[test]
fn isfinite_on_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let a = scalar(&conn, "SELECT ISFINITE(TIMESTAMP 'infinity')");
    assert_eq!(a, Value::Boolean(false));
    let b = scalar(&conn, "SELECT ISFINITE(TIMESTAMP '2024-01-01 00:00:00')");
    assert_eq!(b, Value::Boolean(true));
}

#[test]
fn infinity_compares_greater() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
        "SELECT TIMESTAMP 'infinity' > TIMESTAMP '5000-01-01 00:00:00'",
    );
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn cast_text_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT CAST('2024-06-15' AS DATE)");
    assert_eq!(v.to_string(), "2024-06-15");
}

#[test]
fn cast_integer_to_timestamp_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT CAST(1704067200 AS TIMESTAMP)");
    assert_eq!(v.to_string(), "2024-01-01 00:00:00");
}

#[test]
fn cast_timestamp_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(
        &conn,
        "SELECT CAST(TIMESTAMP '2024-06-15 14:00:00' AS DATE)",
    );
    assert_eq!(v.to_string(), "2024-06-15");
}

#[test]
fn null_arithmetic_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '2024-01-01' + NULL");
    assert!(matches!(v, Value::Null));
}

#[test]
fn extract_null_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT EXTRACT(YEAR FROM NULL)");
    assert!(matches!(v, Value::Null));
}

#[test]
fn bc_date_parses_and_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '0001-01-01 BC'");
    assert_eq!(v.to_string(), "0001-01-01 BC");
}

#[test]
fn bc_date_before_ad() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let v = scalar(&conn, "SELECT DATE '0001-01-01 BC' < DATE '0001-01-01'");
    assert_eq!(v, Value::Boolean(true));
}

#[test]
fn year_0_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.query("SELECT DATE '0000-01-01'").unwrap_err();
    assert!(matches!(err, SqlError::InvalidDateLiteral(_)));
}

fn assert_invalid(conn: &Connection<'_>, sql: &str, message: &str) {
    match conn.query(sql) {
        Err(SqlError::InvalidValue(got)) => assert_eq!(got, message, "{sql}"),
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn infinite_dates_and_timestamps_follow_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE spans (id INTEGER PRIMARY KEY, d DATE, ts TIMESTAMP)",
        "CREATE TABLE strict_spans (id INTEGER PRIMARY KEY, d DATE, ts TIMESTAMP) STRICT",
        "INSERT INTO spans VALUES (1, TIMESTAMP 'infinity', DATE '-infinity')",
        "INSERT INTO strict_spans VALUES (1, TIMESTAMP '-infinity', DATE 'infinity')",
    ] {
        conn.execute(sql).unwrap();
    }
    // PostgreSQL 17's answers. Conversions keep the sign of infinity.
    for (sql, expected) in [
        (
            "SELECT CAST(TIMESTAMP 'infinity' AS DATE)",
            Value::Date(i32::MAX),
        ),
        (
            "SELECT CAST(TIMESTAMP '-infinity' AS DATE)",
            Value::Date(i32::MIN),
        ),
        (
            "SELECT CAST(DATE 'infinity' AS TIMESTAMP)",
            Value::Timestamp(i64::MAX),
        ),
        ("SELECT d FROM spans", Value::Date(i32::MAX)),
        ("SELECT ts FROM spans", Value::Timestamp(i64::MIN)),
        ("SELECT d FROM strict_spans", Value::Date(i32::MIN)),
        ("SELECT ts FROM strict_spans", Value::Timestamp(i64::MAX)),
        (
            "SELECT DATE 'infinity' = TIMESTAMP 'infinity'",
            Value::Boolean(true),
        ),
        ("SELECT DATE 'infinity' - 1", Value::Date(i32::MAX)),
        // Fields that keep growing are ±Infinity; fields that cycle are NULL.
        (
            "SELECT extract(year FROM DATE 'infinity')",
            Value::Real(f64::INFINITY),
        ),
        (
            "SELECT extract(decade FROM DATE '-infinity')",
            Value::Real(f64::NEG_INFINITY),
        ),
        (
            "SELECT extract(julian FROM d) FROM spans",
            Value::Real(f64::INFINITY),
        ),
        (
            "SELECT extract(epoch FROM ts) FROM spans",
            Value::Real(f64::NEG_INFINITY),
        ),
        (
            "SELECT date_part('year', TIMESTAMP 'infinity')",
            Value::Real(f64::INFINITY),
        ),
        ("SELECT extract(month FROM DATE 'infinity')", Value::Null),
        ("SELECT extract(dow FROM d) FROM spans", Value::Null),
        (
            "SELECT date_part('hour', TIMESTAMP 'infinity')",
            Value::Null,
        ),
        ("SELECT extract(second FROM ts) FROM spans", Value::Null),
        // As PostgreSQL's cast to TIME, which has no infinity.
        ("SELECT time(TIMESTAMP 'infinity')", Value::Null),
    ] {
        assert_eq!(scalar(&conn, sql), expected, "{sql}");
    }
    for sql in [
        "SELECT DATE 'infinity' - DATE '2024-01-01'",
        "SELECT DATE '-infinity' - DATE 'infinity'",
    ] {
        assert_invalid(&conn, sql, "cannot subtract infinite dates");
    }
    for sql in [
        "SELECT TIMESTAMP 'infinity' - TIMESTAMP '2024-01-01 00:00:00'",
        "SELECT age(TIMESTAMP 'infinity', TIMESTAMP '2024-01-01 00:00:00')",
        "SELECT timediff(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '-infinity')",
    ] {
        assert_invalid(&conn, sql, "cannot subtract infinite timestamps");
    }
    assert_invalid(
        &conn,
        "SELECT julianday(TIMESTAMP 'infinity')",
        "JULIANDAY is not defined for infinite timestamps",
    );
    assert_invalid(
        &conn,
        "SELECT unixepoch(DATE '-infinity')",
        "UNIXEPOCH is not defined for infinite timestamps",
    );
    // Infinity has no count of days or seconds, and the day counts that stand
    // for it are not days.
    conn.execute("CREATE TABLE counts (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    for sql in [
        "INSERT INTO counts VALUES (1, DATE 'infinity')",
        "INSERT INTO counts VALUES (2, TIMESTAMP '-infinity')",
        "INSERT INTO spans VALUES (2, 2147483647, NULL)",
    ] {
        assert!(
            matches!(conn.execute(sql), Err(SqlError::TypeMismatch { .. })),
            "{sql}"
        );
    }
    for sql in [
        "SELECT CAST(2147483647 AS DATE)",
        "SELECT CAST(-2147483648 AS DATE)",
    ] {
        assert_invalid(&conn, sql, "cannot cast INTEGER to DATE");
    }
}

#[test]
fn dates_past_year_9999_compute_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE far (id INTEGER PRIMARY KEY, d DATE)")
        .unwrap();
    conn.execute("INSERT INTO far VALUES (1, 5000000)").unwrap();
    // PostgreSQL 17's answers.
    for (sql, expected) in [
        ("SELECT DATE '9999-12-31' + 1", "10000-01-01"),
        ("SELECT d FROM far", "15659-07-15"),
        ("SELECT make_date(12345, 6, 7)", "12345-06-07"),
        (
            "SELECT CAST(make_date(12345, 6, 7) AS TIMESTAMP)",
            "12345-06-07 00:00:00",
        ),
        (
            "SELECT date_trunc('month', make_date(12345, 6, 7))",
            "12345-06-01",
        ),
        (
            "SELECT date_trunc('decade', make_date(12345, 6, 7))",
            "12340-01-01",
        ),
        (
            "SELECT date_trunc('week', make_date(10000, 1, 1))",
            "9999-12-27",
        ),
        (
            "SELECT CAST(300000000000 AS TIMESTAMP)",
            "11476-08-15 05:20:00",
        ),
        ("SELECT make_date(5874897, 12, 31)", "5874897-12-31"),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    for (sql, expected) in [
        (
            "SELECT make_date(12345, 6, 7) - DATE '2000-01-01'",
            3_778_591,
        ),
        (
            "SELECT extract(epoch FROM make_date(12345, 6, 7))",
            327_416_947_200,
        ),
        ("SELECT extract(dow FROM make_date(10000, 1, 1))", 6),
        ("SELECT extract(isodow FROM make_date(10000, 1, 1))", 6),
        ("SELECT extract(doy FROM make_date(10000, 3, 1))", 61),
        ("SELECT extract(week FROM make_date(10000, 1, 1))", 52),
        ("SELECT extract(isoyear FROM make_date(10000, 1, 1))", 9999),
        ("SELECT extract(year FROM d) FROM far", 15_659),
    ] {
        assert_eq!(scalar(&conn, sql), Value::Integer(expected), "{sql}");
    }
    // The last finite day comes before the day count that stands for infinity.
    assert_invalid(
        &conn,
        "SELECT DATE '2024-01-01' + 2147463924",
        "date out of range",
    );
    assert_invalid(
        &conn,
        "SELECT make_date(5874897, 12, 31) + INTERVAL '1 day'",
        "date out of range for timestamp",
    );
    for sql in [
        "SELECT datetime(9223372036855)",
        "SELECT date(-9223372036855)",
    ] {
        assert_invalid(&conn, sql, "timestamp out of range");
    }
}

#[test]
fn timestamps_parse_through_9999_and_round_fractions_as_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE ends (id INTEGER PRIMARY KEY, valid_to TIMESTAMP)")
        .unwrap();
    conn.execute("INSERT INTO ends VALUES (1, '9999-12-31 23:59:59')")
        .unwrap();
    // PostgreSQL 17's answers: fractions past the sixth digit round half to
    // even, and a carry reaches the next second, day or 24:00:00.
    for (sql, expected) in [
        ("SELECT valid_to FROM ends", "9999-12-31 23:59:59"),
        (
            "SELECT TIMESTAMP '9999-12-31 23:59:59.999999'",
            "9999-12-31 23:59:59.999999",
        ),
        (
            "SELECT CAST('9999-12-30 22:00:01' AS TIMESTAMP)",
            "9999-12-30 22:00:01",
        ),
        (
            "SELECT TIMESTAMP '2024-01-01 12:00:00.1234567'",
            "2024-01-01 12:00:00.123457",
        ),
        (
            "SELECT TIMESTAMP '2024-01-01 12:00:00.1234565'",
            "2024-01-01 12:00:00.123456",
        ),
        (
            "SELECT TIMESTAMP '1969-12-31 23:59:59.9999995'",
            "1970-01-01 00:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-01-01 12:00:00.1234567+02:00'",
            "2024-01-01 10:00:00.123457",
        ),
        ("SELECT TIME '23:59:59.9999995'", "24:00:00"),
        ("SELECT TIME '10:00:00.1234565'", "10:00:00.123456"),
        ("SELECT TIME '10:00:00.1234575'", "10:00:00.123458"),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
}

#[test]
fn timestamp_arithmetic_spans_the_whole_calendar() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // PostgreSQL 17's answers: months first (the day kept inside the month),
    // then days, then time, on either side of year 9999.
    for (sql, expected) in [
        (
            "SELECT TIMESTAMP '9999-12-31 00:00:00' + INTERVAL '1 day'",
            "10000-01-01 00:00:00",
        ),
        (
            "SELECT TIMESTAMP '9999-12-31 00:00:00' + INTERVAL '1 month'",
            "10000-01-31 00:00:00",
        ),
        (
            "SELECT DATE '9999-12-31' + INTERVAL '1 hour'",
            "9999-12-31 01:00:00",
        ),
        (
            "SELECT TIMESTAMP '9999-12-31 23:59:59.999999' - TIMESTAMP '2000-01-01 00:00:00'",
            "2921939 days 23:59:59.999999",
        ),
        (
            "SELECT TIMESTAMP '0044-03-15 12:00:00 BC' - INTERVAL '1 month'",
            "0044-02-15 12:00:00 BC",
        ),
        (
            "SELECT TIMESTAMP '0001-03-01 00:00:00' - INTERVAL '1 day 1 month'",
            "0001-01-31 00:00:00",
        ),
        (
            "SELECT TIMESTAMP '2024-03-31 10:00:00' - INTERVAL '1 month 1 day'",
            "2024-02-28 10:00:00",
        ),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    assert_eq!(
        scalar(
            &conn,
            "SELECT extract(year FROM TIMESTAMP '9999-12-31 23:59:59.999999' \
             + INTERVAL '1 microsecond')"
        ),
        Value::Integer(10_000)
    );
    for sql in [
        "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '178956970 years'",
        "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '2147483647 days' \
         + INTERVAL '2147483647 days'",
        "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '9223372036854775807 microseconds'",
    ] {
        assert_invalid(&conn, sql, "timestamp out of range");
    }
}

#[test]
fn bc_dates_follow_postgresql_calendar_fields() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // PostgreSQL 17's answers. There is no year 0, and BC decades, centuries
    // and ISO years count away from it.
    for (sql, expected) in [
        ("SELECT extract(year FROM DATE '0001-06-01 BC')", -1),
        ("SELECT extract(year FROM DATE '0044-03-15 BC')", -44),
        ("SELECT extract(decade FROM DATE '0001-06-01 BC')", 0),
        ("SELECT extract(decade FROM DATE '0006-06-01 BC')", -1),
        ("SELECT extract(decade FROM DATE '0011-06-01 BC')", -1),
        ("SELECT extract(decade FROM DATE '0016-06-01 BC')", -2),
        ("SELECT extract(century FROM DATE '0101-01-01 BC')", -2),
        ("SELECT extract(millennium FROM DATE '1001-01-01 BC')", -2),
        ("SELECT extract(isoyear FROM DATE '0001-01-01 BC')", -2),
        ("SELECT extract(week FROM DATE '0001-01-01 BC')", 52),
        ("SELECT extract(isoyear FROM DATE '0044-03-15 BC')", -44),
        ("SELECT extract(week FROM DATE '0044-03-15 BC')", 11),
        ("SELECT extract(dow FROM DATE '0044-03-15 BC')", 5),
        ("SELECT extract(doy FROM DATE '0044-03-15 BC')", 74),
        ("SELECT extract(julian FROM DATE '4714-11-24 BC')", 0),
        (
            "SELECT extract(epoch FROM TIMESTAMP '0044-03-15 12:00:00 BC')",
            -63_517_780_800,
        ),
    ] {
        assert_eq!(scalar(&conn, sql), Value::Integer(expected), "{sql}");
    }
    for (sql, expected) in [
        (
            "SELECT date_trunc('decade', DATE '0001-06-01 BC')",
            "0001-01-01 BC",
        ),
        (
            "SELECT date_trunc('decade', DATE '0006-06-01 BC')",
            "0011-01-01 BC",
        ),
        (
            "SELECT date_trunc('decade', DATE '0016-06-01 BC')",
            "0021-01-01 BC",
        ),
        (
            "SELECT date_trunc('decade', DATE '0005-06-01')",
            "0001-01-01 BC",
        ),
        (
            "SELECT date_trunc('decade', TIMESTAMP '0006-06-01 10:00:00 BC')",
            "0011-01-01 00:00:00 BC",
        ),
        (
            "SELECT date_trunc('century', TIMESTAMP '0101-06-01 10:00:00 BC')",
            "0200-01-01 00:00:00 BC",
        ),
        // The era follows the time, so the text reads back as the same timestamp.
        (
            "SELECT CAST(TIMESTAMP '0044-03-15 12:00:00 BC' AS TEXT)",
            "0044-03-15 12:00:00 BC",
        ),
        (
            "SELECT CAST(CAST(TIMESTAMP '0044-03-15 12:00:00 BC' AS TEXT) AS TIMESTAMP) \
             = TIMESTAMP '0044-03-15 12:00:00 BC'",
            "TRUE",
        ),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    assert_eq!(
        scalar(
            &conn,
            "SELECT extract(julian FROM TIMESTAMP '4714-11-24 06:00:00 BC')"
        ),
        Value::Real(0.25)
    );
}

#[test]
fn extract_folds_field_names_and_keeps_fractions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // PostgreSQL 17's answers.
    for (sql, expected) in [
        (
            "SELECT date_part('WEEK', DATE '2024-12-30')",
            Value::Integer(1),
        ),
        (
            "SELECT date_part('ISOYEAR', DATE '2024-12-30')",
            Value::Integer(2025),
        ),
        (
            "SELECT date_part('HOUR', TIME '10:30:00')",
            Value::Integer(10),
        ),
        (
            "SELECT date_part('Day', INTERVAL '3 days')",
            Value::Integer(3),
        ),
        (
            "SELECT extract(epoch FROM TIME '10:00:00.5')",
            Value::Real(36_000.5),
        ),
        (
            "SELECT extract(epoch FROM TIMESTAMP '1969-12-31 23:59:59.5')",
            Value::Real(-0.5),
        ),
        (
            "SELECT extract(epoch FROM TIMESTAMP '2024-06-01 00:00:00.25')",
            Value::Real(1_717_200_000.25),
        ),
        // A year of months is 365.25 days, a leftover month 30.
        (
            "SELECT extract(epoch FROM INTERVAL '-13 months')",
            Value::Real(-34_149_600.0),
        ),
        (
            "SELECT extract(epoch FROM INTERVAL '25 months 3 days')",
            Value::Real(65_966_400.0),
        ),
        (
            "SELECT extract(julian FROM TIMESTAMP '2024-01-01 12:00:00')",
            Value::Real(2_460_311.5),
        ),
        (
            "SELECT extract(julian FROM TIMESTAMP '2024-01-01 00:00:00')",
            Value::Integer(2_460_311),
        ),
    ] {
        assert_eq!(scalar(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn age_borrows_the_length_of_the_earlier_month() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let interval = |months, days, micros| Value::Interval {
        months,
        days,
        micros,
    };
    // PostgreSQL 17's answers.
    for (a, b, expected) in [
        ("2024-02-29", "2023-02-28", interval(12, 1, 0)),
        ("2024-03-31", "2024-02-29", interval(1, 2, 0)),
        ("2024-03-30", "2024-01-31", interval(1, 30, 0)),
        ("2024-05-31", "2024-02-29", interval(3, 2, 0)),
        ("2025-03-01", "2024-02-29", interval(12, 1, 0)),
        ("2024-02-29", "2025-03-01", interval(-12, -1, 0)),
        ("2024-03-01", "2024-01-31", interval(1, 1, 0)),
        ("2024-01-31", "2024-03-01", interval(-1, -1, 0)),
        (
            "2024-01-01 00:00:00",
            "2023-12-31 23:59:59",
            interval(0, 0, 1_000_000),
        ),
        (
            "2023-12-31 23:59:59",
            "2024-01-01 00:00:00",
            interval(0, 0, -1_000_000),
        ),
        (
            "2024-03-31 10:00:00",
            "2023-01-31 12:00:00",
            interval(13, 30, 22 * 3_600_000_000),
        ),
        (
            "2000-02-29 00:00:00",
            "1999-12-31 23:59:59.999999",
            interval(1, 28, 1),
        ),
        (
            "1999-12-31 23:59:59.999999",
            "2000-02-29 00:00:00",
            interval(-1, -28, -1),
        ),
        (
            "0001-01-01 00:00:00",
            "0001-12-31 00:00:00 BC",
            interval(0, 1, 0),
        ),
    ] {
        let sql = format!("SELECT age(TIMESTAMP '{a}', TIMESTAMP '{b}')");
        assert_eq!(scalar(&conn, &sql), expected, "{sql}");
    }
    assert_eq!(
        scalar(&conn, "SELECT age(DATE '2024-03-31', DATE '2023-12-31')"),
        interval(3, 0, 0)
    );
}

#[test]
fn make_functions_check_fields_as_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // PostgreSQL 17's answers. A negative year is BC.
    for (sql, expected) in [
        ("SELECT make_date(-44, 3, 15)", "0044-03-15 BC"),
        ("SELECT make_date(-1, 12, 31)", "0001-12-31 BC"),
        (
            "SELECT make_timestamp(-44, 3, 15, 12, 0, 0)",
            "0044-03-15 12:00:00 BC",
        ),
        (
            "SELECT make_timestamp(2024, 2, 29, 23, 59, 59.999999)",
            "2024-02-29 23:59:59.999999",
        ),
        ("SELECT make_time(24, 0, 0)", "24:00:00"),
        ("SELECT make_time(23, 59, 60)", "24:00:00"),
    ] {
        assert_eq!(scalar(&conn, sql).to_string(), expected, "{sql}");
    }
    let interval = |months, days, micros| Value::Interval {
        months,
        days,
        micros,
    };
    for (sql, expected) in [
        (
            "SELECT make_time(10, 30, 15.5)",
            Value::Time(37_815_500_000),
        ),
        // Seconds round to the microsecond before the range check.
        (
            "SELECT make_time(10, 30, 59.9999999)",
            Value::Time(37_860_000_000),
        ),
        (
            "SELECT make_time(23, 59, 60.0000001)",
            Value::Time(86_400_000_000),
        ),
        (
            "SELECT make_time(10, 30, 0.0000025)",
            Value::Time(37_800_000_002),
        ),
        (
            "SELECT make_interval(1, 2, 3, 4, 5, 6, 7.5)",
            interval(14, 25, 18_367_500_000),
        ),
        // Seconds round to the microsecond, an exact half to even.
        (
            "SELECT make_interval(0, 0, 0, 0, 0, 0, 0.0000025)",
            interval(0, 0, 2),
        ),
        (
            "SELECT make_interval(0, 0, 0, 0, 0, 0, 0.0000125)",
            interval(0, 0, 12),
        ),
        (
            "SELECT make_interval(0, 0, 0, 0, 0, 0, -0.0000025)",
            interval(0, 0, -2),
        ),
        (
            "SELECT make_interval(0, 0, 0, 0, 0, 0, 1.0000015)",
            interval(0, 0, 1_000_002),
        ),
        (
            "SELECT make_interval(0, 2147483647)",
            interval(i32::MAX, 0, 0),
        ),
        (
            "SELECT make_interval(0, 0, 306783378, 1)",
            interval(0, i32::MAX, 0),
        ),
    ] {
        assert_eq!(scalar(&conn, sql), expected, "{sql}");
    }
    for sql in [
        "SELECT make_date(0, 1, 1)",
        "SELECT make_date(2024, 13, 1)",
        "SELECT make_date(2024, 2, 30)",
        "SELECT make_date(2024, 258, 1)",
        "SELECT make_timestamp(0, 1, 1, 0, 0, 0)",
    ] {
        match conn.query(sql) {
            Err(SqlError::InvalidDateLiteral(message)) => {
                assert!(
                    message.contains("date field value out of range"),
                    "{sql}: {message}"
                )
            }
            other => panic!("{sql}: {other:?}"),
        }
    }
    for sql in [
        "SELECT make_time(24, 0, 0.5)",
        "SELECT make_time(10, 60, 0)",
        "SELECT make_time(-1, 0, 0)",
        "SELECT make_time(280, 0, 0)",
        "SELECT make_time(10, 30, -0.5)",
        "SELECT make_time(10, 30, 60.5)",
        "SELECT make_time(10, 30, CAST('NaN' AS REAL))",
        "SELECT make_timestamp(2024, 1, 1, 25, 0, 0)",
        "SELECT make_timestamp(2024, 1, 1, 10, 30, -0.5)",
    ] {
        match conn.query(sql) {
            Err(SqlError::InvalidTimeLiteral(message)) => {
                assert!(
                    message.contains("time field value out of range"),
                    "{sql}: {message}"
                )
            }
            other => panic!("{sql}: {other:?}"),
        }
    }
    for sql in [
        "SELECT make_interval(178956971)",
        "SELECT make_interval(1, 2147483647)",
        "SELECT make_interval(0, 0, 306783379)",
        "SELECT make_interval(0, 0, 0, 0, 2562047789)",
        "SELECT make_interval(0, 0, 0, 0, 0, 0, 1e300)",
        "SELECT make_interval(1e300)",
        "SELECT make_interval(CAST('NaN' AS REAL))",
    ] {
        assert_invalid(&conn, sql, "interval out of range");
    }
}

#[test]
fn timezone_names_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
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
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT name FROM timezone_names() WHERE name = 'UTC'")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn timezone_abbrevs_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
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
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    let t1 = scalar(&conn, "SELECT CURRENT_TIMESTAMP");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t2 = scalar(&conn, "SELECT CURRENT_TIMESTAMP");
    conn.execute("COMMIT").unwrap();
    assert_eq!(t1, t2, "CURRENT_TIMESTAMP should be stable within a txn");
}

#[test]
fn clock_timestamp_advances_within_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    let t1 = scalar(&conn, "SELECT CLOCK_TIMESTAMP()");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t2 = scalar(&conn, "SELECT CLOCK_TIMESTAMP()");
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
    let conn = Connection::open(&db).unwrap();
    assert_ok(conn.execute("SET TIME ZONE 'America/New_York'").unwrap());
    assert_ok(conn.execute("SET TIME ZONE '+05:00'").unwrap());
    assert_ok(conn.execute("SET TIME ZONE 'UTC'").unwrap());
}

#[test]
fn set_time_zone_rejects_posix_shorthand() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn.execute("SET TIME ZONE 'UTC+5'").unwrap_err();
    assert!(matches!(err, SqlError::InvalidTimezone(_)));
}

#[test]
fn sum_interval_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES (1, INTERVAL '1 day'), (2, INTERVAL '2 days'), (3, INTERVAL '3 hours')",
    )
    .unwrap();
    let v = scalar(&conn, "SELECT SUM(iv) FROM t");
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
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, iv INTERVAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, INTERVAL '2 days'), (2, INTERVAL '4 days')")
        .unwrap();
    let v = scalar(&conn, "SELECT AVG(iv) FROM t");
    match v {
        Value::Interval { days, .. } => assert_eq!(days, 3),
        v => panic!("expected Interval, got {v:?}"),
    }
}

#[test]
fn default_current_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    )
    .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let qr = conn.query("SELECT created_at FROM t WHERE id = 1").unwrap();
    assert!(matches!(qr.rows[0][0], Value::Timestamp(_)));
}
