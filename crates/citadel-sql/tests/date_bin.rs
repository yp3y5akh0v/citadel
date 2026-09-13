use citadel::{Argon2Profile, CancelToken, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

const SQL: &str = "SELECT DATE_BIN($1,$2,$3)";

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"date-bin")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn interval(months: i32, days: i32, micros: i64) -> Value {
    Value::Interval {
        months,
        days,
        micros,
    }
}

fn bin(conn: &Connection<'_>, stride: Value, source: i64, origin: i64) -> Result<Value, SqlError> {
    let params = [stride, Value::Timestamp(source), Value::Timestamp(origin)];
    let direct = conn.query_params(SQL, &params)?.rows[0][0].clone();
    let prepared = conn.prepare(SQL)?.query_collect(&params)?.rows[0][0].clone();
    assert_eq!(direct, prepared);
    Ok(direct)
}

#[test]
fn date_bin_extreme_finite_differences_and_rounded_deltas_remain_valid() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (stride, source, origin, expected) in [
        (1, i64::MAX - 1, i64::MIN + 1, i64::MAX - 1),
        (1, i64::MIN + 1, i64::MAX - 1, i64::MIN + 1),
        (3, i64::MAX - 1, i64::MIN + 1, i64::MAX - 2),
        // The source-origin difference fits i64, but the old rounded product
        // does not. Adding the origin makes the final timestamp valid again.
        (3, i64::MIN + 6, 5, i64::MIN + 4),
        (2, i64::MAX - 1, 0, i64::MAX - 1),
    ] {
        assert_eq!(
            bin(&conn, interval(0, 0, stride), source, origin).unwrap(),
            Value::Timestamp(expected)
        );
    }
}

#[test]
fn date_bin_wide_positive_interval_components_do_not_falsely_overflow() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for stride in [
        interval(0, i32::MAX, i64::MAX),
        interval(0, i32::MAX, i64::MIN),
        // This positive total is wider than i64, despite ordinary day count.
        interval(0, 1, i64::MAX),
    ] {
        assert_eq!(bin(&conn, stride, 10, 0).unwrap(), Value::Timestamp(0));
    }
    // A positive day and negative micros may also combine to a tiny stride.
    assert_eq!(
        bin(&conn, interval(0, 1, -86_400_000_000 + 3), 10, 0).unwrap(),
        Value::Timestamp(9)
    );
    assert_eq!(
        bin(&conn, interval(0, -1, 86_400_000_000 + 3), -1, 0).unwrap(),
        Value::Timestamp(-3)
    );
}

#[test]
fn date_bin_floors_negative_offsets_and_rejects_unrepresentable_finite_bins() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (source, origin, expected) in [
        (-1, 0, -10),
        (-10, 0, -10),
        (-11, 0, -20),
        (1, 7, -3),
        (7, 7, 7),
    ] {
        assert_eq!(
            bin(&conn, interval(0, 0, 10), source, origin).unwrap(),
            Value::Timestamp(expected)
        );
    }
    for stride in [2, 3, i64::MAX - 1] {
        let error = bin(&conn, interval(0, 0, stride), i64::MIN + 1, 0).unwrap_err();
        assert!(
            matches!(error,SqlError::InvalidValue(ref message) if message.contains("timestamp range")),
            "{error:?}"
        );
    }
}

#[test]
fn date_bin_rejects_months_and_nonpositive_complete_strides() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for months in [1, -1, i32::MAX, i32::MIN] {
        let error = bin(&conn, interval(months, 0, 10), 15, 0).unwrap_err();
        assert!(
            matches!(error,SqlError::Unsupported(ref message) if message.contains("months or years")),
            "{error:?}"
        );
    }
    for stride in [
        interval(0, 0, 0),
        interval(0, 0, -1),
        interval(0, 1, -86_400_000_000),
        interval(0, i32::MIN, i64::MAX),
    ] {
        let error = bin(&conn, stride, 15, 0).unwrap_err();
        assert!(
            matches!(error,SqlError::InvalidValue(ref message) if message.contains("positive")),
            "{error:?}"
        );
    }
}

#[test]
fn date_bin_preserves_infinite_sources_and_rejects_infinite_origins() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for source in [i64::MIN, i64::MAX] {
        for origin in [0, i64::MIN, i64::MAX] {
            assert_eq!(
                bin(&conn, interval(0, 0, 10), source, origin).unwrap(),
                Value::Timestamp(source)
            );
        }
    }
    for origin in [i64::MIN, i64::MAX] {
        let error = bin(&conn, interval(0, 0, 10), 0, origin).unwrap_err();
        assert!(
            matches!(error,SqlError::InvalidValue(ref message) if message.contains("origin must be finite")),
            "{error:?}"
        );
    }
    // Existing argument admission precedes infinity handling.
    let error = bin(&conn, interval(0, 0, 0), i64::MAX, 0).unwrap_err();
    assert!(matches!(error,SqlError::InvalidValue(ref message) if message.contains("positive")));
}

#[test]
fn date_bin_retains_null_type_arity_and_cancellation_admission() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    let valid = [
        interval(0, 0, 10),
        Value::Timestamp(15),
        Value::Timestamp(0),
    ];
    for position in 0..3 {
        let mut params = valid.clone();
        params[position] = Value::Null;
        assert_eq!(
            conn.query_params(SQL, &params).unwrap().rows,
            vec![vec![Value::Null]]
        );
    }
    assert_eq!(
        conn.query_params(SQL, &[Value::Integer(1), Value::Null, Value::Integer(2)])
            .unwrap()
            .rows,
        vec![vec![Value::Null]]
    );
    for params in [
        [Value::Integer(1), Value::Timestamp(0), Value::Timestamp(0)],
        [interval(0, 0, 1), Value::Date(0), Value::Timestamp(0)],
        [interval(0, 0, 1), Value::Timestamp(0), Value::Integer(0)],
    ] {
        assert!(matches!(
            conn.query_params(SQL, &params),
            Err(SqlError::TypeMismatch { .. })
        ));
    }
    // Invalid stride remains checked before timestamp argument types.
    assert!(matches!(
        conn.query_params(
            SQL,
            &[interval(0, 0, 0), Value::Integer(0), Value::Integer(0)]
        ),
        Err(SqlError::InvalidValue(_))
    ));
    for sql in [
        "SELECT DATE_BIN()",
        "SELECT DATE_BIN(NULL,NULL)",
        "SELECT DATE_BIN(NULL,NULL,NULL,NULL)",
    ] {
        assert!(matches!(conn.query(sql), Err(SqlError::InvalidValue(_))));
    }
    let prepared = conn.prepare(SQL).unwrap();
    let token = CancelToken::new();
    token.cancel();
    db.set_cancel(Some(token));
    assert!(matches!(
        prepared.query_collect(&valid),
        Err(SqlError::Storage(citadel::Error::Interrupted))
    ));
    db.set_cancel(None);
    assert_eq!(
        prepared.query_collect(&valid).unwrap().rows,
        vec![vec![Value::Timestamp(10)]]
    );
}
