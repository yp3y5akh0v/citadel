use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"date-trunc-timezone")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn timestamp(text: &str) -> Value {
    Value::Timestamp(citadel_sql::datetime::parse_timestamp(text).unwrap())
}

#[test]
fn explicit_zone_date_trunc_matches_common_infinity_behavior_after_zone_validation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let prepared = conn.prepare("SELECT DATE_TRUNC($1, $2, $3)").unwrap();
    for infinity in [i64::MIN, i64::MAX] {
        for unit in ["microseconds", "second", "day", "year"] {
            let input = Value::Timestamp(infinity);
            let expected = citadel_sql::datetime::date_trunc(unit, &input).unwrap();
            for zone in ["UTC", "America/New_York", "+05:45"] {
                assert_eq!(
                    prepared
                        .query_collect(&[
                            Value::Text(unit.into()),
                            input.clone(),
                            Value::Text(zone.into())
                        ])
                        .unwrap()
                        .rows,
                    vec![vec![expected.clone()]],
                    "{infinity}, {unit}, {zone}"
                );
            }
            let error = prepared
                .query_collect(&[
                    Value::Text(unit.into()),
                    input,
                    Value::Text("Invalid/Zone".into()),
                ])
                .unwrap_err();
            assert!(matches!(error, SqlError::InvalidTimezone(_)), "{error:?}");
        }
    }
}

#[test]
fn explicit_zone_date_trunc_reuses_every_civil_unit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let prepared = conn.prepare("SELECT DATE_TRUNC($1, $2, $3)").unwrap();
    for (unit, expected) in [
        ("microseconds", "2024-05-17T12:34:56.789123Z"),
        ("milliseconds", "2024-05-17T12:34:56.789Z"),
        ("second", "2024-05-17T12:34:56Z"),
        ("minute", "2024-05-17T12:34:00Z"),
        ("hour", "2024-05-17T12:00:00Z"),
        ("day", "2024-05-17T04:00:00Z"),
        ("week", "2024-05-13T04:00:00Z"),
        ("month", "2024-05-01T04:00:00Z"),
        ("quarter", "2024-04-01T04:00:00Z"),
        ("year", "2024-01-01T05:00:00Z"),
        ("decade", "2020-01-01T05:00:00Z"),
        ("century", "2001-01-01T05:00:00Z"),
        ("millennium", "2001-01-01T05:00:00Z"),
    ] {
        let expected = timestamp(expected);
        let params = [
            Value::Text(unit.into()),
            timestamp("2024-05-17T12:34:56.789123Z"),
            Value::Text("America/New_York".into()),
        ];
        assert_eq!(
            prepared.query_collect(&params).unwrap().rows,
            vec![vec![expected.clone()]],
            "{unit}"
        );
        let sql = format!("SELECT DATE_TRUNC('{unit}', TIMESTAMP '2024-05-17T12:34:56.789123Z', 'America/New_York')");
        assert_eq!(
            conn.query(&sql).unwrap().rows,
            vec![vec![expected]],
            "{unit}"
        );
    }
}

#[test]
fn explicit_zone_date_trunc_uses_non_whole_hour_and_second_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let prepared = conn.prepare("SELECT DATE_TRUNC($1, $2, $3)").unwrap();
    for (unit, input, zone, expected) in [
        (
            "hour",
            "2024-05-17T12:34:56Z",
            "Asia/Kathmandu",
            "2024-05-17T12:15:00Z",
        ),
        (
            "hour",
            "2024-05-17T12:34:56Z",
            "+05:45",
            "2024-05-17T12:15:00Z",
        ),
        (
            "day",
            "2024-05-17T12:34:56Z",
            "+05:45",
            "2024-05-16T18:15:00Z",
        ),
        (
            "day",
            "2024-05-17T12:34:56Z",
            "-03:30",
            "2024-05-17T03:30:00Z",
        ),
        (
            "minute",
            "2024-05-17T12:34:56Z",
            "+00:00:30",
            "2024-05-17T12:34:30Z",
        ),
        (
            "second",
            "1969-12-31T23:59:59.999999Z",
            "America/New_York",
            "1969-12-31T23:59:59Z",
        ),
        (
            " DAY ",
            "2024-05-17T12:34:56Z",
            "+00:00",
            "2024-05-17T00:00:00Z",
        ),
    ] {
        let params = [
            Value::Text(unit.into()),
            timestamp(input),
            Value::Text(zone.into()),
        ];
        assert_eq!(
            prepared.query_collect(&params).unwrap().rows,
            vec![vec![timestamp(expected)]],
            "{unit}: {input} in {zone}"
        );
    }
}

#[test]
fn explicit_zone_date_trunc_preserves_subday_offsets_and_resolves_calendar_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let prepared = conn.prepare("SELECT DATE_TRUNC($1, $2, $3)").unwrap();
    for (unit, input, zone, expected) in [
        (
            "day",
            "2024-03-10T08:45:00Z",
            "America/New_York",
            "2024-03-10T05:00:00Z",
        ),
        (
            "hour",
            "2024-11-03T05:45:00Z",
            "America/New_York",
            "2024-11-03T05:00:00Z",
        ),
        (
            "hour",
            "2024-11-03T06:45:00Z",
            "America/New_York",
            "2024-11-03T06:00:00Z",
        ),
        (
            "day",
            "2024-11-03T06:45:00Z",
            "America/New_York",
            "2024-11-03T04:00:00Z",
        ),
        // Lord Howe shifts by half an hour. Subday truncation preserves the
        // input offset, including when the truncated civil hour was skipped.
        (
            "hour",
            "2024-04-06T14:45:00Z",
            "Australia/Lord_Howe",
            "2024-04-06T14:00:00Z",
        ),
        (
            "hour",
            "2024-04-06T15:15:00Z",
            "Australia/Lord_Howe",
            "2024-04-06T14:30:00Z",
        ),
        (
            "hour",
            "2024-10-05T15:45:00Z",
            "Australia/Lord_Howe",
            "2024-10-05T15:00:00Z",
        ),
        (
            "day",
            "2024-10-05T15:45:00Z",
            "Australia/Lord_Howe",
            "2024-10-05T13:30:00Z",
        ),
        // Existing civil conversion uses Compatible: this skipped midnight
        // resolves to 01:00 at the start of the new offset.
        (
            "day",
            "2018-11-04T12:00:00Z",
            "America/Sao_Paulo",
            "2018-11-04T03:00:00Z",
        ),
    ] {
        let params = [
            Value::Text(unit.into()),
            timestamp(input),
            Value::Text(zone.into()),
        ];
        assert_eq!(
            prepared.query_collect(&params).unwrap().rows,
            vec![vec![timestamp(expected)]],
            "{unit}: {input} in {zone}"
        );
    }
}

#[test]
fn explicit_zone_date_trunc_keeps_errors_and_ignores_the_session_zone() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let prepared = conn.prepare("SELECT DATE_TRUNC($1, $2, $3)").unwrap();
    for zone in ["UTC", "America/New_York", "+05:45"] {
        let error = prepared
            .query_collect(&[
                Value::Text("fortnight".into()),
                timestamp("2024-05-17T12:34:56Z"),
                Value::Text(zone.into()),
            ])
            .unwrap_err();
        assert!(
            matches!(error, SqlError::InvalidDateTruncUnit(ref unit) if unit == "fortnight"),
            "{error:?}"
        );
    }
    let error = prepared
        .query_collect(&[
            Value::Text("day".into()),
            timestamp("2024-05-17T12:34:56Z"),
            Value::Text("Invalid/Zone".into()),
        ])
        .unwrap_err();
    assert!(matches!(error, SqlError::InvalidTimezone(_)), "{error:?}");
    let params = [
        Value::Text("day".into()),
        timestamp("2024-05-17T12:34:56Z"),
        Value::Text("America/New_York".into()),
    ];
    for session in ["+14:00", "-12:00", "UTC"] {
        conn.set_session_timezone(session).unwrap();
        assert_eq!(
            prepared.query_collect(&params).unwrap().rows,
            vec![vec![timestamp("2024-05-17T04:00:00Z")]]
        );
    }
    assert_eq!(
        conn.query("SELECT DATE_TRUNC('day', CAST(NULL AS TIMESTAMP), 'America/New_York')")
            .unwrap()
            .rows,
        vec![vec![Value::Null]]
    );
}
