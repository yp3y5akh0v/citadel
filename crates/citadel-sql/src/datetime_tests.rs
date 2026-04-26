use super::*;

#[test]
fn ymd_roundtrip_epoch() {
    assert_eq!(days_to_ymd(0), (1970, 1, 1));
    assert_eq!(ymd_to_days(1970, 1, 1), Some(0));
}

#[test]
fn ymd_roundtrip_leap_day() {
    let days = ymd_to_days(2024, 2, 29).unwrap();
    assert_eq!(days_to_ymd(days), (2024, 2, 29));
}

#[test]
fn ymd_pre_epoch() {
    let days = ymd_to_days(1960, 1, 1).unwrap();
    assert!(days < 0);
    assert_eq!(days_to_ymd(days), (1960, 1, 1));
}

#[test]
fn hmsn_roundtrip() {
    let us = hmsn_to_micros(12, 30, 45, 123456).unwrap();
    assert_eq!(micros_to_hmsn(us), (12, 30, 45, 123456));
}

#[test]
fn time_upper_bound_inclusive() {
    assert_eq!(hmsn_to_micros(24, 0, 0, 0), Some(MICROS_PER_DAY));
    assert_eq!(hmsn_to_micros(24, 0, 0, 1), None);
}

#[test]
fn ts_split_pre_1970() {
    let (d, t) = ts_split(-1);
    assert_eq!(d, -1);
    assert_eq!(t, MICROS_PER_DAY - 1);
}

#[test]
fn parse_format_date_roundtrip() {
    let d = parse_date("2024-01-15").unwrap();
    assert_eq!(format_date(d), "2024-01-15");
}

#[test]
fn parse_date_bc() {
    let ad = parse_date("0001-01-01").unwrap();
    let bc = parse_date("0001-01-01 BC").unwrap();
    assert!(bc < ad);
}

#[test]
fn parse_date_rejects_year_0() {
    assert!(parse_date("0000-01-01").is_err());
}

#[test]
fn parse_date_infinity() {
    assert_eq!(parse_date("infinity").unwrap(), DATE_INFINITY_DAYS);
    assert_eq!(parse_date("-infinity").unwrap(), DATE_NEG_INFINITY_DAYS);
}

#[test]
fn parse_time_with_fractional() {
    let t = parse_time("12:30:45.123456").unwrap();
    assert_eq!(format_time(t), "12:30:45.123456");
}

#[test]
fn parse_time_24_00() {
    assert_eq!(parse_time("24:00:00").unwrap(), MICROS_PER_DAY);
}

#[test]
fn parse_timestamp_iso() {
    let t = parse_timestamp("2024-01-15T12:30:45Z").unwrap();
    assert_eq!(format_timestamp(t), "2024-01-15 12:30:45");
}

#[test]
fn parse_timestamp_naive() {
    let t1 = parse_timestamp("2024-01-15 12:30:45").unwrap();
    let t2 = parse_timestamp("2024-01-15T12:30:45").unwrap();
    assert_eq!(t1, t2);
}

#[test]
fn parse_timestamp_infinity() {
    assert_eq!(parse_timestamp("infinity").unwrap(), TS_INFINITY_MICROS);
}

#[test]
fn parse_timestamp_bc() {
    let ad = parse_timestamp("0001-01-01 00:00:00").unwrap();
    let bc = parse_timestamp("0001-12-31 00:00:00 BC").unwrap();
    assert_eq!(ad - bc, MICROS_PER_DAY);
}

#[test]
fn parse_timestamp_rejects_year_0() {
    assert!(parse_timestamp("0000-06-15 12:00:00").is_err());
}

#[test]
fn parse_interval_pg_verbose() {
    let (m, d, us) = parse_interval("1 year 2 months 3 days").unwrap();
    assert_eq!((m, d, us), (14, 3, 0));
}

#[test]
fn parse_interval_with_hms() {
    let (m, d, us) = parse_interval("3 days 04:05:06.789").unwrap();
    assert_eq!(m, 0);
    assert_eq!(d, 3);
    let expected_us = 4 * MICROS_PER_HOUR + 5 * MICROS_PER_MIN + 6 * MICROS_PER_SEC + 789000;
    assert_eq!(us, expected_us);
}

#[test]
fn parse_interval_iso8601() {
    let (m, d, us) = parse_interval("P1Y2M3DT4H5M6S").unwrap();
    assert_eq!(m, 14);
    assert_eq!(d, 3);
    assert_eq!(
        us,
        4 * MICROS_PER_HOUR + 5 * MICROS_PER_MIN + 6 * MICROS_PER_SEC
    );
}

#[test]
fn format_interval_zero() {
    assert_eq!(format_interval(0, 0, 0), "00:00:00");
}

#[test]
fn format_interval_mixed() {
    assert_eq!(
        format_interval(
            14,
            3,
            4 * MICROS_PER_HOUR + 5 * MICROS_PER_MIN + 6 * MICROS_PER_SEC
        ),
        "1 year 2 mons 3 days 04:05:06"
    );
}

#[test]
fn add_interval_month_clamp() {
    let jan31 = parse_date("2024-01-31").unwrap();
    let ts = add_interval_to_date(jan31, 1, 0, 0).unwrap();
    let (d, _t) = ts_split(ts);
    let (y, mo, da) = days_to_ymd(d);
    assert_eq!((y, mo, da), (2024, 2, 29));
}

#[test]
fn add_interval_month_clamp_non_leap() {
    let jan31 = parse_date("2023-01-31").unwrap();
    let ts = add_interval_to_date(jan31, 1, 0, 0).unwrap();
    let (d, _t) = ts_split(ts);
    let (y, mo, da) = days_to_ymd(d);
    assert_eq!((y, mo, da), (2023, 2, 28));
}

#[test]
fn interval_normalized_compare() {
    let a = (1i32, 0i32, 0i64);
    let b = (0i32, 30i32, 0i64);
    assert_eq!(pg_normalized_interval_cmp(a, b), std::cmp::Ordering::Equal);
}

#[test]
fn justify_days_basic() {
    let (m, d, us) = justify_days(0, 65, 0);
    assert_eq!((m, d, us), (2, 5, 0));
}

#[test]
fn justify_hours_basic() {
    let (m, d, us) = justify_hours(0, 0, 50 * MICROS_PER_HOUR + 10 * MICROS_PER_MIN);
    assert_eq!(
        (m, d, us),
        (0, 2, 2 * MICROS_PER_HOUR + 10 * MICROS_PER_MIN)
    );
}

#[test]
fn time_add_wrap() {
    let t = parse_time("23:00:00").unwrap();
    let result = add_interval_to_time(t, 0, 0, 2 * MICROS_PER_HOUR).unwrap();
    assert_eq!(format_time(result), "01:00:00");
}

#[test]
fn time_add_rejects_days() {
    let t = parse_time("12:00:00").unwrap();
    assert!(add_interval_to_time(t, 0, 1, 0).is_err());
}

#[test]
fn subtract_timestamps_basic() {
    let a = parse_timestamp("2024-01-02 12:00:00").unwrap();
    let b = parse_timestamp("2024-01-01 00:00:00").unwrap();
    let (days, micros) = subtract_timestamps(a, b);
    assert_eq!(days, 1);
    assert_eq!(micros, 12 * MICROS_PER_HOUR);
}

#[test]
fn ts_to_date_floor_pre_epoch() {
    assert_eq!(ts_to_date_floor(-1), -1);
    assert_eq!(ts_to_date_floor(0), 0);
    assert_eq!(ts_to_date_floor(MICROS_PER_DAY - 1), 0);
    assert_eq!(ts_to_date_floor(MICROS_PER_DAY), 1);
}

#[test]
fn extract_year_from_date() {
    let d = parse_date("2024-03-15").unwrap();
    assert_eq!(
        extract("year", &Value::Date(d)).unwrap(),
        Value::Integer(2024)
    );
}

#[test]
fn extract_dow_sunday() {
    let d = parse_date("2024-01-07").unwrap();
    assert_eq!(extract("dow", &Value::Date(d)).unwrap(), Value::Integer(0));
    assert_eq!(
        extract("isodow", &Value::Date(d)).unwrap(),
        Value::Integer(7)
    );
}

#[test]
fn date_trunc_month() {
    let ts = parse_timestamp("2024-03-15 12:30:45").unwrap();
    let result = date_trunc("month", &Value::Timestamp(ts)).unwrap();
    if let Value::Timestamp(t) = result {
        assert_eq!(format_timestamp(t), "2024-03-01 00:00:00");
    } else {
        panic!("expected Timestamp");
    }
}

#[test]
fn date_trunc_week_monday() {
    let d = parse_date("2024-01-07").unwrap();
    let Value::Date(trunc) = date_trunc("week", &Value::Date(d)).unwrap() else {
        panic!("expected Date");
    };
    assert_eq!(format_date(trunc), "2024-01-01");
}

#[test]
fn age_basic() {
    let a = parse_timestamp("2024-04-10 00:00:00").unwrap();
    let b = parse_timestamp("2024-01-01 00:00:00").unwrap();
    let (m, d, us) = age(a, b).unwrap();
    assert_eq!(m, 3);
    assert_eq!(d, 9);
    assert_eq!(us, 0);
}

#[test]
fn strftime_basic() {
    let ts = parse_timestamp("2024-03-15 12:30:45").unwrap();
    let s = strftime("%Y-%m-%d", &Value::Timestamp(ts)).unwrap();
    assert_eq!(s, "2024-03-15");
}

#[test]
fn strftime_unix_epoch() {
    let ts = parse_timestamp("2024-01-01 00:00:00").unwrap();
    let s = strftime("%s", &Value::Timestamp(ts)).unwrap();
    assert_eq!(s, (ts / MICROS_PER_SEC).to_string());
}

#[test]
fn is_finite_temporal_sentinels() {
    assert!(!Value::Date(i32::MAX).is_finite_temporal());
    assert!(!Value::Date(i32::MIN).is_finite_temporal());
    assert!(Value::Date(0).is_finite_temporal());
    assert!(!Value::Timestamp(i64::MAX).is_finite_temporal());
    assert!(Value::Timestamp(0).is_finite_temporal());
}

#[test]
fn add_interval_infinity() {
    let result = add_interval_to_timestamp(TS_INFINITY_MICROS, 1, 1, 0).unwrap();
    assert_eq!(result, TS_INFINITY_MICROS);
}

#[test]
fn format_date_bc() {
    let bc1 = parse_date("0001-01-01 BC").unwrap();
    assert_eq!(format_date(bc1), "0001-01-01 BC");
}
