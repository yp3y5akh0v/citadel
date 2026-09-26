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
fn calendar_arithmetic_agrees_with_jiff_over_its_whole_range() {
    let mut date = JDate::MIN;
    let mut days = date.since((Unit::Day, epoch_date())).unwrap().get_days();
    loop {
        let ymd = (i32::from(date.year()), date.month() as u8, date.day() as u8);
        assert_eq!(days_to_ymd(days), ymd, "day {days}");
        assert_eq!(ymd_to_days(ymd.0, ymd.1, ymd.2), Some(days), "{ymd:?}");
        let day = i64::from(days);
        let iso = date.iso_week_date();
        assert_eq!(
            iso_weekday(day),
            i64::from(date.weekday().to_monday_one_offset())
        );
        assert_eq!(
            iso_week(day),
            (i64::from(iso.year()), i64::from(iso.week())),
            "{ymd:?}"
        );
        assert_eq!(date_field("doy", days), Some(i64::from(date.day_of_year())));
        assert_eq!(
            date_field("dow", days),
            Some(i64::from(date.weekday().to_sunday_zero_offset()))
        );
        if date == JDate::MAX {
            break;
        }
        date = date.tomorrow().unwrap();
        days += 1;
    }
}

#[test]
fn every_finite_day_count_is_a_calendar_date() {
    // i32::MAX is 14,699 eras of 146,097 days and 3,844 days past 1970-01-01;
    // i32::MIN is 14,700 eras before it and 142,252 days on.
    assert_eq!(days_to_ymd(i32::MAX), (5_881_580, 7, 11));
    assert_eq!(days_to_ymd(i32::MIN), (-5_877_641, 6, 23));
    for days in [i32::MIN + 1, i32::MAX - 1] {
        let (y, m, d) = days_to_ymd(days);
        assert_eq!(ymd_to_days(y, m, d), Some(days));
    }
    // The extreme counts stand for ±infinity, and nothing lies past them.
    assert_eq!(ymd_to_days(5_881_580, 7, 11), None);
    assert_eq!(ymd_to_days(-5_877_641, 6, 23), None);
    assert_eq!(ymd_to_days(5_881_580, 7, 12), None);
    for (y, m, d) in [(2023, 2, 29), (2024, 13, 1), (2024, 4, 31), (2024, 1, 0)] {
        assert_eq!(ymd_to_days(y, m, d), None, "{y}-{m}-{d}");
    }
}

#[test]
fn date_arithmetic_keeps_infinity_and_the_finite_range() {
    assert_eq!(
        add_days_to_date(DATE_INFINITY_DAYS, -5).unwrap(),
        DATE_INFINITY_DAYS
    );
    assert_eq!(add_days_to_date(i32::MAX - 2, 1).unwrap(), i32::MAX - 1);
    assert!(add_days_to_date(i32::MAX - 1, 1).is_err());
    assert!(add_days_to_date(i32::MIN + 1, -1).is_err());
    assert!(add_days_to_date(0, i64::MAX).is_err());
    assert_eq!(
        subtract_days_from_date(DATE_NEG_INFINITY_DAYS, i64::MIN).unwrap(),
        DATE_NEG_INFINITY_DAYS
    );
    assert_eq!(
        subtract_days_from_date(i32::MIN + 2, 1).unwrap(),
        i32::MIN + 1
    );
    assert!(subtract_days_from_date(i32::MIN + 1, 1).is_err());
    assert!(subtract_days_from_date(0, i64::MIN).is_err());
    assert!(subtract_dates(DATE_INFINITY_DAYS, 0).is_err());
    assert!(subtract_dates(0, DATE_NEG_INFINITY_DAYS).is_err());
    assert_eq!(
        subtract_dates(i32::MAX - 1, i32::MIN + 1).unwrap(),
        i64::from(u32::MAX) - 2
    );
    assert_eq!(date_to_ts(DATE_INFINITY_DAYS).unwrap(), TS_INFINITY_MICROS);
    assert_eq!(
        date_to_ts(DATE_NEG_INFINITY_DAYS).unwrap(),
        TS_NEG_INFINITY_MICROS
    );
    assert!(date_to_ts(i32::MAX - 1).is_err());
    assert!(subtract_timestamps(TS_INFINITY_MICROS, 0).is_err());
    assert!(subtract_timestamps(i64::MAX - 1, i64::MIN + 1).is_err());
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
fn parse_interval_spills_fractions_into_smaller_fields() {
    let hours = |h: i64| h * MICROS_PER_HOUR;
    let ninety_minutes = hours(1) + 30 * MICROS_PER_MIN;
    for (literal, expected) in [
        ("1.5 years", (18, 0, 0)),
        ("1.75 months", (1, 22, hours(12))),
        ("1.5 weeks", (0, 10, hours(12))),
        ("1.5 days", (0, 1, hours(12))),
        ("-1.5 days", (0, -1, -hours(12))),
        ("1.5 days ago", (0, -1, -hours(12))),
        ("1.5 hours", (0, 0, ninety_minutes)),
        ("0.5 minutes", (0, 0, 30 * MICROS_PER_SEC)),
        ("1.000001 seconds", (0, 0, MICROS_PER_SEC + 1)),
        ("0.3 seconds", (0, 0, 300_000)),
        (
            "9007199254740993 microseconds",
            (0, 0, 9_007_199_254_740_993),
        ),
        ("P1.5Y", (18, 0, 0)),
        ("P1.5D", (0, 1, hours(12))),
        ("PT1.5H", (0, 0, ninety_minutes)),
    ] {
        assert_eq!(parse_interval(literal).unwrap(), expected, "{literal}");
    }
}

#[test]
fn parse_interval_rejects_fields_out_of_range() {
    for literal in [
        "153722867281 minutes",
        "2562047789 hours",
        "9223372036854775807 microseconds 1 microsecond",
        "99999999999999999999 seconds",
        "3000000000 years",
        "2147483648 days",
        "99999999999:00:00",
        "1:60:00",
        "1:00:61",
        "PT153722867281M",
    ] {
        match parse_interval(literal) {
            Err(SqlError::InvalidIntervalLiteral(message)) => {
                assert!(
                    message.starts_with("field value out of range"),
                    "{literal}: {message}"
                )
            }
            other => panic!("{literal}: {other:?}"),
        }
    }
    for literal in [
        "1e3 seconds",
        "inf hours",
        "NaN days",
        "1.2.3 days",
        "- days",
    ] {
        assert!(
            matches!(
                parse_interval(literal),
                Err(SqlError::InvalidIntervalLiteral(_))
            ),
            "{literal}"
        );
    }
}

#[test]
fn scaling_an_interval_cascades_fractions_like_postgres() {
    let hours = |h: i64| h * MICROS_PER_HOUR;
    assert_eq!(divide_interval(0, 10, 0, 4.0).unwrap(), (0, 2, hours(12)));
    assert_eq!(divide_interval(1, 15, 0, 2.0).unwrap(), (0, 22, hours(12)));
    assert_eq!(
        divide_interval(0, 1, hours(2), 2.0).unwrap(),
        (0, 0, hours(13))
    );
    assert_eq!(multiply_interval(1, 0, 0, 1.5).unwrap(), (1, 15, 0));
    assert_eq!(multiply_interval(0, 1, 0, 0.5).unwrap(), (0, 0, hours(12)));
    // '1 day 02:00:00', '-3 hours', '1 mon 15 days' and one microsecond.
    assert_eq!(
        average_interval(1, 16, i128::from(hours(2) - hours(3) + 1), 4).unwrap(),
        (0, 11, hours(11) + 45 * MICROS_PER_MIN)
    );
    assert!(matches!(
        divide_interval(0, 1, 0, 0.0),
        Err(SqlError::DivisionByZero)
    ));
    for overflow in [
        multiply_interval(i32::MAX, 0, 0, 2.0),
        multiply_interval(0, 0, i64::MAX, 2.0),
        multiply_interval(1, 0, 0, f64::NAN),
        multiply_interval_by_integer(i32::MAX, 0, 0, 2),
        multiply_interval_by_integer(0, 0, i64::MIN, -1),
        add_intervals((0, i32::MAX, 0), (0, 1, 0)),
        subtract_intervals((0, 0, i64::MIN), (0, 0, 1)),
        negate_interval(i32::MIN, 0, 0),
    ] {
        assert!(
            matches!(&overflow, Err(SqlError::InvalidValue(message)) if message == "interval out of range"),
            "{overflow:?}"
        );
    }
}

#[test]
fn justify_moves_signs_like_postgres() {
    let hours = |h: i64| h * MICROS_PER_HOUR;
    assert_eq!(justify_days(1, -5, 0).unwrap(), (0, 25, 0));
    assert_eq!(justify_days(-1, 5, 0).unwrap(), (0, -25, 0));
    assert_eq!(justify_hours(0, 1, -hours(1)).unwrap(), (0, 0, hours(23)));
    assert_eq!(justify_hours(0, -1, hours(1)).unwrap(), (0, 0, -hours(23)));
    assert_eq!(
        justify_interval(1, 0, -hours(1)).unwrap(),
        (0, 29, hours(23))
    );
    assert_eq!(
        justify_interval(-1, 0, hours(1)).unwrap(),
        (0, -29, -hours(23))
    );
    assert_eq!(
        justify_interval(0, 35, hours(25)).unwrap(),
        (1, 6, hours(1))
    );
    assert!(justify_days(i32::MAX, 30, 0).is_err());
}

#[test]
fn format_interval_hours_past_a_day() {
    assert_eq!(format_interval(0, 0, 300 * MICROS_PER_HOUR), "300:00:00");
    assert_eq!(
        format_interval(0, 0, -300 * MICROS_PER_HOUR + 5 * MICROS_PER_MIN),
        "-299:55:00"
    );
    assert_eq!(format_interval(0, 0, i64::MIN), "-2562047788:00:54.775808");
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
fn canonical_interval_is_shared_by_every_interval_of_a_length() {
    let day = MICROS_PER_DAY;
    let keeps_length = |(months, days, micros): (i32, i32, i64)| {
        let canonical = canonical_interval(months, days, micros);
        assert_eq!(
            interval_to_total_micros(canonical.0, canonical.1, canonical.2),
            interval_to_total_micros(months, days, micros),
            "{months} {days} {micros}"
        );
        assert_eq!(
            canonical_interval(canonical.0, canonical.1, canonical.2),
            canonical
        );
        canonical
    };
    // A length that fits in micros is kept there alone.
    assert_eq!(keeps_length((1, 0, 0)), (0, 0, 30 * day));
    assert_eq!(keeps_length((0, 30, 0)), (0, 0, 30 * day));
    assert_eq!(keeps_length((0, 0, i64::MIN)), (0, 0, i64::MIN));
    // Longer ones meet too, up to the longest and shortest an interval can be.
    assert_eq!(
        keeps_length((i32::MAX, 30, 0)),
        keeps_length((i32::MAX - 1, 60, 0))
    );
    assert_eq!(
        keeps_length((i32::MIN, -30, 0)),
        keeps_length((i32::MIN + 1, -60, 0))
    );
    assert_eq!(
        keeps_length((i32::MAX, i32::MAX, i64::MAX)),
        (i32::MAX, i32::MAX, i64::MAX)
    );
    assert_eq!(
        keeps_length((i32::MIN, i32::MIN, i64::MIN)),
        (i32::MIN, i32::MIN, i64::MIN)
    );
    // Moving a month into days, or a day into micros, keeps the canonical fields.
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    let mut next = || splitmix64(&mut state);
    for _ in 0..20_000 {
        let (months, days, micros) = (next() as i32, next() as i32, next() as i64);
        let canonical = keeps_length((months, days, micros));
        if let (Some(m), Some(d)) = (months.checked_sub(1), days.checked_add(30)) {
            assert_eq!(keeps_length((m, d, micros)), canonical);
        }
        if let (Some(d), Some(u)) = (days.checked_sub(1), micros.checked_add(day)) {
            assert_eq!(keeps_length((months, d, u)), canonical);
        }
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[test]
fn canonical_intervals_order_as_their_lengths() {
    let mut state = 0x1319_8a2e_0370_7344_u64;
    let mut next = || splitmix64(&mut state);
    // Each field near zero, at either limit, or anywhere, so lengths past the micros limit
    // and months clamped at theirs both occur.
    let mut field = |limit: i64| match next() % 4 {
        0 => (next() % 5) as i64 - 2,
        1 => limit - (next() % 3) as i64,
        2 => -limit - 1 + (next() % 3) as i64,
        _ => next() as i64 % limit,
    };
    let intervals: Vec<_> = (0..1_500)
        .map(|_| {
            let months = field(i32::MAX.into()) as i32;
            let days = field(i32::MAX.into()) as i32;
            let micros = field(i64::MAX);
            (
                (months, days, micros),
                canonical_interval(months, days, micros),
            )
        })
        .collect();
    for (a, canonical_a) in &intervals {
        for (b, canonical_b) in &intervals {
            assert_eq!(
                canonical_a.cmp(canonical_b),
                pg_normalized_interval_cmp(*a, *b),
                "{a:?} {b:?}"
            );
        }
    }
}

#[test]
fn justify_days_basic() {
    let (m, d, us) = justify_days(0, 65, 0).unwrap();
    assert_eq!((m, d, us), (2, 5, 0));
}

#[test]
fn justify_hours_basic() {
    let (m, d, us) = justify_hours(0, 0, 50 * MICROS_PER_HOUR + 10 * MICROS_PER_MIN).unwrap();
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
    let (days, micros) = subtract_timestamps(a, b).unwrap();
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

#[test]
fn multibyte_offset_is_rejected_not_panicked() {
    // Four BYTES but three chars, so the +HHMM split lands mid-character.
    assert!(resolve_timezone("+\u{20AC}a").is_err());
    assert!(resolve_timezone("-\u{20AC}a").is_err());
    assert!(resolve_timezone("+\u{00B1}\u{00B1}").is_err());
}

#[test]
fn fixed_offset_format_round_trips_subminute_offsets() {
    for seconds in [-57_599, -1, 0, 1, 57_599] {
        let rendered = format_timezone_offset(seconds);
        let parsed = resolve_timezone(&rendered).unwrap();
        assert_eq!(
            parsed.to_fixed_offset().unwrap().seconds(),
            seconds,
            "{rendered}"
        );
    }
    assert!(resolve_timezone("+00:00:").is_err());
    assert!(resolve_timezone("+00:00:60").is_err());
    assert!(resolve_timezone("+00:00:01:02").is_err());
}

#[test]
#[cfg(panic = "unwind")]
fn scoped_transaction_clock_restores_after_unwind() {
    set_txn_clock(Some(111));
    let panic = std::panic::catch_unwind(|| {
        with_txn_clock(Some(222), || panic!("injected clock-scope panic"));
    });
    assert!(panic.is_err());
    assert_eq!(txn_or_clock_micros(), 111);
    set_txn_clock(None);
}

#[test]
fn current_local_fields_use_the_scoped_named_timezone() {
    let timezone = resolve_timezone("America/New_York").unwrap();
    for (instant, expected) in [
        ("2023-01-15T04:30:00Z", "2023-01-14 23:30:00"),
        ("2023-08-15T04:30:00Z", "2023-08-15 00:30:00"),
    ] {
        let timestamp = parse_timestamp(instant).unwrap();
        with_txn_clock(Some(timestamp), || {
            with_session_timezone(timezone.clone(), || {
                let local = current_local_timestamp_micros().unwrap();
                assert_eq!(format_timestamp(local), expected);
                assert_eq!(current_date_days().unwrap(), ts_split(local).0);
                assert_eq!(current_local_time_micros().unwrap(), ts_split(local).1);
            });
        });
    }
}

#[test]
fn transaction_statement_and_wall_clocks_are_distinct() {
    with_txn_clock(Some(111), || {
        with_statement_clock(Some(222), || {
            assert_eq!(txn_or_clock_micros(), 111);
            assert_eq!(statement_or_clock_micros(), 222);
        });
    });
}
