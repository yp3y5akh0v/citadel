//! Date/Time/Timestamp/Interval support for Citadel SQL.
//!
//! Thin wrapper around [`jiff`] so the rest of the codebase doesn't depend on it
//! directly. Timestamps are UTC microseconds since 1970-01-01. INTERVAL is
//! PG-compatible: `(months: i32, days: i32, micros: i64)`.

use crate::error::{Result, SqlError};
use crate::types::Value;
use jiff::civil::{Date as JDate, DateTime as JDateTime, Time as JTime};
use jiff::tz::TimeZone;
use jiff::{Span, Timestamp as JTimestamp, ToSpan, Unit, Zoned};

/// Microseconds per second.
pub const MICROS_PER_SEC: i64 = 1_000_000;
/// Microseconds per minute.
pub const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
/// Microseconds per hour.
pub const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MIN;
/// Microseconds per day.
pub const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// i64 µs / 86_400_000_000 wraps to i32 for max representable date: ~292k years.
pub const DATE_INFINITY_DAYS: i32 = i32::MAX;
pub const DATE_NEG_INFINITY_DAYS: i32 = i32::MIN;
pub const TS_INFINITY_MICROS: i64 = i64::MAX;
pub const TS_NEG_INFINITY_MICROS: i64 = i64::MIN;

pub fn is_infinity_date(d: i32) -> bool {
    d == DATE_INFINITY_DAYS || d == DATE_NEG_INFINITY_DAYS
}

pub fn is_infinity_ts(t: i64) -> bool {
    t == TS_INFINITY_MICROS || t == TS_NEG_INFINITY_MICROS
}

/// Unix epoch as a jiff civil Date (avoid `JDate::ZERO` which is year 1, not 1970).
fn epoch_date() -> JDate {
    JDate::new(1970, 1, 1).expect("1970-01-01 is a valid date")
}

/// Convert i32 days-since-1970 to civil Gregorian (year, month, day).
pub fn days_to_ymd(days: i32) -> (i32, u8, u8) {
    let epoch = epoch_date();
    let d = epoch.checked_add((days as i64).days()).unwrap_or(epoch);
    (d.year() as i32, d.month() as u8, d.day() as u8)
}

/// Convert (year, month, day) Gregorian to i32 days-since-1970.
pub fn ymd_to_days(y: i32, m: u8, d: u8) -> Option<i32> {
    let date = JDate::new(y as i16, m as i8, d as i8).ok()?;
    let span = date.since((Unit::Day, epoch_date())).ok()?;
    let days = span.get_days() as i64;
    if (i32::MIN as i64..=i32::MAX as i64).contains(&days) {
        Some(days as i32)
    } else {
        None
    }
}

/// Convert µs-since-midnight to (hour, minute, second, subsec_micros).
pub fn micros_to_hmsn(micros: i64) -> (u8, u8, u8, u32) {
    let hour = (micros / MICROS_PER_HOUR) as u8;
    let rem = micros % MICROS_PER_HOUR;
    let min = (rem / MICROS_PER_MIN) as u8;
    let rem = rem % MICROS_PER_MIN;
    let sec = (rem / MICROS_PER_SEC) as u8;
    let subsec = (rem % MICROS_PER_SEC) as u32;
    (hour, min, sec, subsec)
}

/// Convert (hour, minute, second, subsec_micros) to µs since midnight.
pub fn hmsn_to_micros(h: u8, m: u8, s: u8, us: u32) -> Option<i64> {
    let total = (h as i64) * MICROS_PER_HOUR
        + (m as i64) * MICROS_PER_MIN
        + (s as i64) * MICROS_PER_SEC
        + us as i64;
    if (0..=MICROS_PER_DAY).contains(&total) {
        Some(total)
    } else {
        None
    }
}

/// Split µs since 1970-UTC into `(date_days, time_micros)`.
pub fn ts_split(micros: i64) -> (i32, i64) {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rem = micros.rem_euclid(MICROS_PER_DAY);
    (days as i32, rem)
}

/// Combine i32 date-days and i64 µs-of-day into µs-since-1970-UTC.
pub fn ts_combine(date_days: i32, time_micros: i64) -> i64 {
    (date_days as i64) * MICROS_PER_DAY + time_micros
}

/// Convert a date to a timestamp at midnight UTC.
pub fn date_to_ts(days: i32) -> i64 {
    (days as i64).saturating_mul(MICROS_PER_DAY)
}

/// Floor-divide timestamp µs to date days (correct for pre-1970 negative values).
pub fn ts_to_date_floor(micros: i64) -> i32 {
    if micros == TS_INFINITY_MICROS {
        DATE_INFINITY_DAYS
    } else if micros == TS_NEG_INFINITY_MICROS {
        DATE_NEG_INFINITY_DAYS
    } else {
        micros.div_euclid(MICROS_PER_DAY) as i32
    }
}

/// Parse an ISO 8601 DATE literal (`YYYY-MM-DD`, optional `BC` suffix, `'infinity'` / `'-infinity'`).
pub fn parse_date(s: &str) -> Result<i32> {
    let trimmed = s.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "infinity" || lower == "+infinity" {
        return Ok(DATE_INFINITY_DAYS);
    }
    if lower == "-infinity" {
        return Ok(DATE_NEG_INFINITY_DAYS);
    }

    // PG BC convention: "0001 BC" == astronomical year 0; "N BC" == year -(N-1).
    let (body, is_bc) = if let Some(stripped) = trimmed.strip_suffix(" BC") {
        (stripped.trim(), true)
    } else if let Some(stripped) = trimmed.strip_suffix(" bc") {
        (stripped.trim(), true)
    } else {
        (trimmed, false)
    };

    let d = JDate::strptime("%Y-%m-%d", body)
        .map_err(|e| SqlError::InvalidDateLiteral(format!("{body}: {e}")))?;
    if d.year() == 0 {
        return Err(SqlError::InvalidDateLiteral(
            "year 0 is not supported; use '0001-01-01 BC' for 1 BC".into(),
        ));
    }
    let year_adjusted = if is_bc {
        -(d.year() as i32 - 1)
    } else {
        d.year() as i32
    };
    let canonical = JDate::new(year_adjusted as i16, d.month(), d.day())
        .map_err(|e| SqlError::InvalidDateLiteral(format!("{body}: {e}")))?;
    let span = canonical
        .since((Unit::Day, epoch_date()))
        .map_err(|e| SqlError::InvalidDateLiteral(format!("{body}: {e}")))?;
    let days = span.get_days() as i64;
    if (i32::MIN as i64..=i32::MAX as i64).contains(&days) {
        Ok(days as i32)
    } else {
        Err(SqlError::InvalidDateLiteral(format!(
            "{body}: date out of i32 range"
        )))
    }
}

/// Parse an ISO 8601 TIME literal (`HH:MM:SS[.ffffff]`).
pub fn parse_time(s: &str) -> Result<i64> {
    let trimmed = s.trim();
    // Accept 24:00:00 as end-of-day sentinel (PG behavior).
    if trimmed == "24:00:00" || trimmed == "24:00:00.000000" {
        return Ok(MICROS_PER_DAY);
    }
    let t = JTime::strptime("%H:%M:%S%.f", trimmed)
        .or_else(|_| JTime::strptime("%H:%M:%S", trimmed))
        .or_else(|_| JTime::strptime("%H:%M", trimmed))
        .map_err(|e| SqlError::InvalidTimeLiteral(format!("{trimmed}: {e}")))?;
    let subsec_micros = (t.subsec_nanosecond() / 1000) as u32;
    hmsn_to_micros(
        t.hour() as u8,
        t.minute() as u8,
        t.second() as u8,
        subsec_micros,
    )
    .ok_or_else(|| SqlError::InvalidTimeLiteral(format!("{trimmed}: out of range")))
}

/// Parse an ISO 8601 TIMESTAMP literal (naive `YYYY-MM-DD[T ]HH:MM:SS[.ffffff]` or with offset/zone).
/// Accepts `Z`, fixed offsets (`+HH:MM`), and IANA zone names (`America/New_York`).
/// `'infinity'` / `'-infinity'` map to sentinel values.
pub fn parse_timestamp(s: &str) -> Result<i64> {
    let trimmed = s.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "infinity" || lower == "+infinity" {
        return Ok(TS_INFINITY_MICROS);
    }
    if lower == "-infinity" {
        return Ok(TS_NEG_INFINITY_MICROS);
    }

    // Strip trailing " BC" (case-insensitive) same as parse_date.
    let (body, is_bc) = if let Some(stripped) = trimmed.strip_suffix(" BC") {
        (stripped.trim_end(), true)
    } else if let Some(stripped) = trimmed.strip_suffix(" bc") {
        (stripped.trim_end(), true)
    } else {
        (trimmed, false)
    };

    // Try fully-qualified (Zoned with IANA zone or offset). BC+zone combos are rare; skip if BC.
    if !is_bc {
        if let Ok(z) = body.parse::<Zoned>() {
            reject_year_zero_ts(z.timestamp().as_microsecond())?;
            return Ok(z.timestamp().as_microsecond());
        }
        // Try as bare RFC 3339 / ISO 8601 with offset / Z.
        if let Ok(ts) = body.parse::<JTimestamp>() {
            reject_year_zero_ts(ts.as_microsecond())?;
            return Ok(ts.as_microsecond());
        }
    }

    // Try as naive wall-clock: interpret as UTC.
    // Accept both "2024-01-15 12:30:00" and "2024-01-15T12:30:00" and variations.
    let parsers = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ];
    for fmt in &parsers {
        if let Ok(dt) = JDateTime::strptime(fmt, body) {
            let adjusted = apply_bc_and_check_year_zero(dt, is_bc, body)?;
            return adjusted
                .to_zoned(TimeZone::UTC)
                .map(|z| z.timestamp().as_microsecond())
                .map_err(|e| SqlError::InvalidTimestampLiteral(format!("{body}: {e}")));
        }
    }
    // Also try "IANA-zone-suffix" parsing: e.g. "2024-01-15 12:00:00 America/New_York".
    if !is_bc {
        if let Some(space_idx) = body.rfind(' ') {
            let (ts_part, zone_part) = body.split_at(space_idx);
            let zone_name = zone_part.trim();
            if let Ok(tz) = TimeZone::get(zone_name) {
                for fmt in &parsers {
                    if let Ok(dt) = JDateTime::strptime(fmt, ts_part.trim()) {
                        if dt.year() == 0 {
                            return Err(SqlError::InvalidTimestampLiteral(
                                "year 0 is not supported; use 'YYYY-MM-DD HH:MM:SS BC' for 1 BC"
                                    .into(),
                            ));
                        }
                        return dt
                            .to_zoned(tz.clone())
                            .map(|z| z.timestamp().as_microsecond())
                            .map_err(|e| {
                                SqlError::InvalidTimestampLiteral(format!("{body}: {e}"))
                            });
                    }
                }
            }
        }
    }
    Err(SqlError::InvalidTimestampLiteral(format!(
        "{trimmed}: unrecognized timestamp format"
    )))
}

fn reject_year_zero_ts(micros: i64) -> Result<()> {
    let date_days = ts_to_date_floor(micros);
    let (y, _, _) = days_to_ymd(date_days);
    if y == 0 {
        return Err(SqlError::InvalidTimestampLiteral(
            "year 0 is not supported; use 'YYYY-MM-DD HH:MM:SS BC' for 1 BC".into(),
        ));
    }
    Ok(())
}

fn apply_bc_and_check_year_zero(dt: JDateTime, is_bc: bool, body: &str) -> Result<JDateTime> {
    if dt.year() == 0 {
        return Err(SqlError::InvalidTimestampLiteral(
            "year 0 is not supported; use 'YYYY-MM-DD HH:MM:SS BC' for 1 BC".into(),
        ));
    }
    if !is_bc {
        return Ok(dt);
    }
    // N BC → astronomical year -(N-1).
    let astro_year = -(dt.year() as i32 - 1);
    let date = JDate::new(astro_year as i16, dt.month(), dt.day())
        .map_err(|e| SqlError::InvalidTimestampLiteral(format!("{body}: {e}")))?;
    let time = JTime::new(dt.hour(), dt.minute(), dt.second(), dt.subsec_nanosecond())
        .map_err(|e| SqlError::InvalidTimestampLiteral(format!("{body}: {e}")))?;
    Ok(JDateTime::from_parts(date, time))
}

/// Parse a SQL INTERVAL literal. Accepts PG verbose form (`'1 year 2 months 3 days 04:05:06.789'`),
/// SQL standard qualified form (`'5' DAY`), and ISO 8601 duration (`'P1Y2M3DT4H5M6S'`).
pub fn parse_interval(s: &str) -> Result<(i32, i32, i64)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(SqlError::InvalidIntervalLiteral("empty interval".into()));
    }

    // Try ISO 8601 duration first.
    if let Some(rest) = trimmed
        .strip_prefix('P')
        .or_else(|| trimmed.strip_prefix('-').and_then(|r| r.strip_prefix('P')))
    {
        let negate = trimmed.starts_with('-');
        return parse_iso8601_duration(rest, negate);
    }

    // PG verbose: "1 year 2 months 3 days 04:05:06.789" (optional @ prefix, `ago` suffix).
    parse_pg_interval(trimmed)
}

fn parse_iso8601_duration(s: &str, global_negate: bool) -> Result<(i32, i32, i64)> {
    // P[nY][nM][nW][nD][T[nH][nM][nS]]
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let mut in_time = false;
    let mut num_buf = String::new();
    let sign = if global_negate { -1i64 } else { 1 };

    for ch in s.chars() {
        if ch == 'T' {
            in_time = true;
            continue;
        }
        if ch.is_ascii_digit() || ch == '.' || ch == '-' {
            num_buf.push(ch);
            continue;
        }
        if num_buf.is_empty() {
            return Err(SqlError::InvalidIntervalLiteral(format!(
                "expected number before '{ch}'"
            )));
        }
        let v: f64 = num_buf
            .parse()
            .map_err(|_| SqlError::InvalidIntervalLiteral(format!("invalid number: {num_buf}")))?;
        num_buf.clear();
        let v_units = sign * v as i64;
        let v_frac_micros = ((v.fract() * 1_000_000.0) as i64) * sign;
        match ch {
            'Y' if !in_time => months = months.saturating_add(v_units * 12),
            'M' if !in_time => months = months.saturating_add(v_units),
            'W' if !in_time => days = days.saturating_add(v_units * 7),
            'D' if !in_time => days = days.saturating_add(v_units),
            'H' if in_time => micros = micros.saturating_add(v_units * MICROS_PER_HOUR),
            'M' if in_time => micros = micros.saturating_add(v_units * MICROS_PER_MIN),
            'S' if in_time => {
                micros = micros.saturating_add(v_units * MICROS_PER_SEC + v_frac_micros)
            }
            _ => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unknown unit '{ch}' (in_time={in_time})"
                )))
            }
        }
    }
    if !num_buf.is_empty() {
        return Err(SqlError::InvalidIntervalLiteral(format!(
            "trailing number without unit: {num_buf}"
        )));
    }
    Ok((clamp_i32(months)?, clamp_i32(days)?, micros))
}

fn parse_pg_interval(s: &str) -> Result<(i32, i32, i64)> {
    let mut s = s.trim().to_ascii_lowercase();
    if let Some(rest) = s.strip_prefix('@') {
        s = rest.trim().to_string();
    }
    let ago = s.ends_with(" ago");
    if ago {
        s.truncate(s.len() - 4);
        s = s.trim().to_string();
    }
    let sign: i64 = if ago { -1 } else { 1 };

    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;

    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        // "HH:MM:SS[.fff]" form.
        if tok.contains(':') {
            let (h, m, sc, us) = parse_hms_token(tok)?;
            let tok_sign = if tok.starts_with('-') { -1 } else { 1 };
            let tok_micros =
                (h * MICROS_PER_HOUR + m * MICROS_PER_MIN + sc * MICROS_PER_SEC + us as i64)
                    * tok_sign
                    * sign;
            micros = micros.saturating_add(tok_micros);
            i += 1;
            continue;
        }

        // "N unit" form.
        let num: f64 = tok.parse().map_err(|_| {
            SqlError::InvalidIntervalLiteral(format!("expected number, got '{tok}'"))
        })?;
        if i + 1 >= tokens.len() {
            return Err(SqlError::InvalidIntervalLiteral(format!(
                "missing unit after '{tok}'"
            )));
        }
        let unit = tokens[i + 1].trim_end_matches(',');
        let v_units = sign * num as i64;
        let v_frac_micros = ((num.fract() * 1_000_000.0) as i64) * sign;
        match unit {
            "year" | "years" | "yr" | "yrs" | "y" => months = months.saturating_add(v_units * 12),
            "month" | "months" | "mon" | "mons" => months = months.saturating_add(v_units),
            "week" | "weeks" | "w" => days = days.saturating_add(v_units * 7),
            "day" | "days" | "d" => days = days.saturating_add(v_units),
            "hour" | "hours" | "hr" | "hrs" | "h" => {
                micros = micros.saturating_add(v_units * MICROS_PER_HOUR)
            }
            "minute" | "minutes" | "min" | "mins" | "m" => {
                micros = micros.saturating_add(v_units * MICROS_PER_MIN)
            }
            "second" | "seconds" | "sec" | "secs" | "s" => {
                micros = micros.saturating_add(v_units * MICROS_PER_SEC + v_frac_micros)
            }
            "millisecond" | "milliseconds" | "ms" => micros = micros.saturating_add(v_units * 1000),
            "microsecond" | "microseconds" | "us" => micros = micros.saturating_add(v_units),
            other => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unknown unit: {other}"
                )))
            }
        }
        i += 2;
    }
    Ok((clamp_i32(months)?, clamp_i32(days)?, micros))
}

fn parse_hms_token(tok: &str) -> Result<(i64, i64, i64, u32)> {
    let tok = tok.trim_start_matches('-').trim_start_matches('+');
    let mut parts = tok.split(':');
    let h: i64 = parts
        .next()
        .ok_or_else(|| SqlError::InvalidIntervalLiteral(format!("bad hms: {tok}")))?
        .parse()
        .map_err(|_| SqlError::InvalidIntervalLiteral(format!("bad hour: {tok}")))?;
    let m: i64 = parts
        .next()
        .ok_or_else(|| SqlError::InvalidIntervalLiteral(format!("bad hms: {tok}")))?
        .parse()
        .map_err(|_| SqlError::InvalidIntervalLiteral(format!("bad minute: {tok}")))?;
    let (sc, us) = if let Some(sec_part) = parts.next() {
        if let Some((s_whole, s_frac)) = sec_part.split_once('.') {
            let s: i64 = s_whole
                .parse()
                .map_err(|_| SqlError::InvalidIntervalLiteral(format!("bad second: {tok}")))?;
            // Pad / truncate fractional to 6 digits.
            let mut frac = s_frac.to_string();
            while frac.len() < 6 {
                frac.push('0');
            }
            frac.truncate(6);
            let us: u32 = frac
                .parse()
                .map_err(|_| SqlError::InvalidIntervalLiteral(format!("bad subsec: {tok}")))?;
            (s, us)
        } else {
            let s: i64 = sec_part
                .parse()
                .map_err(|_| SqlError::InvalidIntervalLiteral(format!("bad second: {tok}")))?;
            (s, 0u32)
        }
    } else {
        (0, 0u32)
    };
    Ok((h, m, sc, us))
}

fn clamp_i32(n: i64) -> Result<i32> {
    if (i32::MIN as i64..=i32::MAX as i64).contains(&n) {
        Ok(n as i32)
    } else {
        Err(SqlError::InvalidIntervalLiteral(format!(
            "interval component overflow: {n}"
        )))
    }
}

pub fn format_date(days: i32) -> String {
    if days == DATE_INFINITY_DAYS {
        return "infinity".to_string();
    }
    if days == DATE_NEG_INFINITY_DAYS {
        return "-infinity".to_string();
    }
    let (y, m, d) = days_to_ymd(days);
    if y >= 1 {
        format!("{y:04}-{m:02}-{d:02}")
    } else {
        // Astronomical year N ≤ 0 → (1 - N) BC; i.e., year 0 = 1 BC, year -1 = 2 BC.
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    }
}

pub fn format_time(micros: i64) -> String {
    if micros == MICROS_PER_DAY {
        return "24:00:00".to_string();
    }
    let (h, m, s, us) = micros_to_hmsn(micros);
    if us == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{us:06}")
    }
}

pub fn format_timestamp(micros: i64) -> String {
    if micros == TS_INFINITY_MICROS {
        return "infinity".to_string();
    }
    if micros == TS_NEG_INFINITY_MICROS {
        return "-infinity".to_string();
    }
    let (date_days, time_micros) = ts_split(micros);
    let date_part = format_date(date_days);
    let time_part = format_time(time_micros);
    format!("{date_part} {time_part}")
}

pub fn format_timestamp_in_zone(micros: i64, zone: &str) -> Result<String> {
    if micros == TS_INFINITY_MICROS {
        return Ok("infinity".to_string());
    }
    if micros == TS_NEG_INFINITY_MICROS {
        return Ok("-infinity".to_string());
    }
    let tz = resolve_timezone(zone)?;
    let ts = JTimestamp::from_microsecond(micros)
        .map_err(|e| SqlError::InvalidTimestampLiteral(format!("{micros}: {e}")))?;
    let z = ts.to_zoned(tz);
    let subsec = z.subsec_nanosecond() / 1000;
    let fmt = if subsec == 0 {
        "%Y-%m-%d %H:%M:%S%:z"
    } else {
        "%Y-%m-%d %H:%M:%S%.6f%:z"
    };
    z.strftime(fmt).to_string().pipe(Ok)
}

/// Accepts IANA names, `Z`, `UTC`, and ISO-8601 fixed offsets; rejects POSIX
/// `UTC+5` shorthand (sign-inverted in POSIX, ambiguous in practice).
pub fn resolve_timezone(zone: &str) -> Result<TimeZone> {
    let trimmed = zone.trim();
    if let Ok(tz) = TimeZone::get(trimmed) {
        return Ok(tz);
    }
    if let Some(offset) = parse_iso_fixed_offset(trimmed) {
        return jiff::tz::Offset::from_seconds(offset)
            .map(TimeZone::fixed)
            .map_err(|e| SqlError::InvalidTimezone(format!("{zone}: {e}")));
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("utc+")
        || lower.starts_with("utc-")
        || lower.starts_with("gmt+")
        || lower.starts_with("gmt-")
    {
        return Err(SqlError::InvalidTimezone(format!(
            "{zone}: ambiguous POSIX form; use ISO-8601 offset like '+05:00' or a named zone"
        )));
    }
    Err(SqlError::InvalidTimezone(format!(
        "{zone}: not a recognized IANA name or ISO-8601 offset"
    )))
}

/// Parse `Z`, `UTC`, `+HH:MM`, `-HH:MM`, `+HHMM`, `+HH` into signed seconds.
fn parse_iso_fixed_offset(s: &str) -> Option<i32> {
    if s.eq_ignore_ascii_case("z") || s.eq_ignore_ascii_case("utc") {
        return Some(0);
    }
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let sign: i32 = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let rest = &s[1..];
    let (hh, mm) = if let Some((h, m)) = rest.split_once(':') {
        (h, m)
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else if rest.len() == 2 {
        (rest, "00")
    } else {
        return None;
    };
    let h: i32 = hh.parse().ok()?;
    let m: i32 = mm.parse().ok()?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    Some(sign * (h * 3600 + m * 60))
}

pub fn format_interval(months: i32, days: i32, micros: i64) -> String {
    if months == 0 && days == 0 && micros == 0 {
        return "00:00:00".to_string();
    }
    let mut parts = Vec::with_capacity(4);
    if months != 0 {
        let years = months / 12;
        let mon = months % 12;
        if years != 0 {
            parts.push(format!(
                "{} year{}",
                years,
                if years.abs() == 1 { "" } else { "s" }
            ));
        }
        if mon != 0 {
            parts.push(format!(
                "{} mon{}",
                mon,
                if mon.abs() == 1 { "" } else { "s" }
            ));
        }
    }
    if days != 0 {
        parts.push(format!(
            "{} day{}",
            days,
            if days.abs() == 1 { "" } else { "s" }
        ));
    }
    if micros != 0 {
        let sign = if micros < 0 { "-" } else { "" };
        let abs_us = micros.unsigned_abs() as i64;
        let (h, m, s, us) = micros_to_hmsn(abs_us);
        if us == 0 {
            parts.push(format!("{sign}{h:02}:{m:02}:{s:02}"));
        } else {
            parts.push(format!("{sign}{h:02}:{m:02}:{s:02}.{us:06}"));
        }
    }
    parts.join(" ")
}

/// Extension to allow `x.pipe(Ok)` chaining for readability.
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}

pub fn now_micros() -> i64 {
    JTimestamp::now().as_microsecond()
}

thread_local! {
    /// Scoped txn-start timestamp for PG-exact CURRENT_TIMESTAMP (stable per txn).
    static TXN_CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

/// Install a txn-start timestamp for the duration of `f`. Restores the previous
/// value on return (nested-safe).
pub fn with_txn_clock<R>(ts: Option<i64>, f: impl FnOnce() -> R) -> R {
    TXN_CLOCK.with(|slot| {
        let prev = slot.replace(ts);
        let r = f();
        slot.set(prev);
        r
    })
}

pub fn set_txn_clock(ts: Option<i64>) {
    TXN_CLOCK.with(|slot| slot.set(ts));
}

/// Read the cached txn-start clock if one is installed, else a fresh `now_micros()`.
/// Used by `NOW` / `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `LOCALTIMESTAMP`.
pub fn txn_or_clock_micros() -> i64 {
    TXN_CLOCK.with(|slot| slot.get()).unwrap_or_else(now_micros)
}

pub fn today_days() -> i32 {
    ts_to_date_floor(now_micros())
}

pub fn current_time_micros() -> i64 {
    ts_split(now_micros()).1
}

pub fn add_interval_to_timestamp(ts: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    if ts == TS_INFINITY_MICROS || ts == TS_NEG_INFINITY_MICROS {
        return Ok(ts);
    }
    let jts =
        JTimestamp::from_microsecond(ts).map_err(|e| SqlError::InvalidValue(format!("ts: {e}")))?;
    // PG order: months, then days, then micros.
    let span = Span::new()
        .try_months(months as i64)
        .map_err(|e| SqlError::InvalidValue(format!("months overflow: {e}")))?
        .try_days(days as i64)
        .map_err(|e| SqlError::InvalidValue(format!("days overflow: {e}")))?
        .try_microseconds(micros)
        .map_err(|e| SqlError::InvalidValue(format!("micros overflow: {e}")))?;
    let result = jts
        .to_zoned(TimeZone::UTC)
        .checked_add(span)
        .map_err(|_| SqlError::IntegerOverflow)?;
    Ok(result.timestamp().as_microsecond())
}

/// PG rule: DATE + INTERVAL always yields TIMESTAMP.
pub fn add_interval_to_date(days: i32, months: i32, i_days: i32, micros: i64) -> Result<i64> {
    if is_infinity_date(days) {
        return Ok(if days == DATE_INFINITY_DAYS {
            TS_INFINITY_MICROS
        } else {
            TS_NEG_INFINITY_MICROS
        });
    }
    let ts = date_to_ts(days);
    add_interval_to_timestamp(ts, months, i_days, micros)
}

pub fn add_days_to_date(days: i32, n: i64) -> Result<i32> {
    if is_infinity_date(days) {
        return Ok(days);
    }
    let new_days = (days as i64)
        .checked_add(n)
        .ok_or(SqlError::IntegerOverflow)?;
    if new_days >= i32::MIN as i64 && new_days <= i32::MAX as i64 {
        Ok(new_days as i32)
    } else {
        Err(SqlError::IntegerOverflow)
    }
}

pub fn add_interval_to_time(t: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    if months != 0 || days != 0 {
        return Err(SqlError::InvalidValue(
            "cannot add month/day interval to TIME".into(),
        ));
    }
    // PG: TIME + interval wraps mod 24h.
    let combined = t.checked_add(micros).unwrap_or(t);
    Ok(combined.rem_euclid(MICROS_PER_DAY))
}

/// PG `timestamp - timestamp`: returns `(days, remainder_micros)` with months = 0.
pub fn subtract_timestamps(a: i64, b: i64) -> (i32, i64) {
    let diff = a.saturating_sub(b);
    let days = (diff / MICROS_PER_DAY) as i32;
    let micros = diff % MICROS_PER_DAY;
    (days, micros)
}

/// AGE(a, b) — symbolic diff preserving months/years. Uses jiff's Span rounding to Year unit.
pub fn age(ts_a: i64, ts_b: i64) -> Result<(i32, i32, i64)> {
    let a = JTimestamp::from_microsecond(ts_a)
        .map_err(|e| SqlError::InvalidValue(format!("ts_a: {e}")))?
        .to_zoned(TimeZone::UTC);
    let b = JTimestamp::from_microsecond(ts_b)
        .map_err(|e| SqlError::InvalidValue(format!("ts_b: {e}")))?
        .to_zoned(TimeZone::UTC);
    let span = a
        .since((Unit::Year, &b))
        .map_err(|e| SqlError::InvalidValue(format!("age: {e}")))?;
    span_to_triple(&span)
}

fn span_to_triple(span: &Span) -> Result<(i32, i32, i64)> {
    let months = i64::from(span.get_years()) * 12 + i64::from(span.get_months());
    let days = i64::from(span.get_weeks()) * 7 + i64::from(span.get_days());
    let micros = i64::from(span.get_hours()) * MICROS_PER_HOUR
        + span.get_minutes() * MICROS_PER_MIN
        + span.get_seconds() * MICROS_PER_SEC
        + span.get_milliseconds() * 1000
        + span.get_microseconds()
        + span.get_nanoseconds() / 1000;
    Ok((clamp_i32(months)?, clamp_i32(days)?, micros))
}

pub fn justify_days(months: i32, days: i32, micros: i64) -> (i32, i32, i64) {
    // Convert every 30 days into 1 month.
    let extra_months = days / 30;
    let rem_days = days % 30;
    let new_months = months.saturating_add(extra_months);
    (new_months, rem_days, micros)
}

pub fn justify_hours(months: i32, days: i32, micros: i64) -> (i32, i32, i64) {
    // Convert every 24 hours into 1 day.
    let extra_days = (micros / MICROS_PER_DAY) as i32;
    let rem_micros = micros % MICROS_PER_DAY;
    let new_days = days.saturating_add(extra_days);
    (months, new_days, rem_micros)
}

pub fn justify_interval(months: i32, days: i32, micros: i64) -> (i32, i32, i64) {
    let (m1, d1, us1) = justify_hours(months, days, micros);
    let (m2, d2, us2) = justify_days(m1, d1, us1);
    (m2, d2, us2)
}

/// PG-normalized total µs for comparison purposes (30-day month, 24-hour day).
pub fn interval_to_total_micros(months: i32, days: i32, micros: i64) -> i128 {
    (months as i128) * 30 * (MICROS_PER_DAY as i128)
        + (days as i128) * (MICROS_PER_DAY as i128)
        + micros as i128
}

pub fn extract(field: &str, v: &Value) -> Result<Value> {
    let f = field.trim();
    match v {
        Value::Null => Ok(Value::Null),
        Value::Date(d) => extract_from_date(f, *d),
        Value::Time(t) => extract_from_time(f, *t),
        Value::Timestamp(t) => extract_from_timestamp(f, *t),
        Value::Interval {
            months,
            days,
            micros,
        } => extract_from_interval(f, *months, *days, *micros),
        _ => Err(SqlError::TypeMismatch {
            expected: "temporal type".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn extract_from_date(field: &str, days: i32) -> Result<Value> {
    if field.eq_ignore_ascii_case("epoch") {
        return Ok(Value::Integer((days as i64) * 86400));
    }
    let (y, m, d) = days_to_ymd(days);
    if field.eq_ignore_ascii_case("year") {
        return Ok(Value::Integer(y as i64));
    }
    if field.eq_ignore_ascii_case("month") {
        return Ok(Value::Integer(m as i64));
    }
    if field.eq_ignore_ascii_case("day") {
        return Ok(Value::Integer(d as i64));
    }
    if field.eq_ignore_ascii_case("hour")
        || field.eq_ignore_ascii_case("minute")
        || field.eq_ignore_ascii_case("second")
        || field.eq_ignore_ascii_case("microseconds")
        || field.eq_ignore_ascii_case("milliseconds")
    {
        return Ok(Value::Integer(0));
    }
    // Fall-through: use a canonical lowercase form for the remaining rare fields.
    let f = field.to_ascii_lowercase();
    match f.as_str() {
        "dow" => {
            let jd = JDate::new(y as i16, m as i8, d as i8)
                .map_err(|e| SqlError::InvalidValue(format!("{e}")))?;
            // Jiff weekday: Monday=1..Sunday=7. PG dow: Sunday=0..Saturday=6.
            let w = jd.weekday().to_monday_one_offset() as i64;
            let dow = if w == 7 { 0 } else { w }; // Sunday: 7 → 0
            Ok(Value::Integer(dow))
        }
        "isodow" => {
            let jd = JDate::new(y as i16, m as i8, d as i8)
                .map_err(|e| SqlError::InvalidValue(format!("{e}")))?;
            Ok(Value::Integer(jd.weekday().to_monday_one_offset() as i64))
        }
        "doy" => {
            let jd = JDate::new(y as i16, m as i8, d as i8)
                .map_err(|e| SqlError::InvalidValue(format!("{e}")))?;
            Ok(Value::Integer(jd.day_of_year() as i64))
        }
        "quarter" => Ok(Value::Integer(((m - 1) / 3 + 1) as i64)),
        "decade" => Ok(Value::Integer((y / 10) as i64)),
        "century" => Ok(Value::Integer(if y > 0 {
            ((y - 1) / 100 + 1) as i64
        } else {
            (y / 100 - 1) as i64
        })),
        "millennium" => Ok(Value::Integer(if y > 0 {
            ((y - 1) / 1000 + 1) as i64
        } else {
            (y / 1000 - 1) as i64
        })),
        "julian" => Ok(Value::Integer(days as i64 + 2_440_588)),
        "week" | "isoyear" => {
            let jd = JDate::new(y as i16, m as i8, d as i8)
                .map_err(|e| SqlError::InvalidValue(format!("{e}")))?;
            let iso = jd.iso_week_date();
            if field == "week" {
                Ok(Value::Integer(iso.week() as i64))
            } else {
                Ok(Value::Integer(iso.year() as i64))
            }
        }
        _ => Err(SqlError::InvalidExtractField(format!("{field} from DATE"))),
    }
}

fn extract_from_time(field: &str, micros: i64) -> Result<Value> {
    let (h, m, s, us) = micros_to_hmsn(micros);
    match field {
        "hour" => Ok(Value::Integer(h as i64)),
        "minute" => Ok(Value::Integer(m as i64)),
        "second" => {
            if us == 0 {
                Ok(Value::Integer(s as i64))
            } else {
                Ok(Value::Real(s as f64 + (us as f64) / 1_000_000.0))
            }
        }
        "microseconds" => Ok(Value::Integer((s as i64) * 1_000_000 + us as i64)),
        "milliseconds" => Ok(Value::Real(s as f64 * 1000.0 + (us as f64) / 1000.0)),
        "epoch" => Ok(Value::Integer(micros / MICROS_PER_SEC)),
        _ => Err(SqlError::InvalidExtractField(format!("{field} from TIME"))),
    }
}

fn extract_from_timestamp(field: &str, ts: i64) -> Result<Value> {
    if field.eq_ignore_ascii_case("hour") {
        return Ok(Value::Integer(
            ts.rem_euclid(MICROS_PER_DAY) / MICROS_PER_HOUR,
        ));
    }
    if field.eq_ignore_ascii_case("minute") {
        return Ok(Value::Integer(
            ts.rem_euclid(MICROS_PER_HOUR) / MICROS_PER_MIN,
        ));
    }
    if field.eq_ignore_ascii_case("epoch") {
        return Ok(Value::Integer(ts / MICROS_PER_SEC));
    }
    let (date_days, time_micros) = ts_split(ts);
    // Date-level fields.
    let date_fields = [
        "year",
        "month",
        "day",
        "dow",
        "isodow",
        "doy",
        "quarter",
        "decade",
        "century",
        "millennium",
        "julian",
        "week",
        "isoyear",
    ];
    if date_fields.iter().any(|&f| field.eq_ignore_ascii_case(f)) {
        return extract_from_date(field, date_days);
    }
    // Time-of-day fields (second, microseconds, milliseconds).
    let time_fields = ["second", "microseconds", "milliseconds"];
    if time_fields.iter().any(|&f| field.eq_ignore_ascii_case(f)) {
        return extract_from_time(field, time_micros);
    }
    Err(SqlError::InvalidExtractField(format!(
        "{field} from TIMESTAMP"
    )))
}

fn extract_from_interval(field: &str, months: i32, days: i32, micros: i64) -> Result<Value> {
    match field {
        "year" => Ok(Value::Integer((months / 12) as i64)),
        "month" => Ok(Value::Integer((months % 12) as i64)),
        "day" => Ok(Value::Integer(days as i64)),
        "hour" => Ok(Value::Integer(micros / MICROS_PER_HOUR)),
        "minute" => Ok(Value::Integer((micros % MICROS_PER_HOUR) / MICROS_PER_MIN)),
        "second" => {
            let rem = micros % MICROS_PER_MIN;
            let sec_part = rem / MICROS_PER_SEC;
            let us_part = rem % MICROS_PER_SEC;
            if us_part == 0 {
                Ok(Value::Integer(sec_part))
            } else {
                Ok(Value::Real(sec_part as f64 + us_part as f64 / 1_000_000.0))
            }
        }
        "microseconds" => Ok(Value::Integer(micros % MICROS_PER_MIN)),
        "epoch" => {
            let total = interval_to_total_micros(months, days, micros);
            Ok(Value::Real(total as f64 / 1_000_000.0))
        }
        _ => Err(SqlError::InvalidExtractField(format!(
            "{field} from INTERVAL"
        ))),
    }
}

pub fn date_trunc(unit: &str, v: &Value) -> Result<Value> {
    let u = unit.trim().to_ascii_lowercase();
    match v {
        Value::Null => Ok(Value::Null),
        Value::Date(d) => date_trunc_date(&u, *d).map(Value::Date),
        Value::Timestamp(t) => date_trunc_timestamp(&u, *t).map(Value::Timestamp),
        Value::Time(t) => date_trunc_time(&u, *t).map(Value::Time),
        Value::Interval {
            months,
            days,
            micros,
        } => date_trunc_interval(&u, *months, *days, *micros).map(|(m, d, us)| Value::Interval {
            months: m,
            days: d,
            micros: us,
        }),
        _ => Err(SqlError::TypeMismatch {
            expected: "temporal type".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn date_trunc_date(unit: &str, days: i32) -> Result<i32> {
    if is_infinity_date(days) {
        return Ok(days);
    }
    let (y, m, d) = days_to_ymd(days);
    match unit {
        "microseconds" | "milliseconds" | "second" | "minute" | "hour" | "day" => Ok(days),
        "week" => {
            // Monday-based ISO 8601.
            let jd = JDate::new(y as i16, m as i8, d as i8)
                .map_err(|e| SqlError::InvalidValue(format!("{e}")))?;
            let dow = jd.weekday().to_monday_one_offset() as i32; // 1=Mon..7=Sun
            add_days_to_date(days, -(dow - 1) as i64)
        }
        "month" => {
            ymd_to_days(y, m, 1).ok_or_else(|| SqlError::InvalidValue("date_trunc month".into()))
        }
        "quarter" => {
            let qm = ((m - 1) / 3) * 3 + 1;
            ymd_to_days(y, qm, 1).ok_or_else(|| SqlError::InvalidValue("date_trunc quarter".into()))
        }
        "year" => {
            ymd_to_days(y, 1, 1).ok_or_else(|| SqlError::InvalidValue("date_trunc year".into()))
        }
        "decade" => ymd_to_days(y - (y % 10), 1, 1)
            .ok_or_else(|| SqlError::InvalidValue("date_trunc decade".into())),
        "century" => {
            let cy = if y > 0 {
                ((y - 1) / 100) * 100 + 1
            } else {
                (y / 100) * 100 - 99
            };
            ymd_to_days(cy, 1, 1).ok_or_else(|| SqlError::InvalidValue("date_trunc century".into()))
        }
        "millennium" => {
            let my = if y > 0 {
                ((y - 1) / 1000) * 1000 + 1
            } else {
                (y / 1000) * 1000 - 999
            };
            ymd_to_days(my, 1, 1)
                .ok_or_else(|| SqlError::InvalidValue("date_trunc millennium".into()))
        }
        _ => Err(SqlError::InvalidDateTruncUnit(unit.into())),
    }
}

fn date_trunc_timestamp(unit: &str, ts: i64) -> Result<i64> {
    if is_infinity_ts(ts) {
        return Ok(ts);
    }
    let (date_days, time_micros) = ts_split(ts);
    // time_micros is in 0..MICROS_PER_DAY (ts_split uses div_euclid), so `% unit_size` works.
    match unit {
        "microseconds" => Ok(ts),
        "milliseconds" => Ok(ts_combine(date_days, time_micros - time_micros % 1000)),
        "second" => Ok(ts_combine(
            date_days,
            time_micros - time_micros % MICROS_PER_SEC,
        )),
        "minute" => Ok(ts_combine(
            date_days,
            time_micros - time_micros % MICROS_PER_MIN,
        )),
        "hour" => Ok(ts_combine(
            date_days,
            time_micros - time_micros % MICROS_PER_HOUR,
        )),
        "day" => Ok(ts_combine(date_days, 0)),
        _ => {
            // Weekly+ units delegate to date-level truncation (time zeroed).
            let trunc_days = date_trunc_date(unit, date_days)?;
            Ok(ts_combine(trunc_days, 0))
        }
    }
}

fn date_trunc_time(unit: &str, micros: i64) -> Result<i64> {
    match unit {
        "microseconds" => Ok(micros),
        "milliseconds" => Ok(micros - (micros % 1000)),
        "second" => Ok(micros - (micros % MICROS_PER_SEC)),
        "minute" => Ok(micros - (micros % MICROS_PER_MIN)),
        "hour" => Ok(micros - (micros % MICROS_PER_HOUR)),
        _ => Err(SqlError::InvalidDateTruncUnit(format!(
            "{unit} is invalid for TIME"
        ))),
    }
}

fn date_trunc_interval(unit: &str, months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    match unit {
        "microseconds" => Ok((months, days, micros)),
        "milliseconds" => Ok((months, days, micros - (micros % 1000))),
        "second" => Ok((months, days, micros - (micros % MICROS_PER_SEC))),
        "minute" => Ok((months, days, micros - (micros % MICROS_PER_MIN))),
        "hour" => Ok((months, days, micros - (micros % MICROS_PER_HOUR))),
        "day" => Ok((months, days, 0)),
        "month" => Ok((months, 0, 0)),
        "year" => Ok(((months / 12) * 12, 0, 0)),
        "quarter" => Ok(((months / 3) * 3, 0, 0)),
        "decade" => Ok(((months / 120) * 120, 0, 0)),
        "century" => Ok(((months / 1200) * 1200, 0, 0)),
        "millennium" => Ok(((months / 12000) * 12000, 0, 0)),
        _ => Err(SqlError::InvalidDateTruncUnit(unit.into())),
    }
}

pub fn strftime(fmt: &str, v: &Value) -> Result<String> {
    let ts_micros = match v {
        Value::Null => return Ok(String::new()),
        Value::Timestamp(t) => *t,
        Value::Date(d) => date_to_ts(*d),
        Value::Time(t) => *t, // time-only: use epoch date as anchor
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "temporal type".into(),
                got: v.data_type().to_string(),
            })
        }
    };
    let z = JTimestamp::from_microsecond(ts_micros)
        .map_err(|e| SqlError::InvalidValue(format!("ts: {e}")))?
        .to_zoned(TimeZone::UTC);
    // Rewrite %J, %f, %s — not supported by jiff — before formatting.
    let mut prepared = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.peek() {
                Some('J') => {
                    chars.next();
                    // Julian Day 2440587.5 = 1970-01-01 00:00:00 UTC (Julian days start at noon).
                    let julian = ts_to_date_floor(ts_micros) as f64
                        + 2_440_587.5
                        + (ts_split(ts_micros).1 as f64) / (MICROS_PER_DAY as f64);
                    prepared.push_str(&format!("{julian}"));
                }
                Some('f') => {
                    chars.next();
                    let subsec = ts_split(ts_micros).1 % MICROS_PER_SEC;
                    prepared.push_str(&format!("{:06}", subsec));
                }
                Some('s') => {
                    chars.next();
                    prepared.push_str(&format!("{}", ts_micros / MICROS_PER_SEC));
                }
                Some(&next) => {
                    prepared.push('%');
                    prepared.push(next);
                    chars.next();
                }
                None => prepared.push('%'),
            }
        } else {
            prepared.push(c);
        }
    }
    Ok(z.strftime(&prepared).to_string())
}

/// Session-agnostic util used by eval.rs for SQL INTERVAL comparison normalization.
pub fn pg_normalized_interval_cmp(a: (i32, i32, i64), b: (i32, i32, i64)) -> std::cmp::Ordering {
    let at = interval_to_total_micros(a.0, a.1, a.2);
    let bt = interval_to_total_micros(b.0, b.1, b.2);
    at.cmp(&bt)
}

#[cfg(test)]
mod tests {
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
        // -1 µs is 1969-12-31 23:59:59.999999.
        let (d, t) = ts_split(-1);
        assert_eq!(d, -1); // day = 1969-12-31
        assert_eq!(t, MICROS_PER_DAY - 1);
    }

    #[test]
    fn parse_format_date_roundtrip() {
        let d = parse_date("2024-01-15").unwrap();
        assert_eq!(format_date(d), "2024-01-15");
    }

    #[test]
    fn parse_date_bc() {
        // "0001-01-01 BC" = astronomical year 0 = 1 day before "0001-01-01"
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
        // AD 0001-01-01 at midnight minus 1 day should equal BC 0001-12-31 at midnight.
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
        // Jan 31 + 1 month = Feb 29 (leap year 2024).
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
        // 1 month == 30 days.
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
        // 2024-01-07 is a Sunday.
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
        // 2024-01-07 is Sunday; trunc week → 2024-01-01 (Monday).
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
        // 2024-04-10 - 2024-01-01 = 3 months 9 days.
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
        // Astronomical year 0 = 1 BC in PG convention.
        let bc1 = parse_date("0001-01-01 BC").unwrap();
        assert_eq!(format_date(bc1), "0001-01-01 BC");
    }
}
