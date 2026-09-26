//! Date/Time/Timestamp/Interval support for Citadel SQL.
//!
//! Thin wrapper around [`jiff`] so the rest of the codebase doesn't depend on it
//! directly. Timestamps are UTC microseconds since 1970-01-01. INTERVAL is
//! PG-compatible: `(months: i32, days: i32, micros: i64)`.

use crate::error::{Result, SqlError};
use crate::types::Value;
use jiff::civil::{Date as JDate, DateTime as JDateTime, Time as JTime};
use jiff::tz::TimeZone;
use jiff::{Span, Timestamp as JTimestamp, Unit, Zoned};

pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
pub const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MIN;
pub const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const MAX_TIMEZONE_OFFSET_SECONDS: i32 = 15 * 3_600 + 59 * 60 + 59;

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

/// Days from 0000-03-01 to 1970-01-01. The civil conversions count 400-year
/// eras of 146,097 days from 0000-03-01, so a leap day ends each counted year.
const EPOCH_FROM_MARCH_ZERO: i64 = 719_468;
const DAYS_PER_ERA: i64 = 146_097;

/// Civil Gregorian (year, month, day) of a day count since 1970, with
/// astronomical years (0 is 1 BC). Exact for every i64 day that fits.
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let z = days + EPOCH_FROM_MARCH_ZERO;
    let era = z.div_euclid(DAYS_PER_ERA);
    let day_of_era = z.rem_euclid(DAYS_PER_ERA);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = era * 400 + year_of_era + i64::from(month <= 2);
    (year, month as u8, day as u8)
}

/// Day count since 1970 of a civil Gregorian date whose month and day are valid.
fn days_from_civil(year: i64, month: u8, day: u8) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_from_march = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let day_of_year = (153 * month_from_march + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * DAYS_PER_ERA + day_of_era - EPOCH_FROM_MARCH_ZERO
}

/// Convert i32 days-since-1970 to civil Gregorian (year, month, day), with
/// astronomical years. Total: every i32 day has a calendar date.
pub fn days_to_ymd(days: i32) -> (i32, u8, u8) {
    let (year, month, day) = civil_from_days(i64::from(days));
    // |days| / 365 bounds the year well inside i32.
    (year as i32, month, day)
}

/// Convert (year, month, day) Gregorian to i32 days-since-1970. `None` for a
/// day that does not exist, or one outside the finite DATE range.
pub fn ymd_to_days(y: i32, m: u8, d: u8) -> Option<i32> {
    if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        return None;
    }
    i32::try_from(days_from_civil(i64::from(y), m, d))
        .ok()
        .filter(|days| !is_infinity_date(*days))
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

/// Convert a date to a timestamp at midnight UTC; an infinite date is the
/// infinite timestamp of the same sign.
pub fn date_to_ts(days: i32) -> Result<i64> {
    match days {
        DATE_INFINITY_DAYS => Ok(TS_INFINITY_MICROS),
        DATE_NEG_INFINITY_DAYS => Ok(TS_NEG_INFINITY_MICROS),
        _ => i64::from(days)
            .checked_mul(MICROS_PER_DAY)
            .ok_or_else(|| SqlError::InvalidValue("date out of range for timestamp".into())),
    }
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
    let mut fields = IntervalFields::default();
    let mut in_time = false;
    let mut num_buf = String::new();

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
        let (whole, frac) = interval_number(&num_buf)?;
        num_buf.clear();
        let added = match ch {
            'Y' if !in_time => fields.years(whole, frac),
            'M' if !in_time => fields.months(whole, frac),
            'W' if !in_time => fields.days(whole, frac, 7),
            'D' if !in_time => fields.days(whole, frac, 1),
            'H' if in_time => fields.micros(whole, frac, MICROS_PER_HOUR),
            'M' if in_time => fields.micros(whole, frac, MICROS_PER_MIN),
            'S' if in_time => fields.micros(whole, frac, MICROS_PER_SEC),
            _ => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unknown unit '{ch}' (in_time={in_time})"
                )))
            }
        };
        added.ok_or_else(|| interval_out_of_range(s))?;
    }
    if !num_buf.is_empty() {
        return Err(SqlError::InvalidIntervalLiteral(format!(
            "trailing number without unit: {num_buf}"
        )));
    }
    if global_negate {
        fields.negate().ok_or_else(|| interval_out_of_range(s))?;
    }
    fields.finish().ok_or_else(|| interval_out_of_range(s))
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

    let mut fields = IntervalFields::default();
    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        // "HH:MM:SS[.fff]" form.
        if tok.contains(':') {
            let clock = interval_clock(tok)?;
            fields
                .micros(clock, 0.0, 1)
                .ok_or_else(|| interval_out_of_range(&s))?;
            i += 1;
            continue;
        }

        // "N unit" form.
        let (whole, frac) = interval_number(tok)?;
        let Some(unit) = tokens.get(i + 1) else {
            return Err(SqlError::InvalidIntervalLiteral(format!(
                "missing unit after '{tok}'"
            )));
        };
        let added = match unit.trim_end_matches(',') {
            "year" | "years" | "yr" | "yrs" | "y" => fields.years(whole, frac),
            "month" | "months" | "mon" | "mons" => fields.months(whole, frac),
            "week" | "weeks" | "w" => fields.days(whole, frac, 7),
            "day" | "days" | "d" => fields.days(whole, frac, 1),
            "hour" | "hours" | "hr" | "hrs" | "h" => fields.micros(whole, frac, MICROS_PER_HOUR),
            "minute" | "minutes" | "min" | "mins" | "m" => {
                fields.micros(whole, frac, MICROS_PER_MIN)
            }
            "second" | "seconds" | "sec" | "secs" | "s" => {
                fields.micros(whole, frac, MICROS_PER_SEC)
            }
            "millisecond" | "milliseconds" | "ms" => fields.micros(whole, frac, 1000),
            "microsecond" | "microseconds" | "us" => fields.micros(whole, frac, 1),
            other => {
                return Err(SqlError::InvalidIntervalLiteral(format!(
                    "unknown unit: {other}"
                )))
            }
        };
        added.ok_or_else(|| interval_out_of_range(&s))?;
        i += 2;
    }
    if ago {
        fields.negate().ok_or_else(|| interval_out_of_range(&s))?;
    }
    fields.finish().ok_or_else(|| interval_out_of_range(&s))
}

fn interval_out_of_range(input: &str) -> SqlError {
    SqlError::InvalidIntervalLiteral(format!("field value out of range: {input}"))
}

/// An interval assembled field by field as PostgreSQL's `DecodeInterval` does:
/// years, months and days in whole numbers, the rest in microseconds, each step
/// checked for overflow. A field's fraction spills into the smaller fields.
#[derive(Default)]
struct IntervalFields {
    years: i32,
    months: i32,
    days: i32,
    micros: i64,
}

impl IntervalFields {
    /// Years; the fraction rounds to whole months.
    fn years(&mut self, whole: i64, frac: f64) -> Option<()> {
        self.years = self.years.checked_add(i32::try_from(whole).ok()?)?;
        let extra = (frac * 12.0).round_ties_even() as i32;
        self.months = self.months.checked_add(extra)?;
        Some(())
    }

    /// Months; the fraction counts in 30-day months.
    fn months(&mut self, whole: i64, frac: f64) -> Option<()> {
        self.months = self.months.checked_add(i32::try_from(whole).ok()?)?;
        self.fract_days(frac, 30)
    }

    /// Units of `scale` days (7 for weeks); the fraction counts in 24-hour days.
    fn days(&mut self, whole: i64, frac: f64, scale: i32) -> Option<()> {
        let days = i32::try_from(whole).ok()?.checked_mul(scale)?;
        self.days = self.days.checked_add(days)?;
        self.fract_days(frac, scale)
    }

    /// Units of `scale` microseconds; the fraction rounds to a microsecond.
    fn micros(&mut self, whole: i64, frac: f64, scale: i64) -> Option<()> {
        self.micros = self.micros.checked_add(whole.checked_mul(scale)?)?;
        self.fract_micros(frac, scale)
    }

    fn fract_days(&mut self, frac: f64, scale: i32) -> Option<()> {
        let days = frac * f64::from(scale);
        // A fraction is below one, so its whole days fit.
        let whole = days as i32;
        self.days = self.days.checked_add(whole)?;
        self.fract_micros(days - f64::from(whole), MICROS_PER_DAY)
    }

    /// A half microsecond rounds toward zero, as in PostgreSQL.
    fn fract_micros(&mut self, frac: f64, scale: i64) -> Option<()> {
        let micros = frac * scale as f64;
        let mut whole = micros as i64;
        let rest = micros - whole as f64;
        if rest > 0.5 {
            whole += 1;
        } else if rest < -0.5 {
            whole -= 1;
        }
        self.micros = self.micros.checked_add(whole)?;
        Some(())
    }

    fn negate(&mut self) -> Option<()> {
        self.years = self.years.checked_neg()?;
        self.months = self.months.checked_neg()?;
        self.days = self.days.checked_neg()?;
        self.micros = self.micros.checked_neg()?;
        Some(())
    }

    fn finish(self) -> Option<(i32, i32, i64)> {
        let months = i64::from(self.years) * 12 + i64::from(self.months);
        Some((i32::try_from(months).ok()?, self.days, self.micros))
    }
}

/// A field's number split as PostgreSQL splits it: the whole part exactly and
/// the fraction apart, both carrying the number's sign.
fn interval_number(token: &str) -> Result<(i64, f64)> {
    let invalid = || SqlError::InvalidIntervalLiteral(format!("invalid number: {token}"));
    let unsigned = token.strip_prefix(['-', '+']).unwrap_or(token);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if whole.len() + fraction.len() == 0 || !digits(whole) || !digits(fraction) {
        return Err(invalid());
    }
    let whole = if whole.is_empty() {
        0
    } else {
        // Parsed with its sign, so the most negative value fits.
        token[..token.len() - unsigned.len() + whole.len()]
            .parse::<i64>()
            .map_err(|_| interval_out_of_range(token))?
    };
    let fraction = if fraction.is_empty() {
        0.0
    } else {
        let fraction: f64 = format!("0.{fraction}").parse().map_err(|_| invalid())?;
        if token.starts_with('-') {
            -fraction
        } else {
            fraction
        }
    };
    Ok((whole, fraction))
}

/// A clock field `[+-]h:m[:s[.f]]` in microseconds, its sign applying to the
/// whole field. Minutes run to 59 and seconds to 60, as in PostgreSQL.
fn interval_clock(token: &str) -> Result<i64> {
    let invalid = || SqlError::InvalidIntervalLiteral(format!("invalid time field: {token}"));
    let unsigned = token.strip_prefix(['-', '+']).unwrap_or(token);
    let mut parts = unsigned.split(':');
    let (Some(hours), Some(minutes), seconds, None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    let (seconds, fraction) = seconds.map_or(("0", ""), |s| s.split_once('.').unwrap_or((s, "")));
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(hours)
        || !digits(minutes)
        || !digits(seconds)
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    let out_of_range = || interval_out_of_range(token);
    let hours: i64 = hours.parse().map_err(|_| out_of_range())?;
    let minutes: i64 = minutes.parse().map_err(|_| out_of_range())?;
    let seconds: i64 = seconds.parse().map_err(|_| out_of_range())?;
    if minutes > 59 || seconds > 60 {
        return Err(out_of_range());
    }
    let fraction = if fraction.is_empty() {
        0
    } else {
        let fraction: f64 = format!("0.{fraction}").parse().map_err(|_| invalid())?;
        (fraction * MICROS_PER_SEC as f64).round_ties_even() as i64
    };
    let micros = hours
        .checked_mul(MICROS_PER_HOUR)
        .and_then(|hours| {
            hours.checked_add(minutes * MICROS_PER_MIN + seconds * MICROS_PER_SEC + fraction)
        })
        .ok_or_else(out_of_range)?;
    Ok(if token.starts_with('-') {
        -micros
    } else {
        micros
    })
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
    // The era follows the time, as PostgreSQL writes it and parse_timestamp reads it.
    match date_part.strip_suffix(" BC") {
        Some(date) => format!("{date} {time_part} BC"),
        None => format!("{date_part} {time_part}"),
    }
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
        return fixed_timezone(offset)
            .map_err(|error| SqlError::InvalidTimezone(format!("{zone}: {error}")));
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

pub fn fixed_timezone(offset_seconds: i32) -> Result<TimeZone> {
    if offset_seconds.unsigned_abs() > MAX_TIMEZONE_OFFSET_SECONDS as u32 {
        return Err(SqlError::InvalidTimezone(format!(
            "UTC offset {} is outside PostgreSQL's -15:59:59..+15:59:59 range",
            format_timezone_offset(offset_seconds)
        )));
    }
    jiff::tz::Offset::from_seconds(offset_seconds)
        .map(TimeZone::fixed)
        .map_err(|error| SqlError::InvalidTimezone(error.to_string()))
}

pub fn format_timezone_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let absolute = offset_seconds.unsigned_abs();
    let hours = absolute / 3_600;
    let minutes = (absolute % 3_600) / 60;
    let seconds = absolute % 60;
    if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

/// Parse `Z`, `UTC`, `+HH:MM[:SS]`, `-HH:MM[:SS]`, `+HHMM`, or `+HH`.
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
    let (hh, mm, ss) = if rest.contains(':') {
        let mut fields = rest.split(':');
        let hours = fields.next()?;
        let minutes = fields.next()?;
        let seconds = fields.next();
        if seconds.is_some_and(str::is_empty) || fields.next().is_some() {
            return None;
        }
        (hours, minutes, seconds.unwrap_or("00"))
    } else if rest.len() == 4 {
        // len() counts bytes, so a multi-byte char can put index 2 mid-character.
        let (hours, minutes) = rest.split_at_checked(2)?;
        (hours, minutes, "00")
    } else if rest.len() == 2 {
        (rest, "00", "00")
    } else {
        return None;
    };
    let h: i32 = hh.parse().ok()?;
    let m: i32 = mm.parse().ok()?;
    let s: i32 = ss.parse().ok()?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) || !(0..=59).contains(&s) {
        return None;
    }
    Some(sign * (h * 3600 + m * 60 + s))
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
        let abs_us = micros.unsigned_abs();
        // An interval's hours are not bounded by a day.
        let h = abs_us / MICROS_PER_HOUR as u64;
        let (_, m, s, us) = micros_to_hmsn((abs_us % MICROS_PER_HOUR as u64) as i64);
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
    /// Scoped statement-start timestamp for PG-exact STATEMENT_TIMESTAMP.
    static STATEMENT_CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
    /// Session time zone for current-date/time functions evaluated by the executor.
    static SESSION_TIMEZONE: std::cell::RefCell<Option<TimeZone>> = const {
        std::cell::RefCell::new(None)
    };
}

/// Install a txn-start timestamp for the duration of `f`.
pub fn with_txn_clock<R>(ts: Option<i64>, f: impl FnOnce() -> R) -> R {
    struct Guard(Option<i64>);

    impl Drop for Guard {
        fn drop(&mut self) {
            TXN_CLOCK.with(|slot| slot.set(self.0));
        }
    }

    let previous = TXN_CLOCK.with(|slot| slot.replace(ts));
    let _guard = Guard(previous);
    f()
}

/// Install a statement-start timestamp for the duration of `f`.
pub fn with_statement_clock<R>(ts: Option<i64>, f: impl FnOnce() -> R) -> R {
    struct Guard(Option<i64>);

    impl Drop for Guard {
        fn drop(&mut self) {
            STATEMENT_CLOCK.with(|slot| slot.set(self.0));
        }
    }

    let previous = STATEMENT_CLOCK.with(|slot| slot.replace(ts));
    let _guard = Guard(previous);
    f()
}

/// Install the connection's session time zone for the duration of `f`.
pub fn with_session_timezone<R>(timezone: TimeZone, f: impl FnOnce() -> R) -> R {
    struct Guard(Option<TimeZone>);

    impl Drop for Guard {
        fn drop(&mut self) {
            SESSION_TIMEZONE.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }

    let previous = SESSION_TIMEZONE.with(|slot| slot.borrow_mut().replace(timezone));
    let _guard = Guard(previous);
    f()
}

#[cfg(test)]
pub fn set_txn_clock(ts: Option<i64>) {
    TXN_CLOCK.with(|slot| slot.set(ts));
}

/// Read the cached txn-start clock if one is installed, else a fresh `now_micros()`.
/// Used by `NOW` / `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `LOCALTIMESTAMP`.
pub fn txn_or_clock_micros() -> i64 {
    TXN_CLOCK.with(|slot| slot.get()).unwrap_or_else(now_micros)
}

/// Read the statement-start clock if one is installed, else a fresh clock.
pub fn statement_or_clock_micros() -> i64 {
    STATEMENT_CLOCK
        .with(|slot| slot.get())
        .unwrap_or_else(now_micros)
}

fn session_timezone() -> TimeZone {
    SESSION_TIMEZONE.with(|slot| slot.borrow().clone().unwrap_or(TimeZone::UTC))
}

fn local_parts(micros: i64) -> Result<(i32, i64)> {
    let timestamp = JTimestamp::from_microsecond(micros)
        .map_err(|error| SqlError::InvalidTimestampLiteral(error.to_string()))?;
    let zoned = timestamp.to_zoned(session_timezone());
    let date = ymd_to_days(zoned.year() as i32, zoned.month() as u8, zoned.day() as u8)
        .ok_or(SqlError::IntegerOverflow)?;
    let time = hmsn_to_micros(
        zoned.hour() as u8,
        zoned.minute() as u8,
        zoned.second() as u8,
        (zoned.subsec_nanosecond() / 1_000) as u32,
    )
    .ok_or(SqlError::IntegerOverflow)?;
    Ok((date, time))
}

/// Transaction-start date in the current session time zone.
pub fn current_date_days() -> Result<i32> {
    local_parts(txn_or_clock_micros()).map(|(date, _)| date)
}

/// Transaction-start local time in the current session time zone.
pub fn current_local_time_micros() -> Result<i64> {
    local_parts(txn_or_clock_micros()).map(|(_, time)| time)
}

/// Transaction-start local timestamp, represented as a zone-less wall clock.
pub fn current_local_timestamp_micros() -> Result<i64> {
    let (date, time) = local_parts(txn_or_clock_micros())?;
    (date as i64)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|value| value.checked_add(time))
        .ok_or(SqlError::IntegerOverflow)
}

pub fn round_time_precision(micros: i64, precision: u32) -> Result<i64> {
    if precision > 6 {
        return Err(SqlError::InvalidValue(format!(
            "time precision {precision} must be between 0 and 6"
        )));
    }
    let quantum = 10_i64.pow(6 - precision);
    if quantum == 1 {
        return Ok(micros);
    }
    let half = quantum / 2;
    let adjusted = if micros >= 0 {
        micros.checked_add(half)
    } else {
        micros.checked_sub(half)
    }
    .ok_or(SqlError::IntegerOverflow)?;
    Ok(adjusted / quantum * quantum)
}

pub fn today_days() -> Result<i32> {
    current_date_days()
}

pub fn current_time_micros() -> Result<i64> {
    current_local_time_micros()
}

pub fn add_interval_to_timestamp(ts: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    if ts == TS_INFINITY_MICROS || ts == TS_NEG_INFINITY_MICROS {
        return Ok(ts);
    }
    let jts =
        JTimestamp::from_microsecond(ts).map_err(|e| SqlError::InvalidValue(format!("ts: {e}")))?;
    // PG order: the months (keeping the day inside the month), then the days,
    // then the time, each with its own sign. One jiff span has a single sign,
    // so '1 month -1 day' cannot be one span.
    let mut zoned = jts.to_zoned(TimeZone::UTC);
    if months != 0 {
        let span = Span::new()
            .try_months(months as i64)
            .map_err(|e| SqlError::InvalidValue(format!("months overflow: {e}")))?;
        zoned = zoned
            .checked_add(span)
            .map_err(|_| SqlError::IntegerOverflow)?;
    }
    if days != 0 {
        let span = Span::new()
            .try_days(days as i64)
            .map_err(|e| SqlError::InvalidValue(format!("days overflow: {e}")))?;
        zoned = zoned
            .checked_add(span)
            .map_err(|_| SqlError::IntegerOverflow)?;
    }
    let result = zoned
        .timestamp()
        .as_microsecond()
        .checked_add(micros)
        .ok_or(SqlError::IntegerOverflow)?;
    JTimestamp::from_microsecond(result).map_err(|_| SqlError::IntegerOverflow)?;
    Ok(result)
}

/// PG rule: DATE + INTERVAL always yields TIMESTAMP.
pub fn add_interval_to_date(days: i32, months: i32, i_days: i32, micros: i64) -> Result<i64> {
    add_interval_to_timestamp(date_to_ts(days)?, months, i_days, micros)
}

/// DATE ± INTEGER: an infinite date stays infinite, and a finite result must
/// be a finite date.
pub fn add_days_to_date(days: i32, n: i64) -> Result<i32> {
    if is_infinity_date(days) {
        return Ok(days);
    }
    i64::from(days)
        .checked_add(n)
        .and_then(|sum| i32::try_from(sum).ok())
        .filter(|sum| !is_infinity_date(*sum))
        .ok_or_else(|| SqlError::InvalidValue("date out of range".into()))
}

/// DATE - DATE in days; an infinite date has no finite difference.
pub fn subtract_dates(a: i32, b: i32) -> Result<i64> {
    if is_infinity_date(a) || is_infinity_date(b) {
        return Err(SqlError::InvalidValue(
            "cannot subtract infinite dates".into(),
        ));
    }
    Ok(i64::from(a) - i64::from(b))
}

pub fn add_interval_to_time(t: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    if months != 0 || days != 0 {
        return Err(SqlError::InvalidValue(
            "cannot add month/day interval to TIME".into(),
        ));
    }
    // PG: TIME + interval wraps mod 24h.
    let combined = i128::from(t) + i128::from(micros);
    Ok(combined.rem_euclid(i128::from(MICROS_PER_DAY)) as i64)
}

/// PG `timestamp - timestamp`: returns `(days, remainder_micros)` with months = 0.
/// An infinite timestamp has no finite difference.
pub fn subtract_timestamps(a: i64, b: i64) -> Result<(i32, i64)> {
    if is_infinity_ts(a) || is_infinity_ts(b) {
        return Err(infinite_timestamps_difference());
    }
    let diff = a.checked_sub(b).ok_or_else(interval_overflow)?;
    // At most i64::MAX / MICROS_PER_DAY, about 1.07e8 days, which fits i32.
    Ok(((diff / MICROS_PER_DAY) as i32, diff % MICROS_PER_DAY))
}

fn infinite_timestamps_difference() -> SqlError {
    SqlError::InvalidValue("cannot subtract infinite timestamps".into())
}

/// AGE(a, b): `a - b` field by field, as PostgreSQL's timestamp_age computes
/// it. A negative time borrows a day, a negative day the length of the earlier
/// timestamp's month, and a negative month a year.
pub fn age(ts_a: i64, ts_b: i64) -> Result<(i32, i32, i64)> {
    if is_infinity_ts(ts_a) || is_infinity_ts(ts_b) {
        return Err(infinite_timestamps_difference());
    }
    let (days_a, time_a) = ts_split(ts_a);
    let (days_b, time_b) = ts_split(ts_b);
    let (year_a, month_a, day_a) = days_to_ymd(days_a);
    let (year_b, month_b, day_b) = days_to_ymd(days_b);
    // The later minus the earlier, borrowing as needed; the sign returns last.
    let sign: i64 = if ts_a < ts_b { -1 } else { 1 };
    let (earlier_year, earlier_month) = if sign < 0 {
        (year_a, month_a)
    } else {
        (year_b, month_b)
    };
    let mut micros = sign * (time_a - time_b);
    let mut days = sign * (i64::from(day_a) - i64::from(day_b));
    let mut months = sign * (i64::from(month_a) - i64::from(month_b));
    let mut years = sign * (i64::from(year_a) - i64::from(year_b));
    if micros < 0 {
        micros += MICROS_PER_DAY;
        days -= 1;
    }
    while days < 0 {
        days += i64::from(days_in_month(earlier_year, earlier_month));
        months -= 1;
    }
    while months < 0 {
        months += 12;
        years -= 1;
    }
    let months = i32::try_from(sign * (years * 12 + months)).map_err(|_| interval_overflow())?;
    // Borrowing leaves fewer days than a month and less time than a day.
    Ok((months, (sign * days) as i32, sign * micros))
}

/// Days in `month` of the astronomical `year`, in the proleptic Gregorian calendar.
fn days_in_month(year: i32, month: u8) -> u8 {
    let leap = year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 31,
    }
}

/// An interval result whose fields leave their range.
pub fn interval_overflow() -> SqlError {
    SqlError::InvalidValue("interval out of range".into())
}

/// PostgreSQL's justify_days: whole 30-day periods become months, and the
/// days take the months' sign.
pub fn justify_days(months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    let mut months = months
        .checked_add(days / 30)
        .ok_or_else(interval_overflow)?;
    let mut days = days % 30;
    if months > 0 && days < 0 {
        days += 30;
        months -= 1;
    } else if months < 0 && days > 0 {
        days -= 30;
        months += 1;
    }
    Ok((months, days, micros))
}

/// PostgreSQL's justify_hours: whole 24-hour periods become days, and the
/// time takes the days' sign.
pub fn justify_hours(months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    // Whole days of an i64 of microseconds stay far inside i32.
    let mut days = days
        .checked_add((micros / MICROS_PER_DAY) as i32)
        .ok_or_else(interval_overflow)?;
    let mut micros = micros % MICROS_PER_DAY;
    if days > 0 && micros < 0 {
        micros += MICROS_PER_DAY;
        days -= 1;
    } else if days < 0 && micros > 0 {
        micros -= MICROS_PER_DAY;
        days += 1;
    }
    Ok((months, days, micros))
}

/// PostgreSQL's justify_interval: whole days become months and whole 24-hour
/// periods days, then every field takes one sign.
pub fn justify_interval(months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    let (mut months, mut days, mut micros) = (months, days, micros);
    // Days that share the time's sign are justified first, so adding the
    // time's whole days below cannot overflow; opposite signs cannot either.
    if (days > 0 && micros > 0) || (days < 0 && micros < 0) {
        months = months
            .checked_add(days / 30)
            .ok_or_else(interval_overflow)?;
        days %= 30;
    }
    days += (micros / MICROS_PER_DAY) as i32;
    micros %= MICROS_PER_DAY;
    months = months
        .checked_add(days / 30)
        .ok_or_else(interval_overflow)?;
    days %= 30;
    if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
        days += 30;
        months -= 1;
    } else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
        days -= 30;
        months += 1;
    }
    if days > 0 && micros < 0 {
        micros += MICROS_PER_DAY;
        days -= 1;
    } else if days < 0 && micros > 0 {
        micros -= MICROS_PER_DAY;
        days += 1;
    }
    Ok((months, days, micros))
}

/// `-interval`.
pub fn negate_interval(months: i32, days: i32, micros: i64) -> Result<(i32, i32, i64)> {
    match (
        months.checked_neg(),
        days.checked_neg(),
        micros.checked_neg(),
    ) {
        (Some(months), Some(days), Some(micros)) => Ok((months, days, micros)),
        _ => Err(interval_overflow()),
    }
}

/// `a + b`, field by field.
pub fn add_intervals(a: (i32, i32, i64), b: (i32, i32, i64)) -> Result<(i32, i32, i64)> {
    match (
        a.0.checked_add(b.0),
        a.1.checked_add(b.1),
        a.2.checked_add(b.2),
    ) {
        (Some(months), Some(days), Some(micros)) => Ok((months, days, micros)),
        _ => Err(interval_overflow()),
    }
}

/// `a - b`, field by field.
pub fn subtract_intervals(a: (i32, i32, i64), b: (i32, i32, i64)) -> Result<(i32, i32, i64)> {
    match (
        a.0.checked_sub(b.0),
        a.1.checked_sub(b.1),
        a.2.checked_sub(b.2),
    ) {
        (Some(months), Some(days), Some(micros)) => Ok((months, days, micros)),
        _ => Err(interval_overflow()),
    }
}

/// `interval * factor` for an integer factor, exactly.
pub fn multiply_interval_by_integer(
    months: i32,
    days: i32,
    micros: i64,
    factor: i64,
) -> Result<(i32, i32, i64)> {
    let field = |value: i32| {
        i64::from(value)
            .checked_mul(factor)
            .and_then(|product| i32::try_from(product).ok())
    };
    match (field(months), field(days), micros.checked_mul(factor)) {
        (Some(months), Some(days), Some(micros)) => Ok((months, days, micros)),
        _ => Err(interval_overflow()),
    }
}

/// `interval * factor` as PostgreSQL's interval_mul computes it.
pub fn multiply_interval(
    months: i32,
    days: i32,
    micros: i64,
    factor: f64,
) -> Result<(i32, i32, i64)> {
    scale_interval(
        f64::from(months),
        f64::from(days),
        micros as f64,
        factor,
        false,
    )
}

/// `interval / divisor` as PostgreSQL's interval_div computes it.
pub fn divide_interval(
    months: i32,
    days: i32,
    micros: i64,
    divisor: f64,
) -> Result<(i32, i32, i64)> {
    scale_interval(
        f64::from(months),
        f64::from(days),
        micros as f64,
        divisor,
        true,
    )
}

/// The average of `count` intervals whose fields sum to these totals: the
/// sum divided by the count, as PostgreSQL averages intervals.
pub fn average_interval(
    months: i64,
    days: i64,
    micros: i128,
    count: i64,
) -> Result<(i32, i32, i64)> {
    scale_interval(
        months as f64,
        days as f64,
        micros as f64,
        count as f64,
        true,
    )
}

/// Multiplies (or divides) each field by `factor`: months and days keep their
/// whole parts, and their fractions cascade into days and microseconds at 30
/// days a month and 86,400 seconds a day, each rounded to a microsecond.
fn scale_interval(
    months: f64,
    days: f64,
    micros: f64,
    factor: f64,
    divide: bool,
) -> Result<(i32, i32, i64)> {
    if factor.is_nan() {
        return Err(interval_overflow());
    }
    if factor.is_infinite() {
        // Without an infinite interval, only a zero one can be scaled up.
        let zero = months == 0.0 && days == 0.0 && micros == 0.0;
        return if divide || zero {
            Ok((0, 0, 0))
        } else {
            Err(interval_overflow())
        };
    }
    if divide && factor == 0.0 {
        return Err(SqlError::DivisionByZero);
    }
    let scale = |value: f64| {
        if divide {
            value / factor
        } else {
            value * factor
        }
    };
    let whole = |value: f64| {
        if (f64::from(i32::MIN)..-f64::from(i32::MIN)).contains(&value) {
            Ok(value as i32)
        } else {
            Err(interval_overflow())
        }
    };
    let round_to_micros = |value: f64| (value * 1_000_000.0).round_ties_even() / 1_000_000.0;
    let month_product = scale(months);
    let whole_months = whole(month_product)?;
    let day_product = scale(days);
    let mut whole_days = whole(day_product)?;
    let month_remainder_days = round_to_micros((month_product - f64::from(whole_months)) * 30.0);
    let mut second_remainder = round_to_micros(
        (day_product - f64::from(whole_days) + month_remainder_days.fract()) * 86_400.0,
    );
    if second_remainder.abs() >= 86_400.0 {
        let carried = (second_remainder / 86_400.0) as i32;
        whole_days = whole_days
            .checked_add(carried)
            .ok_or_else(interval_overflow)?;
        second_remainder -= f64::from(carried) * 86_400.0;
    }
    whole_days = whole_days
        .checked_add(month_remainder_days as i32)
        .ok_or_else(interval_overflow)?;
    let time = (scale(micros) + second_remainder * 1_000_000.0).round_ties_even();
    if !(i64::MIN as f64..-(i64::MIN as f64)).contains(&time) {
        return Err(interval_overflow());
    }
    Ok((whole_months, whole_days, time as i64))
}

/// PG-normalized total µs for comparison purposes (30-day month, 24-hour day).
pub fn interval_to_total_micros(months: i32, days: i32, micros: i64) -> i128 {
    (months as i128) * 30 * (MICROS_PER_DAY as i128)
        + (days as i128) * (MICROS_PER_DAY as i128)
        + micros as i128
}

pub fn extract(field: &str, v: &Value) -> Result<Value> {
    // PostgreSQL folds field names to lower case.
    let field = field.trim().to_ascii_lowercase();
    let field = field.as_str();
    match v {
        Value::Null => Ok(Value::Null),
        Value::Date(d) if is_infinity_date(*d) => {
            extract_from_infinity(field, *d == DATE_NEG_INFINITY_DAYS, "DATE")
        }
        Value::Date(d) => extract_from_date(field, *d),
        Value::Time(t) => extract_from_time(field, *t),
        Value::Timestamp(t) if is_infinity_ts(*t) => {
            extract_from_infinity(field, *t == TS_NEG_INFINITY_MICROS, "TIMESTAMP")
        }
        Value::Timestamp(t) => extract_from_timestamp(field, *t),
        Value::Interval {
            months,
            days,
            micros,
        } => extract_from_interval(field, *months, *days, *micros),
        _ => Err(SqlError::TypeMismatch {
            expected: "temporal type".into(),
            got: v.data_type().to_string(),
        }),
    }
}

/// EXTRACT from ±infinity, as PostgreSQL gives it: fields that grow with time
/// are ±Infinity, fields that cycle are NULL.
fn extract_from_infinity(field: &str, negative: bool, type_name: &str) -> Result<Value> {
    match field {
        "year" | "decade" | "century" | "millennium" | "julian" | "isoyear" | "epoch" => {
            Ok(Value::Real(if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }))
        }
        "month" | "day" | "hour" | "minute" | "second" | "milliseconds" | "microseconds"
        | "quarter" | "week" | "dow" | "isodow" | "doy" => Ok(Value::Null),
        _ => Err(SqlError::InvalidExtractField(format!(
            "{field} from {type_name}"
        ))),
    }
}

/// Julian day number of 1970-01-01.
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;

/// ISO 8601 weekday of a day count since 1970: Monday 1 through Sunday 7.
fn iso_weekday(days: i64) -> i64 {
    // 1970-01-01 was a Thursday.
    (days + 3).rem_euclid(7) + 1
}

/// ISO 8601 (week-numbering year, week) of a day count since 1970, with an
/// astronomical year: a week belongs to the year of its Thursday.
fn iso_week(days: i64) -> (i64, i64) {
    let thursday = days - iso_weekday(days) + 4;
    let (year, _, _) = civil_from_days(thursday);
    (year, (thursday - days_from_civil(year, 1, 1)) / 7 + 1)
}

/// A calendar field of a finite date, as PostgreSQL's EXTRACT gives it; `None`
/// when `field` is not a calendar field.
fn date_field(field: &str, days: i32) -> Option<i64> {
    let (year, month, day) = days_to_ymd(days);
    let (year, days) = (i64::from(year), i64::from(days));
    // PostgreSQL has no year 0: astronomical year 0 is 1 BC, given as -1.
    let era_year = |year: i64| if year > 0 { year } else { year - 1 };
    Some(match field {
        "year" => era_year(year),
        "month" => i64::from(month),
        "day" => i64::from(day),
        // Sunday is 0.
        "dow" => iso_weekday(days) % 7,
        "isodow" => iso_weekday(days),
        "doy" => days - days_from_civil(year, 1, 1) + 1,
        "quarter" => i64::from((month - 1) / 3 + 1),
        // PostgreSQL's rules, over the astronomical year.
        "decade" if year >= 0 => year / 10,
        "decade" => -((8 - (year - 1)) / 10),
        "century" if year > 0 => (year - 1) / 100 + 1,
        "century" => year / 100 - 1,
        "millennium" if year > 0 => (year - 1) / 1000 + 1,
        "millennium" => year / 1000 - 1,
        "julian" => days + JULIAN_DAY_OF_EPOCH,
        "week" => iso_week(days).1,
        "isoyear" => era_year(iso_week(days).0),
        _ => return None,
    })
}

fn extract_from_date(field: &str, days: i32) -> Result<Value> {
    match field {
        // A date is its midnight.
        "hour" | "minute" | "second" | "microseconds" | "milliseconds" => Ok(Value::Integer(0)),
        "epoch" => Ok(Value::Integer(i64::from(days) * 86_400)),
        _ => date_field(field, days)
            .map(Value::Integer)
            .ok_or_else(|| SqlError::InvalidExtractField(format!("{field} from DATE"))),
    }
}

/// Seconds since the epoch (or midnight) of a microsecond count: an integer
/// when whole, else with its fraction.
fn epoch_seconds(micros: i64) -> Value {
    if micros % MICROS_PER_SEC == 0 {
        Value::Integer(micros / MICROS_PER_SEC)
    } else {
        Value::Real(micros as f64 / MICROS_PER_SEC as f64)
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
        "epoch" => Ok(epoch_seconds(micros)),
        _ => Err(SqlError::InvalidExtractField(format!("{field} from TIME"))),
    }
}

fn extract_from_timestamp(field: &str, ts: i64) -> Result<Value> {
    let (days, time) = ts_split(ts);
    match field {
        "hour" | "minute" | "second" | "microseconds" | "milliseconds" => {
            extract_from_time(field, time)
        }
        "epoch" => Ok(epoch_seconds(ts)),
        // The Julian day carries the fraction of the day elapsed.
        "julian" if time != 0 => Ok(Value::Real(
            (i64::from(days) + JULIAN_DAY_OF_EPOCH) as f64 + time as f64 / MICROS_PER_DAY as f64,
        )),
        _ => date_field(field, days)
            .map(Value::Integer)
            .ok_or_else(|| SqlError::InvalidExtractField(format!("{field} from TIMESTAMP"))),
    }
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
        // PostgreSQL counts 365.25 days in each whole year of months, 30 in
        // each remaining month.
        "epoch" => {
            let days =
                365.25 * f64::from(months / 12) + 30.0 * f64::from(months % 12) + f64::from(days);
            Ok(Value::Real(
                days * 86_400.0 + micros as f64 / MICROS_PER_SEC as f64,
            ))
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
    let (y, m, _) = days_to_ymd(days);
    match unit {
        "microseconds" | "milliseconds" | "second" | "minute" | "hour" | "day" => Ok(days),
        // The Monday of the ISO 8601 week.
        "week" => add_days_to_date(days, 1 - iso_weekday(i64::from(days))),
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
        // PostgreSQL's rule, over the astronomical year.
        "decade" => ymd_to_days(
            if y > 0 {
                y / 10 * 10
            } else {
                -((8 - (y - 1)) / 10) * 10
            },
            1,
            1,
        )
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

/// Truncate the local civil fields of an instant in an explicit SQL timezone.
/// Subday units preserve the input offset (including either occurrence of a
/// folded hour). Calendar units resolve the truncated local date anew, using
/// the same compatible gap/fold policy as parsing civil timestamps in a zone.
pub(crate) fn date_trunc_timestamp_in_zone(unit: &str, ts: i64, zone: &str) -> Result<i64> {
    let zone = resolve_timezone(zone)?;
    let unit = unit.trim().to_ascii_lowercase();
    if is_infinity_ts(ts) {
        return date_trunc_timestamp(&unit, ts);
    }
    let zoned = JTimestamp::from_microsecond(ts)
        .map_err(|error| SqlError::InvalidValue(format!("ts: {error}")))?
        .to_zoned(zone.clone());
    let offset = i64::from(zoned.offset().seconds()) * MICROS_PER_SEC;
    let local = ts
        .checked_add(offset)
        .ok_or_else(|| SqlError::InvalidValue("date_trunc local timestamp overflow".into()))?;
    // Keep one implementation of every unit's civil truncation rules.
    let truncated = date_trunc_timestamp(&unit, local)?;
    if matches!(
        unit.as_str(),
        "microseconds" | "milliseconds" | "second" | "minute" | "hour"
    ) {
        return truncated
            .checked_sub(offset)
            .ok_or_else(|| SqlError::InvalidValue("date_trunc timestamp overflow".into()));
    }
    let civil = JTimestamp::from_microsecond(truncated)
        .map_err(|error| SqlError::InvalidValue(error.to_string()))?
        .to_zoned(TimeZone::UTC)
        .datetime();
    civil
        .to_zoned(zone)
        .map(|rounded| rounded.timestamp().as_microsecond())
        .map_err(|error| SqlError::InvalidValue(error.to_string()))
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
        Value::Date(d) => date_to_ts(*d)?,
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
    // Display is non-lenient: an unknown directive makes it fail, and `to_string` on a
    // failing Display panics. Format fallibly so a bad format string is a SQL error.
    jiff::fmt::strtime::format(&prepared, &z)
        .map_err(|e| SqlError::InvalidValue(format!("strftime: invalid format '{fmt}': {e}")))
}

/// Session-agnostic util used by eval.rs for SQL INTERVAL comparison normalization.
pub fn pg_normalized_interval_cmp(a: (i32, i32, i64), b: (i32, i32, i64)) -> std::cmp::Ordering {
    let at = interval_to_total_micros(a.0, a.1, a.2);
    let bt = interval_to_total_micros(b.0, b.1, b.2);
    at.cmp(&bt)
}

#[cfg(test)]
#[path = "datetime_tests.rs"]
mod tests;
