// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new module - no upstream basis.

//! PostgreSQL's civil-date and timestamp representation.

use jiff::tz::{AmbiguousOffset, Offset, TimeZone};

const MICROS_PER_SECOND: i64 = 1_000_000;
pub(crate) const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;

// `src/include/datatype/timestamp.h` at REL_17_STABLE. The upper bound is
// exclusive, just as it is in `IS_VALID_TIMESTAMP`.
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;
const MAX_DATE_YEAR: i32 = 5_874_897;

const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
const DAYS_1970_TO_2000: i64 = 10_957;
const EQUIVALENT_YEAR_BASE: i32 = 9_200;
const MIN_DATE_DAYS: i64 = -2_451_545;
const END_DATE_DAYS: i64 = 2_145_031_949;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Date {
    pub(crate) year: i32,
    pub(crate) month: u8,
    pub(crate) day: u8,
}

impl Date {
    pub(crate) fn new(year: i32, month: u8, day: u8) -> Option<Self> {
        let result = Self::new_civil(year, month, day)?;
        (MIN_DATE_DAYS..END_DATE_DAYS)
            .contains(&result.days_since_epoch())
            .then_some(result)
    }

    fn new_civil(year: i32, month: u8, day: u8) -> Option<Self> {
        let julian_max_year = MAX_DATE_YEAR + 1;
        let in_julian_workspace = (year > -4_713 || (year == -4_713 && month >= 11))
            && (year < julian_max_year || (year == julian_max_year && month < 6));
        if !in_julian_workspace || !(1..=12).contains(&month) {
            return None;
        }
        if day == 0 || day > days_in_month(year, month) {
            return None;
        }
        Some(Self { year, month, day })
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        let (year, month, day) = parse_date_fields(value)?;
        Self::new(year, month, day)
    }

    fn parse_civil(value: &str) -> Option<Self> {
        let (year, month, day) = parse_date_fields(value)?;
        Self::new_civil(year, month, day)
    }

    pub(crate) fn is_valid(self) -> bool {
        (MIN_DATE_DAYS..END_DATE_DAYS).contains(&self.days_since_epoch())
    }

    pub(crate) fn format(self) -> String {
        format!(
            "{}-{:02}-{:02}",
            format_year(self.year),
            self.month,
            self.day
        )
    }

    pub(crate) fn at_midnight(self) -> DateTime {
        DateTime {
            date: self,
            micros_of_day: 0,
        }
    }

    pub(crate) fn days_since_epoch(self) -> i64 {
        days_from_civil(self.year, self.month, self.day) - DAYS_1970_TO_2000
    }
}

fn parse_date_fields(value: &str) -> Option<(i32, u8, u8)> {
    let year_start = usize::from(value.starts_with(['+', '-']));
    let first = value[year_start..].find('-')? + year_start;
    let second = value[first + 1..].find('-')? + first + 1;
    if value[second + 1..].contains('-') {
        return None;
    }
    let year = value[..first].parse().ok()?;
    let month = parse_two_digits(&value[first + 1..second])?;
    let day = parse_two_digits(&value[second + 1..])?;
    Some((year, month, day))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateTime {
    pub(crate) date: Date,
    pub(crate) micros_of_day: i64,
}

impl DateTime {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let separator = value.find(['T', ' '])?;
        if value[separator + 1..].contains(['T', ' ']) {
            return None;
        }
        let date = Date::parse_civil(&value[..separator])?;
        let micros_of_day = parse_time_micros(&value[separator + 1..])?;
        let result = Self {
            date,
            micros_of_day,
        };
        Some(result)
    }

    pub(crate) fn format(self) -> String {
        format!("{}T{}", self.date.format(), format_time(self.micros_of_day))
    }

    pub(crate) fn timestamp_micros(self) -> Option<i64> {
        let value = i64::try_from(self.civil_micros()?).ok()?;
        is_valid_timestamp(value).then_some(value)
    }

    fn civil_micros(self) -> Option<i128> {
        i128::from(self.date.days_since_epoch())
            .checked_mul(i128::from(MICROS_PER_DAY))?
            .checked_add(i128::from(self.micros_of_day))
    }

    pub(crate) fn from_timestamp_micros(value: i64) -> Option<Self> {
        if !is_valid_timestamp(value) {
            return None;
        }
        Self::from_unchecked_micros(value)
    }

    fn from_unchecked_micros(value: i64) -> Option<Self> {
        let days = value.div_euclid(MICROS_PER_DAY);
        let micros_of_day = value.rem_euclid(MICROS_PER_DAY);
        let (year, month, day) = civil_from_days(days.checked_add(DAYS_1970_TO_2000)?);
        Some(Self {
            date: Date::new_civil(year, month, day)?,
            micros_of_day,
        })
    }

    pub(crate) fn to_jiff_equivalent(self) -> Option<jiff::civil::DateTime> {
        // PostgreSQL's tzcode and Jiff both apply the TZif POSIX footer after
        // the last explicit transition. Those rules depend on month/day or
        // day-of-year and weekday, so the proleptic Gregorian calendar repeats
        // them exactly every 400 years. Keep mapped values well inside Jiff's
        // timestamp ceiling while remaining far beyond explicit TZif records.
        // A TZif file without a footer freezes its last offset in both engines,
        // which is likewise unchanged by the mapping.
        let year = if self.date.year <= 9_000 {
            self.date.year
        } else {
            EQUIVALENT_YEAR_BASE + self.date.year.rem_euclid(400)
        };
        let hour = self.micros_of_day / 3_600_000_000;
        let minute = (self.micros_of_day / 60_000_000) % 60;
        let second = (self.micros_of_day / MICROS_PER_SECOND) % 60;
        let nanosecond = (self.micros_of_day % MICROS_PER_SECOND) * 1_000;
        jiff::civil::DateTime::new(
            i16::try_from(year).ok()?,
            i8::try_from(self.date.month).ok()?,
            i8::try_from(self.date.day).ok()?,
            i8::try_from(hour).ok()?,
            i8::try_from(minute).ok()?,
            i8::try_from(second).ok()?,
            i32::try_from(nanosecond).ok()?,
        )
        .ok()
    }
}

pub(crate) fn parse_time_micros(value: &str) -> Option<i64> {
    let mut fields = value.split(':');
    let hour = parse_two_digits(fields.next()?)?;
    let minute = parse_two_digits(fields.next()?)?;
    let seconds = fields.next()?;
    if fields.next().is_some() || hour > 23 || minute > 59 {
        return None;
    }
    let (second, fraction) = match seconds.split_once('.') {
        Some((second, fraction)) => (second, Some(fraction)),
        None => (seconds, None),
    };
    let second = parse_two_digits(second)?;
    if second > 59 {
        return None;
    }
    let fraction = match fraction {
        Some(value)
            if !value.is_empty()
                && value.len() <= 6
                && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            let parsed: i64 = value.parse().ok()?;
            parsed.checked_mul(10_i64.pow(u32::try_from(6 - value.len()).ok()?))?
        }
        Some(_) => return None,
        None => 0,
    };
    Some(
        (i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second)) * MICROS_PER_SECOND
            + fraction,
    )
}

pub(crate) fn format_time(micros: i64) -> String {
    let hour = micros / 3_600_000_000;
    let minute = (micros / 60_000_000) % 60;
    let second = (micros / MICROS_PER_SECOND) % 60;
    let fraction = micros % MICROS_PER_SECOND;
    if fraction == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        let fraction = format!("{fraction:06}");
        format!(
            "{hour:02}:{minute:02}:{second:02}.{}",
            fraction.trim_end_matches('0')
        )
    }
}

pub(crate) fn local_timestamp_micros(value: &str) -> Option<i64> {
    DateTime::parse(value)?.timestamp_micros()
}

pub(crate) fn fixed_offset_instant(value: &str, offset_seconds: i32) -> Option<i64> {
    let local = DateTime::parse(value)?.civil_micros()?;
    let result = local.checked_sub(i128::from(offset_seconds) * i128::from(MICROS_PER_SECOND))?;
    let result = i64::try_from(result).ok()?;
    is_valid_timestamp(result).then_some(result)
}

pub(crate) fn local_instant(value: &str, tz: &TimeZone) -> Option<i64> {
    let dt = DateTime::parse(value)?;
    let offset = resolve_local_offset(tz, dt);
    checked_offset(dt.timestamp_micros()?, -offset.seconds())
}

pub(crate) fn date_instant(value: &str, tz: &TimeZone) -> Option<i64> {
    let dt = Date::parse(value)?.at_midnight();
    let offset = resolve_local_offset(tz, dt);
    checked_offset(dt.timestamp_micros()?, -offset.seconds())
}

pub(crate) fn wide_date_instant(value: &str, tz: &TimeZone) -> Option<i128> {
    let dt = Date::parse(value)?.at_midnight();
    let offset = resolve_local_offset(tz, dt);
    dt.civil_micros()?
        .checked_sub(i128::from(offset.seconds()) * i128::from(MICROS_PER_SECOND))
}

pub(crate) fn shift_instant_to_zone(value: i64, tz: &TimeZone) -> Option<(DateTime, Offset)> {
    if !is_valid_timestamp(value) {
        return None;
    }
    let offset = offset_at_instant(tz, value)?;
    let local = i128::from(value)
        .checked_add(i128::from(offset.seconds()) * i128::from(MICROS_PER_SECOND))?;
    let local = i64::try_from(local).ok()?;
    Some((DateTime::from_unchecked_micros(local)?, offset))
}

pub(crate) fn resolve_local_offset(tz: &TimeZone, value: DateTime) -> Offset {
    let equivalent = value
        .to_jiff_equivalent()
        .expect("a validated PostgreSQL datetime has a Gregorian equivalent");
    match tz.to_ambiguous_timestamp(equivalent).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Gap { before, .. } => before,
        AmbiguousOffset::Fold { after, .. } => after,
    }
}

pub(crate) fn round_timestamp(value: i64, scale: i64) -> Option<i64> {
    let half = scale / 2;
    let rounded = if value >= 0 {
        value.checked_add(half).map(|v| (v / scale) * scale)?
    } else {
        let magnitude = value.checked_neg()?;
        -((magnitude.checked_add(half)? / scale) * scale)
    };
    is_valid_timestamp(rounded).then_some(rounded)
}

const fn is_valid_timestamp(value: i64) -> bool {
    value >= MIN_TIMESTAMP && value < END_TIMESTAMP
}

fn offset_at_instant(tz: &TimeZone, value: i64) -> Option<Offset> {
    if let Some(unix) = value.checked_add(PG_EPOCH_UNIX_MICROS) {
        if let Ok(timestamp) = jiff::Timestamp::from_microsecond(unix) {
            return Some(timestamp.to_zoned(tz.clone()).offset());
        }
    }

    let utc = DateTime::from_timestamp_micros(value)?;
    let equivalent = utc.to_jiff_equivalent()?;
    let timestamp = Offset::UTC.to_timestamp(equivalent).ok()?;
    Some(timestamp.to_zoned(tz.clone()).offset())
}

fn checked_offset(value: i64, offset_seconds: i32) -> Option<i64> {
    let result = i128::from(value)
        .checked_add(i128::from(offset_seconds) * i128::from(MICROS_PER_SECOND))?;
    let result = i64::try_from(result).ok()?;
    is_valid_timestamp(result).then_some(result)
}

const fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

const fn is_leap_year(year: i32) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

fn parse_two_digits(value: &str) -> Option<u8> {
    if value.len() != 2 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn format_year(year: i32) -> String {
    if (0..10_000).contains(&year) {
        format!("{year:04}")
    } else if year < 0 {
        format!("-{:04}", year.unsigned_abs())
    } else {
        year.to_string()
    }
}

// Howard Hinnant's proleptic-Gregorian conversion, with Euclidean division so
// it remains exact before 1 AD. The returned epoch is 1970-01-01.
fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let mut year = i64::from(year);
    let month = i64::from(month);
    let day = i64::from(day);
    year -= i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days_since_1970: i64) -> (i32, u8, u8) {
    let z = days_since_1970 + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (
        i32::try_from(year).expect("PostgreSQL date years fit i32"),
        u8::try_from(month).expect("Gregorian month fits u8"),
        u8::try_from(day).expect("Gregorian day fits u8"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_epoch_and_boundaries_are_exact() {
        let epoch = DateTime::parse("2000-01-01T00:00:00").unwrap();
        assert_eq!(epoch.timestamp_micros(), Some(0));
        assert_eq!(DateTime::from_timestamp_micros(0), Some(epoch));

        let maximum = DateTime::from_timestamp_micros(END_TIMESTAMP - 1).unwrap();
        assert_eq!(maximum.format(), "294276-12-31T23:59:59.999999");
        assert_eq!(maximum.timestamp_micros(), Some(END_TIMESTAMP - 1));
        assert!(DateTime::from_timestamp_micros(END_TIMESTAMP).is_none());
        assert!(DateTime::parse("294277-01-01T00:00:00")
            .unwrap()
            .timestamp_micros()
            .is_none());

        let minimum = DateTime::from_timestamp_micros(MIN_TIMESTAMP).unwrap();
        // `pg_tm` year -4713 means 4714 BC (`abs(year) + 1`), matching
        // timestamp.h's Julian-day-zero example rather than a raw AD label.
        assert_eq!(minimum.format(), "-4713-11-24T00:00:00");
        assert_eq!(minimum.timestamp_micros(), Some(MIN_TIMESTAMP));
        assert!(Date::new(-4_713, 11, 23).is_none());

        let west = TimeZone::fixed(Offset::from_seconds(-3_600).unwrap());
        let (rotated, _) = shift_instant_to_zone(MIN_TIMESTAMP, &west).unwrap();
        assert_eq!(rotated.format(), "-4713-11-23T23:00:00");
    }

    #[test]
    fn fixed_offsets_remain_exact_past_jiffs_year_limit() {
        let instant = fixed_offset_instant("10000-01-01T00:00:00", -3_600).unwrap();
        let utc = fixed_offset_instant("10000-01-01T01:00:00", 0).unwrap();
        assert_eq!(instant, utc);
    }

    #[test]
    fn future_named_zone_rules_repeat_on_the_gregorian_cycle() {
        let zone = TimeZone::get("America/New_York").unwrap();
        for month in 1..=12 {
            for day in 1..=31 {
                for hour in [0, 6, 12, 18] {
                    let Some(equivalent) = Date::new(9_224, month, day) else {
                        continue;
                    };
                    let future = Date::new(10_024, month, day).unwrap();
                    let micros = i64::from(hour) * 3_600_000_000;
                    assert_eq!(
                        resolve_local_offset(
                            &zone,
                            DateTime {
                                date: equivalent,
                                micros_of_day: micros,
                            }
                        ),
                        resolve_local_offset(
                            &zone,
                            DateTime {
                                date: future,
                                micros_of_day: micros,
                            }
                        )
                    );
                }
            }
        }

        // Pin PostgreSQL's before-gap and after-fold policy at the two ambiguous
        // wall clocks as well; a daily grid alone cannot distinguish them.
        for (month, day, hour) in [(3, 10, 2), (11, 3, 1)] {
            let equivalent =
                DateTime::parse(&format!("9224-{month:02}-{day:02}T{hour:02}:30:00")).unwrap();
            let future =
                DateTime::parse(&format!("10024-{month:02}-{day:02}T{hour:02}:30:00")).unwrap();
            assert_eq!(
                resolve_local_offset(&zone, equivalent),
                resolve_local_offset(&zone, future)
            );
        }
    }

    #[test]
    fn instant_to_named_zone_uses_the_same_future_rule() {
        let zone = TimeZone::get("America/New_York").unwrap();
        let current = fixed_offset_instant("9224-07-15T16:00:00", 0).unwrap();
        let future = fixed_offset_instant("10024-07-15T16:00:00", 0).unwrap();
        let (current_local, current_offset) = shift_instant_to_zone(current, &zone).unwrap();
        let (future_local, future_offset) = shift_instant_to_zone(future, &zone).unwrap();
        assert_eq!(current_offset, future_offset);
        assert_eq!(current_local.micros_of_day, future_local.micros_of_day);
        assert_eq!((future_local.date.month, future_local.date.day), (7, 15));
    }

    #[test]
    fn jiff_instant_ceiling_does_not_shorten_postgres_range() {
        let value = fixed_offset_instant("9999-12-31T23:59:59.999999", 0).unwrap();
        let (shifted, offset) = shift_instant_to_zone(value, &TimeZone::UTC).unwrap();
        assert_eq!(offset, Offset::UTC);
        assert_eq!(shifted.format(), "9999-12-31T23:59:59.999999");
    }
}
