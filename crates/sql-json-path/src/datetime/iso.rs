// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new module — no upstream basis.

//! Mirrors PG `jsonpath_exec.c::executeDateTimeMethod`: first matching
//! ISO 8601 trial wins, else error.

use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::{AmbiguousOffset, Offset};

use super::{DatetimeKind, ParsedDatetime};
use crate::eval::{Error, Result};

const ISO_FORMATS: &[(&str, DatetimeKind)] = &[
    ("%Y-%m-%d", DatetimeKind::Date),
    ("%H:%M:%S.%6f%:z", DatetimeKind::TimeTz),
    ("%H:%M:%S%:z", DatetimeKind::TimeTz),
    ("%H:%M:%S.%6f", DatetimeKind::Time),
    ("%H:%M:%S", DatetimeKind::Time),
    ("%Y-%m-%d %H:%M:%S.%6f%:z", DatetimeKind::TimestampTz),
    ("%Y-%m-%d %H:%M:%S%:z", DatetimeKind::TimestampTz),
    ("%Y-%m-%dT%H:%M:%S.%6f%:z", DatetimeKind::TimestampTz),
    ("%Y-%m-%dT%H:%M:%S%:z", DatetimeKind::TimestampTz),
    ("%Y-%m-%d %H:%M:%S.%6f", DatetimeKind::Timestamp),
    ("%Y-%m-%d %H:%M:%S", DatetimeKind::Timestamp),
    ("%Y-%m-%dT%H:%M:%S.%6f", DatetimeKind::Timestamp),
    ("%Y-%m-%dT%H:%M:%S", DatetimeKind::Timestamp),
];

const NANOS_PER_SECOND: i64 = 1_000_000_000;

pub(crate) fn try_13_formats(input: &str) -> Result<ParsedDatetime> {
    let trimmed = input.trim();
    let year_normalized = normalize_pg_bc_year(trimmed).unwrap_or_else(|| trimmed.to_owned());
    let dezoned = normalize_zone_abbrev(&year_normalized);
    // A named abbreviation can legitimately resolve to a historical offset with
    // seconds. Numeric input to PG's `TZ` directive, however, is only TZH or
    // TZH:TZM and is bounded to 15:59. Keep those two paths distinct.
    let normalized = match dezoned {
        Some(value) => value,
        None => normalize_tz_offset(&year_normalized)
            .ok_or_else(|| Error::DatetimeFormatNotRecognized(format!("\"{trimmed}\"").into()))?,
    };
    for (fmt, kind) in ISO_FORMATS {
        if let Ok(bdt) = BrokenDownTime::parse(fmt, normalized.as_str()) {
            if let Some(iso) = render(bdt, *kind) {
                let parsed = ParsedDatetime { iso, kind: *kind };
                if is_in_pg_range(&parsed) {
                    return Ok(parsed);
                }
            }
        }
    }
    if let Some(pd) = try_wide_datetime(&normalized) {
        return Ok(pd);
    }
    Err(Error::DatetimeFormatNotRecognized(
        format!("\"{trimmed}\"").into(),
    ))
}

/// Convert PostgreSQL's signed source-year convention to `pg_tm` numbering.
///
/// `do_to_timestamp` treats a negative `YYYY` as BC, then adds one because its
/// internal year zero denotes 1 BC. Thus source year -4714 is internal year
/// -4713, the Julian-day-zero timestamp boundary.
fn normalize_pg_bc_year(input: &str) -> Option<String> {
    let rest = input.strip_prefix('-')?;
    let separator = rest.find('-')?;
    let source: i32 = rest[..separator].parse::<i32>().ok()?.checked_neg()?;
    let internal = source.checked_add(1)?;
    let year = if internal < 0 {
        format!("-{}", internal.unsigned_abs())
    } else {
        format!("{internal:04}")
    };
    Some(format!("{year}{}", &rest[separator..]))
}

fn try_wide_datetime(value: &str) -> Option<ParsedDatetime> {
    let year_start = usize::from(value.starts_with(['+', '-']));
    let year_end = value[year_start..].find('-')? + year_start;
    let year = &value[year_start..year_end];
    if (year.len() <= 4 && year_start == 0)
        || year.is_empty()
        || !year.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    if !value.contains(['T', ' ']) {
        let date = super::pg::Date::parse(value)?;
        return Some(ParsedDatetime {
            iso: date.format(),
            kind: DatetimeKind::Date,
        });
    }

    let (datetime, offset) = match split_iso_offset(value) {
        Some((datetime, offset)) => (datetime, Some(offset)),
        None => (value, None),
    };
    let parsed = super::pg::DateTime::parse(datetime)?;
    let kind = match offset {
        Some(value) => {
            let offset = parse_iso_offset(value)?;
            super::pg::fixed_offset_instant(datetime, offset.seconds())?;
            DatetimeKind::TimestampTz
        }
        None => {
            parsed.timestamp_micros()?;
            DatetimeKind::Timestamp
        }
    };
    Some(ParsedDatetime {
        iso: match offset {
            Some(offset) => format!("{}{offset}", parsed.format()),
            None => parsed.format(),
        },
        kind,
    })
}

fn is_in_pg_range(parsed: &ParsedDatetime) -> bool {
    match parsed.kind {
        DatetimeKind::Date => super::pg::Date::parse(&parsed.iso).is_some(),
        DatetimeKind::Timestamp => super::pg::local_timestamp_micros(&parsed.iso).is_some(),
        DatetimeKind::TimestampTz => {
            split_iso_offset(&parsed.iso).is_some_and(|(value, offset)| {
                parse_iso_offset(offset).is_some_and(|offset| {
                    super::pg::fixed_offset_instant(value, offset.seconds()).is_some()
                })
            })
        }
        DatetimeKind::Time | DatetimeKind::TimeTz => true,
    }
}

/// Replace a trailing time zone abbreviation with its numeric offset.
///
/// PostgreSQL's `TZ` directive resolves an alphabetic zone through the session
/// abbreviation table and only falls back to a numeric `±HH[:MM]`, so a plain `%:z`
/// parser rejects `"...EST"` and `"...Z"`, which PostgreSQL accepts.
fn normalize_zone_abbrev(input: &str) -> Option<String> {
    let end = input.trim_end();
    let start = end.rfind(|c: char| !c.is_ascii_alphabetic())? + 1;
    let name = end.get(start..).filter(|n| !n.is_empty())?;
    // The built-in ISO templates spell `...SSTZ` with no separator. In PG's
    // standard parsing mode `TZ` therefore sees a leading blank literally: it
    // cannot skip it before attempting abbreviation lookup. Do not silently
    // accept `"... EST"` as though the template contained a space.
    if end.as_bytes()[start - 1].is_ascii_whitespace() {
        return None;
    }
    let head = &end[..start];
    let secs = super::tzabbrev::offset_seconds(name, civil_for_lookup(head)?)?;
    Some(format!("{head}{}", format_offset_seconds(secs)))
}

/// The civil datetime a zone-referencing abbreviation is resolved against.
///
/// `do_to_timestamp` zero-initializes PostgreSQL's `pg_tm`, whose month/day
/// defaults are January 1 while its year remains astronomical year zero when
/// the format contains only a time. This is deliberately not the transaction
/// date: only a later `time` -> `timetz` cast consults that date.
fn civil_for_lookup(head: &str) -> Option<jiff::civil::DateTime> {
    let spaced = head.replacen(' ', "T", 1);
    for candidate in [head, spaced.as_str()] {
        if let Ok(dt) = candidate.parse::<jiff::civil::DateTime>() {
            return Some(dt);
        }
    }
    if let Some(dt) = super::pg::DateTime::parse(head) {
        return dt.to_jiff_equivalent();
    }
    let t: jiff::civil::Time = head.parse().ok()?;
    Some(jiff::civil::date(0, 1, 1).at(t.hour(), t.minute(), t.second(), t.subsec_nanosecond()))
}

fn normalize_tz_offset(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let n = bytes.len();
    let mut start: Option<usize> = None;
    let mut sign = '+';
    let mut rest_start = 0;
    for i in (0..n).rev() {
        let c = bytes[i];
        if (c == b'+' || c == b'-') && i >= 8 && bytes[..i].contains(&b':') {
            let prev = bytes[i - 1];
            if prev.is_ascii_digit() || prev == b' ' {
                let rest = &bytes[i + 1..];
                if !rest.is_empty() && rest.iter().all(|&b| b.is_ascii_digit() || b == b':') {
                    start = Some(i);
                    sign = c as char;
                    rest_start = i + 1;
                    break;
                }
            }
        }
    }

    // PG's `TZ` falls through to `OF` for numeric input. `OF` treats an ASCII
    // blank as a positive sign, while its integer parser also skips the other
    // C-locale whitespace characters. Preserve that slightly surprising
    // `"12:35:00 3:10"` spelling without confusing the one space inside a
    // normal timestamp: an offset has at most one colon.
    if start.is_none() {
        if let Some(i) = bytes.iter().rposition(|b| b.is_ascii_whitespace()) {
            let rest = input[i + 1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
            if i >= 8
                && bytes[..i].contains(&b':')
                && !rest.is_empty()
                && rest.bytes().all(|b| b.is_ascii_digit() || b == b':')
                && rest.bytes().filter(|&b| b == b':').count() <= 1
            {
                start = Some(i);
                rest_start = input.len() - rest.len();
            }
        }
    }

    let Some(s) = start else {
        return Some(input.to_string());
    };
    let rest = &input[rest_start..];
    let (h_str, m_str) = match rest.split_once(':') {
        Some((h, m)) if !m.contains(':') && !m.is_empty() => (h, m),
        Some(_) => return None,
        None => (rest, "0"),
    };
    if h_str.is_empty()
        || !h_str.bytes().all(|b| b.is_ascii_digit())
        || !m_str.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let h: u32 = h_str.parse().ok()?;
    let m: u32 = m_str.parse().ok()?;
    // PostgreSQL permits numeric displacements through 15:59, not Jiff's
    // wider fixed-offset range.
    if h > 15 || m >= 60 {
        return None;
    }
    let head = input[..s].trim_end_matches(|c: char| c.is_ascii_whitespace());
    Some(format!("{head}{sign}{:02}:{:02}", h, m))
}

/// Split a canonical ISO string into its value and trailing offset, if it has one.
///
/// Canonical numeric input uses `±HH:MM`; historical named abbreviations can
/// additionally render `±HH:MM:SS`. Matching only those two shapes keeps a
/// date's `-` separator from being mistaken for an offset sign.
pub(crate) fn split_iso_offset(iso: &str) -> Option<(&str, &str)> {
    for idx in (1..iso.len()).rev() {
        if !matches!(iso.as_bytes()[idx], b'+' | b'-') {
            continue;
        }
        let tail = iso.get(idx..)?;
        if parse_iso_offset(tail).is_some() {
            return Some(iso.split_at(idx));
        }
    }
    None
}

pub(crate) fn parse_iso_offset(value: &str) -> Option<Offset> {
    let bytes = value.as_bytes();
    let shape_ok = (bytes.len() == 6 || bytes.len() == 9)
        && matches!(bytes[0], b'+' | b'-')
        && bytes[1].is_ascii_digit()
        && bytes[2].is_ascii_digit()
        && bytes[3] == b':'
        && bytes[4].is_ascii_digit()
        && bytes[5].is_ascii_digit()
        && (bytes.len() == 6
            || (bytes[6] == b':' && bytes[7].is_ascii_digit() && bytes[8].is_ascii_digit()));
    if !shape_ok {
        return None;
    }
    let hour: i32 = value[1..3].parse().ok()?;
    let minute: i32 = value[4..6].parse().ok()?;
    let second: i32 = if bytes.len() == 9 {
        value[7..9].parse().ok()?
    } else {
        0
    };
    if minute >= 60 || second >= 60 {
        return None;
    }
    let sign = if bytes[0] == b'-' { -1 } else { 1 };
    Offset::from_seconds(sign * (hour * 3_600 + minute * 60 + second)).ok()
}

/// Re-express a fixed-offset timestamp at the session zone.
fn shift_to_session(iso: &str, tz: &jiff::tz::TimeZone) -> Option<(super::pg::DateTime, Offset)> {
    let (datetime, offset) = split_iso_offset(iso)?;
    let offset = parse_iso_offset(offset)?;
    let instant = super::pg::fixed_offset_instant(datetime, offset.seconds())?;
    super::pg::shift_instant_to_zone(instant, tz)
}

/// Resolve a civil datetime exactly as PostgreSQL's
/// `DetermineTimeZoneOffsetInternal`: use the offset before a gap and the offset
/// after a fold. Jiff's `compatible()` policy also uses the before-offset for a
/// gap, but uses the before-offset for a fold, so it is not generally PostgreSQL
/// compatible.
pub(crate) fn resolve_pg_offset(tz: &jiff::tz::TimeZone, dt: jiff::civil::DateTime) -> Offset {
    match tz.to_ambiguous_timestamp(dt).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Gap { before, .. } => before,
        AmbiguousOffset::Fold { after, .. } => after,
    }
}

/// The session zone's offset at a local time, as `±HH:MM[:SS]`.
///
/// The offset moves with daylight saving, so it is resolved at the value being converted
/// rather than taken as a constant. A bare time carries no date; PostgreSQL resolves those
/// against the current date, and a fixed-offset zone ignores the date entirely.
fn session_offset(
    tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
    local: &str,
) -> Option<String> {
    if let Some(dt) = super::pg::DateTime::parse(local) {
        return Some(format_offset(super::pg::resolve_local_offset(tz, dt)));
    }
    if let Some(date) = super::pg::Date::parse(local) {
        return Some(format_offset(super::pg::resolve_local_offset(
            tz,
            date.at_midnight(),
        )));
    }
    let dt = local
        .parse::<jiff::civil::DateTime>()
        .ok()
        .or_else(|| {
            local
                .parse::<jiff::civil::Date>()
                .ok()
                .map(|d| d.at(0, 0, 0, 0))
        })
        .or_else(|| {
            local
                .parse::<jiff::civil::Time>()
                .ok()
                .map(|t| session_date.at(t.hour(), t.minute(), t.second(), t.subsec_nanosecond()))
        })?;
    session_offset_at(tz, dt)
}

/// The session zone's offset at a local datetime, as `±HH:MM[:SS]`.
fn session_offset_at(tz: &jiff::tz::TimeZone, dt: jiff::civil::DateTime) -> Option<String> {
    Some(format_offset(resolve_pg_offset(tz, dt)))
}

fn format_offset(offset: Offset) -> String {
    format_offset_seconds(offset.seconds())
}

fn format_offset_seconds(seconds: i32) -> String {
    let sign = if seconds < 0 { '-' } else { '+' };
    let absolute = seconds.unsigned_abs();
    let hour = absolute / 3_600;
    let minute = (absolute % 3_600) / 60;
    let second = absolute % 60;
    if second == 0 {
        format!("{sign}{hour:02}:{minute:02}")
    } else {
        format!("{sign}{hour:02}:{minute:02}:{second:02}")
    }
}

/// Resolve a parsed value against the kind a method asks for.
///
/// Mirrors the `switch (jsp->type)` of PG `jsonpath_exec.c::executeDateTimeMethod`. The
/// rule is a per-target accept list, **not** a comparison of time-zone-ness: a pair
/// outside the list is "format is not recognized" whether or not the zones differ, and
/// only pairs inside it consult `checkTimezoneIsUsedForCast`.
pub(crate) fn cast_kind(
    parsed: ParsedDatetime,
    target: DatetimeKind,
    input: &str,
    method: &'static str,
    use_tz: bool,
    tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Result<ParsedDatetime> {
    use DatetimeKind as K;
    if parsed.kind == target {
        return Ok(parsed);
    }
    let unrecognized = || Error::FormatNotRecognized(method, input.into());
    let require_tz = |from: K, to: K| -> Result<()> {
        if use_tz {
            Ok(())
        } else {
            Err(Error::DatetimeConvertWithoutTz(
                from.as_tag().into(),
                to.as_tag().into(),
            ))
        }
    };
    let iso = &parsed.iso;
    let converted = match (parsed.kind, target) {
        (K::Timestamp, K::Date) => iso.split('T').next().map(str::to_owned),
        (K::TimestampTz, K::Date) => {
            require_tz(K::TimestampTz, K::Date)?;
            shift_to_session(iso, tz)
                .filter(|(dt, _)| dt.date.is_valid())
                .map(|(dt, _)| dt.date.format())
        }
        (K::Timestamp, K::Time) => iso.split_once('T').map(|(_, t)| t.to_owned()),
        (K::TimeTz, K::Time) => {
            require_tz(K::TimeTz, K::Time)?;
            // PG calls timetz_time, which swallows the zone rather than shifting.
            split_iso_offset(iso).map(|(t, _)| t.to_owned())
        }
        (K::TimestampTz, K::Time) => {
            require_tz(K::TimestampTz, K::Time)?;
            shift_to_session(iso, tz).map(|(dt, _)| super::pg::format_time(dt.micros_of_day))
        }
        (K::Time, K::TimeTz) => {
            require_tz(K::Time, K::TimeTz)?;
            session_offset(tz, session_date, iso).map(|off| format!("{iso}{off}"))
        }
        // The one cross-zone cell PG performs without a time-zone check. The offset is
        // taken from the shifted datetime, which still carries the date daylight saving
        // depends on; the rendered time alone would not.
        (K::TimestampTz, K::TimeTz) => shift_to_session(iso, tz).map(|(dt, offset)| {
            format!(
                "{}{}",
                super::pg::format_time(dt.micros_of_day),
                format_offset(offset)
            )
        }),
        (K::Date, K::Timestamp) => {
            let value = format!("{iso}T00:00:00");
            super::pg::local_timestamp_micros(&value).map(|_| value)
        }
        (K::TimestampTz, K::Timestamp) => {
            require_tz(K::TimestampTz, K::Timestamp)?;
            shift_to_session(iso, tz).and_then(|(dt, _)| dt.timestamp_micros().map(|_| dt.format()))
        }
        (K::Date, K::TimestampTz) => {
            require_tz(K::Date, K::TimestampTz)?;
            session_offset(tz, session_date, iso).and_then(|offset| {
                super::pg::date_instant(iso, tz).map(|_| format!("{iso}T00:00:00{offset}"))
            })
        }
        (K::Timestamp, K::TimestampTz) => {
            require_tz(K::Timestamp, K::TimestampTz)?;
            session_offset(tz, session_date, iso).and_then(|offset| {
                super::pg::local_instant(iso, tz).map(|_| format!("{iso}{offset}"))
            })
        }
        _ => return Err(unrecognized()),
    };
    Ok(ParsedDatetime {
        iso: converted.ok_or_else(unrecognized)?,
        kind: target,
    })
}

/// Round the fractional seconds to `precision` digits.
///
/// PG's `AdjustTimeForTypmod` / `AdjustTimestampForTypmod` round the whole microsecond
/// count, so a carry propagates into minutes, hours and days on its own. Rounding the
/// fractional field alone would produce `12:35:60`.
pub(crate) fn round_fractional(parsed: ParsedDatetime, precision: u8) -> Result<ParsedDatetime> {
    use DatetimeKind as K;
    let (value, offset) = match split_iso_offset(&parsed.iso) {
        Some((v, o)) => (v, Some(o.to_owned())),
        None => (parsed.iso.as_str(), None),
    };
    let exponent = 6_u32
        .checked_sub(u32::from(precision))
        .ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?;
    let scale = 10_i64.pow(exponent);
    let half = scale / 2;
    let rounded = match parsed.kind {
        K::Time | K::TimeTz => {
            let micros = parse_pg_time_nanos(value)
                .map(|nanos| nanos / 1_000)
                .ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?;
            let micros = ((micros + half) / scale) * scale;
            if micros >= super::pg::MICROS_PER_DAY {
                // PG's TimeADT permits 24:00:00; jiff's civil::Time does not.
                "24:00:00".to_owned()
            } else {
                let time = jiff::civil::Time::new(
                    (micros / 3_600_000_000) as i8,
                    ((micros / 60_000_000) % 60) as i8,
                    ((micros / 1_000_000) % 60) as i8,
                    ((micros % 1_000_000) * 1_000) as i32,
                )
                .map_err(|_| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?;
                render(BrokenDownTime::from(time), K::Time)
                    .ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?
            }
        }
        K::Timestamp | K::TimestampTz => {
            let rounded = match offset.as_deref() {
                Some(offset) => {
                    let display_offset = parse_iso_offset(offset)
                        .ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?;
                    let instant = super::pg::fixed_offset_instant(value, display_offset.seconds())
                        .and_then(|value| super::pg::round_timestamp(value, scale))
                        .ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?;
                    super::pg::shift_instant_to_zone(
                        instant,
                        &jiff::tz::TimeZone::fixed(display_offset),
                    )
                    .map(|(dt, _)| dt.format())
                }
                None => super::pg::local_timestamp_micros(value)
                    .and_then(|value| super::pg::round_timestamp(value, scale))
                    .and_then(super::pg::DateTime::from_timestamp_micros)
                    .map(super::pg::DateTime::format),
            };
            rounded.ok_or_else(|| Error::InvalidDatetimeInput(parsed.iso.clone().into()))?
        }
        K::Date => parsed.iso.clone(),
    };
    Ok(ParsedDatetime {
        iso: match offset {
            Some(o) => format!("{rounded}{o}"),
            None => rounded,
        },
        kind: parsed.kind,
    })
}

/// Parse the canonical time payload carried by an internal datetime item. PostgreSQL's
/// `TimeADT` has one value Jiff's `civil::Time` intentionally lacks: the exact
/// end-of-day sentinel `24:00:00`, which typmod rounding can produce.
pub(crate) fn parse_pg_time_nanos(value: &str) -> Option<i64> {
    if let Some(rest) = value.strip_prefix("24:00:00") {
        if rest.is_empty()
            || rest
                .strip_prefix('.')
                .is_some_and(|fraction| !fraction.is_empty() && fraction.bytes().all(|b| b == b'0'))
        {
            return Some(86_400 * NANOS_PER_SECOND);
        }
        return None;
    }
    let time: jiff::civil::Time = value.parse().ok()?;
    Some(
        (i64::from(time.hour()) * 3_600 + i64::from(time.minute()) * 60 + i64::from(time.second()))
            * NANOS_PER_SECOND
            + i64::from(time.subsec_nanosecond()),
    )
}

fn render(bdt: BrokenDownTime, kind: DatetimeKind) -> Option<String> {
    let canon: &str = match kind {
        DatetimeKind::Date => "%Y-%m-%d",
        DatetimeKind::Time => "%H:%M:%S%.f",
        DatetimeKind::TimeTz => "%H:%M:%S%.f%:z",
        DatetimeKind::Timestamp => "%Y-%m-%dT%H:%M:%S%.f",
        DatetimeKind::TimestampTz => "%Y-%m-%dT%H:%M:%S%.f%:z",
    };
    bdt.to_string(canon).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_parses_date() {
        let pd = try_13_formats("2024-01-15").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Date);
        assert_eq!(pd.iso, "2024-01-15");
    }

    #[test]
    fn iso_parses_timestamp_no_tz() {
        let pd = try_13_formats("2024-01-15 12:30:45").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Timestamp);
        assert!(pd.iso.starts_with("2024-01-15T12:30:45"));
    }

    #[test]
    fn iso_parses_timestamp_t_separator() {
        let pd = try_13_formats("2024-01-15T12:30:45").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Timestamp);
    }

    #[test]
    fn iso_parses_timestamp_tz_offset() {
        let pd = try_13_formats("2024-01-15T12:30:45+05:00").unwrap();
        assert_eq!(pd.kind, DatetimeKind::TimestampTz);
        assert!(pd.iso.ends_with("+05:00") || pd.iso.ends_with("+0500"));
    }

    #[test]
    fn iso_parses_time_only() {
        let pd = try_13_formats("12:30:45").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Time);
    }

    #[test]
    fn iso_parses_time_with_tz() {
        let pd = try_13_formats("12:30:45+02:00").unwrap();
        assert_eq!(pd.kind, DatetimeKind::TimeTz);
    }

    #[test]
    fn iso_rejects_garbage() {
        let err = try_13_formats("not-a-date").unwrap_err();
        assert!(matches!(err, Error::DatetimeFormatNotRecognized(_)));
    }

    #[test]
    fn iso_rejects_partial_match() {
        let err = try_13_formats("2024-01").unwrap_err();
        assert!(matches!(err, Error::DatetimeFormatNotRecognized(_)));
    }

    #[test]
    fn iso_microseconds_round_trip() {
        let pd = try_13_formats("2024-01-15 12:30:45.123456").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Timestamp);
        assert!(pd.iso.contains("123456") || pd.iso.contains(".123"));
    }

    #[test]
    fn iso_normalizes_short_tz_offset() {
        // "12:35:00+01" → "12:35:00+01:00" (short to padded)
        assert_eq!(
            normalize_tz_offset("12:35:00+01").as_deref(),
            Some("12:35:00+01:00")
        );
        assert_eq!(
            normalize_tz_offset("12:35:00+1").as_deref(),
            Some("12:35:00+01:00")
        );
        assert_eq!(
            normalize_tz_offset("12:35:00 +1").as_deref(),
            Some("12:35:00+01:00")
        );
        assert_eq!(
            normalize_tz_offset("12:35:00-2").as_deref(),
            Some("12:35:00-02:00")
        );
    }

    #[test]
    fn iso_parses_timetz_short_offset() {
        let pd = try_13_formats("12:35:00+01").unwrap();
        assert_eq!(pd.kind, DatetimeKind::TimeTz);
    }

    #[test]
    fn numeric_timezone_obeys_pg_tz_directive_bounds() {
        assert!(try_13_formats("12:35:00+15:59").is_ok());
        assert!(try_13_formats("12:35:00+16:00").is_err());
        assert!(try_13_formats("12:35:00+05:30:45").is_err());
        assert!(try_13_formats("12:35:00+0530").is_err());

        // PG's `TZ` falls through to `OF`, whose integer parser accepts
        // variable-width fields before a separator/end despite the HH:MM
        // display shape.
        assert_eq!(
            try_13_formats("12:35:00+3:1").unwrap().iso,
            "12:35:00+03:01"
        );
        assert_eq!(
            try_13_formats("12:35:00+0003:001").unwrap().iso,
            "12:35:00+03:01"
        );

        // `TZ` falls through to `OF`; in PG, a blank is a positive sign.
        assert_eq!(
            try_13_formats("12:35:00 3:1").unwrap().iso,
            "12:35:00+03:01"
        );

        // The timestamp's date/time separator is not an offset boundary.
        assert_eq!(
            try_13_formats("2024-01-15 12:30:45").unwrap().kind,
            DatetimeKind::Timestamp
        );
    }

    #[test]
    fn named_timezone_must_immediately_follow_iso_time() {
        assert_eq!(try_13_formats("12:35:00EST").unwrap().iso, "12:35:00-05:00");
        assert!(try_13_formats("12:35:00 EST").is_err());
    }

    #[test]
    fn bare_time_abbreviation_uses_pg_zero_initialized_date() {
        let lookup = civil_for_lookup("12:34:56").unwrap();
        assert_eq!(lookup.date(), jiff::civil::date(0, 1, 1));
        assert_eq!(lookup.time(), jiff::civil::time(12, 34, 56, 0));
    }

    #[test]
    fn wide_year_date_is_gregorian_and_pg_range_checked() {
        assert!(try_13_formats("1000000-02-29").is_ok());
        assert!(try_13_formats("1000000-02-30").is_err());
        assert!(try_13_formats("1000001-02-29").is_err());
        assert!(try_13_formats("5874897-12-31").is_ok());
        assert!(try_13_formats("5874898-01-01").is_err());
    }

    #[test]
    fn wide_timestamps_use_postgres_bounds_not_jiffs() {
        let timestamp = try_13_formats("10000-01-01T12:34:56.123456").unwrap();
        assert_eq!(timestamp.kind, DatetimeKind::Timestamp);
        assert_eq!(timestamp.iso, "10000-01-01T12:34:56.123456");

        let maximum = try_13_formats("294276-12-31T23:59:59.999999+00:00").unwrap();
        assert_eq!(maximum.kind, DatetimeKind::TimestampTz);
        assert_eq!(maximum.iso, "294276-12-31T23:59:59.999999+00:00");
        assert!(try_13_formats("294277-01-01T00:00:00+00:00").is_err());

        // The local civil value may cross the boundary if its offset maps the
        // represented instant back into PostgreSQL's half-open range.
        assert!(try_13_formats("294277-01-01T00:00:00+01:00").is_ok());

        let minimum = try_13_formats("-4714-11-24T00:00:00").unwrap();
        assert_eq!(minimum.iso, "-4713-11-24T00:00:00");
        assert!(try_13_formats("-4714-11-23T23:59:59.999999").is_err());
        assert!(try_13_formats("-4714-11-23T23:00:00-01:00").is_ok());
    }

    #[test]
    fn wide_date_cannot_silently_overflow_a_timestamp_cast() {
        let date = try_13_formats("1000000-01-01").unwrap();
        assert!(matches!(
            cast_kind(
                date,
                DatetimeKind::Timestamp,
                "1000000-01-01",
                "timestamp",
                false,
                &jiff::tz::TimeZone::UTC,
                jiff::civil::date(2024, 1, 1),
            ),
            Err(Error::FormatNotRecognized("timestamp", _))
        ));
    }

    #[test]
    fn wide_timestamp_typmod_rounds_and_enforces_the_upper_bound() {
        let ordinary = try_13_formats("10000-12-31T23:59:59.999499").unwrap();
        assert_eq!(
            round_fractional(ordinary, 3).unwrap().iso,
            "10000-12-31T23:59:59.999"
        );

        let maximum = try_13_formats("294276-12-31T23:59:59.999999").unwrap();
        assert!(matches!(
            round_fractional(maximum, 0),
            Err(Error::InvalidDatetimeInput(_))
        ));
    }

    #[test]
    fn wide_named_zone_casts_preserve_pg_gap_and_fold_policies() {
        let ny = jiff::tz::TimeZone::get("America/New_York").unwrap();
        let session_date = jiff::civil::date(2024, 1, 1);
        for (input, expected) in [
            ("10024-03-10T02:30:00", "10024-03-10T02:30:00-05:00"),
            ("10024-11-03T01:30:00", "10024-11-03T01:30:00-05:00"),
        ] {
            let converted = cast_kind(
                try_13_formats(input).unwrap(),
                DatetimeKind::TimestampTz,
                input,
                "timestamp_tz",
                true,
                &ny,
                session_date,
            )
            .unwrap();
            assert_eq!(converted.iso, expected);
        }
    }

    #[test]
    fn pg_gap_before_and_fold_after_resolution() {
        let ny = jiff::tz::TimeZone::get("America/New_York").unwrap();
        let gap = jiff::civil::date(2023, 3, 12).at(2, 30, 0, 0);
        let fold = jiff::civil::date(2023, 11, 5).at(1, 30, 0, 0);
        assert_eq!(resolve_pg_offset(&ny, gap).seconds(), -18_000);
        assert_eq!(resolve_pg_offset(&ny, fold).seconds(), -18_000);

        // Jiff's compatible policy imputes the same instant for a gap, but
        // selects the first occurrence in a fold where PG selects the second.
        assert_eq!(
            ny.to_ambiguous_timestamp(gap)
                .compatible()
                .unwrap()
                .as_second(),
            resolve_pg_offset(&ny, gap)
                .to_timestamp(gap)
                .unwrap()
                .as_second()
        );
        assert_ne!(
            ny.to_ambiguous_timestamp(fold)
                .compatible()
                .unwrap()
                .as_second(),
            resolve_pg_offset(&ny, fold)
                .to_timestamp(fold)
                .unwrap()
                .as_second()
        );
    }

    #[test]
    fn time_to_timetz_uses_supplied_session_date() {
        let ny = jiff::tz::TimeZone::get("America/New_York").unwrap();
        let parsed = ParsedDatetime {
            iso: "12:00:00".to_owned(),
            kind: DatetimeKind::Time,
        };
        let winter = cast_kind(
            parsed.clone(),
            DatetimeKind::TimeTz,
            "12:00:00",
            "time_tz",
            true,
            &ny,
            jiff::civil::date(2023, 1, 15),
        )
        .unwrap();
        let summer = cast_kind(
            parsed,
            DatetimeKind::TimeTz,
            "12:00:00",
            "time_tz",
            true,
            &ny,
            jiff::civil::date(2023, 7, 15),
        )
        .unwrap();
        assert_eq!(winter.iso, "12:00:00-05:00");
        assert_eq!(summer.iso, "12:00:00-04:00");
    }

    #[test]
    fn timestamptz_to_timetz_preserves_fold_instant_offset() {
        let ny = jiff::tz::TimeZone::get("America/New_York").unwrap();
        let parsed = ParsedDatetime {
            // 06:30Z is the second 01:30 during New York's fall-back fold.
            iso: "2023-11-05T06:30:00+00:00".to_owned(),
            kind: DatetimeKind::TimestampTz,
        };
        let converted = cast_kind(
            parsed,
            DatetimeKind::TimeTz,
            "2023-11-05T06:30:00+00:00",
            "time_tz",
            true,
            &ny,
            jiff::civil::date(2023, 11, 5),
        )
        .unwrap();
        assert_eq!(converted.iso, "01:30:00-05:00");
    }

    #[test]
    fn timestamp_typmod_ties_round_away_from_pg_epoch() {
        let before = ParsedDatetime {
            iso: "1999-12-31T23:59:59.5".to_owned(),
            kind: DatetimeKind::Timestamp,
        };
        let after = ParsedDatetime {
            iso: "2000-01-01T00:00:00.5".to_owned(),
            kind: DatetimeKind::Timestamp,
        };
        assert_eq!(
            round_fractional(before, 0).unwrap().iso,
            "1999-12-31T23:59:59"
        );
        assert_eq!(
            round_fractional(after, 0).unwrap().iso,
            "2000-01-01T00:00:01"
        );
    }

    #[test]
    fn timestamptz_typmod_uses_instant_sign_not_wall_clock_sign() {
        let parsed = ParsedDatetime {
            // This wall time is after the PG epoch, but its +14 instant is before it.
            iso: "2000-01-01T00:00:00.5+14:00".to_owned(),
            kind: DatetimeKind::TimestampTz,
        };
        assert_eq!(
            round_fractional(parsed, 0).unwrap().iso,
            "2000-01-01T00:00:00+14:00"
        );
    }

    #[test]
    fn rounded_end_of_day_remains_parseable_internally() {
        let parsed = ParsedDatetime {
            iso: "23:59:59.999999+01:00".to_owned(),
            kind: DatetimeKind::TimeTz,
        };
        let rounded = round_fractional(parsed, 0).unwrap();
        assert_eq!(rounded.iso, "24:00:00+01:00");
        assert_eq!(parse_pg_time_nanos("24:00:00"), Some(86_400_000_000_000));

        // SQL/JSON's strict HH24 parser rejects 24 as direct source text;
        // the sentinel is valid only once core time typmod rounding creates it.
        assert!(try_13_formats("24:00:00").is_err());
    }

    #[test]
    fn fractional_rounding_rejects_out_of_contract_precision_without_panicking() {
        let parsed = ParsedDatetime {
            iso: "12:34:56.789".to_owned(),
            kind: DatetimeKind::Time,
        };
        assert!(round_fractional(parsed, 7).is_err());
    }
}
