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

pub(crate) fn try_13_formats(input: &str) -> Result<ParsedDatetime> {
    let trimmed = input.trim();
    let normalized = normalize_tz_offset(trimmed);
    for (fmt, kind) in ISO_FORMATS {
        if let Ok(bdt) = BrokenDownTime::parse(fmt, normalized.as_str()) {
            if let Some(iso) = render(bdt, *kind) {
                return Ok(ParsedDatetime { iso, kind: *kind });
            }
        }
    }
    if let Some(pd) = try_wide_year_date(trimmed) {
        return Ok(pd);
    }
    Err(Error::DatetimeFormatNotRecognized(
        format!("\"{trimmed}\"").into(),
    ))
}

fn try_wide_year_date(s: &str) -> Option<ParsedDatetime> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year_str = parts[0];
    if year_str.len() <= 4 || !year_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let year: u64 = year_str.parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let iso = format!("{}-{:02}-{:02}", year, month, day);
    Some(ParsedDatetime {
        iso,
        kind: DatetimeKind::Date,
    })
}

fn normalize_tz_offset(input: &str) -> String {
    let bytes = input.as_bytes();
    let n = bytes.len();
    let mut start: Option<usize> = None;
    for i in (0..n).rev() {
        let c = bytes[i];
        if (c == b'+' || c == b'-') && i >= 8 {
            let prev = bytes[i - 1];
            if prev.is_ascii_digit() || prev == b' ' {
                let rest = &bytes[i + 1..];
                if !rest.is_empty() && rest.iter().all(|&b| b.is_ascii_digit() || b == b':') {
                    start = Some(i);
                    break;
                }
            }
        }
    }
    let Some(s) = start else {
        return input.to_string();
    };
    let sign = bytes[s] as char;
    let rest = &input[s + 1..];
    let (h_str, m_str) = match rest.split_once(':') {
        Some((h, m)) => (h, m),
        None if rest.len() == 4 => (&rest[..2], &rest[2..]),
        None => (rest, "0"),
    };
    let h: u32 = match h_str.parse() {
        Ok(v) => v,
        Err(_) => return input.to_string(),
    };
    let m: u32 = match m_str.parse() {
        Ok(v) => v,
        Err(_) => return input.to_string(),
    };
    let head = input[..s].trim_end_matches(' ');
    format!("{head}{sign}{:02}:{:02}", h, m)
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
        assert_eq!(normalize_tz_offset("12:35:00+01"), "12:35:00+01:00");
        assert_eq!(normalize_tz_offset("12:35:00+1"), "12:35:00+01:00");
        assert_eq!(normalize_tz_offset("12:35:00 +1"), "12:35:00+01:00");
        assert_eq!(normalize_tz_offset("12:35:00-2"), "12:35:00-02:00");
    }

    #[test]
    fn iso_parses_timetz_short_offset() {
        let pd = try_13_formats("12:35:00+01").unwrap();
        assert_eq!(pd.kind, DatetimeKind::TimeTz);
    }
}
