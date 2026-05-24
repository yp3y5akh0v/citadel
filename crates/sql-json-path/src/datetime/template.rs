// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new module. Algorithm ported from PG `formatting.c` (BSD-
// style PostgreSQL License, Apache-2.0 compatible). No upstream Rust basis.

//! PG `to_timestamp`-style template directive parser for `.datetime("tpl")`.
//! Output type derived from PG's `DCH_DATED | DCH_TIMED | DCH_ZONED` bitmask.

use super::{DatetimeKind, ParsedDatetime};
use crate::eval::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Directive(Directive),
    Sep(char),
    Literal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Directive {
    kind: DirectiveKind,
    fm: bool,
    th: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectiveKind {
    // Year
    Y4,
    Y3,
    Y2,
    Y1,
    Iyyy4,
    Iyyy3,
    Iyyy2,
    Iyyy1,
    Rr2,
    Rr4,
    // Month
    Mm,
    MonShort,
    MonShortMixed,
    MonShortLower,
    MonthLong,
    MonthLongMixed,
    MonthLongLower,
    RomanUpper,
    RomanLower,
    // Day
    Dd,
    Ddd,
    D,
    Id,
    Iddd,
    DayLong,
    DayLongMixed,
    DayLongLower,
    DyShort,
    DyShortMixed,
    DyShortLower,
    // ISO week
    Iw,
    W,
    Ww,
    // Quarter / century
    Q,
    Cc,
    // Era
    Ad,
    Bc,
    AdDots,
    BcDots,
    AdLower,
    BcLower,
    AdDotsLower,
    BcDotsLower,
    // Time
    Hh12,
    Hh24,
    Mi,
    Ss,
    Ms,
    Us,
    Ff1,
    Ff2,
    Ff3,
    Ff4,
    Ff5,
    Ff6,
    Ssss,
    // Meridiem
    Am,
    Pm,
    AmDots,
    PmDots,
    AmLower,
    PmLower,
    AmDotsLower,
    PmDotsLower,
    // TZ
    Tzh,
    Tzm,
    Tz,
    Of,
    // Julian
    J,
}

#[derive(Default, Clone, Copy)]
struct TypeFlags {
    dated: bool,
    timed: bool,
    zoned: bool,
}

#[derive(Default)]
struct Fields {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    doy: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
    micros: Option<u32>,
    tz_off: Option<i32>,
    pm: Option<bool>,
    bc: bool,
    julian: Option<i32>,
}

pub(crate) fn parse_apply(input: &str, template: &str) -> Result<ParsedDatetime> {
    let nodes = parse_template(template)?;
    let mut flags = TypeFlags::default();
    for node in &nodes {
        if let Node::Directive(d) = node {
            mark_flags(d.kind, &mut flags);
        }
    }
    let kind = derive_kind(flags)?;
    let mut fields = Fields::default();
    apply_nodes(&nodes, input.trim(), &mut fields)?;
    materialize(&fields, kind)
}

// ---- template parse -------------------------------------------------------

fn parse_template(template: &str) -> Result<Vec<Node>> {
    let bytes = template.as_bytes();
    let mut nodes = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let mut j = i + 1;
            let mut lit = String::new();
            while j < bytes.len() && bytes[j] != b'"' {
                if bytes[j] == b'\\' && j + 1 < bytes.len() {
                    lit.push(bytes[j + 1] as char);
                    j += 2;
                } else {
                    lit.push(bytes[j] as char);
                    j += 1;
                }
            }
            if j >= bytes.len() {
                return Err(Error::InvalidDatetimeTemplate(
                    "unterminated literal".into(),
                ));
            }
            nodes.push(Node::Literal(lit));
            i = j + 1;
            continue;
        }
        let c = bytes[i] as char;
        if matches!(c, '-' | '.' | '/' | ',' | '\'' | ';' | ':' | ' ') {
            nodes.push(Node::Sep(c));
            i += 1;
            continue;
        }
        let (consumed, dir) = match_directive(&template[i..])?;
        nodes.push(Node::Directive(dir));
        i += consumed;
    }
    Ok(nodes)
}

fn match_directive(s: &str) -> Result<(usize, Directive)> {
    let (mut consumed, fm) = if s.starts_with("FM") || s.starts_with("fm") {
        (2, true)
    } else if s.starts_with("FX") || s.starts_with("fx") {
        (2, false) // FX is no-op (jsonpath is always strict)
    } else {
        (0, false)
    };
    let rest = &s[consumed..];

    // Order matters: `MONTH` matches before `MON`.
    const TABLE: &[(&str, DirectiveKind)] = &[
        // Time fractions first (FF1-FF6 before FF)
        ("FF1", DirectiveKind::Ff1),
        ("FF2", DirectiveKind::Ff2),
        ("FF3", DirectiveKind::Ff3),
        ("FF4", DirectiveKind::Ff4),
        ("FF5", DirectiveKind::Ff5),
        ("FF6", DirectiveKind::Ff6),
        ("ff1", DirectiveKind::Ff1),
        ("ff2", DirectiveKind::Ff2),
        ("ff3", DirectiveKind::Ff3),
        ("ff4", DirectiveKind::Ff4),
        ("ff5", DirectiveKind::Ff5),
        ("ff6", DirectiveKind::Ff6),
        // Years
        ("IYYY", DirectiveKind::Iyyy4),
        ("IYY", DirectiveKind::Iyyy3),
        ("IY", DirectiveKind::Iyyy2),
        ("iyyy", DirectiveKind::Iyyy4),
        ("iyy", DirectiveKind::Iyyy3),
        ("iy", DirectiveKind::Iyyy2),
        ("RRRR", DirectiveKind::Rr4),
        ("rrrr", DirectiveKind::Rr4),
        ("YYYY", DirectiveKind::Y4),
        ("YYY", DirectiveKind::Y3),
        ("YY", DirectiveKind::Y2),
        ("yyyy", DirectiveKind::Y4),
        ("yyy", DirectiveKind::Y3),
        ("yy", DirectiveKind::Y2),
        ("RR", DirectiveKind::Rr2),
        ("rr", DirectiveKind::Rr2),
        // Months
        ("MONTH", DirectiveKind::MonthLong),
        ("Month", DirectiveKind::MonthLongMixed),
        ("month", DirectiveKind::MonthLongLower),
        ("MON", DirectiveKind::MonShort),
        ("Mon", DirectiveKind::MonShortMixed),
        ("mon", DirectiveKind::MonShortLower),
        ("RM", DirectiveKind::RomanUpper),
        ("rm", DirectiveKind::RomanLower),
        ("MM", DirectiveKind::Mm),
        ("MI", DirectiveKind::Mi),
        ("mm", DirectiveKind::Mm),
        ("mi", DirectiveKind::Mi),
        // Days
        ("IDDD", DirectiveKind::Iddd),
        ("DDD", DirectiveKind::Ddd),
        ("iddd", DirectiveKind::Iddd),
        ("ddd", DirectiveKind::Ddd),
        ("DAY", DirectiveKind::DayLong),
        ("Day", DirectiveKind::DayLongMixed),
        ("day", DirectiveKind::DayLongLower),
        ("DY", DirectiveKind::DyShort),
        ("Dy", DirectiveKind::DyShortMixed),
        ("dy", DirectiveKind::DyShortLower),
        ("DD", DirectiveKind::Dd),
        ("ID", DirectiveKind::Id),
        ("dd", DirectiveKind::Dd),
        ("id", DirectiveKind::Id),
        // ISO week / Q / CC
        ("IW", DirectiveKind::Iw),
        ("WW", DirectiveKind::Ww),
        ("CC", DirectiveKind::Cc),
        ("iw", DirectiveKind::Iw),
        ("ww", DirectiveKind::Ww),
        ("cc", DirectiveKind::Cc),
        // Time / TZ
        ("HH24", DirectiveKind::Hh24),
        ("HH12", DirectiveKind::Hh12),
        ("HH", DirectiveKind::Hh12),
        ("hh24", DirectiveKind::Hh24),
        ("hh12", DirectiveKind::Hh12),
        ("hh", DirectiveKind::Hh12),
        ("SSSSS", DirectiveKind::Ssss),
        ("SSSS", DirectiveKind::Ssss),
        ("sssss", DirectiveKind::Ssss),
        ("ssss", DirectiveKind::Ssss),
        ("MS", DirectiveKind::Ms),
        ("US", DirectiveKind::Us),
        ("SS", DirectiveKind::Ss),
        ("ms", DirectiveKind::Ms),
        ("us", DirectiveKind::Us),
        ("ss", DirectiveKind::Ss),
        ("TZH", DirectiveKind::Tzh),
        ("TZM", DirectiveKind::Tzm),
        ("OF", DirectiveKind::Of),
        ("TZ", DirectiveKind::Tz),
        ("tzh", DirectiveKind::Tzh),
        ("tzm", DirectiveKind::Tzm),
        ("of", DirectiveKind::Of),
        ("tz", DirectiveKind::Tz),
        // Meridiem
        ("A.M.", DirectiveKind::AmDots),
        ("P.M.", DirectiveKind::PmDots),
        ("a.m.", DirectiveKind::AmDotsLower),
        ("p.m.", DirectiveKind::PmDotsLower),
        ("AM", DirectiveKind::Am),
        ("PM", DirectiveKind::Pm),
        ("am", DirectiveKind::AmLower),
        ("pm", DirectiveKind::PmLower),
        // Era
        ("A.D.", DirectiveKind::AdDots),
        ("B.C.", DirectiveKind::BcDots),
        ("a.d.", DirectiveKind::AdDotsLower),
        ("b.c.", DirectiveKind::BcDotsLower),
        ("AD", DirectiveKind::Ad),
        ("BC", DirectiveKind::Bc),
        ("ad", DirectiveKind::AdLower),
        ("bc", DirectiveKind::BcLower),
        // Single-letter
        ("D", DirectiveKind::D),
        ("Q", DirectiveKind::Q),
        ("W", DirectiveKind::W),
        ("Y", DirectiveKind::Y1),
        ("I", DirectiveKind::Iyyy1),
        ("J", DirectiveKind::J),
    ];

    for tm in &["SP", "sp", "TM", "tm"] {
        if rest.starts_with(tm) {
            return Err(Error::UnsupportedDatetimeDirective((*tm).into()));
        }
    }

    for (pat, kind) in TABLE {
        if rest.starts_with(pat) {
            consumed += pat.len();
            let after = &s[consumed..];
            let th = if after.starts_with("TH") || after.starts_with("th") {
                consumed += 2;
                true
            } else {
                false
            };
            return Ok((
                consumed,
                Directive {
                    kind: *kind,
                    fm,
                    th,
                },
            ));
        }
    }

    let bad = rest.chars().next().unwrap_or('?');
    Err(Error::DatetimeInvalidSeparator(format!("\"{bad}\"").into()))
}

// ---- apply --------------------------------------------------------------

fn apply_nodes(nodes: &[Node], input: &str, fields: &mut Fields) -> Result<()> {
    let mut cur = input;
    for node in nodes {
        cur = apply_node(node, cur, fields)?;
    }
    if !cur.is_empty() {
        return Err(Error::DatetimeTrailingInput);
    }
    Ok(())
}

fn apply_node<'a>(node: &Node, input: &'a str, f: &mut Fields) -> Result<&'a str> {
    match node {
        Node::Sep(c) => {
            if input.is_empty() {
                return Err(Error::DatetimeInputTooShort);
            }
            let rest = input
                .strip_prefix(*c)
                .ok_or_else(|| Error::DatetimeUnmatchedChar(format!("\"{c}\"").into()))?;
            Ok(rest)
        }
        Node::Literal(lit) => {
            let rest = input.strip_prefix(lit.as_str()).ok_or_else(|| {
                let c = lit.chars().next().unwrap_or('?');
                Error::DatetimeUnmatchedChar(format!("\"{c}\"").into())
            })?;
            Ok(rest)
        }
        Node::Directive(d) => apply_directive(d, input, f),
    }
}

fn apply_directive<'a>(d: &Directive, input: &'a str, f: &mut Fields) -> Result<&'a str> {
    use DirectiveKind::*;
    match d.kind {
        Y4 | Iyyy4 | Rr4 => {
            let name = match d.kind {
                Y4 => "YYYY",
                Iyyy4 => "IYYY",
                Rr4 => "RRRR",
                _ => unreachable!(),
            };
            let (n, rest) = take_digits_for(input, 4, name)?;
            f.year = Some(n as i32);
            Ok(rest)
        }
        Y3 | Iyyy3 => {
            let (n, rest) = take_digits_for(input, 3, "YYY")?;
            f.year = Some(2000 + n as i32);
            Ok(rest)
        }
        Y2 | Iyyy2 | Rr2 => {
            let (n, rest) = take_digits_for(input, 2, "YY")?;
            f.year = Some(2000 + n as i32);
            Ok(rest)
        }
        Y1 | Iyyy1 => {
            let (n, rest) = take_digits_for(input, 1, "Y")?;
            f.year = Some(2020 + n as i32);
            Ok(rest)
        }
        Mm => {
            let (n, rest) = take_digits_for(input, 2, "MM")?;
            f.month = Some(n);
            Ok(rest)
        }
        Mi => {
            let (n, rest) = take_digits_for(input, 2, "MI")?;
            f.minute = Some(n);
            Ok(rest)
        }
        Dd => {
            let (n, rest) = take_digits_for(input, 2, "DD")?;
            f.day = Some(n);
            Ok(rest)
        }
        Ddd | Iddd => {
            let (n, rest) = take_digits(input, 3)?;
            f.doy = Some(n);
            Ok(rest)
        }
        D | Id => {
            let (_, rest) = take_digits(input, 1)?;
            Ok(rest)
        }
        Iw | W | Ww | Q | Cc => {
            let (_, rest) = take_digits(input, 2)?;
            Ok(rest)
        }
        Hh12 | Hh24 => {
            let name = match d.kind {
                Hh24 => "HH24",
                Hh12 => "HH",
                _ => unreachable!(),
            };
            let (n, rest) = take_digits_for(input, 2, name)?;
            f.hour = Some(n);
            Ok(rest)
        }
        Ss => {
            let (n, rest) = take_digits_for(input, 2, "SS")?;
            f.second = Some(n);
            Ok(rest)
        }
        Ms => {
            let (n, rest) = take_digits(input, 3)?;
            f.micros = Some(n * 1_000);
            Ok(rest)
        }
        Us => {
            let (n, rest) = take_digits(input, 6)?;
            f.micros = Some(n);
            Ok(rest)
        }
        Ff1 | Ff2 | Ff3 | Ff4 | Ff5 | Ff6 => {
            let n_digits = match d.kind {
                Ff1 => 1,
                Ff2 => 2,
                Ff3 => 3,
                Ff4 => 4,
                Ff5 => 5,
                _ => 6,
            };
            let (n, rest) = take_digits(input, n_digits)?;
            let scale = 10u32.pow((6 - n_digits) as u32);
            f.micros = Some(n * scale);
            Ok(rest)
        }
        Ssss => {
            let (n, rest) = take_digits(input, 5)?;
            f.hour = Some(n / 3600);
            f.minute = Some((n % 3600) / 60);
            f.second = Some(n % 60);
            Ok(rest)
        }
        Tzh => {
            let (sign, rest) = take_sign(input);
            let (h, rest) = take_digits_1_or_2(rest)?;
            let prev = f.tz_off.unwrap_or(0);
            let mins = prev.rem_euclid(3600) / 60;
            f.tz_off = Some(sign * (h as i32 * 3600 + mins * 60));
            Ok(rest)
        }
        Tzm => {
            let (m, rest) = take_digits_1_or_2(input)?;
            let sign = f.tz_off.map(|t| t.signum()).unwrap_or(1);
            let h = f.tz_off.unwrap_or(0).abs() / 3600;
            f.tz_off = Some(sign * (h * 3600 + m as i32 * 60));
            Ok(rest)
        }
        Of | Tz => {
            let (sign, rest) = take_sign(input);
            let (h, rest) = take_digits(rest, 2)?;
            let mut total = h as i32 * 3600;
            let rest = if let Some(rest2) = rest.strip_prefix(':') {
                let (m, rest3) = take_digits(rest2, 2)?;
                total += m as i32 * 60;
                rest3
            } else {
                rest
            };
            f.tz_off = Some(sign * total);
            Ok(rest)
        }
        J => {
            let (n, rest) = take_digits_var(input)?;
            f.julian = Some(n as i32);
            Ok(rest)
        }
        Am | AmDots | AmLower | AmDotsLower => {
            let len = if matches!(d.kind, AmDots | AmDotsLower) {
                4
            } else {
                2
            };
            input
                .get(..len)
                .ok_or_else(|| Error::InvalidDatetimeInput("expected AM marker".into()))?;
            f.pm = Some(false);
            Ok(&input[len..])
        }
        Pm | PmDots | PmLower | PmDotsLower => {
            let len = if matches!(d.kind, PmDots | PmDotsLower) {
                4
            } else {
                2
            };
            input
                .get(..len)
                .ok_or_else(|| Error::InvalidDatetimeInput("expected PM marker".into()))?;
            f.pm = Some(true);
            Ok(&input[len..])
        }
        Ad | AdLower => {
            f.bc = false;
            Ok(input.get(2..).unwrap_or(""))
        }
        Bc | BcLower => {
            f.bc = true;
            Ok(input.get(2..).unwrap_or(""))
        }
        AdDots | AdDotsLower => {
            f.bc = false;
            Ok(input.get(4..).unwrap_or(""))
        }
        BcDots | BcDotsLower => {
            f.bc = true;
            Ok(input.get(4..).unwrap_or(""))
        }
        MonShort | MonShortMixed | MonShortLower => {
            let head = input
                .get(..3)
                .ok_or_else(|| Error::InvalidDatetimeInput("expected month name".into()))?;
            f.month = Some(month_short_to_num(head)?);
            Ok(&input[3..])
        }
        MonthLong | MonthLongMixed | MonthLongLower => {
            let (m, len) = month_long_match(input)?;
            f.month = Some(m);
            Ok(&input[len..])
        }
        DayLong | DayLongMixed | DayLongLower | DyShort | DyShortMixed | DyShortLower => {
            let len = weekday_match_len(input)?;
            Ok(&input[len..])
        }
        RomanUpper | RomanLower => {
            let (m, len) = roman_month_match(input)?;
            f.month = Some(m);
            Ok(&input[len..])
        }
    }
}

// ---- materialize --------------------------------------------------------

fn materialize(f: &Fields, kind: DatetimeKind) -> Result<ParsedDatetime> {
    let year = if f.bc {
        f.year.map(|y| -y + 1).unwrap_or(1970)
    } else {
        f.year.unwrap_or(1970)
    };
    let month = f.month.unwrap_or(1) as i8;
    let day = f.day.unwrap_or(1) as i8;
    let hour = {
        let h = f.hour.unwrap_or(0);
        match f.pm {
            Some(true) if h < 12 => h + 12,
            Some(false) if h == 12 => 0,
            _ => h,
        }
    } as i8;
    let minute = f.minute.unwrap_or(0) as i8;
    let second = f.second.unwrap_or(0) as i8;
    let micros = f.micros.unwrap_or(0) as i32;

    let iso = match kind {
        DatetimeKind::Date => {
            let d = jiff::civil::Date::new(year as i16, month, day)
                .map_err(|e| Error::InvalidDatetimeInput(format!("date: {e}").into()))?;
            d.to_string()
        }
        DatetimeKind::Time => {
            let t = jiff::civil::Time::new(hour, minute, second, micros * 1_000)
                .map_err(|e| Error::InvalidDatetimeInput(format!("time: {e}").into()))?;
            t.to_string()
        }
        DatetimeKind::TimeTz => {
            let t = jiff::civil::Time::new(hour, minute, second, micros * 1_000)
                .map_err(|e| Error::InvalidDatetimeInput(format!("time: {e}").into()))?;
            let off_s = f.tz_off.unwrap_or(0);
            format!("{}{}", t, format_offset(off_s))
        }
        DatetimeKind::Timestamp => {
            let d = jiff::civil::DateTime::new(
                year as i16,
                month,
                day,
                hour,
                minute,
                second,
                micros * 1_000,
            )
            .map_err(|e| Error::InvalidDatetimeInput(format!("timestamp: {e}").into()))?;
            d.to_string()
        }
        DatetimeKind::TimestampTz => {
            let d = jiff::civil::DateTime::new(
                year as i16,
                month,
                day,
                hour,
                minute,
                second,
                micros * 1_000,
            )
            .map_err(|e| Error::InvalidDatetimeInput(format!("timestamp: {e}").into()))?;
            let off_s = f.tz_off.unwrap_or(0);
            format!("{}{}", d, format_offset(off_s))
        }
    };
    Ok(ParsedDatetime { iso, kind })
}

fn format_offset(off_s: i32) -> String {
    let sign = if off_s < 0 { '-' } else { '+' };
    let abs = off_s.unsigned_abs();
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    format!("{sign}{h:02}:{m:02}")
}

// ---- bookkeeping ---------------------------------------------------------

fn mark_flags(k: DirectiveKind, t: &mut TypeFlags) {
    use DirectiveKind::*;
    match k {
        Y4 | Y3 | Y2 | Y1 | Iyyy4 | Iyyy3 | Iyyy2 | Iyyy1 | Rr2 | Rr4 | Mm | MonShort
        | MonShortMixed | MonShortLower | MonthLong | MonthLongMixed | MonthLongLower
        | RomanUpper | RomanLower | Dd | Ddd | D | Id | Iddd | DayLong | DayLongMixed
        | DayLongLower | DyShort | DyShortMixed | DyShortLower | Iw | W | Ww | Q | Cc | Ad | Bc
        | AdDots | BcDots | AdLower | BcLower | AdDotsLower | BcDotsLower | J => {
            t.dated = true;
        }
        Hh12 | Hh24 | Mi | Ss | Ms | Us | Ff1 | Ff2 | Ff3 | Ff4 | Ff5 | Ff6 | Ssss | Am | Pm
        | AmDots | PmDots | AmLower | PmLower | AmDotsLower | PmDotsLower => {
            t.timed = true;
        }
        Tzh | Tzm | Tz | Of => {
            t.zoned = true;
        }
    }
}

fn derive_kind(t: TypeFlags) -> Result<DatetimeKind> {
    match (t.dated, t.timed, t.zoned) {
        (true, true, true) => Ok(DatetimeKind::TimestampTz),
        (true, true, false) => Ok(DatetimeKind::Timestamp),
        (true, false, true) => Err(Error::DatetimeZonedNotTimed),
        (true, false, false) => Ok(DatetimeKind::Date),
        (false, true, true) => Ok(DatetimeKind::TimeTz),
        (false, true, false) => Ok(DatetimeKind::Time),
        (false, false, _) => Err(Error::InvalidDatetimeTemplate(
            "template has no date or time fields".into(),
        )),
    }
}

// ---- helpers ------------------------------------------------------------

fn take_digits(input: &str, n: usize) -> Result<(u32, &str)> {
    take_digits_for(input, n, "field")
}

fn take_digits_for<'a>(input: &'a str, n: usize, directive: &str) -> Result<(u32, &'a str)> {
    let bytes = input.as_bytes();
    if bytes.len() < n {
        return Err(Error::DatetimeInputTooShort);
    }
    let mut acc: u32 = 0;
    for i in 0..n {
        let c = bytes[i];
        if !c.is_ascii_digit() {
            let partial = &input[..n.min(bytes.len())];
            return Err(Error::DatetimeInvalidValue(
                format!("\"{partial}\"").into(),
                format!("\"{directive}\"").into(),
            ));
        }
        acc = acc * 10 + (c - b'0') as u32;
    }
    Ok((acc, &input[n..]))
}

/// Take 1 or 2 digits (used by TZH/TZM where PG accepts single-digit hours).
fn take_digits_1_or_2(input: &str) -> Result<(u32, &str)> {
    let bytes = input.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return Err(Error::DatetimeInputTooShort);
    }
    let mut n = (bytes[0] - b'0') as u32;
    if bytes.len() >= 2 && bytes[1].is_ascii_digit() {
        n = n * 10 + (bytes[1] - b'0') as u32;
        return Ok((n, &input[2..]));
    }
    Ok((n, &input[1..]))
}

fn take_digits_var(input: &str) -> Result<(u64, &str)> {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        return Err(Error::InvalidDatetimeInput("expected digits".into()));
    }
    let n: u64 = input[..i].parse().unwrap();
    Ok((n, &input[i..]))
}

fn take_sign(input: &str) -> (i32, &str) {
    if let Some(rest) = input.strip_prefix('+') {
        (1, rest)
    } else if let Some(rest) = input.strip_prefix('-') {
        (-1, rest)
    } else {
        (1, input)
    }
}

fn month_short_to_num(s: &str) -> Result<u32> {
    const MONS: &[(&str, u32)] = &[
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("may", 5),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ];
    let lower = s.to_ascii_lowercase();
    for (m, n) in MONS {
        if lower == *m {
            return Ok(*n);
        }
    }
    Err(Error::InvalidDatetimeInput(
        format!("unknown month: {s:?}").into(),
    ))
}

fn month_long_match(s: &str) -> Result<(u32, usize)> {
    const MONS: &[(&str, u32)] = &[
        ("january", 1),
        ("february", 2),
        ("march", 3),
        ("april", 4),
        ("may", 5),
        ("june", 6),
        ("july", 7),
        ("august", 8),
        ("september", 9),
        ("october", 10),
        ("november", 11),
        ("december", 12),
    ];
    let lower = s.to_ascii_lowercase();
    for (m, n) in MONS {
        if lower.starts_with(m) {
            return Ok((*n, m.len()));
        }
    }
    Err(Error::InvalidDatetimeInput(
        format!("unknown month: {:?}", &s[..s.len().min(10)]).into(),
    ))
}

fn weekday_match_len(s: &str) -> Result<usize> {
    const DAYS: &[&str] = &[
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "mon",
        "tue",
        "wed",
        "thu",
        "fri",
        "sat",
        "sun",
    ];
    let lower = s.to_ascii_lowercase();
    for d in DAYS {
        if lower.starts_with(d) {
            return Ok(d.len());
        }
    }
    Err(Error::InvalidDatetimeInput("unknown weekday".into()))
}

fn roman_month_match(s: &str) -> Result<(u32, usize)> {
    const ROMANS: &[(&str, u32)] = &[
        ("XII", 12),
        ("XI", 11),
        ("X", 10),
        ("IX", 9),
        ("VIII", 8),
        ("VII", 7),
        ("VI", 6),
        ("V", 5),
        ("IV", 4),
        ("III", 3),
        ("II", 2),
        ("I", 1),
    ];
    let upper = s.to_ascii_uppercase();
    for (r, n) in ROMANS {
        if upper.starts_with(r) {
            return Ok((*n, r.len()));
        }
    }
    Err(Error::InvalidDatetimeInput("invalid roman month".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_yyyy_mm_dd() {
        let pd = parse_apply("2024-01-15", "YYYY-MM-DD").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Date);
        assert_eq!(pd.iso, "2024-01-15");
    }

    #[test]
    fn template_timestamp() {
        let pd = parse_apply("2024-01-15 12:30:45", "YYYY-MM-DD HH24:MI:SS").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Timestamp);
        assert!(pd.iso.starts_with("2024-01-15T12:30:45"));
    }

    #[test]
    fn template_timestamp_tz() {
        let pd = parse_apply("2024-01-15 12:30:45+05:30", "YYYY-MM-DD HH24:MI:SSTZH:TZM").unwrap();
        assert_eq!(pd.kind, DatetimeKind::TimestampTz);
    }

    #[test]
    fn template_microseconds() {
        let pd = parse_apply("2024-01-15 12:30:45.123456", "YYYY-MM-DD HH24:MI:SS.US").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Timestamp);
    }

    #[test]
    fn template_month_name() {
        let pd = parse_apply("Jan 15 2024", "MON DD YYYY").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Date);
        assert_eq!(pd.iso, "2024-01-15");
    }

    #[test]
    fn template_roman_month() {
        let pd = parse_apply("VII 04 2024", "RM DD YYYY").unwrap();
        assert_eq!(pd.kind, DatetimeKind::Date);
        assert_eq!(pd.iso, "2024-07-04");
    }

    #[test]
    fn template_unknown_directive_errors() {
        let err = parse_apply("2024", "XYZ").unwrap_err();
        assert!(matches!(err, Error::DatetimeInvalidSeparator(_)));
    }

    #[test]
    fn template_sp_directive_unsupported() {
        let err = parse_apply("January", "MONTHSP").unwrap_err();
        assert!(matches!(err, Error::UnsupportedDatetimeDirective(_)));
    }

    #[test]
    fn template_separator_mismatch_errors() {
        let err = parse_apply("2024/01/15", "YYYY-MM-DD").unwrap_err();
        assert!(matches!(err, Error::DatetimeUnmatchedChar(_)));
    }

    #[test]
    fn template_zoned_not_timed_errors() {
        let err = parse_apply("2024", "YYYYTZH").unwrap_err();
        assert!(matches!(err, Error::DatetimeZonedNotTimed));
    }
}
