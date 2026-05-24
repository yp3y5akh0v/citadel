// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new — module-level integration tests for `.datetime()`
// parsing, applied through the full JsonPath evaluator.

use serde_json::{json, Value};

use crate::JsonPath;

fn query_first(jp: &str, input: &Value) -> Option<Value> {
    let p = JsonPath::new(jp).unwrap();
    p.query_first(input).unwrap().map(|c| c.into_owned())
}

#[test]
fn datetime_no_template_iso_date() {
    let v = json!("2024-01-15");
    let got = query_first("$.datetime()", &v).unwrap();
    assert_eq!(got, json!("2024-01-15"));
}

#[test]
fn datetime_no_template_iso_timestamp() {
    let v = json!("2024-01-15T12:30:45");
    let got = query_first("$.datetime()", &v).unwrap();
    let s = got.as_str().unwrap();
    assert!(s.starts_with("2024-01-15T12:30:45"));
}

#[test]
fn datetime_with_template() {
    let v = json!("2024-01-15");
    let got = query_first("$.datetime(\"YYYY-MM-DD\")", &v).unwrap();
    assert_eq!(got, json!("2024-01-15"));
}

#[test]
fn datetime_template_month_name() {
    let v = json!("Mar 05 2024");
    let got = query_first("$.datetime(\"MON DD YYYY\")", &v).unwrap();
    assert_eq!(got, json!("2024-03-05"));
}

#[test]
fn datetime_garbage_input_errors() {
    let v = json!("not-a-date");
    let p = JsonPath::new("$.datetime()").unwrap();
    let err = p.query(&v).unwrap_err();
    assert!(matches!(
        err,
        crate::EvalError::DatetimeFormatNotRecognized(_)
    ));
}

#[test]
fn datetime_non_string_input_errors() {
    let v = json!(123);
    let p = JsonPath::new("$.datetime()").unwrap();
    let err = p.query(&v).unwrap_err();
    assert!(matches!(err, crate::EvalError::DatetimeNotString));
}

fn match_tz_bool(jp: &str, input: &Value) -> bool {
    let p = JsonPath::new(jp).unwrap();
    match p.query_first_tz(input).unwrap() {
        Some(c) => c.into_owned() == json!(true),
        None => false,
    }
}

#[test]
fn cmp_wide_year_date_vs_in_range_timestamptz() {
    let v = json!("1000000-01-01");
    assert!(match_tz_bool(
        "$.datetime() > \"2020-01-01T12:00:00+00:00\".datetime()",
        &v
    ));
}

#[test]
fn cmp_two_in_range_timestamptz_still_uses_instant() {
    let v = json!("2024-03-10T10:00:00+05:00");
    assert!(match_tz_bool(
        "$.datetime() == \"2024-03-10T05:00:00+00:00\".datetime()",
        &v
    ));
}

#[test]
fn cmp_two_wide_year_dates_numeric_ymd() {
    let v = json!("1000000-01-01");
    assert!(match_tz_bool(
        "$.datetime() < \"2000000-01-01\".datetime()",
        &v
    ));
}
