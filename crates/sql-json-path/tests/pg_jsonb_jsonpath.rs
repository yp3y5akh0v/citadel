// Copyright 2023 RisingWave Labs
// Modifications Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file has been modified by Citadel contributors.

//! Runner for PostgreSQL's JSONPath regression corpus at commit
//! `e96b27296bec1c6212874eceb394b4f8b7d06dcc` (`REL_17_STABLE` at import).

use libtest_mimic::{Arguments, Failed, Trial};
use sql_json_path::{EvalError, JsonPath};
use std::str::FromStr;

fn main() {
    let args = Arguments::from_args();
    let corpus = include_str!("jsonb_jsonpath.out");
    let parsed = parse_script(corpus);
    let parsed_count = parsed.len();
    let mut tests = vec![Trial::test(
        "jsonb_jsonpath.out:REL_17_STABLE sha256",
        move || {
            use sha2::Digest;

            let actual = format!("{:X}", sha2::Sha256::digest(corpus.as_bytes()));
            let expected = "31EC2F164D66968164E952CD0850A371D59E86B5B8AACCF6561A9322697BFA83";
            (actual == expected)
                .then_some(())
                .ok_or_else(|| format!("expected corpus sha256 {expected}, got {actual}").into())
        },
    )];
    tests.push(Trial::test("jsonb_jsonpath.out:trial count", move || {
        (parsed_count == 815)
            .then_some(())
            .ok_or_else(|| format!("expected 815 corpus trials, parsed {parsed_count}").into())
    }));
    tests.extend(parsed);
    libtest_mimic::run(&args, tests).exit();
}

/// Drop a trailing `-- ...` comment so the statement accumulator can still see the `;`.
///
/// Several cases in PostgreSQL's corpus are annotated `-- should work`. Without this the
/// `ends_with(';')` loop keeps swallowing lines until the next statement, losing both.
/// Only a comment after the terminator is removed, so a `--` inside a literal is safe.
fn strip_trailing_comment(line: &str) -> &str {
    match line.rfind("--") {
        Some(i) if line[..i].trim_end().ends_with(';') => line[..i].trim_end(),
        _ => line,
    }
}

/// The zone specification of a `SET TIME ZONE` statement, in any spelling the corpus uses:
/// `set time zone`, `set timezone`, and either with `local` and/or `=`.
fn strip_set_timezone(line: &str) -> Option<&str> {
    let rest = line.get(..4).filter(|p| p.eq_ignore_ascii_case("set "))?;
    let rest = &line[rest.len()..];
    let rest = strip_word(rest, "local").unwrap_or(rest);
    let rest = strip_word(rest, "timezone")
        .or_else(|| strip_word(rest, "time").and_then(|r| strip_word(r, "zone")))?;
    Some(rest.trim_start())
}

/// Consume `word` and any following spaces, case-insensitively.
fn strip_word<'a>(input: &'a str, word: &str) -> Option<&'a str> {
    let head = input.get(..word.len())?;
    head.eq_ignore_ascii_case(word)
        .then(|| input[word.len()..].trim_start())
}

/// The zone `pg_regress` starts the suite under, via `PGTZ=PST8PDT`.
fn regress_default_tz() -> jiff::tz::TimeZone {
    jiff::tz::TimeZone::get("PST8PDT").expect("PST8PDT is in the bundled tz database")
}

/// The zone a `set time zone` line selects.
///
/// `default` restores what `pg_regress` starts with. A bare `+10` is a fixed offset east
/// of UTC, which the corpus pins: under it `07:04:56` UTC is expected to read `17:04:56`.
fn session_tz(spec: &str) -> jiff::tz::TimeZone {
    let spec = spec
        .trim()
        .trim_start_matches('=')
        .trim()
        .trim_matches('\'');
    if spec.eq_ignore_ascii_case("default") || spec.eq_ignore_ascii_case("local") {
        return regress_default_tz();
    }
    if let Ok(tz) = jiff::tz::TimeZone::get(spec) {
        return tz;
    }
    if let Ok(hours) = spec.parse::<i32>() {
        let offset = jiff::tz::Offset::from_seconds(hours * 3600)
            .expect("corpus offsets are within the representable range");
        return jiff::tz::TimeZone::fixed(offset);
    }
    // A POSIX zone such as `UTC-10` inverts the sign relative to ISO 8601: it names the
    // offset added to local time to reach UTC, so `UTC-10` is ten hours east.
    if let Ok(tz) = jiff::tz::TimeZone::posix(spec) {
        return tz;
    }
    panic!("unhandled session time zone: {spec:?}");
}

fn parse_script(script: &'static str) -> Vec<Trial> {
    let mut tests = vec![];
    let mut tz = regress_default_tz();
    // Zone to restore when the current transaction ends, for `SET LOCAL`.
    let mut outer_tz: Option<jiff::tz::TimeZone> = None;
    let mut lines = script
        .lines()
        .enumerate()
        // skip comments
        .filter(|(_, line)| !line.trim_start().starts_with("-- "));
    while let Some((line_no, line)) = lines.next() {
        let line = line.trim();
        // The corpus sets the zone with `SET LOCAL` inside a transaction, which reverts at
        // transaction end. Without restoring it here, a `set local timezone = 'UTC'` leaks
        // past its `rollback` and silently rezones every statement that follows.
        if line.eq_ignore_ascii_case("begin;") {
            outer_tz = Some(tz.clone());
            continue;
        }
        if line.eq_ignore_ascii_case("commit;") || line.eq_ignore_ascii_case("rollback;") {
            if let Some(saved) = outer_tz.take() {
                tz = saved;
            }
            continue;
        }
        // `set time zone 'X';`, and the `set local timezone = 'X';` spelling the corpus
        // also uses. Both change the zone for everything that follows.
        if let Some(spec) = strip_set_timezone(line) {
            tz = session_tz(spec.trim_end_matches(';'));
            continue;
        }
        if !line.starts_with("select") && !line.starts_with("SELECT") {
            continue;
        }
        let mut sql = strip_trailing_comment(&line[6..]).trim_start().to_string();
        while !sql.ends_with(';') {
            let (_, line) = lines.next().expect("eof");
            sql.push_str(strip_trailing_comment(line.trim()));
        }

        // A precision above the maximum is reduced with a warning rather than an error,
        // so the warning sits between the statement and its result.
        let mut line = lines.next().expect("eof").1.trim_start();
        while line.starts_with("WARNING:") || line.starts_with("NOTICE:") {
            line = lines.next().expect("eof").1.trim_start();
        }

        if let Some(msg) = line.strip_prefix("ERROR:  ") {
            let trial_tz = tz.clone();
            tests.push(Trial::test(
                format!("jsonb_jsonpath.out:{}", line_no + 1),
                move || test(&sql, Err(msg), &trial_tz),
            ));
            continue;
        }
        // skip '----' line
        lines.next().expect("eof");
        let mut results = vec![];
        loop {
            let (_, line) = lines.next().expect("eof");
            if line.starts_with('(') {
                // "(1 row)"
                break;
            }
            if let Ok(json) = serde_json::Value::from_str(line) {
                results.push(json.to_string());
            } else {
                results.push(line.trim().to_string());
            }
        }
        let trial_tz = tz.clone();
        tests.push(Trial::test(
            format!("jsonb_jsonpath.out:{}", line_no + 1),
            move || test(&sql, Ok(results), &trial_tz),
        ));
    }
    tests
}

/// Compare a multi-column result cell by cell.
///
/// PostgreSQL pads columns to a common width and prints JSON with a space after the colon,
/// so comparing raw lines would test layout rather than values.
fn assert_table(
    actual: Result<Vec<Vec<String>>, EvalError>,
    expected: Result<Vec<String>, &str>,
) -> Result<(), Failed> {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            let actual: Vec<Vec<String>> = actual
                .iter()
                .map(|row| row.iter().map(|c| normalize_cell(c)).collect())
                .collect();
            let expected: Vec<Vec<String>> = expected
                .iter()
                .map(|row| row.split('|').map(normalize_cell).collect())
                .collect();
            if actual == expected {
                Ok(())
            } else {
                Err(format!("expected: {expected:?}, actual: {actual:?}").into())
            }
        }
        (Err(e), Err(msg)) if e.to_string().contains(msg) => Ok(()),
        (actual, expected) => Err(format!("expected: {expected:?}, actual: {actual:?}").into()),
    }
}

/// Canonicalise one printed cell so column padding and JSON spacing do not matter.
fn normalize_cell(cell: &str) -> String {
    let cell = cell.trim();
    serde_json::Value::from_str(cell).map_or_else(|_| cell.to_string(), |v| v.to_string())
}

/// `select x, y, jsonb_path_query(J, P, jsonb_build_object('x', x, 'y', y)) as "..."
///  from (values ...) x(x), (values ...) y(y);`
///
/// A cross join of two single-column `VALUES` lists with one jsonpath call per pair, the
/// left column varying slowest. The corpus uses it for the ternary-logic tables.
fn eval_values_cross_join(
    sql: &str,
    tz: &jiff::tz::TimeZone,
) -> Option<Result<Vec<Vec<String>>, EvalError>> {
    let re = regex::Regex::new(
        r#"^x, y,jsonb_path_query\('([^']*)','(.*)',jsonb_build_object\('x', x, 'y', y\)\) as "[^"]*"from\(values (.*?)\) x\(x\),\(values (.*?)\) y\(y\);$"#,
    )
    .unwrap();
    let c = re.captures(sql)?;
    let json = c.get(1).unwrap().as_str();
    let path = c.get(2).unwrap().as_str();
    let xs = parse_values_items(c.get(3).unwrap().as_str());
    let ys = parse_values_items(c.get(4).unwrap().as_str());
    let mut rows = vec![];
    for x in &xs {
        for y in &ys {
            let vars = format!(r#"{{"x": {x}, "y": {y}}}"#);
            match jsonb_path_query(json, path, &vars, false, tz) {
                Ok(values) => {
                    rows.extend(values.into_iter().map(|v| vec![x.clone(), y.clone(), v]))
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
    Some(Ok(rows))
}

/// Items of a `VALUES (a), (b), (c)` list: one single-column literal each, optionally
/// carrying a `jsonb` type prefix.
fn parse_values_items(list: &str) -> Vec<String> {
    list.split("), (")
        .map(|item| {
            item.trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim()
                .trim_start_matches("jsonb")
                .trim()
                .trim_matches('\'')
                .to_string()
        })
        .collect()
}

/// The string-comparison table: a CTE over `unnest(...) WITH ORDINALITY`, cross joined with
/// itself and ordered by both ordinals, running one `jsonb_path_query_first` per operator
/// with the right-hand row supplied as `vars`.
fn eval_string_compare_cte(
    sql: &str,
    tz: &jiff::tz::TimeZone,
) -> Option<Result<Vec<Vec<String>>, EvalError>> {
    if !sql.contains("FROM str s1, str s2") {
        return None;
    }
    let arr = regex::Regex::new(r#"unnest\('\{(.*?)\}'::text\[\]\)"#).unwrap();
    let strings: Vec<String> = arr
        .captures(sql)?
        .get(1)
        .unwrap()
        .as_str()
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();
    let call =
        regex::Regex::new(r#"jsonb_path_query_first\(s1\.j, '([^']*)', vars => s2\.j\)"#).unwrap();
    let paths: Vec<&str> = call
        .captures_iter(sql)
        .map(|c| c.get(1).unwrap().as_str())
        .collect();
    if paths.is_empty() {
        return None;
    }
    let mut rows = vec![];
    for s1 in &strings {
        for s2 in &strings {
            let j1 = serde_json::json!({ "s": s1 }).to_string();
            let j2 = serde_json::json!({ "s": s2 }).to_string();
            let mut row = vec![j1.clone(), j2.clone()];
            for path in &paths {
                match jsonb_path_query_first(&j1, path, &j2, false, tz) {
                    Ok(value) => row.push(value.unwrap_or_default()),
                    Err(e) => return Some(Err(e)),
                }
            }
            rows.push(row);
        }
    }
    Some(Ok(rows))
}

fn test(
    sql: &str,
    expected: Result<Vec<String>, &str>,
    tz: &jiff::tz::TimeZone,
) -> Result<(), Failed> {
    // Multi-row shapes first: their inner calls would otherwise match the single-statement
    // patterns below on a substring.
    if let Some(actual) = eval_values_cross_join(sql, tz) {
        return assert_table(actual, expected);
    }
    if let Some(actual) = eval_string_compare_cte(sql, tz) {
        return assert_table(actual, expected);
    }
    // match one of:
    // jsonb 'json' @? 'path';
    // jsonb 'json' @@ 'path';
    // 'json'::jsonb @? 'path';   -- the cast spelling; both capture json, op, path
    let r1 = regex::Regex::new(r#"jsonb '(.*)' (@\?|@@) '(.*)';"#).unwrap();
    let r1_cast = regex::Regex::new(r#"'(.*)'::jsonb (@\?|@@) '(.*)';"#).unwrap();
    if let Some(capture) = r1.captures(sql).or_else(|| r1_cast.captures(sql)) {
        let json = capture.get(1).unwrap().as_str();
        let op = capture.get(2).unwrap().as_str();
        let path = capture.get(3).unwrap().as_str();
        let actual = match op {
            "@?" => jsonb_path_exists(json, path, "{}", true, tz),
            "@@" => jsonb_path_match(json, path, "{}", true, tz),
            _ => return Err(format!("invalid operator: {}", op).into()),
        };
        return assert_match(actual, expected);
    }
    // match one of:
    // jsonb_path_*('json', 'path');
    // jsonb_path_*('json', 'path', 'vars');
    // jsonb_path_*('json', 'path', vars => 'vars');
    // jsonb_path_*('json', 'path', silent => [true|false]);
    let r2 = regex::Regex::new(
        r#"([a-z_]+)\('([^']*)',\s*'([^']*)'(?:::jsonpath)?(?:,\s*(?:vars =>)? '([^']*)')?(?:,\s*silent => (\w+))?\);"#,
    )
    .unwrap();
    if let Some(capture) = r2.captures(sql) {
        let func = capture.get(1).unwrap().as_str();
        let json = capture.get(2).unwrap().as_str();
        let path = capture.get(3).unwrap().as_str();
        let vars = capture.get(4).map_or("{}", |s| s.as_str());
        let silent = capture.get(5).is_some_and(|s| s.as_str() == "true");
        // A path that fails to compile is PostgreSQL's "syntax error at or near ...".
        // Every helper below unwraps the parse, so compare that error here instead.
        if let Err(e) = JsonPath::from_str(path) {
            return match expected {
                Err(msg) if e.to_string().contains(msg) => Ok(()),
                expected => Err(format!("expected: {expected:?}, actual: Err({e})").into()),
            };
        }
        let actual = match func {
            "jsonb_path_exists" => jsonb_path_exists(json, path, vars, silent, tz),
            "jsonb_path_match" => jsonb_path_match(json, path, vars, silent, tz),
            "jsonb_path_query" => jsonb_path_query(json, path, vars, silent, tz),
            "jsonb_path_query_array" => {
                jsonb_path_query_array(json, path, vars, silent, tz).map(|s| vec![s])
            }
            "jsonb_path_query_first" => {
                jsonb_path_query_first(json, path, vars, silent, tz).map(|s| match s {
                    Some(s) => vec![s],
                    None => vec!["".into()],
                })
            }
            "jsonb_path_exists_tz" => jsonb_path_exists_tz(json, path, vars, silent, tz),
            "jsonb_path_match_tz" => jsonb_path_match_tz(json, path, vars, silent, tz),
            "jsonb_path_query_tz" => jsonb_path_query_tz(json, path, vars, silent, tz),
            "jsonb_path_query_array_tz" => {
                jsonb_path_query_array_tz(json, path, vars, silent, tz).map(|s| vec![s])
            }
            "jsonb_path_query_first_tz" => jsonb_path_query_first_tz(json, path, vars, silent, tz)
                .map(|s| match s {
                    Some(s) => vec![s],
                    None => vec!["".into()],
                }),
            _ => return Err(format!("invalid function: {}", func).into()),
        };
        return assert_match(actual, expected);
    }
    Err("unrecognized query".into())
}

// PG `.keyvalue()` ids are address-derived; collapse to a sentinel so
// stable-but-different ids compare equal.
fn mask_keyvalue_ids(s: &str) -> String {
    let re = regex::Regex::new(r#""id":\s*-?\d+"#).unwrap();
    re.replace_all(s, r#""id": 0"#).into_owned()
}

fn assert_keyvalue_id_partition(actual: &[String], expected: &[String]) -> Result<(), Failed> {
    if actual.len() != expected.len() {
        return Ok(());
    }
    let ids = |values: &[String]| {
        values
            .iter()
            .map(|value| {
                serde_json::from_str::<serde_json::Value>(value)
                    .ok()
                    .and_then(|value| value.get("id").and_then(serde_json::Value::as_i64))
            })
            .collect::<Vec<_>>()
    };
    let actual = ids(actual);
    let expected = ids(expected);
    for (left, expected_left) in expected.iter().enumerate() {
        let Some(expected_left) = expected_left else {
            continue;
        };
        let Some(actual_left) = actual[left] else {
            return Err(format!("result {left} lost its keyvalue object id").into());
        };
        for (right, expected_right) in expected.iter().enumerate() {
            let Some(expected_right) = expected_right else {
                continue;
            };
            let Some(actual_right) = actual[right] else {
                return Err(format!("result {right} lost its keyvalue object id").into());
            };
            if (actual_left == actual_right) != (expected_left == expected_right) {
                return Err(format!(
                    "keyvalue id relationship differs at results {left} and {right}"
                )
                .into());
            }
        }
    }
    Ok(())
}

fn assert_match(
    actual: Result<Vec<String>, EvalError>,
    expected: Result<Vec<String>, &str>,
) -> Result<(), Failed> {
    match (actual, expected) {
        (Ok(b), Ok(expected)) => {
            assert_keyvalue_id_partition(&b, &expected)?;
            let b_norm: Vec<String> = b.iter().map(|s| mask_keyvalue_ids(s)).collect();
            let exp_norm: Vec<String> = expected.iter().map(|s| mask_keyvalue_ids(s)).collect();
            if b_norm == exp_norm {
                Ok(())
            } else {
                Err(format!("expected: {expected:?}, actual: {b:?}").into())
            }
        }
        (Err(e), Err(msg)) if e.to_string().contains(msg) => Ok(()),
        (actual, expected) => Err(format!("expected: {expected:?}, actual: {actual:?}").into()),
    }
}

fn jsonb_path_exists(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = if silent {
        path.exists_with_vars_silent(&json, &vars)
    } else {
        path.exists_with_vars(&json, &vars)
    };
    let exist = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec!["".into()]),
        Err(e) => return Err(e),
    };
    Ok(vec![if exist { "t" } else { "f" }.to_string()])
}

fn jsonb_path_match(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = if silent {
        path.query_with_vars_silent(&json, &vars)
    } else {
        path.query_with_vars(&json, &vars)
    };
    let result = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec!["".into()]),
        Err(e) => return Err(e),
    };
    if result.len() != 1 {
        if silent {
            return Ok(vec!["".into()]);
        } else {
            return Err(EvalError::ExpectSingleBoolean);
        }
    }
    if result[0].as_ref().is_null() {
        Ok(vec!["".to_string()])
    } else if let Some(b) = result[0].as_ref().as_bool() {
        Ok(vec![if b { "t" } else { "f" }.to_string()])
    } else if silent {
        Ok(vec!["".to_string()])
    } else {
        Err(EvalError::ExpectSingleBoolean)
    }
}

fn jsonb_path_query(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = if silent {
        path.query_with_vars_silent(&json, &vars)
    } else {
        path.query_with_vars(&json, &vars)
    };
    let list = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    Ok(list.into_iter().map(|v| v.to_string()).collect())
}

fn jsonb_path_query_array(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<String, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = if silent {
        path.query_with_vars_silent(&json, &vars)
    } else {
        path.query_with_vars(&json, &vars)
    };
    let list = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok("".into()),
        Err(e) => return Err(e),
    };
    let array = serde_json::Value::Array(list.into_iter().map(|v| v.into_owned()).collect());
    Ok(array.to_string())
}

fn jsonb_path_query_first(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Option<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = if silent {
        path.query_first_with_vars_silent(&json, &vars)
    } else {
        path.query_first_with_vars(&json, &vars)
    };
    let list = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(list.map(|v| v.to_string()))
}

// ---- `_tz` variants ------------------------------------------------------
//
// PG's `_tz` SQL functions enable session-TZ-dependent evaluation. The
// harness mirrors this by calling the vendor crate's `_tz` entry points.

fn jsonb_path_exists_tz(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let r = path.exists_with_vars_tz(&json, &vars);
    let exist = match r {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec!["".into()]),
        Err(e) => return Err(e),
    };
    Ok(vec![if exist { "t" } else { "f" }.to_string()])
}

fn jsonb_path_match_tz(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let result = match path.query_with_vars_tz(&json, &vars) {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec!["".into()]),
        Err(e) => return Err(e),
    };
    if result.len() != 1 {
        if silent {
            return Ok(vec!["".into()]);
        }
        return Err(EvalError::ExpectSingleBoolean);
    }
    if result[0].as_ref().is_null() {
        Ok(vec!["".to_string()])
    } else if let Some(b) = result[0].as_ref().as_bool() {
        Ok(vec![if b { "t" } else { "f" }.to_string()])
    } else if silent {
        Ok(vec!["".to_string()])
    } else {
        Err(EvalError::ExpectSingleBoolean)
    }
}

fn jsonb_path_query_tz(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Vec<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let list = match path.query_with_vars_tz(&json, &vars) {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    Ok(list.into_iter().map(|v| v.to_string()).collect())
}

fn jsonb_path_query_array_tz(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<String, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let list = match path.query_with_vars_tz(&json, &vars) {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok("".into()),
        Err(e) => return Err(e),
    };
    let array = serde_json::Value::Array(list.into_iter().map(|v| v.into_owned()).collect());
    Ok(array.to_string())
}

fn jsonb_path_query_first_tz(
    json: &str,
    path: &str,
    vars: &str,
    silent: bool,
    tz: &jiff::tz::TimeZone,
) -> Result<Option<String>, EvalError> {
    let json = serde_json::Value::from_str(json).unwrap();
    let vars = serde_json::Value::from_str(vars).unwrap();
    let path = JsonPath::from_str(path)
        .unwrap()
        .with_session_tz(tz.clone());
    let list = match path.query_first_with_vars_tz(&json, &vars) {
        Ok(x) => x,
        Err(e) if silent && e.can_silent() => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(list.map(|v| v.to_string()))
}
