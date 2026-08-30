//! SQL:2023 item methods, checked against PostgreSQL 17's recorded behaviour.
//!
//! Expected values come from `src/test/regress/expected/jsonb_jsonpath.out` in
//! REL_17_STABLE. Where PostgreSQL and RisingWave's upstream `sql-json-path` disagree,
//! PostgreSQL wins and the divergence is called out on the case.

use sql_json_path::JsonPath;
use std::str::FromStr;

fn query(input: &str, path: &str) -> Result<Vec<serde_json::Value>, String> {
    let value: serde_json::Value = serde_json::from_str(input).unwrap();
    let path = JsonPath::from_str(path).map_err(|e| e.to_string())?;
    path.query(&value)
        .map(|v| v.into_iter().map(|c| c.into_owned()).collect())
        .map_err(|e| e.to_string())
}

fn query_tz(input: &str, path: &str) -> Result<Vec<serde_json::Value>, String> {
    let value: serde_json::Value = serde_json::from_str(input).unwrap();
    let path = JsonPath::from_str(path).map_err(|e| e.to_string())?;
    path.query_tz(&value)
        .map(|v| v.into_iter().map(|c| c.into_owned()).collect())
        .map_err(|e| e.to_string())
}

fn one(input: &str, path: &str) -> serde_json::Value {
    let mut rows = query(input, path).expect("query should succeed");
    assert_eq!(rows.len(), 1, "expected exactly one row for {path}");
    rows.pop().unwrap()
}

fn err(input: &str, path: &str) -> String {
    query(input, path).expect_err("query should fail")
}

// ---- numeric conversions -------------------------------------------------

#[test]
fn integers_round_but_strings_must_be_exact() {
    // The JSON type decides: PG casts a numeric with rounding, but parses a string
    // with int8in/int4in, which demands an exact integer.
    assert_eq!(one("1.83", "$.bigint()"), serde_json::json!(2));
    assert_eq!(one("1.23", "$.bigint()"), serde_json::json!(1));
    assert_eq!(one("1.83", "$.integer()"), serde_json::json!(2));
    assert_eq!(one("\"123\"", "$.bigint()"), serde_json::json!(123));
    assert_eq!(one("\"+123\"", "$.bigint()"), serde_json::json!(123));
    assert!(err("\"1.23\"", "$.integer()").contains("invalid for type integer"));
    assert!(err("\"1.23aaa\"", "$.bigint()").contains("invalid for type bigint"));
}

#[test]
fn integer_overflow_is_a_conversion_error() {
    assert!(err("12345678901234567890", "$.bigint()").contains("invalid for type bigint"));
    assert!(err("12345678901", "$.integer()").contains("invalid for type integer"));
}

#[test]
fn numeric_integer_rounding_is_exact_at_f64_and_i64_boundaries() {
    assert_eq!(
        one("9007199254740993.4", "$.bigint()"),
        serde_json::json!(9_007_199_254_740_993_i64)
    );
    assert_eq!(
        one("9007199254740993.5", "$.bigint()"),
        serde_json::json!(9_007_199_254_740_994_i64)
    );
    assert_eq!(
        one("9223372036854775807.4", "$.bigint()"),
        serde_json::json!(i64::MAX)
    );
    assert!(err("9223372036854775807.5", "$.bigint()").contains("invalid for type bigint"));
    assert_eq!(
        one("-9223372036854775808.4", "$.bigint()"),
        serde_json::json!(i64::MIN)
    );
    assert!(err("-9223372036854775808.5", "$.bigint()").contains("invalid for type bigint"));
}

#[test]
fn integer_string_input_accepts_postgres_bases_and_separators() {
    assert_eq!(one("\"0xFF\"", "$.bigint()"), serde_json::json!(255));
    assert_eq!(one("\"-0o10\"", "$.integer()"), serde_json::json!(-8));
    assert_eq!(one("\"1_000\"", "$.integer()"), serde_json::json!(1000));
    assert_eq!(one("\" +0b1_010 \"", "$.bigint()"), serde_json::json!(10));
}

#[test]
fn number_and_decimal_are_exact() {
    // Routing through f64 would return 12345678901234567000 here.
    assert_eq!(
        one("\"12345678901234567890\"", "$.number()").to_string(),
        "12345678901234567890"
    );
    assert_eq!(one("1.23", "$.number()").to_string(), "1.23");
    assert_eq!(one("\"+12.3\"", "$.number()").to_string(), "12.3");
    assert_eq!(one("\"-12.3\"", "$.decimal()").to_string(), "-12.3");
}

#[test]
fn numeric_string_input_is_canonical_and_typmod_pads_scale() {
    assert_eq!(one("\"0001.20\"", "$.number()").to_string(), "1.20");
    assert_eq!(one("\"0xFF\"", "$.number()").to_string(), "255");
    assert_eq!(one("\"1_000.5_0\"", "$.decimal()").to_string(), "1000.50");
    assert_eq!(one("1.2", "$.decimal(5,3)").to_string(), "1.200");
}

#[test]
fn numeric_storage_bounds_fail_without_panicking() {
    assert!(query("\"1e1073741824\"", "$.number()").is_err());
    assert!(query("1e200000", "$.decimal()").is_err());
}

#[test]
fn a_huge_exponent_is_expanded_not_echoed() {
    // PG canonicalises with numeric_out before rendering, so 1e1000 is 1001 digits.
    let v = one("1e1000", "$.decimal()").to_string();
    assert_eq!(v.len(), 1001);
    assert!(v.starts_with('1'));
    assert_eq!(one("1e1000", "$.number()").to_string().len(), 1001);
}

#[test]
fn nan_and_infinity_are_rejected_for_number_and_decimal() {
    for input in ["\"nan\"", "\"NaN\"", "\"inf\"", "\"-inf\""] {
        assert!(
            err(input, "$.decimal()").contains("NaN or Infinity is not allowed"),
            "{input}"
        );
        assert!(err(input, "$.number()").contains("NaN or Infinity is not allowed"));
        // .bigint() uses the generic conversion error instead.
        assert!(err(input, "$.bigint()").contains("invalid for type bigint"));
    }
}

#[test]
fn double_string_input_uses_postgres_float8_conversion() {
    assert_eq!(one("\"  1.25\\t\"", "$.double()").to_string(), "1.25");
    assert_eq!(
        one("\"1.2345678901234567\"", "$.double()").to_string(),
        "1.23456789012346"
    );
    assert_eq!(one("\"-0\"", "$.double()").to_string(), "0");
    assert_eq!(one("\"0x1.8p+1\"", "$.double()").to_string(), "3");
    assert!(err("\"nan(payload)\"", "$.double()").contains("NaN or Infinity"));
}

#[test]
fn double_rejects_nonzero_values_that_underflow_float8() {
    let string_error = err("\"1e-9999\"", "$.double()");
    assert!(
        string_error.contains("invalid for type double precision"),
        "{string_error}"
    );

    let numeric_error = err("1e-9999", "$.double()");
    assert!(
        numeric_error.contains("invalid for type double precision"),
        "{numeric_error}"
    );

    assert_eq!(one("\"0e-9999\"", "$.double()").to_string(), "0");
}

#[test]
fn decimal_applies_precision_and_scale() {
    assert_eq!(one("12345.678", "$.decimal(6, 1)").to_string(), "12345.7");
    // A single argument means scale 0.
    assert_eq!(one("1234.5678", "$.decimal(6)").to_string(), "1235");
    assert!(err("12345.678", "$.decimal(6, 2)").contains("invalid for type numeric"));
}

#[test]
fn decimal_overflow_uses_weight_so_small_values_fit() {
    // precision - scale is negative here; the value's weight is negative too.
    assert_eq!(one("0.0123456", "$.decimal(1, 2)").to_string(), "0.01");
    assert_eq!(one("0.0012345", "$.decimal(2, 4)").to_string(), "0.0012");
    // Rounded away to zero, and rendered without a sign.
    assert_eq!(one("-0.00123456", "$.decimal(2, -4)").to_string(), "0");
}

#[test]
fn decimal_argument_ranges_are_checked_during_evaluation() {
    // Out of i32 range: the argument must survive parsing to report this.
    assert!(err("12.3", "$.decimal(12345678901, 1)")
        .contains("precision of jsonpath item method .decimal() is out of range"));
    assert!(err("12.3", "$.decimal(1, 12345678901)")
        .contains("scale of jsonpath item method .decimal() is out of range"));
    // In range for i32 but outside NUMERIC's bounds.
    assert!(err("1234.5678", "$.decimal(-6, +2)")
        .contains("NUMERIC precision -6 must be between 1 and 1000"));
    assert!(err("1234.5678", "$.decimal(6, -1001)")
        .contains("NUMERIC scale -1001 must be between -1000 and 1000"));
}

// ---- exact numeric operators -------------------------------------------

#[test]
fn arithmetic_and_comparison_never_narrow_through_f64() {
    assert_eq!(
        one("0", "9007199254740993 + 1").to_string(),
        "9007199254740994"
    );
    assert_eq!(
        one("0", "9223372036854775807 + 1").to_string(),
        "9223372036854775808"
    );

    let huge = one("0", "1e1000 + 1").to_string();
    assert_eq!(huge.len(), 1001);
    assert!(huge.starts_with('1'));
    assert!(huge.ends_with('1'));

    assert_eq!(
        query(
            "[9007199254740992,9007199254740993]",
            "$[*] ? (@ > 9007199254740992)",
        )
        .unwrap(),
        vec![serde_json::json!(9_007_199_254_740_993_i64)]
    );
}

#[test]
fn multiplication_division_and_remainder_use_postgres_numeric_rules() {
    assert_eq!(one("0", "1.20 * 3.0").to_string(), "3.600");
    assert_eq!(one("0", "1 / 3").to_string(), "0.33333333333333333333");
    assert_eq!(one("0", "2 / 3").to_string(), "0.66666666666666666667");
    assert_eq!(one("0", "5.50 % 2").to_string(), "1.50");
    assert_eq!(one("0", "-5.50 % 2").to_string(), "-1.50");
}

#[test]
fn unary_numeric_operations_handle_values_outside_i64() {
    assert_eq!(
        one("0", "-(-9223372036854775808)").to_string(),
        "9223372036854775808"
    );
    assert_eq!(
        one("0", "(-9007199254740993.00).abs()").to_string(),
        "9007199254740993.00"
    );
    assert_eq!(
        one("0", "9007199254740993.1.ceiling()").to_string(),
        "9007199254740994"
    );
    assert_eq!(
        one("0", "(-9007199254740993.1).floor()").to_string(),
        "-9007199254740994"
    );
}

#[test]
fn array_subscripts_truncate_exactly_and_require_int4() {
    assert_eq!(
        query("[10,20]", "$[1.9]").unwrap(),
        vec![serde_json::json!(20)]
    );
    assert!(err("[10]", "$[2147483648]").contains("array subscript is out of integer range"));
    assert!(err("[10]", "$[1e1000]").contains("array subscript is out of integer range"));
}

// ---- boolean and string --------------------------------------------------

#[test]
fn boolean_requires_an_exact_integer() {
    // Upstream sql-json-path rounds here and returns true; PostgreSQL errors.
    assert!(err("1.23", "$.boolean()").contains("invalid for type boolean"));
    assert!(err("\"1.23\"", "$.boolean()").contains("invalid for type boolean"));
    assert_eq!(one("1", "$.boolean()"), serde_json::json!(true));
    assert_eq!(one("0", "$.boolean()"), serde_json::json!(false));
    assert_eq!(one("-1", "$.boolean()"), serde_json::json!(true));
    assert_eq!(one("100", "$.boolean()"), serde_json::json!(true));
}

#[test]
fn boolean_numeric_input_uses_numeric_out_then_int4_input() {
    assert_eq!(one("1e0", "$.boolean()"), serde_json::json!(true));
    assert_eq!(one("0e0", "$.boolean()"), serde_json::json!(false));
    assert!(err("1.0", "$.boolean()").contains("invalid for type boolean"));
    assert!(err("100e-2", "$.boolean()").contains("invalid for type boolean"));
    assert!(err("2147483648", "$.boolean()").contains("invalid for type boolean"));
}

#[test]
fn boolean_accepts_unique_prefixes_without_trimming() {
    for (input, expected) in [
        ("\"true\"", true),
        ("\"tr\"", true),
        ("\"YES\"", true),
        ("\"y\"", true),
        ("\"on\"", true),
        ("\"1\"", true),
        ("\"false\"", false),
        ("\"fal\"", false),
        ("\"no\"", false),
        ("\"off\"", false),
        ("\"0\"", false),
    ] {
        assert_eq!(
            one(input, "$.boolean()"),
            serde_json::json!(expected),
            "{input}"
        );
    }
    // "o" is ambiguous between on and off; whitespace is not stripped.
    assert!(err("\"o\"", "$.boolean()").contains("invalid for type boolean"));
    assert!(err("\" true\"", "$.boolean()").contains("invalid for type boolean"));
}

#[test]
fn string_converts_scalars_and_datetimes() {
    assert_eq!(one("1234", "$.string()"), serde_json::json!("1234"));
    assert_eq!(one("true", "$.string()"), serde_json::json!("true"));
    assert_eq!(one("\"xyz\"", "$.string()"), serde_json::json!("xyz"));
    assert_eq!(
        query("[1.23, \"xyz\", false]", "$[*].string()").unwrap(),
        vec![
            serde_json::json!("1.23"),
            serde_json::json!("xyz"),
            serde_json::json!("false")
        ]
    );
    // `.string()` converts the internal datetime item rather than its JSON rendering.
    assert_eq!(
        one(
            "\"2023-08-15 12:34:56 +05:30\"",
            "$.timestamp_tz().string()"
        ),
        serde_json::json!("2023-08-15T12:34:56+05:30")
    );
    assert!(err("{}", "$.string()").contains("can only be applied to a boolean"));
}

#[test]
fn keyvalue_ids_identify_object_instances_not_object_shapes() {
    fn ids(values: &[serde_json::Value]) -> Vec<i64> {
        values
            .iter()
            .map(|value| value.get("id").and_then(serde_json::Value::as_i64).unwrap())
            .collect()
    }

    let repeated = query(r#"[{"a":1,"b":2}]"#, "$[0,0].keyvalue()").unwrap();
    let repeated_ids = ids(&repeated);
    assert_eq!(repeated_ids.len(), 4);
    assert!(repeated_ids.iter().all(|id| *id == repeated_ids[0]));

    let distinct = query(r#"[{"a":1},{"a":1}]"#, "$[*].keyvalue()").unwrap();
    let distinct_ids = ids(&distinct);
    assert_eq!(distinct_ids.len(), 2);
    assert_ne!(distinct_ids[0], distinct_ids[1]);
}

// ---- datetime ------------------------------------------------------------

#[test]
fn datetime_methods_parse_their_own_kind() {
    assert_eq!(
        one("\"2023-08-15\"", "$.date()"),
        serde_json::json!("2023-08-15")
    );
    assert_eq!(
        one("\"12:34:56.789\"", "$.time()"),
        serde_json::json!("12:34:56.789")
    );
    assert_eq!(
        one("\"12:34:56 +05:30\"", "$.time_tz()"),
        serde_json::json!("12:34:56+05:30")
    );
    assert_eq!(
        one("\"2023-08-15 12:34:56\"", "$.timestamp()"),
        serde_json::json!("2023-08-15T12:34:56")
    );
    assert_eq!(
        one("\"2023-08-15 12:34:56 +05:30\"", "$.timestamp_tz()"),
        serde_json::json!("2023-08-15T12:34:56+05:30")
    );
}

#[test]
fn accepted_conversions_need_no_time_zone() {
    // timestamp -> date and timestamp -> time carry no zone, so no _tz is required.
    assert_eq!(
        one("\"2023-08-15 12:34:56\"", "$.date()"),
        serde_json::json!("2023-08-15")
    );
    assert_eq!(
        one("\"2023-08-15 12:34:56\"", "$.time()"),
        serde_json::json!("12:34:56")
    );
    assert_eq!(
        one("\"2023-08-15\"", "$.timestamp()"),
        serde_json::json!("2023-08-15T00:00:00")
    );
    // The one cross-zone cell PostgreSQL performs without a check, rendered at the
    // session zone rather than kept at its own offset.
    assert_eq!(
        one("\"2023-08-15 12:34:56 +05:30\"", "$.time_tz()"),
        serde_json::json!("07:04:56+00:00")
    );
}

#[test]
fn pairs_outside_the_accept_list_are_not_recognized() {
    // Not a time-zone question: these fail whichever entry point is used.
    for (input, path, kind) in [
        ("\"12:34:56\"", "$.date()", "date"),
        ("\"12:34:56 +05:30\"", "$.date()", "date"),
        ("\"2023-08-15\"", "$.time()", "time"),
        ("\"2023-08-15\"", "$.time_tz()", "time_tz"),
        ("\"2023-08-15 12:34:56\"", "$.time_tz()", "time_tz"),
        ("\"12:34:56\"", "$.timestamp()", "timestamp"),
        ("\"12:34:56 +05:30\"", "$.timestamp()", "timestamp"),
        ("\"12:34:56\"", "$.timestamp_tz()", "timestamp_tz"),
    ] {
        let message = err(input, path);
        assert!(
            message.contains(&format!("{kind} format is not recognized")),
            "{path} on {input}: {message}"
        );
        assert!(
            query_tz(input, path).is_err(),
            "{path} must fail under _tz too"
        );
    }
}

#[test]
fn cross_zone_conversions_require_the_tz_entry_point() {
    for (input, path, from, to) in [
        (
            "\"2023-08-15 12:34:56 +05:30\"",
            "$.date()",
            "timestamptz",
            "date",
        ),
        (
            "\"2023-08-15 12:34:56 +05:30\"",
            "$.time()",
            "timestamptz",
            "time",
        ),
        (
            "\"2023-08-15 12:34:56 +05:30\"",
            "$.timestamp()",
            "timestamptz",
            "timestamp",
        ),
        ("\"12:34:56 +05:30\"", "$.time()", "timetz", "time"),
        ("\"12:34:56\"", "$.time_tz()", "time", "timetz"),
        ("\"2023-08-15\"", "$.timestamp_tz()", "date", "timestamptz"),
        (
            "\"2023-08-15 12:34:56\"",
            "$.timestamp_tz()",
            "timestamp",
            "timestamptz",
        ),
    ] {
        let message = err(input, path);
        assert!(
            message.contains(&format!("cannot convert value from {from} to {to}")),
            "{path} on {input}: {message}"
        );
    }
}

#[test]
fn the_tz_entry_point_performs_the_conversion() {
    assert_eq!(
        query_tz("\"2023-08-15 12:34:56 +05:30\"", "$.date()").unwrap(),
        vec![serde_json::json!("2023-08-15")]
    );
    // timetz -> time swallows the offset rather than shifting: not "07:04:56".
    assert_eq!(
        query_tz("\"12:34:56 +05:30\"", "$.time()").unwrap(),
        vec![serde_json::json!("12:34:56")]
    );
    assert_eq!(
        query_tz("\"12:34:56\"", "$.time_tz()").unwrap(),
        vec![serde_json::json!("12:34:56+00:00")]
    );
    assert_eq!(
        query_tz("\"2023-08-15\"", "$.timestamp_tz()").unwrap(),
        vec![serde_json::json!("2023-08-15T00:00:00+00:00")]
    );
}

#[test]
fn precision_rounds_and_clamps() {
    assert_eq!(
        one("\"12:34:56.789\"", "$.time(0)"),
        serde_json::json!("12:34:57")
    );
    assert_eq!(
        one("\"12:34:56.789\"", "$.time(2)"),
        serde_json::json!("12:34:56.79")
    );
    // Over the maximum, PG warns and clamps to 6; this clamps silently.
    assert_eq!(
        one("\"12:34:56.789\"", "$.time(10)"),
        serde_json::json!("12:34:56.789")
    );
    assert_eq!(
        one("\"2023-08-15 12:34:56.789\"", "$.timestamp(2)"),
        serde_json::json!("2023-08-15T12:34:56.79")
    );
}

#[test]
fn precision_rounding_carries_past_the_seconds_field() {
    // The corpus only covers 56 -> 57. Rounding the whole microsecond count is what
    // keeps this from producing 12:35:60 and 2023-08-16T00:00:00 correct.
    assert_eq!(
        one("\"12:34:59.789\"", "$.time(0)"),
        serde_json::json!("12:35:00")
    );
    assert_eq!(
        one("\"2023-08-15 23:59:59.789\"", "$.timestamp(0)"),
        serde_json::json!("2023-08-16T00:00:00")
    );
}

#[test]
fn datetime_items_keep_their_sql_json_type() {
    assert_eq!(
        one("\"2023-08-15\"", "$.date().type()"),
        serde_json::json!("date")
    );
    assert_eq!(
        one("\"12:34:56 +05:30\"", "$.time_tz().type()"),
        serde_json::json!("time with time zone")
    );
}

#[test]
fn datetime_items_are_comparable_in_filters() {
    // Comparison operates on internal datetime items, not their rendered strings.
    assert_eq!(
        query(
            r#"["2017-03-10", "2017-03-11", "2017-03-09"]"#,
            r#"$[*].date() ? (@ == "2017-03-10".date())"#
        )
        .unwrap(),
        vec![serde_json::json!("2017-03-10")]
    );
}

// ---- argument syntax -----------------------------------------------------

#[test]
fn only_the_documented_methods_take_arguments() {
    // .date() takes none, and a datetime precision may not carry a sign: both are
    // syntax errors in PostgreSQL rather than evaluation errors.
    assert!(JsonPath::from_str("$.date(2)").is_err());
    assert!(JsonPath::from_str("$.time(-1)").is_err());
    assert!(JsonPath::from_str("$.bigint(1)").is_err());
    // These are accepted.
    assert!(JsonPath::from_str("$.decimal(6,2)").is_ok());
    assert!(JsonPath::from_str("$.decimal(-6,+2)").is_ok());
    assert!(JsonPath::from_str("$.decimal(1_0,0b10)").is_ok());
    assert!(JsonPath::from_str("$.decimal(0xA,2)").is_ok());
    assert!(JsonPath::from_str("$.time(2)").is_ok());
    assert!(JsonPath::from_str("$.time(1_0)").is_ok());
    assert!(JsonPath::from_str("$.time(0xA)").is_ok());
    assert!(JsonPath::from_str("$.decimal(01)").is_err());
    assert!(JsonPath::from_str("$.decimal(0x_A)").is_err());
}

#[test]
fn oversized_integer_method_arguments_are_evaluation_errors_not_syntax_errors() {
    let precision = err("1", "$.decimal(9223372036854775808,1)");
    assert!(
        precision.contains("precision of jsonpath item method .decimal() is out of range"),
        "{precision}"
    );
    let time = err("\"12:34:56\"", "$.time(0xffffffffffffffffffff)");
    assert!(
        time.contains("time precision of jsonpath item method .time() is out of range"),
        "{time}"
    );
}

#[test]
fn paths_round_trip_through_display() {
    // ast.rs's AccessorOp Display has a catch-all that would silently drop arguments.
    for path in [
        "$.bigint()",
        "$.decimal()",
        "$.decimal(6)",
        "$.decimal(6,2)",
        "$.time(2)",
        "$.time_tz(3)",
        "$.timestamp(0)",
        "$.timestamp_tz(6)",
        "$.date()",
        "$.string()",
        "$.boolean()",
    ] {
        let parsed = JsonPath::from_str(path).unwrap();
        assert_eq!(parsed.to_string(), path, "round-trip of {path}");
    }
}
