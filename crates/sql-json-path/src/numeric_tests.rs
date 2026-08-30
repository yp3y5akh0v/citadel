use super::*;

#[test]
fn canonical_expands_exponents_and_retains_numeric_scale() {
    let big = canonical("1e1000").unwrap();
    assert_eq!(big.len(), 1001);
    assert!(big.starts_with('1'));
    assert!(big[1..].bytes().all(|byte| byte == b'0'));

    assert_eq!(canonical("1.5e3").as_deref(), Some("1500"));
    assert_eq!(canonical("15e-2").as_deref(), Some("0.15"));
    assert_eq!(canonical("1.2300e1").as_deref(), Some("12.300"));
    assert_eq!(canonical("100e-2").as_deref(), Some("1.00"));
    assert_eq!(canonical("1e0").as_deref(), Some("1"));
}

#[test]
fn canonical_normalises_leading_zeroes_but_preserves_trailing_scale() {
    assert_eq!(canonical("0001.20").as_deref(), Some("1.20"));
    assert_eq!(canonical(".00120").as_deref(), Some("0.00120"));
    assert_eq!(canonical("1.").as_deref(), Some("1"));
    assert_eq!(canonical("-0.00").as_deref(), Some("0.00"));
    assert_eq!(canonical("0e-2").as_deref(), Some("0.00"));
    assert_eq!(
        canonical("12345678901234567890").as_deref(),
        Some("12345678901234567890")
    );
}

#[test]
fn numeric_input_accepts_postgres_bases_underscores_and_ascii_space() {
    assert_eq!(canonical("  +0xFF\t").as_deref(), Some("255"));
    // String input permits a separator directly after a non-decimal prefix.
    assert_eq!(canonical("0x_FF").as_deref(), Some("255"));
    assert_eq!(canonical("-0o10").as_deref(), Some("-8"));
    assert_eq!(canonical("0b1_010").as_deref(), Some("10"));
    assert_eq!(canonical("1_000.5_0e+1").as_deref(), Some("10005.0"));
}

#[test]
fn malformed_numeric_separators_are_rejected() {
    for input in [
        "_1", "1_", "1__0", "1_.0", "1._0", "1e_2", "1e2_", "0xFF_", "0xF__F", "+", ".", "1e",
    ] {
        assert!(canonical(input).is_none(), "{input} should be rejected");
    }
}

#[test]
fn unconstrained_numeric_storage_bounds_are_enforced() {
    let maximum_integer = format!("1{}", "0".repeat(NUMERIC_MAX_INTEGER_DIGITS as usize - 1));
    assert!(canonical(&maximum_integer).is_some());
    let too_wide_integer = format!("{maximum_integer}0");
    assert!(canonical(&too_wide_integer).is_none());

    let maximum_scale = format!("0.{}1", "0".repeat(NUMERIC_MAX_DISPLAY_SCALE as usize - 1));
    assert!(canonical(&maximum_scale).is_some());
    let too_much_scale = format!("0.{}1", "0".repeat(NUMERIC_MAX_DISPLAY_SCALE as usize));
    assert!(canonical(&too_much_scale).is_none());

    assert!(canonical("1e1073741824").is_none());
    assert!(canonical("1e-1073741824").is_none());
}

#[test]
fn exact_integer_rounding_does_not_cross_the_f64_precision_cliff() {
    assert_eq!(
        round_to_i64_exact("9007199254740993.4"),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(
        round_to_i64_exact("9007199254740993.5"),
        Some(9_007_199_254_740_994)
    );
    assert_eq!(
        round_to_i64_exact("-9007199254740993.5"),
        Some(-9_007_199_254_740_994)
    );
}

#[test]
fn exact_integer_rounding_handles_both_i64_edges() {
    assert_eq!(
        round_to_i64_exact("9223372036854775806.4"),
        Some(i64::MAX - 1)
    );
    assert_eq!(round_to_i64_exact("9223372036854775807.4"), Some(i64::MAX));
    assert_eq!(round_to_i64_exact("9223372036854775807.5"), None);
    assert_eq!(round_to_i64_exact("-9223372036854775808.4"), Some(i64::MIN));
    assert_eq!(round_to_i64_exact("-9223372036854775808.5"), None);
}

#[test]
fn integer_input_matches_postgres_prefix_and_separator_syntax() {
    assert_eq!(parse_pg_i64(" +0x7fff_ffff_ffff_ffff "), Some(i64::MAX));
    assert_eq!(parse_pg_i64("-0x8000_0000_0000_0000"), Some(i64::MIN));
    assert_eq!(parse_pg_i32("0o177"), Some(127));
    assert_eq!(parse_pg_i32("0x_FF"), Some(255));
    assert_eq!(parse_pg_i32("1_000"), Some(1_000));
    assert_eq!(parse_pg_i32("2147483648"), None);
    assert_eq!(parse_pg_i32("-2147483649"), None);
    assert_eq!(parse_pg_i64("0x8000000000000000"), None);
    assert_eq!(parse_pg_i64("-0x8000000000000001"), None);
}

#[test]
fn saturating_integer_parser_preserves_evaluation_errors() {
    assert_eq!(
        parse_pg_i64_saturating("9223372036854775808"),
        Some(i64::MAX)
    );
    assert_eq!(
        parse_pg_i64_saturating("-9223372036854775809"),
        Some(i64::MIN)
    );
    assert_eq!(parse_pg_i64_saturating("0xA"), Some(10));
    assert_eq!(parse_pg_i64_saturating("1__0"), None);
}

#[test]
fn boolean_numeric_path_uses_numeric_out_then_int4_input() {
    assert_eq!(numeric_to_i32_exact("1e0"), Some(1));
    assert_eq!(numeric_to_i32_exact("0e0"), Some(0));
    assert_eq!(numeric_to_i32_exact("1.0"), None);
    assert_eq!(numeric_to_i32_exact("100e-2"), None);
    assert_eq!(numeric_to_i32_exact("2147483647"), Some(i32::MAX));
    assert_eq!(numeric_to_i32_exact("2147483648"), None);
}

#[test]
fn array_index_conversion_truncates_then_checks_int4_range() {
    assert_eq!(trunc_to_i32_exact("1.9"), Some(1));
    assert_eq!(trunc_to_i32_exact("-1.9"), Some(-1));
    assert_eq!(trunc_to_i32_exact("2147483647.9"), Some(i32::MAX));
    assert_eq!(trunc_to_i32_exact("2147483648"), None);
    assert_eq!(trunc_to_i32_exact("1e1000"), None);
}

#[test]
fn bool_string_input_uses_unique_prefixes_without_trimming() {
    for input in ["t", "tr", "TRUE", "yes", "y", "on", "1"] {
        assert_eq!(parse_pg_bool(input), Some(true), "{input}");
    }
    for input in ["f", "fal", "FALSE", "no", "n", "off", "0"] {
        assert_eq!(parse_pg_bool(input), Some(false), "{input}");
    }
    for input in ["o", " true", "true ", "", "truth"] {
        assert_eq!(parse_pg_bool(input), None, "{input}");
    }
}

#[test]
fn typmod_rounds_pads_and_checks_post_rounding_weight() {
    assert_eq!(apply_typmod("12345.678", 6, 1).as_deref(), Some("12345.7"));
    assert_eq!(apply_typmod("1.2", 5, 3).as_deref(), Some("1.200"));
    assert_eq!(apply_typmod("1234.5678", 6, 0).as_deref(), Some("1235"));
    assert_eq!(apply_typmod("9.6", 2, 0).as_deref(), Some("10"));
    assert_eq!(apply_typmod("99.5", 3, 0).as_deref(), Some("100"));
    assert!(apply_typmod("12345.678", 6, 2).is_none());
}

#[test]
fn typmod_handles_negative_scales_small_values_and_signed_zero() {
    assert_eq!(apply_typmod("1234.5678", 6, -2).as_deref(), Some("1200"));
    assert_eq!(apply_typmod("0.0123456", 1, 2).as_deref(), Some("0.01"));
    assert_eq!(apply_typmod("0.0012345", 2, 4).as_deref(), Some("0.0012"));
    assert_eq!(apply_typmod("-0.00123456", 2, -4).as_deref(), Some("0"));
    assert_eq!(apply_typmod("-1.5", 2, 0).as_deref(), Some("-2"));
}

#[test]
fn typmod_rejects_parameter_and_value_bounds() {
    assert!(apply_typmod("1", 0, 0).is_none());
    assert!(apply_typmod("1", 1001, 0).is_none());
    assert!(apply_typmod("1", 1, -1001).is_none());
    assert!(apply_typmod("1", 1, 1001).is_none());
}

#[test]
fn numeric_and_float_special_spellings_are_distinguished() {
    for input in ["NaN", "inf", "-inf", "+Infinity"] {
        assert!(is_numeric_nan_or_inf(input), "{input}");
        assert!(is_nan_or_inf(input), "{input}");
    }
    assert!(is_nan_or_inf("-NaN"));
    assert!(!is_numeric_nan_or_inf("-NaN"));
    assert!(!is_numeric_nan_or_inf("1e1000"));
}

#[test]
fn float8_input_matches_postgres_whitespace_and_underflow_rules() {
    assert_eq!(parse_pg_finite_float8(" \t1.25\r\n"), Some(1.25));
    assert_eq!(parse_pg_finite_float8("+1.25"), Some(1.25));
    assert_eq!(parse_pg_finite_float8("-0"), Some(-0.0));
    assert_eq!(parse_pg_finite_float8("0e-9999"), Some(0.0));
    assert_eq!(parse_pg_finite_float8("1e-9999"), None);
    assert_eq!(parse_pg_finite_float8("1e9999"), None);
    assert_eq!(parse_pg_finite_float8("not-a-number"), None);
}

#[test]
fn float8_input_supports_c99_hex_without_host_strtod() {
    assert_eq!(parse_pg_finite_float8("0x1p2"), Some(4.0));
    assert_eq!(parse_pg_finite_float8(" -0X1.8P+1\n"), Some(-3.0));
    assert_eq!(parse_pg_finite_float8("0x1p-1074"), Some(f64::from_bits(1)));
    assert_eq!(parse_pg_finite_float8("0x1p-1075"), None);
    assert_eq!(parse_pg_finite_float8("0x0p-99999"), Some(0.0));
    assert_eq!(
        parse_pg_finite_float8("0x1.fffffffffffffp1023"),
        Some(f64::MAX)
    );
    assert_eq!(parse_pg_finite_float8("0x1p1024"), None);
    assert_eq!(parse_pg_finite_float8("0x1.00000000000008p0"), Some(1.0));
    assert_eq!(
        parse_pg_finite_float8("0x1.000000000000081p0"),
        Some(f64::from_bits(1.0_f64.to_bits() + 1))
    );
    for invalid in ["0x", "0x.p1", "0x1p", "0x1p1_0", "0x1p0junk"] {
        assert_eq!(parse_pg_finite_float8(invalid), None, "{invalid}");
    }
}

#[test]
fn float8_special_detection_includes_c99_nan_payloads() {
    for input in ["NaN()", "nan(payload)", "-NAN(123_abc)"] {
        assert!(is_nan_or_inf(input), "{input}");
    }
    for input in ["nan(", "nan(payload!)", "nan(payload)junk"] {
        assert!(!is_nan_or_inf(input), "{input}");
    }
}

#[test]
fn float8_numeric_uses_postgres_dbl_dig_and_normalises_negative_zero() {
    assert_eq!(
        pg_float8_to_numeric(1.234_567_890_123_456_7).as_deref(),
        Some("1.23456789012346")
    );
    assert_eq!(pg_float8_to_numeric(-0.0).as_deref(), Some("0"));
    assert_eq!(pg_float8_to_numeric(1.0).as_deref(), Some("1"));
    assert_eq!(pg_float8_to_numeric(1.23).as_deref(), Some("1.23"));
    assert_eq!(pg_float8_to_numeric(1e-5).as_deref(), Some("0.00001"));
    assert_eq!(
        pg_float8_to_numeric(1_000_000_000_000_000.0).as_deref(),
        Some("1000000000000000")
    );
    assert_eq!(pg_float8_to_numeric(0.000_123).as_deref(), Some("0.000123"));
}

#[test]
fn exact_comparison_discriminates_values_that_f64_merges() {
    assert_eq!(
        compare_exact("9007199254740993", "9007199254740992"),
        Some(Ordering::Greater)
    );
    assert_eq!(compare_exact("1.0", "1.00"), Some(Ordering::Equal));
    assert_eq!(compare_exact("-1e1000", "0"), Some(Ordering::Less));
}

#[test]
fn exact_add_and_sub_preserve_scale_and_cross_i64_bounds() {
    assert_eq!(
        add_exact("9007199254740993", "1").unwrap(),
        "9007199254740994"
    );
    assert_eq!(add_exact("1.20", "2.3").unwrap(), "3.50");
    assert_eq!(sub_exact("1.20", "2.3").unwrap(), "-1.10");
    assert_eq!(
        add_exact("9223372036854775807", "1").unwrap(),
        "9223372036854775808"
    );
}

#[test]
fn exact_add_reports_postgres_numeric_storage_overflow() {
    let maximum = "9".repeat(NUMERIC_MAX_INTEGER_DIGITS as usize);
    assert_eq!(
        add_exact(&maximum, "1"),
        Err(NumericArithmeticError::Overflow)
    );
}

#[test]
fn exact_multiplication_preserves_product_scale_and_large_exponents() {
    assert_eq!(mul_exact("1.20", "3.0").unwrap(), "3.600");
    let product = mul_exact("1e1000", "1e1000").unwrap();
    assert_eq!(product.len(), 2001);
    assert!(product.starts_with('1'));
    assert!(product[1..].bytes().all(|byte| byte == b'0'));
}

#[test]
fn multiplication_rounds_at_the_numeric_display_scale_limit() {
    let tiny = format!("0.{}1", "0".repeat(8_999));
    let product = mul_exact(&tiny, &tiny).unwrap();
    assert_eq!(product, format!("0.{}", "0".repeat(16_383)));
}

#[test]
fn exact_division_uses_postgres_scale_selection_and_rounding() {
    assert_eq!(div_exact("1", "3").unwrap(), "0.33333333333333333333");
    assert_eq!(div_exact("2", "3").unwrap(), "0.66666666666666666667");
    assert_eq!(div_exact("10", "4").unwrap(), "2.5000000000000000");
    assert_eq!(div_exact("-1", "8").unwrap(), "-0.12500000000000000000");
    assert_eq!(
        div_exact("1", "0"),
        Err(NumericArithmeticError::DivisionByZero)
    );
}

#[test]
fn exact_remainder_truncates_quotient_toward_zero() {
    assert_eq!(rem_exact("5.50", "2").unwrap(), "1.50");
    assert_eq!(rem_exact("-5.50", "2").unwrap(), "-1.50");
    assert_eq!(rem_exact("5.50", "-2").unwrap(), "1.50");
    assert_eq!(
        rem_exact("5", "0"),
        Err(NumericArithmeticError::DivisionByZero)
    );
}

#[test]
fn exact_unary_operations_handle_i64_min_and_arbitrary_precision() {
    assert_eq!(
        neg_exact("-9223372036854775808").unwrap(),
        "9223372036854775808"
    );
    assert_eq!(
        abs_exact("-9007199254740993.00").unwrap(),
        "9007199254740993.00"
    );
    assert_eq!(
        ceil_exact("9007199254740993.1").unwrap(),
        "9007199254740994"
    );
    assert_eq!(
        floor_exact("9007199254740993.9").unwrap(),
        "9007199254740993"
    );
    assert_eq!(ceil_exact("-1.2").unwrap(), "-1");
    assert_eq!(floor_exact("-1.2").unwrap(), "-2");
}

#[test]
fn large_products_and_quotients_avoid_decimal_digit_quadratic_work() {
    let operand = "9".repeat(8_192);
    let product = mul_exact(&operand, &operand).unwrap();
    assert_eq!(product.len(), 16_384);
    assert!(product.starts_with("99999999"));
    assert!(product.ends_with("00000001"));
    assert_eq!(div_exact(&product, &operand).unwrap(), operand);
}
