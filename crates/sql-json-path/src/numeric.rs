// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new module - no upstream basis.

//! Exact decimal and PostgreSQL-compatible scalar input helpers.
//!
//! PostgreSQL's SQL/JSON item methods route JSON numbers through `numeric`, not
//! `float8`. In particular, integer conversion must round without losing bits,
//! `numeric_out` must retain its display scale, and string input accepts the same
//! base prefixes and underscore separators as PostgreSQL's numeric input routines.

use std::cmp::Ordering;

use num_bigint::BigUint;
use num_integer::Integer;

/// Limits imposed by PostgreSQL's packed `Numeric` representation.
const NUMERIC_MAX_INTEGER_DIGITS: i64 = 131_072;
const NUMERIC_MAX_DISPLAY_SCALE: i64 = 16_383;
const NUMERIC_MAX_EXPONENT: i64 = (i32::MAX as i64) / 2;
const NUMERIC_MIN_SIG_DIGITS: i64 = 16;

/// Limits accepted by a `numeric(p, s)` typmod in PostgreSQL 17.
const NUMERIC_MAX_PRECISION: i32 = 1_000;
const NUMERIC_MIN_SCALE: i32 = -1_000;
const NUMERIC_MAX_SCALE: i32 = 1_000;

/// A finite decimal laid out as a sign, significant digits and decimal point.
///
/// `point` counts significant-digit positions to the left of the decimal point.
/// It can be zero or negative for values below one. `dscale` is the display scale
/// retained by `numeric_out`, including trailing fractional zeroes.
#[derive(Debug, Clone)]
struct Decimal {
    neg: bool,
    digits: Vec<u8>,
    point: i64,
    dscale: i32,
}

impl Decimal {
    fn parse(raw: &str) -> Option<Self> {
        let raw = trim_pg_ascii_whitespace(raw);
        let (neg, unsigned) = strip_sign(raw)?;

        if let Some((radix, body)) = prefixed_radix(unsigned) {
            return Self::parse_radix(neg, body, radix);
        }

        let bytes = unsigned.as_bytes();
        let mut cursor = 0;
        let integer = decimal_digits(bytes, &mut cursor)?;
        let fraction = if bytes.get(cursor) == Some(&b'.') {
            cursor += 1;
            decimal_digits(bytes, &mut cursor)?
        } else {
            Vec::new()
        };
        if integer.is_empty() && fraction.is_empty() {
            return None;
        }

        let exponent = if matches!(bytes.get(cursor), Some(b'e' | b'E')) {
            cursor += 1;
            parse_exponent(bytes, &mut cursor)?
        } else {
            0
        };
        if cursor != bytes.len() {
            return None;
        }

        let integer_len = i64::try_from(integer.len()).ok()?;
        let fraction_len = i64::try_from(fraction.len()).ok()?;
        let raw_point = integer_len.checked_add(exponent)?;
        let dscale = fraction_len.checked_sub(exponent)?.max(0);
        if dscale > NUMERIC_MAX_DISPLAY_SCALE {
            return None;
        }

        let mut digits = integer;
        digits.extend(fraction);
        Self::normalise(neg, digits, raw_point, dscale as i32)
    }

    fn parse_radix(neg: bool, body: &[u8], radix: u32) -> Option<Self> {
        let source_digits = validated_radix_digits(body, radix)?;
        let value = BigUint::from_radix_be(&source_digits, radix)?;
        let decimal_digits = value.to_str_radix(10).into_bytes();
        if i64::try_from(decimal_digits.len()).ok()? > NUMERIC_MAX_INTEGER_DIGITS {
            return None;
        }
        let point = i64::try_from(decimal_digits.len()).ok()?;
        Self::normalise(neg, decimal_digits, point, 0)
    }

    fn normalise(neg: bool, digits: Vec<u8>, point: i64, dscale: i32) -> Option<Self> {
        let Some(first_nonzero) = digits.iter().position(|digit| *digit != b'0') else {
            return Some(Self {
                neg: false,
                digits: vec![b'0'],
                point: 0,
                dscale,
            });
        };

        let point = point.checked_sub(i64::try_from(first_nonzero).ok()?)?;
        if point > NUMERIC_MAX_INTEGER_DIGITS {
            return None;
        }
        let digits = digits[first_nonzero..].to_vec();
        Some(Self {
            neg,
            digits,
            point,
            dscale,
        })
    }

    fn is_zero(&self) -> bool {
        self.digits.iter().all(|digit| *digit == b'0')
    }

    /// Round half away from zero to `scale` fractional places.
    fn round(&mut self, scale: i32) {
        if self.is_zero() {
            self.neg = false;
            return;
        }

        let keep = self.point + i64::from(scale);
        if keep >= self.digits.len() as i64 {
            return;
        }
        if keep < 0 {
            self.digits.clear();
            self.point = 0;
            self.neg = false;
            return;
        }

        let keep = keep as usize;
        let round_up = self.digits[keep] >= b'5';
        self.digits.truncate(keep);
        if round_up {
            let mut cursor = keep;
            loop {
                if cursor == 0 {
                    self.digits.insert(0, b'1');
                    self.point += 1;
                    break;
                }
                cursor -= 1;
                if self.digits[cursor] == b'9' {
                    self.digits[cursor] = b'0';
                } else {
                    self.digits[cursor] += 1;
                    break;
                }
            }
        }

        if self.is_zero() {
            self.neg = false;
        }
    }

    fn render(&self, scale: i32) -> String {
        if self.is_zero() {
            if scale > 0 {
                return format!("0.{}", "0".repeat(scale as usize));
            }
            return "0".to_owned();
        }

        let fractional_len = scale.max(0) as usize;
        let integer_len = usize::try_from(self.point.max(1)).unwrap_or(1);
        let mut output = String::with_capacity(
            usize::from(self.neg) + integer_len + usize::from(fractional_len != 0) + fractional_len,
        );
        if self.neg {
            output.push('-');
        }

        if self.point <= 0 {
            output.push('0');
        } else {
            for index in 0..self.point {
                let digit = usize::try_from(index)
                    .ok()
                    .and_then(|index| self.digits.get(index))
                    .copied()
                    .unwrap_or(b'0');
                output.push(char::from(digit));
            }
        }

        if fractional_len != 0 {
            output.push('.');
            for offset in 0..fractional_len {
                let index = self.point + offset as i64;
                let digit = if index < 0 {
                    b'0'
                } else {
                    usize::try_from(index)
                        .ok()
                        .and_then(|index| self.digits.get(index))
                        .copied()
                        .unwrap_or(b'0')
                };
                output.push(char::from(digit));
            }
        }
        output
    }
}

/// Render a finite PostgreSQL numeric input the way `numeric_out` does.
pub(crate) fn canonical(raw: &str) -> Option<String> {
    let decimal = Decimal::parse(raw)?;
    Some(decimal.render(decimal.dscale))
}

/// Round an exact PostgreSQL numeric input to `i64`, half away from zero.
pub(crate) fn round_to_i64_exact(raw: &str) -> Option<i64> {
    let mut decimal = Decimal::parse(raw)?;
    decimal.round(0);
    parse_pg_i64(&decimal.render(0))
}

/// Apply PostgreSQL's numeric-to-`int4` *input* path without rounding.
///
/// The boolean item method first runs `numeric_out` and then `int4in`. Therefore
/// `1e0` becomes `"1"` and succeeds, while `1.0` remains `"1.0"` and fails.
pub(crate) fn numeric_to_i32_exact(raw: &str) -> Option<i32> {
    parse_pg_i32(&canonical(raw)?)
}

/// Array subscripts truncate a numeric toward zero and then require PostgreSQL `int4`.
pub(crate) fn trunc_to_i32_exact(raw: &str) -> Option<i32> {
    let value = Decimal::parse(raw)?;
    let truncated = decimal_from_coefficient(value.neg, integer_magnitude(&value), 0).ok()?;
    parse_pg_i32(&truncated.render(0))
}

/// PostgreSQL `apply_typmod`: round first, then check the post-rounding weight.
pub(crate) fn apply_typmod(value: &str, precision: i32, scale: i32) -> Option<String> {
    if !(1..=NUMERIC_MAX_PRECISION).contains(&precision)
        || !(NUMERIC_MIN_SCALE..=NUMERIC_MAX_SCALE).contains(&scale)
    {
        return None;
    }

    let mut decimal = Decimal::parse(value)?;
    decimal.round(scale);
    let maximum_integer_digits = i64::from(precision) - i64::from(scale);
    if !decimal.is_zero() && decimal.point > maximum_integer_digits {
        return None;
    }
    Some(decimal.render(scale))
}

/// Failures from exact numeric arithmetic. Callers distinguish division by zero from
/// PostgreSQL's generic numeric-format overflow; `Invalid` is defensive because JSON
/// numbers supplied by the evaluator should already be valid finite numerics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumericArithmeticError {
    Invalid,
    Overflow,
    DivisionByZero,
}

pub(crate) type ArithmeticResult<T> = Result<T, NumericArithmeticError>;

/// Compare two finite numerics without passing through `f64`.
pub(crate) fn compare_exact(left: &str, right: &str) -> Option<Ordering> {
    let left = Decimal::parse(left)?;
    let right = Decimal::parse(right)?;
    Some(compare_decimals(&left, &right))
}

/// Exact PostgreSQL numeric addition, retaining the larger input display scale.
pub(crate) fn add_exact(left: &str, right: &str) -> ArithmeticResult<String> {
    add_or_sub_exact(left, right, false)
}

/// Exact PostgreSQL numeric subtraction, retaining the larger input display scale.
pub(crate) fn sub_exact(left: &str, right: &str) -> ArithmeticResult<String> {
    add_or_sub_exact(left, right, true)
}

/// Exact PostgreSQL numeric multiplication. If the exact product needs more than
/// 16,383 fractional digits, PostgreSQL rounds it to that storage limit.
pub(crate) fn mul_exact(left: &str, right: &str) -> ArithmeticResult<String> {
    let left = Decimal::parse(left).ok_or(NumericArithmeticError::Invalid)?;
    let right = Decimal::parse(right).ok_or(NumericArithmeticError::Invalid)?;
    let full_scale = left
        .dscale
        .checked_add(right.dscale)
        .ok_or(NumericArithmeticError::Overflow)?;
    let negative = left.neg != right.neg;
    let left_coefficient = coefficient(&left, left.dscale)?;
    let right_coefficient = coefficient(&right, right.dscale)?;
    let product = multiply_digits(&left_coefficient, &right_coefficient);
    let mut result = decimal_from_coefficient(negative, product, full_scale)?;
    if full_scale > NUMERIC_MAX_DISPLAY_SCALE as i32 {
        result.round(NUMERIC_MAX_DISPLAY_SCALE as i32);
        result.dscale = NUMERIC_MAX_DISPLAY_SCALE as i32;
    }
    validate_numeric_result(&result)?;
    Ok(result.render(result.dscale))
}

/// PostgreSQL numeric division, including `select_div_scale` and exact half-away
/// rounding of the quotient.
pub(crate) fn div_exact(left: &str, right: &str) -> ArithmeticResult<String> {
    let left = Decimal::parse(left).ok_or(NumericArithmeticError::Invalid)?;
    let right = Decimal::parse(right).ok_or(NumericArithmeticError::Invalid)?;
    if right.is_zero() {
        return Err(NumericArithmeticError::DivisionByZero);
    }

    let scale = select_div_scale(&left, &right);
    let mut numerator = coefficient(&left, left.dscale)?;
    let mut denominator = coefficient(&right, right.dscale)?;
    let power = i64::from(right.dscale) + i64::from(scale) - i64::from(left.dscale);
    if power >= 0 {
        append_decimal_zeroes(&mut numerator, power)?;
    } else {
        append_decimal_zeroes(&mut denominator, -power)?;
    }

    let (mut quotient, remainder) = divide_digits(&numerator, &denominator);
    let twice_remainder = multiply_small(&remainder, 2);
    if compare_digit_vectors(&twice_remainder, &denominator) != Ordering::Less {
        increment_digits(&mut quotient);
    }

    let result = decimal_from_coefficient(left.neg != right.neg, quotient, scale)?;
    validate_numeric_result(&result)?;
    Ok(result.render(scale))
}

/// Exact PostgreSQL numeric remainder. The quotient is truncated toward zero, so the
/// nonzero remainder always has the dividend's sign.
pub(crate) fn rem_exact(left: &str, right: &str) -> ArithmeticResult<String> {
    let left = Decimal::parse(left).ok_or(NumericArithmeticError::Invalid)?;
    let right = Decimal::parse(right).ok_or(NumericArithmeticError::Invalid)?;
    if right.is_zero() {
        return Err(NumericArithmeticError::DivisionByZero);
    }

    let scale = left.dscale.max(right.dscale);
    let left_coefficient = coefficient(&left, scale)?;
    let right_coefficient = coefficient(&right, scale)?;
    let (_, remainder) = divide_digits(&left_coefficient, &right_coefficient);
    let result = decimal_from_coefficient(left.neg, remainder, scale)?;
    validate_numeric_result(&result)?;
    Ok(result.render(scale))
}

pub(crate) fn neg_exact(value: &str) -> ArithmeticResult<String> {
    let mut value = Decimal::parse(value).ok_or(NumericArithmeticError::Invalid)?;
    if !value.is_zero() {
        value.neg = !value.neg;
    }
    Ok(value.render(value.dscale))
}

pub(crate) fn abs_exact(value: &str) -> ArithmeticResult<String> {
    let mut value = Decimal::parse(value).ok_or(NumericArithmeticError::Invalid)?;
    value.neg = false;
    Ok(value.render(value.dscale))
}

pub(crate) fn ceil_exact(value: &str) -> ArithmeticResult<String> {
    integer_bound_exact(value, true)
}

pub(crate) fn floor_exact(value: &str) -> ArithmeticResult<String> {
    integer_bound_exact(value, false)
}

fn add_or_sub_exact(left: &str, right: &str, subtract: bool) -> ArithmeticResult<String> {
    let left = Decimal::parse(left).ok_or(NumericArithmeticError::Invalid)?;
    let mut right = Decimal::parse(right).ok_or(NumericArithmeticError::Invalid)?;
    if subtract && !right.is_zero() {
        right.neg = !right.neg;
    }
    let scale = left.dscale.max(right.dscale);
    let left_coefficient = coefficient(&left, scale)?;
    let right_coefficient = coefficient(&right, scale)?;

    let (negative, digits) = if left.neg == right.neg {
        (left.neg, add_digits(&left_coefficient, &right_coefficient))
    } else {
        match compare_digit_vectors(&left_coefficient, &right_coefficient) {
            Ordering::Greater => (
                left.neg,
                subtract_digits(&left_coefficient, &right_coefficient),
            ),
            Ordering::Less => (
                right.neg,
                subtract_digits(&right_coefficient, &left_coefficient),
            ),
            Ordering::Equal => (false, vec![0]),
        }
    };

    let result = decimal_from_coefficient(negative, digits, scale)?;
    validate_numeric_result(&result)?;
    Ok(result.render(scale))
}

fn integer_bound_exact(value: &str, ceiling: bool) -> ArithmeticResult<String> {
    let value = Decimal::parse(value).ok_or(NumericArithmeticError::Invalid)?;
    let fractional_nonzero = if value.point <= 0 {
        !value.is_zero()
    } else {
        value
            .digits
            .get(value.point as usize..)
            .is_some_and(|fraction| fraction.iter().any(|digit| *digit != b'0'))
    };

    let mut magnitude = integer_magnitude(&value);

    if fractional_nonzero && ((ceiling && !value.neg) || (!ceiling && value.neg)) {
        increment_digits(&mut magnitude);
    }
    let result = decimal_from_coefficient(value.neg, magnitude, 0)?;
    validate_numeric_result(&result)?;
    Ok(result.render(0))
}

fn integer_magnitude(value: &Decimal) -> Vec<u8> {
    if value.point <= 0 {
        return vec![0];
    }
    let integer_digits = value.point as usize;
    let mut magnitude: Vec<u8> = value
        .digits
        .iter()
        .take(integer_digits)
        .map(|digit| digit - b'0')
        .collect();
    magnitude.resize(integer_digits, 0);
    magnitude
}

fn compare_decimals(left: &Decimal, right: &Decimal) -> Ordering {
    if left.is_zero() && right.is_zero() {
        return Ordering::Equal;
    }
    if left.neg != right.neg {
        return if left.neg {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let absolute = compare_decimal_magnitudes(left, right);
    if left.neg {
        absolute.reverse()
    } else {
        absolute
    }
}

fn compare_decimal_magnitudes(left: &Decimal, right: &Decimal) -> Ordering {
    match left.point.cmp(&right.point) {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    let length = left.digits.len().max(right.digits.len());
    for index in 0..length {
        let left_digit = left.digits.get(index).copied().unwrap_or(b'0');
        let right_digit = right.digits.get(index).copied().unwrap_or(b'0');
        match left_digit.cmp(&right_digit) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn coefficient(decimal: &Decimal, scale: i32) -> ArithmeticResult<Vec<u8>> {
    if decimal.is_zero() {
        return Ok(vec![0]);
    }
    let trailing_zeroes = decimal.point + i64::from(scale)
        - i64::try_from(decimal.digits.len()).map_err(|_| NumericArithmeticError::Overflow)?;
    if trailing_zeroes < 0 {
        return Err(NumericArithmeticError::Invalid);
    }
    let mut digits: Vec<u8> = decimal.digits.iter().map(|digit| digit - b'0').collect();
    append_decimal_zeroes(&mut digits, trailing_zeroes)?;
    Ok(digits)
}

fn decimal_from_coefficient(
    negative: bool,
    mut digits: Vec<u8>,
    scale: i32,
) -> ArithmeticResult<Decimal> {
    strip_leading_zeroes(&mut digits);
    let point = i64::try_from(digits.len())
        .map_err(|_| NumericArithmeticError::Overflow)?
        .checked_sub(i64::from(scale))
        .ok_or(NumericArithmeticError::Overflow)?;
    for digit in &mut digits {
        *digit += b'0';
    }
    Decimal::normalise(negative, digits, point, scale).ok_or(NumericArithmeticError::Overflow)
}

fn validate_numeric_result(value: &Decimal) -> ArithmeticResult<()> {
    if value.dscale < 0
        || i64::from(value.dscale) > NUMERIC_MAX_DISPLAY_SCALE
        || (!value.is_zero() && value.point > NUMERIC_MAX_INTEGER_DIGITS)
    {
        Err(NumericArithmeticError::Overflow)
    } else {
        Ok(())
    }
}

/// Parse PostgreSQL `int8` string syntax, including base prefixes and underscores.
pub(crate) fn parse_pg_i64(input: &str) -> Option<i64> {
    let (negative, magnitude, overflowed) =
        parse_integer_magnitude(input, i64::MAX as u64, (i64::MAX as u64) + 1)?;
    if overflowed {
        return None;
    }
    if negative {
        if magnitude == (i64::MAX as u64) + 1 {
            Some(i64::MIN)
        } else {
            Some(-(magnitude as i64))
        }
    } else {
        Some(magnitude as i64)
    }
}

/// Parse PostgreSQL `int4` string syntax, including base prefixes and underscores.
pub(crate) fn parse_pg_i32(input: &str) -> Option<i32> {
    let (negative, magnitude, overflowed) =
        parse_integer_magnitude(input, i32::MAX as u64, (i32::MAX as u64) + 1)?;
    if overflowed {
        return None;
    }
    if negative {
        if magnitude == (i32::MAX as u64) + 1 {
            Some(i32::MIN)
        } else {
            Some(-(magnitude as i32))
        }
    } else {
        Some(magnitude as i32)
    }
}

/// Parse a lexically valid PostgreSQL integer token, saturating only when the local
/// AST's `i64` storage is narrower than the token. Evaluation can then emit its
/// method-specific int4-range error instead of turning it into a syntax error.
pub(crate) fn parse_pg_i64_saturating(input: &str) -> Option<i64> {
    let (negative, magnitude, _) =
        parse_integer_magnitude(input, i64::MAX as u64, (i64::MAX as u64) + 1)?;
    if negative {
        if magnitude > i64::MAX as u64 {
            Some(i64::MIN)
        } else {
            Some(-(magnitude as i64))
        }
    } else if magnitude >= i64::MAX as u64 {
        Some(i64::MAX)
    } else {
        Some(magnitude as i64)
    }
}

/// PostgreSQL `parse_bool_with_len`: a unique, case-insensitive prefix. The input
/// is intentionally not trimmed, and `"o"` is ambiguous between `on` and `off`.
pub(crate) fn parse_pg_bool(input: &str) -> Option<bool> {
    let lower = input.to_ascii_lowercase();
    match lower.as_bytes().first()? {
        b't' => "true".starts_with(&lower).then_some(true),
        b'f' => "false".starts_with(&lower).then_some(false),
        b'y' => "yes".starts_with(&lower).then_some(true),
        b'n' => "no".starts_with(&lower).then_some(false),
        b'o' if lower.len() >= 2 => {
            if "on".starts_with(&lower) {
                Some(true)
            } else if "off".starts_with(&lower) {
                Some(false)
            } else {
                None
            }
        }
        b'1' if lower.len() == 1 => Some(true),
        b'0' if lower.len() == 1 => Some(false),
        _ => None,
    }
}

/// Special spellings accepted by floating-point input. This deliberately includes
/// signed NaN, which `float8in` accepts but `numeric_in` does not.
pub(crate) fn is_nan_or_inf(raw: &str) -> bool {
    let value = trim_pg_ascii_whitespace(raw);
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    value.eq_ignore_ascii_case("nan")
        || value.eq_ignore_ascii_case("inf")
        || value.eq_ignore_ascii_case("infinity")
        || is_extended_nan(value)
}

/// Special spellings accepted by PostgreSQL numeric input. NaN may not carry a sign;
/// Infinity may. SQL/JSON `.number()`/`.decimal()` reject these after parsing.
pub(crate) fn is_numeric_nan_or_inf(raw: &str) -> bool {
    let value = trim_pg_ascii_whitespace(raw);
    if value.eq_ignore_ascii_case("nan") {
        return true;
    }
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("infinity")
}

/// Parse the finite subset of PostgreSQL `float8in` used by `.double()`.
///
/// Rust's `f64::from_str` differs at two important boundaries: it does not skip
/// PostgreSQL's ASCII whitespace, and it reports an underflow as a successful
/// zero. PostgreSQL accepts a nonzero subnormal but rejects a nonzero input that
/// rounded all the way to zero. C99 hex floats are parsed here rather than left
/// to the host C library, keeping PostgreSQL's platform-dependent extension
/// deterministic across Citadel targets.
pub(crate) fn parse_pg_finite_float8(raw: &str) -> Option<f64> {
    let input = trim_pg_ascii_whitespace(raw);
    let decimal_input = input.strip_prefix('+').unwrap_or(input);
    if let Ok(value) = decimal_input.parse::<f64>() {
        if !value.is_finite() {
            return None;
        }
        if value == 0.0 && !decimal_float_input_is_zero(input)? {
            return None;
        }
        return Some(value);
    }
    parse_hex_float(input)
}

/// Convert a finite `float8` through PostgreSQL's `float8_numeric` contract.
///
/// PostgreSQL formats with `DBL_DIG` significant digits before parsing the
/// result as `numeric`. Scientific notation is used here unconditionally; after
/// canonicalisation it produces the same numeric value and display scale as
/// `%.*g`, without depending on a C locale.
pub(crate) fn pg_float8_to_numeric(value: f64) -> Option<String> {
    if !value.is_finite() {
        return None;
    }

    // One leading digit plus fourteen fractional digits is DBL_DIG == 15.
    let scientific = format!("{value:.14e}");
    let (mantissa, exponent) = scientific.split_once('e')?;
    let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
    canonical(&format!("{mantissa}e{exponent}"))
}

fn decimal_float_input_is_zero(input: &str) -> Option<bool> {
    let (_, unsigned) = strip_sign(input)?;
    let mantissa = unsigned
        .split_once(['e', 'E'])
        .map_or(unsigned, |(mantissa, _)| mantissa);
    let mut saw_digit = false;
    for byte in mantissa.bytes() {
        match byte {
            b'0' => saw_digit = true,
            b'1'..=b'9' => return Some(false),
            b'.' => {}
            _ => return None,
        }
    }
    saw_digit.then_some(true)
}

fn is_extended_nan(value: &str) -> bool {
    let Some((prefix, rest)) = value.get(..3).zip(value.get(3..)) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case("nan") || !rest.starts_with('(') || !rest.ends_with(')') {
        return false;
    }
    rest[1..rest.len() - 1]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn parse_hex_float(input: &str) -> Option<f64> {
    let (negative, unsigned) = strip_sign(input)?;
    let body = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))?;
    let bytes = body.as_bytes();
    let mut cursor = 0;
    let mut digits = Vec::new();

    while let Some(digit) = bytes.get(cursor).and_then(|byte| digit_value(*byte)) {
        digits.push(digit);
        cursor += 1;
    }

    let mut fractional_nibbles = 0_i64;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        while let Some(digit) = bytes.get(cursor).and_then(|byte| digit_value(*byte)) {
            digits.push(digit);
            fractional_nibbles = fractional_nibbles.checked_add(1)?;
            cursor += 1;
        }
    }
    if digits.is_empty() {
        return None;
    }

    let exponent = if matches!(bytes.get(cursor), Some(b'p' | b'P')) {
        cursor += 1;
        parse_saturating_binary_exponent(bytes, &mut cursor)?
    } else {
        0
    };
    if cursor != bytes.len() {
        return None;
    }

    let significand = BigUint::from_radix_be(&digits, 16)?;
    if significand == BigUint::default() {
        return Some(f64::from_bits(u64::from(negative) << 63));
    }
    let scale = exponent.saturating_sub(fractional_nibbles.saturating_mul(4));
    binary_rational_to_float8(negative, &significand, scale)
}

fn parse_saturating_binary_exponent(bytes: &[u8], cursor: &mut usize) -> Option<i64> {
    let negative = match bytes.get(*cursor) {
        Some(b'-') => {
            *cursor += 1;
            true
        }
        Some(b'+') => {
            *cursor += 1;
            false
        }
        _ => false,
    };
    let start = *cursor;
    let mut magnitude = 0_i64;
    while let Some(digit @ b'0'..=b'9') = bytes.get(*cursor) {
        magnitude = magnitude
            .saturating_mul(10)
            .saturating_add(i64::from(*digit - b'0'));
        *cursor += 1;
    }
    if *cursor == start {
        return None;
    }
    Some(if negative {
        magnitude.saturating_neg()
    } else {
        magnitude
    })
}

/// Round an exact `significand * 2^scale` to binary64, ties to even.
fn binary_rational_to_float8(negative: bool, significand: &BigUint, scale: i64) -> Option<f64> {
    let bit_length = i64::try_from(significand.bits()).ok()?;
    let mut exponent = bit_length.checked_sub(1)?.saturating_add(scale);
    if exponent > 1023 {
        return None;
    }

    let sign_bit = u64::from(negative) << 63;
    if exponent >= -1022 {
        let mut rounded = if bit_length > 53 {
            rounded_shift_right(significand, u64::try_from(bit_length - 53).ok()?)
        } else {
            significand << usize::try_from(53 - bit_length).ok()?
        };
        if rounded.bits() > 53 {
            rounded >>= 1_usize;
            exponent = exponent.saturating_add(1);
            if exponent > 1023 {
                return None;
            }
        }
        let significand_bits = low_u64(&rounded)?;
        let fraction = significand_bits.checked_sub(1_u64 << 52)?;
        let exponent_bits = u64::try_from(exponent + 1023).ok()? << 52;
        return Some(f64::from_bits(sign_bit | exponent_bits | fraction));
    }

    let subnormal_scale = scale.saturating_add(1074);
    let rounded = if subnormal_scale >= 0 {
        significand << usize::try_from(subnormal_scale).ok()?
    } else {
        rounded_shift_right(significand, subnormal_scale.unsigned_abs())
    };
    let fraction = low_u64(&rounded)?;
    if fraction == 0 || fraction > 1_u64 << 52 {
        return None;
    }
    Some(f64::from_bits(sign_bit | fraction))
}

fn rounded_shift_right(value: &BigUint, shift: u64) -> BigUint {
    if shift == 0 {
        return value.clone();
    }
    if shift > value.bits() {
        return BigUint::default();
    }

    let shift = usize::try_from(shift).expect("a BigUint bit index fits usize");
    let quotient = value >> shift;
    let remainder = value - (&quotient << shift);
    let halfway = BigUint::from(1_u8) << (shift - 1);
    let round_up = remainder > halfway
        || (remainder == halfway && low_u64(&quotient).is_some_and(|word| word & 1 != 0));
    if round_up {
        quotient + 1_u8
    } else {
        quotient
    }
}

fn low_u64(value: &BigUint) -> Option<u64> {
    let words = value.to_u64_digits();
    match words.as_slice() {
        [] => Some(0),
        [word] => Some(*word),
        _ => None,
    }
}

fn strip_sign(input: &str) -> Option<(bool, &str)> {
    match input.as_bytes().first() {
        Some(b'-') => Some((true, &input[1..])),
        Some(b'+') => Some((false, &input[1..])),
        Some(_) => Some((false, input)),
        None => None,
    }
}

fn prefixed_radix(input: &str) -> Option<(u32, &[u8])> {
    let bytes = input.as_bytes();
    match bytes.get(0..2)? {
        [b'0', b'x' | b'X'] => Some((16, &bytes[2..])),
        [b'0', b'o' | b'O'] => Some((8, &bytes[2..])),
        [b'0', b'b' | b'B'] => Some((2, &bytes[2..])),
        _ => None,
    }
}

fn decimal_digits(bytes: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(&byte) = bytes.get(*cursor) {
        if byte.is_ascii_digit() {
            output.push(byte);
            *cursor += 1;
        } else if byte == b'_' {
            if output.is_empty() || !bytes.get(*cursor + 1).is_some_and(u8::is_ascii_digit) {
                return None;
            }
            *cursor += 1;
        } else {
            break;
        }
    }
    Some(output)
}

fn parse_exponent(bytes: &[u8], cursor: &mut usize) -> Option<i64> {
    let negative = match bytes.get(*cursor) {
        Some(b'-') => {
            *cursor += 1;
            true
        }
        Some(b'+') => {
            *cursor += 1;
            false
        }
        _ => false,
    };
    let digits = decimal_digits(bytes, cursor)?;
    if digits.is_empty() {
        return None;
    }

    let mut exponent = 0_i64;
    for digit in digits {
        exponent = exponent
            .checked_mul(10)?
            .checked_add(i64::from(digit - b'0'))?;
        if exponent > NUMERIC_MAX_EXPONENT {
            return None;
        }
    }
    Some(if negative { -exponent } else { exponent })
}

fn validated_radix_digits(input: &[u8], radix: u32) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    for (index, &byte) in input.iter().enumerate() {
        if let Some(value) = digit_value(byte).filter(|value| u32::from(*value) < radix) {
            output.push(value);
        } else if byte == b'_'
            && input
                .get(index + 1)
                .and_then(|next| digit_value(*next))
                .is_some_and(|value| u32::from(value) < radix)
        {
            continue;
        } else {
            return None;
        }
    }
    (!output.is_empty()).then_some(output)
}

fn parse_integer_magnitude(
    input: &str,
    positive_limit: u64,
    negative_limit: u64,
) -> Option<(bool, u64, bool)> {
    let input = trim_pg_ascii_whitespace(input);
    let (negative, unsigned) = strip_sign(input)?;
    let (radix, digits) = prefixed_radix(unsigned).unwrap_or((10, unsigned.as_bytes()));
    let digits = validated_radix_digits(digits, radix)?;
    let limit = if negative {
        negative_limit
    } else {
        positive_limit
    };

    let mut magnitude = 0_u64;
    let mut overflowed = false;
    for digit in digits {
        if overflowed {
            continue;
        }
        match magnitude
            .checked_mul(u64::from(radix))
            .and_then(|value| value.checked_add(u64::from(digit)))
        {
            Some(value) if value <= limit => magnitude = value,
            _ => {
                magnitude = limit;
                overflowed = true;
            }
        }
    }
    Some((negative, magnitude, overflowed))
}

fn digit_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn select_div_scale(left: &Decimal, right: &Decimal) -> i32 {
    let (left_weight, left_first) = base_weight_and_first_digit(left);
    let (right_weight, right_first) = base_weight_and_first_digit(right);
    let mut quotient_weight = left_weight - right_weight;
    if left_first <= right_first {
        quotient_weight -= 1;
    }
    let scale = NUMERIC_MIN_SIG_DIGITS - quotient_weight * 4;
    scale
        .max(i64::from(left.dscale))
        .max(i64::from(right.dscale))
        .clamp(0, NUMERIC_MAX_DISPLAY_SCALE) as i32
}

fn base_weight_and_first_digit(value: &Decimal) -> (i64, u16) {
    if value.is_zero() {
        return (0, 0);
    }
    let weight = (value.point - 1).div_euclid(4);
    let group_width = ((value.point - 1).rem_euclid(4) + 1) as usize;
    let mut first = 0_u16;
    for index in 0..group_width {
        first = first * 10 + u16::from(value.digits.get(index).copied().unwrap_or(b'0') - b'0');
    }
    (weight, first)
}

fn add_digits(left: &[u8], right: &[u8]) -> Vec<u8> {
    let length = left.len().max(right.len());
    let mut result = Vec::with_capacity(length + 1);
    let mut carry = 0_u8;
    for offset in 0..length {
        let left_digit = left
            .len()
            .checked_sub(offset + 1)
            .and_then(|index| left.get(index))
            .copied()
            .unwrap_or(0);
        let right_digit = right
            .len()
            .checked_sub(offset + 1)
            .and_then(|index| right.get(index))
            .copied()
            .unwrap_or(0);
        let sum = left_digit + right_digit + carry;
        result.push(sum % 10);
        carry = sum / 10;
    }
    if carry != 0 {
        result.push(carry);
    }
    result.reverse();
    result
}

/// Subtract absolute decimal digit vectors. `left` must be at least `right`.
fn subtract_digits(left: &[u8], right: &[u8]) -> Vec<u8> {
    debug_assert!(compare_digit_vectors(left, right) != Ordering::Less);
    let mut result = Vec::with_capacity(left.len());
    let mut borrow = 0_i16;
    for offset in 0..left.len() {
        let mut digit = i16::from(left[left.len() - offset - 1]) - borrow;
        let right_digit = right
            .len()
            .checked_sub(offset + 1)
            .and_then(|index| right.get(index))
            .copied()
            .map(i16::from)
            .unwrap_or(0);
        if digit < right_digit {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result.push((digit - right_digit) as u8);
    }
    debug_assert_eq!(borrow, 0);
    result.reverse();
    strip_leading_zeroes(&mut result);
    result
}

fn multiply_digits(left: &[u8], right: &[u8]) -> Vec<u8> {
    let product = decimal_digits_to_biguint(left) * decimal_digits_to_biguint(right);
    biguint_to_decimal_digits(&product)
}

fn divide_digits(numerator: &[u8], denominator: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let numerator = decimal_digits_to_biguint(numerator);
    let denominator = decimal_digits_to_biguint(denominator);
    debug_assert_ne!(denominator, BigUint::default());
    let (quotient, remainder) = numerator.div_rem(&denominator);
    (
        biguint_to_decimal_digits(&quotient),
        biguint_to_decimal_digits(&remainder),
    )
}

fn decimal_digits_to_biguint(digits: &[u8]) -> BigUint {
    let ascii: Vec<u8> = digits.iter().map(|digit| digit + b'0').collect();
    BigUint::parse_bytes(&ascii, 10).expect("decimal coefficient digits are non-empty and valid")
}

fn biguint_to_decimal_digits(value: &BigUint) -> Vec<u8> {
    value
        .to_str_radix(10)
        .bytes()
        .map(|digit| digit - b'0')
        .collect()
}

fn multiply_small(digits: &[u8], multiplier: u8) -> Vec<u8> {
    if multiplier == 0 || is_zero_digits(digits) {
        return vec![0];
    }
    let mut result = Vec::with_capacity(digits.len() + 1);
    let mut carry = 0_u16;
    for digit in digits.iter().rev() {
        let product = u16::from(*digit) * u16::from(multiplier) + carry;
        result.push((product % 10) as u8);
        carry = product / 10;
    }
    while carry != 0 {
        result.push((carry % 10) as u8);
        carry /= 10;
    }
    result.reverse();
    result
}

fn increment_digits(digits: &mut Vec<u8>) {
    let mut cursor = digits.len();
    loop {
        if cursor == 0 {
            digits.insert(0, 1);
            return;
        }
        cursor -= 1;
        if digits[cursor] == 9 {
            digits[cursor] = 0;
        } else {
            digits[cursor] += 1;
            return;
        }
    }
}

fn compare_digit_vectors(left: &[u8], right: &[u8]) -> Ordering {
    let left_start = left
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(left.len());
    let right_start = right
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(right.len());
    let left = &left[left_start..];
    let right = &right[right_start..];
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn strip_leading_zeroes(digits: &mut Vec<u8>) {
    let first = digits.iter().position(|digit| *digit != 0);
    match first {
        Some(0) => {}
        Some(first) => {
            digits.drain(..first);
        }
        None => {
            digits.clear();
            digits.push(0);
        }
    }
}

fn is_zero_digits(digits: &[u8]) -> bool {
    digits.iter().all(|digit| *digit == 0)
}

fn append_decimal_zeroes(digits: &mut Vec<u8>, count: i64) -> ArithmeticResult<()> {
    if count <= 0 || is_zero_digits(digits) {
        return Ok(());
    }
    let count = usize::try_from(count).map_err(|_| NumericArithmeticError::Overflow)?;
    let length = digits
        .len()
        .checked_add(count)
        .ok_or(NumericArithmeticError::Overflow)?;
    digits.resize(length, 0);
    Ok(())
}

fn trim_pg_ascii_whitespace(input: &str) -> &str {
    fn is_space(byte: u8) -> bool {
        matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
    }

    let bytes = input.as_bytes();
    let start = bytes
        .iter()
        .position(|byte| !is_space(*byte))
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !is_space(*byte))
        .map_or(start, |index| index + 1);
    &input[start..end]
}

#[cfg(test)]
#[path = "numeric_tests.rs"]
mod tests;
