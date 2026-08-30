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

//! JSON Path parser written in [nom].

use crate::{ast::*, numeric};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, multispace0 as s, one_of, u32},
    combinator::{cut, eof, map, opt, value, verify},
    error::context,
    multi::{fold_many0, many0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    Err, Finish, IResult, Offset,
};
use serde_json::Number;
use std::str::FromStr;

impl JsonPath {
    /// Compiles a JSON Path expression.
    pub fn new(s: &str) -> Result<Self, Error> {
        Self::from_str(s)
    }
}

impl FromStr for JsonPath {
    type Err = Error;

    /// Parse a JSON Path from string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(Error {
                position: 0,
                message: "empty jsonpath".into(),
            });
        }
        let (_, json_path) = json_path(s)
            .finish()
            .map_err(|e| Error::from_input_error(s, e))?;
        Checker::default()
            .visit_json_path(&json_path)
            .map_err(|msg| Error {
                position: 0,
                message: msg.into(),
            })?;
        Ok(json_path)
    }
}

/// The error type returned when parsing a JSON Path.
#[derive(Debug, thiserror::Error)]
#[error("at position {position}, {message}")]
pub struct Error {
    position: usize,
    message: Box<str>,
}

impl Error {
    fn from_input_error(input: &str, err: nom::error::Error<&str>) -> Self {
        let position = input.offset(err.input);
        let message = if position >= input.len() {
            "syntax error at end of jsonpath input".into()
        } else {
            format!(
                "syntax error at or near \"{}\" of jsonpath input",
                error_token(input, position)
            )
            .into()
        };
        Self { position, message }
    }
}

/// The token PostgreSQL's jsonpath lexer names at a syntax error.
///
/// A numeric literal is reported whole, so a failure part-way through `2.0` names
/// `2.0` rather than the dot this parser stopped at. Identifiers are likewise named
/// whole. Punctuation is a one-character token. The boundary checks also make this
/// safe if a future parser error supplies an offset inside a UTF-8 code point.
fn error_token(input: &str, position: usize) -> &str {
    let mut position = position.min(input.len());
    while position > 0 && !input.is_char_boundary(position) {
        position -= 1;
    }
    if position == input.len() {
        return "";
    }

    let bytes = input.as_bytes();
    let at_numeric_start = bytes[position].is_ascii_digit();
    let after_numeric_prefix =
        bytes[position] == b'.' && position > 0 && bytes[position - 1].is_ascii_digit();
    if at_numeric_start || after_numeric_prefix {
        let mut start = position;
        if after_numeric_prefix {
            while start > 0 && matches!(bytes[start - 1], b'0'..=b'9' | b'_') {
                start -= 1;
            }
        }
        let mut end = position;
        while let Some(byte) = bytes.get(end) {
            let is_numeric_token = byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'.')
                || (matches!(byte, b'+' | b'-')
                    && end > start
                    && matches!(bytes[end - 1], b'e' | b'E'));
            if is_numeric_token {
                end += 1;
            } else {
                break;
            }
        }
        return &input[start..end];
    }

    let first = input[position..]
        .chars()
        .next()
        .expect("position is in bounds");
    if first.is_ascii_alphanumeric() || first == '_' || !first.is_ascii() {
        let end = input[position..]
            .char_indices()
            .take_while(|(_, character)| {
                character.is_ascii_alphanumeric() || *character == '_' || !character.is_ascii()
            })
            .map(|(offset, character)| position + offset + character.len_utf8())
            .last()
            .unwrap_or(position + first.len_utf8());
        &input[position..end]
    } else {
        &input[position..position + first.len_utf8()]
    }
}

fn json_path(input: &str) -> IResult<&str, JsonPath> {
    map(
        preceded(s, separated_pair(mode, s, expr_or_predicate_eof)),
        |(mode, expr)| JsonPath {
            mode,
            expr,
            session_tz: None,
            session_date: None,
        },
    )(input)
}

fn expr_or_predicate_eof(input: &str) -> IResult<&str, ExprOrPredicate> {
    alt((
        map(terminated(predicate, pair(s, eof)), ExprOrPredicate::Pred),
        map(terminated(expr, pair(s, eof)), ExprOrPredicate::Expr),
    ))(input)
}

fn expr_or_predicate(input: &str) -> IResult<&str, ExprOrPredicate> {
    alt((
        map(predicate, ExprOrPredicate::Pred),
        map(expr, ExprOrPredicate::Expr),
    ))(input)
}

fn mode(input: &str) -> IResult<&str, Mode> {
    alt((
        value(Mode::Strict, tag_no_case("strict")),
        value(Mode::Lax, tag_no_case("lax")),
        value(Mode::Lax, tag_no_case("")),
    ))(input)
}

fn predicate(input: &str) -> IResult<&str, Predicate> {
    let (input, first) = predicate1(input)?;
    let mut first0 = Some(first);
    fold_many0(
        preceded(delimited(s, tag("||"), s), predicate1),
        move || first0.take().unwrap(),
        |acc, pred| Predicate::Or(Box::new(acc), Box::new(pred)),
    )(input)
}

fn predicate1(input: &str) -> IResult<&str, Predicate> {
    let (input, first) = predicate2(input)?;
    let mut first0 = Some(first);
    fold_many0(
        preceded(delimited(s, tag("&&"), s), predicate2),
        move || first0.take().unwrap(),
        |acc, pred| Predicate::And(Box::new(acc), Box::new(pred)),
    )(input)
}

fn predicate2(input: &str) -> IResult<&str, Predicate> {
    alt((
        map(
            tuple((expr, delimited(s, cmp_op, s), expr)),
            |(left, op, right)| Predicate::Compare(op, Box::new(left), Box::new(right)),
        ),
        map(
            delimited(
                pair(char('('), s),
                predicate,
                tuple((
                    s,
                    char(')'),
                    s,
                    tag_no_case("is"),
                    s,
                    tag_no_case("unknown"),
                )),
            ),
            |p| Predicate::IsUnknown(Box::new(p)),
        ),
        map(
            separated_pair(
                expr,
                tuple((s, tag_no_case("starts"), s, tag_no_case("with"), s)),
                starts_with_literal,
            ),
            |(expr, literal)| Predicate::StartsWith(Box::new(expr), literal),
        ),
        like_regex,
        map(preceded(pair(tag("!"), s), delimited_predicate), |p| {
            Predicate::Not(Box::new(p))
        }),
        delimited_predicate,
    ))(input)
}

fn like_regex(input: &str) -> IResult<&str, Predicate> {
    let (rest, ((expr, pattern), flags)) = pair(
        separated_pair(expr, tuple((s, tag_no_case("like_regex"), s)), string),
        opt(preceded(tuple((s, tag_no_case("flag"), s)), string)),
    )(input)?;
    let regex = Regex::with_flags(&pattern, flags).map_err(|_| {
        Err::Failure(nom::error::Error::new(
            input,
            // FIXME: should return a custom error
            nom::error::ErrorKind::RegexpMatch,
        ))
    })?;
    Ok((rest, Predicate::LikeRegex(Box::new(expr), Box::new(regex))))
}

fn delimited_predicate(input: &str) -> IResult<&str, Predicate> {
    alt((
        delimited(pair(char('('), s), predicate, pair(s, char(')'))),
        map(
            delimited(
                tuple((tag_no_case("exists"), s, char('('), s)),
                expr,
                pair(s, char(')')),
            ),
            |expr| Predicate::Exists(Box::new(expr)),
        ),
    ))(input)
}

fn expr(input: &str) -> IResult<&str, Expr> {
    let (input, first) = expr1(input)?;
    let mut first0 = Some(first);
    fold_many0(
        pair(delimited(s, alt((char('+'), char('-'))), s), expr1),
        move || first0.take().unwrap(),
        |acc, (op, expr)| match op {
            '+' => Expr::BinaryOp(BinaryOp::Add, Box::new(acc), Box::new(expr)),
            '-' => Expr::BinaryOp(BinaryOp::Sub, Box::new(acc), Box::new(expr)),
            _ => unreachable!(),
        },
    )(input)
}

fn expr1(input: &str) -> IResult<&str, Expr> {
    let (input, first) = expr2(input)?;
    let mut first0 = Some(first);
    fold_many0(
        pair(
            delimited(s, alt((char('*'), char('/'), char('%'))), s),
            expr2,
        ),
        move || first0.take().unwrap(),
        |acc, (op, expr)| match op {
            '*' => Expr::BinaryOp(BinaryOp::Mul, Box::new(acc), Box::new(expr)),
            '/' => Expr::BinaryOp(BinaryOp::Div, Box::new(acc), Box::new(expr)),
            '%' => Expr::BinaryOp(BinaryOp::Rem, Box::new(acc), Box::new(expr)),
            _ => unreachable!(),
        },
    )(input)
}

fn expr2(input: &str) -> IResult<&str, Expr> {
    alt((
        accessor_expr,
        map(preceded(pair(char('+'), s), expr2), |expr| match &expr {
            // constant folding
            Expr::PathPrimary(PathPrimary::Value(Value::Number(_))) => expr,
            _ => Expr::UnaryOp(UnaryOp::Plus, Box::new(expr)),
        }),
        map(preceded(pair(char('-'), s), expr2), |expr| {
            Expr::UnaryOp(UnaryOp::Minus, Box::new(expr))
        }),
    ))(input)
}

fn accessor_expr(input: &str) -> IResult<&str, Expr> {
    map(
        pair(path_primary, many0(preceded(s, accessor_op))),
        |(primary, ops)| {
            let mut expr = Expr::PathPrimary(primary.unnest());
            for op in ops {
                expr = Expr::Accessor(Box::new(expr), op);
            }
            expr
        },
    )(input)
}

fn path_primary(input: &str) -> IResult<&str, PathPrimary> {
    alt((
        map(scalar_value, PathPrimary::Value),
        value(PathPrimary::Root, char('$')),
        value(PathPrimary::Current, char('@')),
        value(PathPrimary::Last, tag_no_case("last")),
        map(
            delimited(pair(char('('), s), expr_or_predicate, pair(s, char(')'))),
            |expr| PathPrimary::ExprOrPred(Box::new(expr)),
        ),
    ))(input)
}

fn accessor_op(input: &str) -> IResult<&str, AccessorOp> {
    alt((
        map(
            preceded(tag(".**"), level_range),
            AccessorOp::DescendantMemberWildcard,
        ),
        value(AccessorOp::MemberWildcard, tag(".*")),
        value(AccessorOp::ElementWildcard, element_wildcard),
        map(item_method, AccessorOp::Method),
        map(member_accessor, AccessorOp::Member),
        map(array_accessor, AccessorOp::Element),
        map(filter_expr, |expr| AccessorOp::FilterExpr(Box::new(expr))),
    ))(input)
}

fn level_range(input: &str) -> IResult<&str, LevelRange> {
    alt((
        map(
            delimited(
                pair(char('{'), s),
                separated_pair(level, delimited(s, tag_no_case("to"), s), level),
                pair(s, char('}')),
            ),
            |(start, end)| {
                if start == end {
                    LevelRange::One(start)
                } else {
                    LevelRange::Range(start, end)
                }
            },
        ),
        map(
            delimited(pair(char('{'), s), level, pair(s, char('}'))),
            LevelRange::One,
        ),
        value(LevelRange::All, tag("")),
    ))(input)
}

fn level(input: &str) -> IResult<&str, Level> {
    alt((value(Level::Last, tag_no_case("last")), map(u32, Level::N)))(input)
}

fn element_wildcard(input: &str) -> IResult<&str, ()> {
    value((), tuple((char('['), s, char('*'), s, char(']'))))(input)
}

fn member_accessor(input: &str) -> IResult<&str, String> {
    preceded(pair(char('.'), s), alt((string, raw_string)))(input)
}

fn array_accessor(input: &str) -> IResult<&str, Vec<ArrayIndex>> {
    delimited(
        char('['),
        separated_list1(char(','), delimited(s, index_elem, s)),
        char(']'),
    )(input)
}

fn index_elem(input: &str) -> IResult<&str, ArrayIndex> {
    alt((
        map(
            separated_pair(expr, delimited(s, tag_no_case("to"), s), expr),
            |(start, end)| ArrayIndex::Slice(start, end),
        ),
        map(expr, ArrayIndex::Index),
    ))(input)
}

fn filter_expr(input: &str) -> IResult<&str, Predicate> {
    delimited(
        tuple((char('?'), s, char('('), s)),
        predicate,
        tuple((s, char(')'))),
    )(input)
}

fn cmp_op(input: &str) -> IResult<&str, CompareOp> {
    alt((
        value(CompareOp::Eq, tag("==")),
        value(CompareOp::Ne, tag("!=")),
        value(CompareOp::Ne, tag("<>")),
        value(CompareOp::Le, tag("<=")),
        value(CompareOp::Lt, char('<')),
        value(CompareOp::Ge, tag(">=")),
        value(CompareOp::Gt, char('>')),
    ))(input)
}

fn item_method(input: &str) -> IResult<&str, Method> {
    let (input, _) = pair(char('.'), s)(input)?;
    let (input, method) = method(input)?;
    let (input, _) = tuple((s, char('(')))(input)?;
    // A matched name and `(` commit the call. Without this, a bad argument backtracks and
    // the failure is reported at the `(` rather than at the argument PostgreSQL names.
    let (input, method) = cut(|i| method_args(i, method.clone()))(input)?;
    let (input, _) = cut(tuple((s, char(')'))))(input)?;
    Ok((input, method))
}

/// Parse the argument list of a method that takes one, leaving `)` for the caller.
fn method_args(input: &str, method: Method) -> IResult<&str, Method> {
    match method {
        Method::Datetime { .. } => {
            let (input, _) = s(input)?;
            let (input, template) = opt(string)(input)?;
            Ok((input, Method::Datetime { template }))
        }
        Method::Decimal { .. } => {
            let (input, _) = s(input)?;
            let (input, precision) = opt(signed_arg)(input)?;
            if precision.is_none() {
                return Ok((
                    input,
                    Method::Decimal {
                        precision: None,
                        scale: None,
                    },
                ));
            }
            let (input, _) = s(input)?;
            // Once the comma is present a scale is mandatory. `cut` keeps a missing
            // scale from rewinding to the comma, so `decimal(2,)` reports `)`.
            let (input, scale) = opt(preceded(pair(char(','), s), cut(signed_arg)))(input)?;
            Ok((input, Method::Decimal { precision, scale }))
        }
        Method::Time { .. } => {
            let (input, precision) = time_precision(input)?;
            Ok((input, Method::Time { precision }))
        }
        Method::TimeTz { .. } => {
            let (input, precision) = time_precision(input)?;
            Ok((input, Method::TimeTz { precision }))
        }
        Method::Timestamp { .. } => {
            let (input, precision) = time_precision(input)?;
            Ok((input, Method::Timestamp { precision }))
        }
        Method::TimestampTz { .. } => {
            let (input, precision) = time_precision(input)?;
            Ok((input, Method::TimestampTz { precision }))
        }
        other => Ok((input, other)),
    }
}

/// Datetime precision: digits only. Rejecting a sign here is what makes `$.time(-1)`
/// a syntax error, as PostgreSQL reports it.
fn time_precision(input: &str) -> IResult<&str, Option<i64>> {
    let (input, _) = s(input)?;
    opt(integer_arg)(input)
}

/// `.decimal()` precision and scale accept a sign; PostgreSQL range-checks them during
/// evaluation, so out-of-range values must survive parsing to report their own error.
fn signed_arg(input: &str) -> IResult<&str, i64> {
    let (input, sign) = opt(terminated(one_of("+-"), s))(input)?;
    let (input, token) = integer_token(input)?;
    let mut signed = String::with_capacity(token.len() + usize::from(sign.is_some()));
    if let Some(sign) = sign {
        signed.push(sign);
    }
    signed.push_str(token);
    let value = numeric::parse_pg_i64_saturating(&signed)
        .expect("the JSONPath integer lexer only emits valid PostgreSQL integers");
    Ok((input, value))
}

fn integer_arg(input: &str) -> IResult<&str, i64> {
    let (input, token) = integer_token(input)?;
    let value = numeric::parse_pg_i64_saturating(token)
        .expect("the JSONPath integer lexer only emits valid PostgreSQL integers");
    Ok((input, value))
}

/// PostgreSQL 17's `INT_P` lexer token: ECMAScript decimal integers plus explicit
/// hexadecimal, octal and binary forms, with separators only between digits.
fn integer_token(input: &str) -> IResult<&str, &str> {
    let bytes = input.as_bytes();
    let Some(&first) = bytes.first() else {
        return Err(Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Digit,
        )));
    };

    let (radix, mut end) = if first == b'0' {
        match bytes.get(1) {
            Some(b'x' | b'X') => (16, 2),
            Some(b'o' | b'O') => (8, 2),
            Some(b'b' | b'B') => (2, 2),
            _ => return Ok((&input[1..], &input[..1])),
        }
    } else if matches!(first, b'1'..=b'9') {
        (10, 0)
    } else {
        return Err(Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Digit,
        )));
    };

    if radix == 10 {
        end = 1;
    } else if !bytes
        .get(end)
        .is_some_and(|byte| integer_digit(*byte, radix))
    {
        return Err(Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Digit,
        )));
    }

    while let Some(&byte) = bytes.get(end) {
        if integer_digit(byte, radix) {
            end += 1;
        } else if byte == b'_'
            && bytes
                .get(end + 1)
                .is_some_and(|next| integer_digit(*next, radix))
        {
            end += 2;
        } else {
            break;
        }
    }
    Ok((&input[end..], &input[..end]))
}

fn integer_digit(byte: u8, radix: u32) -> bool {
    match radix {
        2 => matches!(byte, b'0'..=b'1'),
        8 => matches!(byte, b'0'..=b'7'),
        10 => byte.is_ascii_digit(),
        16 => byte.is_ascii_hexdigit(),
        _ => false,
    }
}

fn method(input: &str) -> IResult<&str, Method> {
    // Order matters: `alt` commits to the first match, and these names are prefixes of
    // one another (`time` of `timestamp`, `date` of `datetime`). Longest first.
    alt((
        value(Method::Type, tag_no_case("type")),
        value(Method::Size, tag_no_case("size")),
        value(Method::Double, tag_no_case("double")),
        value(Method::Ceiling, tag_no_case("ceiling")),
        value(Method::Floor, tag_no_case("floor")),
        value(Method::Abs, tag_no_case("abs")),
        value(Method::Keyvalue, tag_no_case("keyvalue")),
        value(Method::Datetime { template: None }, tag_no_case("datetime")),
        value(
            Method::TimestampTz { precision: None },
            tag_no_case("timestamp_tz"),
        ),
        value(
            Method::Timestamp { precision: None },
            tag_no_case("timestamp"),
        ),
        value(Method::TimeTz { precision: None }, tag_no_case("time_tz")),
        value(Method::Time { precision: None }, tag_no_case("time")),
        value(Method::Date, tag_no_case("date")),
        value(
            Method::Decimal {
                precision: None,
                scale: None,
            },
            tag_no_case("decimal"),
        ),
        value(Method::Bigint, tag_no_case("bigint")),
        value(Method::Boolean, tag_no_case("boolean")),
        value(Method::Integer, tag_no_case("integer")),
        value(Method::Number, tag_no_case("number")),
        value(Method::String, tag_no_case("string")),
    ))(input)
}

fn scalar_value(input: &str) -> IResult<&str, Value> {
    alt((
        value(Value::Null, tag("null")),
        value(Value::Boolean(true), tag("true")),
        value(Value::Boolean(false), tag("false")),
        map(number, Value::Number),
        map(string, Value::String),
        map(variable, Value::Variable),
    ))(input)
}

fn number(input: &str) -> IResult<&str, Number> {
    let (input, token) = numeric_token(input)?;
    let canonical = numeric::canonical(token)
        .ok_or_else(|| Err::Error(nom::error::Error::new(token, nom::error::ErrorKind::Float)))?;
    let number = serde_json::from_str(&canonical)
        .map_err(|_| Err::Error(nom::error::Error::new(token, nom::error::ErrorKind::Float)))?;
    Ok((input, number))
}

/// PostgreSQL's JSONPath numeric lexer, kept exact through `numeric_out` rather than
/// first converting to `f64`. This is what lets path literals beyond 53 bits and
/// exponents such as `1e1000` reach the evaluator intact.
fn numeric_token(input: &str) -> IResult<&str, &str> {
    let bytes = input.as_bytes();
    let Some(&first) = bytes.first() else {
        return Err(Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Float,
        )));
    };

    if first == b'0' && matches!(bytes.get(1), Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')) {
        return integer_token(input);
    }

    let mut end;
    let had_integer = if first == b'0' {
        end = 1;
        true
    } else if matches!(first, b'1'..=b'9') {
        end = consume_decimal_digits(bytes, 0);
        true
    } else if first == b'.' {
        end = 0;
        false
    } else {
        return Err(Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Float,
        )));
    };

    if bytes.get(end) == Some(&b'.') {
        end += 1;
        let fractional_end = consume_decimal_digits(bytes, end);
        if !had_integer && fractional_end == end {
            return Err(Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Float,
            )));
        }
        end = fractional_end;
    }

    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        end += 1;
        if matches!(bytes.get(end), Some(b'+' | b'-')) {
            end += 1;
        }
        let exponent_end = consume_decimal_digits(bytes, end);
        if exponent_end == end {
            return Err(Err::Error(nom::error::Error::new(
                &input[end..],
                nom::error::ErrorKind::Float,
            )));
        }
        end = exponent_end;
    }

    Ok((&input[end..], &input[..end]))
}

fn consume_decimal_digits(bytes: &[u8], start: usize) -> usize {
    let Some(first) = bytes.get(start).filter(|byte| byte.is_ascii_digit()) else {
        return start;
    };
    let _ = first;
    let mut end = start + 1;
    while let Some(byte) = bytes.get(end) {
        if byte.is_ascii_digit() {
            end += 1;
        } else if *byte == b'_' && bytes.get(end + 1).is_some_and(|next| next.is_ascii_digit()) {
            end += 2;
        } else {
            break;
        }
    }
    end
}

fn starts_with_literal(input: &str) -> IResult<&str, Value> {
    alt((map(string, Value::String), map(variable, Value::Variable)))(input)
}

fn variable(input: &str) -> IResult<&str, String> {
    preceded(char('$'), raw_string)(input)
}

fn string(input: &str) -> IResult<&str, String> {
    context(
        "double quoted string",
        delimited(
            char('"'),
            fold_many0(
                alt((
                    map(unescaped_str, String::from),
                    map(escaped_char, String::from),
                )),
                String::new,
                |mut string, fragment| {
                    if string.is_empty() {
                        fragment
                    } else {
                        string.push_str(&fragment);
                        string
                    }
                },
            ),
            cut(char('"')),
        ),
    )(input)
}

fn escaped_char(input: &str) -> IResult<&str, char> {
    context(
        "escaped character",
        preceded(
            char('\\'),
            alt((
                value('\u{0008}', char('b')),
                value('\u{0009}', char('t')),
                value('\u{000A}', char('n')),
                value('\u{000C}', char('f')),
                value('\u{000D}', char('r')),
                value('\u{002F}', char('/')),
                value('\u{005C}', char('\\')),
                value('\u{0022}', char('"')),
                // unicode_sequence,
            )),
        ),
    )(input)
}

fn unescaped_str(input: &str) -> IResult<&str, &str> {
    context(
        "unescaped character",
        verify(take_while(is_valid_unescaped_char), |s: &str| !s.is_empty()),
    )(input)
}

fn is_valid_unescaped_char(chr: char) -> bool {
    match chr {
        '"' => false,
        '\u{20}'..='\u{5B}' // Omit control characters
        | '\u{5D}'..='\u{10FFFF}' => true, // Omit \
        _ => false,
    }
}

fn raw_string(input: &str) -> IResult<&str, String> {
    map(
        take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_' || c >= '\u{0080}'),
        String::from,
    )(input)
}

/// A visitor that checks if a JSON Path is valid.
///
/// An error is returned if:
///
/// - `@` is used in a non-root expression
/// - `last` is used in a non-array subscript
#[derive(Debug, Clone, Copy, Default)]
struct Checker {
    non_root: bool,
    inside_element_accessor: bool,
}

impl Checker {
    fn visit_json_path(&self, json_path: &JsonPath) -> Result<(), &'static str> {
        self.visit_expr_or_predicate(&json_path.expr)
    }

    fn visit_expr_or_predicate(&self, expr_or_pred: &ExprOrPredicate) -> Result<(), &'static str> {
        match expr_or_pred {
            ExprOrPredicate::Expr(expr) => self.visit_expr(expr),
            ExprOrPredicate::Pred(pred) => self.visit_predicate(pred),
        }
    }

    fn visit_expr(&self, expr: &Expr) -> Result<(), &'static str> {
        match expr {
            Expr::PathPrimary(primary) => self.visit_path_primary(primary),
            Expr::Accessor(base, accessor) => {
                self.visit_expr(base)?;
                self.visit_accessor_op(accessor)
            }
            Expr::UnaryOp(_, expr) => self.visit_expr(expr),
            Expr::BinaryOp(_, left, right) => {
                self.visit_expr(left)?;
                self.visit_expr(right)
            }
        }
    }

    fn visit_predicate(&self, pred: &Predicate) -> Result<(), &'static str> {
        match pred {
            Predicate::Compare(_, left, right) => {
                self.visit_expr(left)?;
                self.visit_expr(right)
            }
            Predicate::Exists(expr) => self.visit_expr(expr),
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.visit_predicate(left)?;
                self.visit_predicate(right)
            }
            Predicate::Not(pred) => self.visit_predicate(pred),
            Predicate::IsUnknown(pred) => self.visit_predicate(pred),
            Predicate::StartsWith(expr, _) => self.visit_expr(expr),
            Predicate::LikeRegex(expr, _) => self.visit_expr(expr),
        }
    }

    fn visit_path_primary(&self, primary: &PathPrimary) -> Result<(), &'static str> {
        match primary {
            PathPrimary::Last if !self.inside_element_accessor => {
                Err("LAST is allowed only in array subscripts")
            }
            PathPrimary::Current if !self.non_root => Err("@ is not allowed in root expressions"),
            _ => Ok(()),
        }
    }

    fn visit_accessor_op(&self, accessor_op: &AccessorOp) -> Result<(), &'static str> {
        match accessor_op {
            AccessorOp::ElementWildcard | AccessorOp::MemberWildcard => Ok(()),
            AccessorOp::DescendantMemberWildcard(_) => Ok(()),
            AccessorOp::Member(_) | AccessorOp::Method(_) => Ok(()),
            AccessorOp::FilterExpr(pred) => Self {
                non_root: true,
                ..*self
            }
            .visit_predicate(pred),
            AccessorOp::Element(indices) => {
                let next = Self {
                    non_root: true,
                    inside_element_accessor: true,
                };
                for index in indices {
                    match index {
                        ArrayIndex::Index(i) => next.visit_expr(i)?,
                        ArrayIndex::Slice(s, e) => {
                            next.visit_expr(s)?;
                            next.visit_expr(e)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path() {
        JsonPath::from_str(r#"lax $.name ? (@ starts with "O''")"#).unwrap();
        JsonPath::from_str(r#"lax $.name ? (@ starts with "\"hello")"#).unwrap();
        // JsonPath::from_str(r#"lax $.name ? (@ starts with "O\u0027")"#).unwrap();
        // JsonPath::from_str(r#"lax $.name ? (@ starts with "\u0022hello")"#).unwrap();
    }

    #[test]
    fn method_integer_arguments_use_postgres_int_tokens() {
        for (input, rendered) in [
            ("$.decimal(1_0,2)", "$.decimal(10,2)"),
            ("$.decimal(0xA,0b10)", "$.decimal(10,2)"),
            ("$.decimal(+ 0o12,- 2)", "$.decimal(10,-2)"),
            ("$.time(1_0)", "$.time(10)"),
            ("$.timestamp(0xA)", "$.timestamp(10)"),
        ] {
            let path = JsonPath::from_str(input).unwrap_or_else(|error| panic!("{input}: {error}"));
            assert_eq!(path.to_string(), rendered, "{input}");
        }
    }

    #[test]
    fn method_integer_tokens_reject_non_ecmascript_forms() {
        for input in [
            "$.decimal(01)",
            "$.decimal(0x_A)",
            "$.decimal(1__0)",
            "$.decimal(1.0)",
            "$.time(+1)",
            "$.time(-1)",
        ] {
            assert!(JsonPath::from_str(input).is_err(), "{input} should fail");
        }
    }

    #[test]
    fn oversized_method_arguments_reach_evaluation() {
        for input in [
            "$.decimal(9223372036854775808,1)",
            "$.decimal(-9223372036854775809,1)",
            "$.time(0xffffffffffffffffffff)",
        ] {
            assert!(JsonPath::from_str(input).is_ok(), "{input} should parse");
        }
    }

    #[test]
    fn numeric_path_literals_are_never_narrowed_through_f64() {
        let wide = JsonPath::from_str("9007199254740993 + 1").unwrap();
        assert_eq!(wide.to_string(), "(9007199254740993 + 1)");

        let exponent = JsonPath::from_str("1e1000").unwrap();
        let rendered = exponent.to_string();
        assert_eq!(rendered.len(), 1001);
        assert!(rendered.starts_with('1'));
        assert!(rendered[1..].bytes().all(|byte| byte == b'0'));

        assert_eq!(JsonPath::from_str("0xFF").unwrap().to_string(), "255");
        assert_eq!(
            JsonPath::from_str("1_000.5_0").unwrap().to_string(),
            "1000.50"
        );
    }

    #[test]
    fn syntax_errors_name_end_punctuation_numeric_and_unicode_tokens() {
        let at_end = JsonPath::from_str("$.decimal(2").unwrap_err().to_string();
        assert!(
            at_end.contains("syntax error at end of jsonpath input"),
            "{at_end}"
        );

        let missing_scale = JsonPath::from_str("$.decimal(2,)").unwrap_err().to_string();
        assert!(
            missing_scale.contains("at or near \")\""),
            "{missing_scale}"
        );

        let decimal_token = JsonPath::from_str("$.decimal(2.0)")
            .unwrap_err()
            .to_string();
        assert!(
            decimal_token.contains("at or near \"2.0\""),
            "{decimal_token}"
        );

        let unicode_token = JsonPath::from_str("$.date(é)").unwrap_err().to_string();
        assert!(
            unicode_token.contains("at or near \"é\""),
            "{unicode_token}"
        );
    }

    #[test]
    fn error_token_never_slices_inside_utf8_or_past_the_input() {
        assert_eq!(error_token("é", 1), "é");
        assert_eq!(error_token("é", usize::MAX), "");
        assert_eq!(error_token("2.0", 1), "2.0");
        assert_eq!(error_token(")", 0), ")");
    }
}
