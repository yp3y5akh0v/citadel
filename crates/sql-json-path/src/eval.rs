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

use rustc_hash::FxHashMap;
use serde_json::Number;

use crate::{
    ast::*,
    datetime::{DatetimeKind, ParsedDatetime},
    json::{ArrayRef, Cow, Json, JsonRef, ObjectRef},
};

pub type Result<T> = std::result::Result<T, Error>;

/// The error type returned when evaluating a JSON path.
#[non_exhaustive]
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    // structural errors
    #[error("JSON object does not contain key \"{0}\"")]
    NoKey(Box<str>),
    #[error("jsonpath array accessor can only be applied to an array")]
    ArrayAccess,
    #[error("jsonpath wildcard array accessor can only be applied to an array")]
    WildcardArrayAccess,
    #[error("jsonpath member accessor can only be applied to an object")]
    MemberAccess,
    #[error("jsonpath wildcard member accessor can only be applied to an object")]
    WildcardMemberAccess,
    #[error("jsonpath array subscript is out of bounds")]
    ArrayIndexOutOfBounds,

    #[error("jsonpath array subscript is out of integer range")]
    ArrayIndexOutOfRange,
    #[error("jsonpath array subscript is not a single numeric value")]
    ArrayIndexNotNumeric,
    #[error("could not find jsonpath variable \"{0}\"")]
    NoVariable(Box<str>),
    #[error("\"vars\" argument is not an object")]
    VarsNotObject,
    #[error("operand of unary jsonpath operator {0} is not a numeric value")]
    UnaryOperandNotNumeric(UnaryOp),
    #[error("left operand of jsonpath operator {0} is not a single numeric value")]
    LeftOperandNotNumeric(BinaryOp),
    #[error("right operand of jsonpath operator {0} is not a single numeric value")]
    RightOperandNotNumeric(BinaryOp),
    #[error("jsonpath item method .{0}() can only be applied to a numeric value")]
    MethodNotNumeric(&'static str),
    #[error("jsonpath item method .size() can only be applied to an array")]
    SizeNotArray,
    #[error("jsonpath item method .keyvalue() can only be applied to an object")]
    KeyValueNotObject,
    #[error("division by zero")]
    DivisionByZero,
    #[error("value overflows numeric format")]
    NumericOverflow,
    #[error("single boolean result is expected")]
    ExpectSingleBoolean,
    #[error("jsonpath item method .datetime() can only be applied to a string")]
    DatetimeNotString,
    #[error("datetime format is not recognized: {0}")]
    DatetimeFormatNotRecognized(Box<str>),
    #[error("datetime format is zoned but not timed")]
    DatetimeZonedNotTimed,
    #[error("invalid datetime input: {0}")]
    InvalidDatetimeInput(Box<str>),
    #[error("invalid datetime format separator: {0}")]
    DatetimeInvalidSeparator(Box<str>),
    #[error("invalid value {0} for {1}")]
    DatetimeInvalidValue(Box<str>, Box<str>),
    #[error("trailing characters remain in input string after datetime format")]
    DatetimeTrailingInput,
    #[error("unmatched format character {0}")]
    DatetimeUnmatchedChar(Box<str>),
    #[error("input string is too short for datetime format")]
    DatetimeInputTooShort,
    #[error("cannot convert value from {0} to {1} without time zone usage")]
    DatetimeConvertWithoutTz(Box<str>, Box<str>),
    #[error("invalid datetime template: {0}")]
    InvalidDatetimeTemplate(Box<str>),
    #[error("template directive {0} is not supported by jsonpath")]
    UnsupportedDatetimeDirective(Box<str>),
    #[error("jsonpath item method .{0}() can only be applied to a string or numeric value")]
    NumericConversionType(&'static str),
    #[error("argument \"{0}\" of jsonpath item method .{1}() is invalid for type {2}")]
    InvalidConversion(Box<str>, &'static str, &'static str),
    #[error(
        "jsonpath item method .boolean() can only be applied to a boolean, string, or numeric value"
    )]
    BooleanTypeError,
    #[error("jsonpath item method .string() can only be applied to a boolean, string, numeric, or datetime value")]
    StringTypeError,
    #[error("jsonpath item method .{0}() can only be applied to a string")]
    DatetimeMethodNotString(&'static str),
    /// Takes the raw input; the quotes belong to this format string, unlike
    /// [`Error::DatetimeFormatNotRecognized`], whose argument arrives pre-quoted.
    #[error("{0} format is not recognized: \"{1}\"")]
    FormatNotRecognized(&'static str, Box<str>),
    #[error("NaN or Infinity is not allowed for jsonpath item method .{0}()")]
    NanOrInfinity(&'static str),
    #[error("NUMERIC precision {0} must be between 1 and 1000")]
    NumericPrecisionOutOfBounds(i64),
    #[error("NUMERIC scale {0} must be between -1000 and 1000")]
    NumericScaleOutOfBounds(i64),
    #[error("precision of jsonpath item method .decimal() is out of range for type integer")]
    DecimalPrecisionOutOfRange,
    #[error("scale of jsonpath item method .decimal() is out of range for type integer")]
    DecimalScaleOutOfRange,
    #[error("time precision of jsonpath item method .{0}() is out of range for type integer")]
    TimePrecisionOutOfRange(&'static str),
}

impl Error {
    pub const fn can_silent(&self) -> bool {
        !matches!(
            self,
            Self::NoVariable(_) | Self::DatetimeConvertWithoutTz(_, _)
        )
    }

    // A structural error is an attempt to access a non-existent member of an object or element of an array.
    pub const fn is_structural(&self) -> bool {
        matches!(
            self,
            Self::NoKey(_)
                | Self::ArrayAccess
                | Self::WildcardArrayAccess
                | Self::MemberAccess
                | Self::WildcardMemberAccess
                | Self::ArrayIndexOutOfBounds
        )
    }
}

/// Truth value used in SQL/JSON path predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl From<bool> for Truth {
    fn from(b: bool) -> Self {
        if b {
            Truth::True
        } else {
            Truth::False
        }
    }
}

impl Truth {
    /// Returns true if the value is true.
    fn is_true(self) -> bool {
        matches!(self, Truth::True)
    }

    /// Returns true if the value is false.
    #[allow(unused)]
    fn is_false(self) -> bool {
        matches!(self, Truth::False)
    }

    /// Returns true if the value is unknown.
    fn is_unknown(self) -> bool {
        matches!(self, Truth::Unknown)
    }

    /// AND operation.
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Truth::True, Truth::True) => Truth::True,
            (Truth::False, _) | (_, Truth::False) => Truth::False,
            _ => Truth::Unknown,
        }
    }

    /// OR operation.
    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Truth::True, _) | (_, Truth::True) => Truth::True,
            (Truth::False, Truth::False) => Truth::False,
            _ => Truth::Unknown,
        }
    }

    /// NOT operation.
    fn not(self) -> Self {
        match self {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
        }
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Truth::True, _) | (_, Truth::True) => Truth::True,
            (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
            (Truth::False, Truth::False) => Truth::False,
        }
    }

    /// Converts to JSON value.
    fn to_json<T: Json>(self) -> T {
        match self {
            Truth::True => T::bool(true),
            Truth::False => T::bool(false),
            Truth::Unknown => T::null(),
        }
    }
}

#[derive(Debug)]
enum EvalItem<'a, T: Json + 'a> {
    Json(Cow<'a, T>),
    Datetime(ParsedDatetime),
}

impl<'a, T: Json> EvalItem<'a, T> {
    fn borrowed(value: T::Borrowed<'a>) -> Self {
        Self::Json(Cow::Borrowed(value))
    }

    fn owned(value: T) -> Self {
        Self::Json(Cow::Owned(value))
    }

    fn as_json<'b>(&'b self) -> Option<T::Borrowed<'b>>
    where
        'a: 'b,
    {
        match self {
            Self::Json(value) => Some(value.as_ref()),
            Self::Datetime(_) => None,
        }
    }

    fn as_current<'b>(&'b self) -> Current<'b, T>
    where
        'a: 'b,
    {
        match self {
            Self::Json(value) => Current::Json(value.as_ref()),
            Self::Datetime(value) => Current::Datetime(value),
        }
    }

    fn into_owned<'b>(self) -> EvalItem<'b, T> {
        match self {
            Self::Json(value) => EvalItem::owned(value.into_owned()),
            Self::Datetime(value) => EvalItem::Datetime(value),
        }
    }

    fn into_output(self) -> Cow<'a, T> {
        match self {
            Self::Json(value) => value,
            Self::Datetime(value) => Cow::Owned(T::from_string(&value.iso)),
        }
    }
}

#[derive(Debug)]
enum Current<'a, T: Json + 'a> {
    Json(T::Borrowed<'a>),
    Datetime(&'a ParsedDatetime),
}

impl<T: Json> Copy for Current<'_, T> {}

impl<T: Json> Clone for Current<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: Json> Current<'a, T> {
    fn as_json(self) -> Option<T::Borrowed<'a>> {
        match self {
            Self::Json(value) => Some(value),
            Self::Datetime(_) => None,
        }
    }

    fn as_datetime(self) -> Option<&'a ParsedDatetime> {
        match self {
            Self::Json(_) => None,
            Self::Datetime(value) => Some(value),
        }
    }

    fn to_item(self) -> EvalItem<'a, T> {
        match self {
            Self::Json(value) => EvalItem::borrowed(value),
            Self::Datetime(value) => EvalItem::Datetime(value.clone()),
        }
    }
}

fn into_outputs<'a, T: Json>(set: Vec<EvalItem<'a, T>>) -> Vec<Cow<'a, T>> {
    set.into_iter().map(EvalItem::into_output).collect()
}

#[derive(Debug, Default)]
struct ObjectIds {
    next: i64,
    by_identity: FxHashMap<usize, i64>,
}

impl ObjectIds {
    fn id_for(&mut self, identity: usize) -> i64 {
        if let Some(id) = self.by_identity.get(&identity) {
            return *id;
        }
        self.next = self
            .next
            .checked_add(1)
            .expect("one JSONPath evaluation cannot contain more than i64::MAX objects");
        self.by_identity.insert(identity, self.next);
        self.next
    }
}

impl JsonPath {
    /// Set the session time zone the `_tz` entry points resolve against.
    ///
    /// PostgreSQL reads `SET TIME ZONE`; without this the zone is UTC, which is what
    /// PostgreSQL itself falls back to when `TimeZone` is unset.
    #[must_use]
    pub fn with_session_tz(mut self, tz: jiff::tz::TimeZone) -> Self {
        self.session_tz = Some(tz);
        self
    }

    /// Set the transaction-start date used by time-to-timetz conversions.
    ///
    /// PostgreSQL resolves a bare `time` using the current date in the session zone,
    /// because that zone's UTC offset may depend on daylight saving time.
    #[must_use]
    pub fn with_session_date(mut self, date: jiff::civil::Date) -> Self {
        self.session_date = Some(date);
        self
    }

    /// The zone `_tz` evaluation resolves against, defaulting to UTC as PostgreSQL does.
    fn tz(&self) -> &jiff::tz::TimeZone {
        static UTC: jiff::tz::TimeZone = jiff::tz::TimeZone::UTC;
        self.session_tz.as_ref().unwrap_or(&UTC)
    }

    fn date(&self) -> jiff::civil::Date {
        self.session_date
            .unwrap_or_else(|| jiff::Timestamp::now().to_zoned(self.tz().clone()).date())
    }

    fn evaluate<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
        first: bool,
        use_tz: bool,
        silent: bool,
    ) -> Result<Vec<Cow<'a, T::Owned>>> {
        let object_ids = std::cell::RefCell::new(ObjectIds::default());
        Evaluator {
            root: value,
            current: Current::Json(value),
            vars,
            array_len: None,
            mode: self.mode,
            session_tz: self.tz(),
            session_date: self.date(),
            object_ids: &object_ids,
            first,
            use_tz,
            silent,
        }
        .eval_expr_or_predicate(&self.expr)
        .map(into_outputs)
    }

    /// Evaluate the JSON path against the given JSON value.
    pub fn query<'a, T: JsonRef<'a>>(&self, value: T) -> Result<Vec<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), false, false, false)
    }

    /// Evaluate the JSON path against the given JSON value with variables.
    pub fn query_with_vars<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Vec<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, false, false, false)
    }

    /// Evaluate the JSON path against the given JSON value.
    pub fn query_first<'a, T: JsonRef<'a>>(&self, value: T) -> Result<Option<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), true, false, false)
            .map(|set| set.into_iter().next())
    }

    /// Evaluate the JSON path against the given JSON value with variables.
    pub fn query_first_with_vars<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Option<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, true, false, false)
            .map(|set| set.into_iter().next())
    }

    /// Checks whether the JSON path returns any item for the specified JSON value.
    pub fn exists<'a, T: JsonRef<'a>>(&self, value: T) -> Result<bool> {
        self.query_first(value).map(|v| v.is_some())
    }

    /// Checks whether the JSON path returns any item for the specified JSON value,
    /// with variables.
    pub fn exists_with_vars<'a, T: JsonRef<'a>>(&self, value: T, vars: T) -> Result<bool> {
        self.query_first_with_vars(value, vars).map(|v| v.is_some())
    }

    // ---- Citadel `_tz` entry points -----------------------------------

    pub fn query_tz<'a, T: JsonRef<'a>>(&self, value: T) -> Result<Vec<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), false, true, false)
    }

    pub fn query_with_vars_tz<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Vec<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, false, true, false)
    }

    pub fn query_first_tz<'a, T: JsonRef<'a>>(
        &self,
        value: T,
    ) -> Result<Option<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), true, true, false)
            .map(|set| set.into_iter().next())
    }

    pub fn query_first_with_vars_tz<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Option<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, true, true, false)
            .map(|set| set.into_iter().next())
    }

    pub fn exists_tz<'a, T: JsonRef<'a>>(&self, value: T) -> Result<bool> {
        self.query_first_tz(value).map(|v| v.is_some())
    }

    pub fn exists_with_vars_tz<'a, T: JsonRef<'a>>(&self, value: T, vars: T) -> Result<bool> {
        self.query_first_with_vars_tz(value, vars)
            .map(|v| v.is_some())
    }

    // ---- Citadel `_silent` entry points -------------------------------

    pub fn query_silent<'a, T: JsonRef<'a>>(&self, value: T) -> Result<Vec<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), false, false, true)
    }

    pub fn query_with_vars_silent<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Vec<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, false, false, true)
    }

    pub fn query_first_silent<'a, T: JsonRef<'a>>(
        &self,
        value: T,
    ) -> Result<Option<Cow<'a, T::Owned>>> {
        self.evaluate(value, T::null(), true, false, true)
            .map(|set| set.into_iter().next())
    }

    pub fn query_first_with_vars_silent<'a, T: JsonRef<'a>>(
        &self,
        value: T,
        vars: T,
    ) -> Result<Option<Cow<'a, T::Owned>>> {
        if !vars.is_object() {
            return Err(Error::VarsNotObject);
        }
        self.evaluate(value, vars, true, false, true)
            .map(|set| set.into_iter().next())
    }

    pub fn exists_silent<'a, T: JsonRef<'a>>(&self, value: T) -> Result<bool> {
        self.query_silent(value).map(|set| !set.is_empty())
    }

    pub fn exists_with_vars_silent<'a, T: JsonRef<'a>>(&self, value: T, vars: T) -> Result<bool> {
        self.query_with_vars_silent(value, vars)
            .map(|set| !set.is_empty())
    }
}

/// Evaluation context.
#[derive(Debug, Clone, Copy)]
struct Evaluator<'a, 'p, T: Json + 'a> {
    /// The current value referenced by `@`.
    current: Current<'a, T>,
    /// The root value referenced by `$`.
    root: T::Borrowed<'a>,
    /// The length of the innermost array referenced by `last`.
    array_len: Option<usize>,
    /// An object containing the variables referenced by `$var`.
    vars: T::Borrowed<'a>,
    /// The path mode.
    /// If the query is in lax mode, then errors are ignored and the result is empty or unknown.
    mode: Mode,
    /// Only return the first result.
    first: bool,
    use_tz: bool,
    /// Zone the `_tz` entry points resolve against. Held by reference so the evaluator
    /// stays `Copy`; `TimeZone` is not.
    session_tz: &'p jiff::tz::TimeZone,
    /// Transaction-start date in `session_tz`, used by time-to-timetz casts.
    session_date: jiff::civil::Date,
    object_ids: &'p std::cell::RefCell<ObjectIds>,
    silent: bool,
}

/// Unwrap the result or return an empty result if the evaluator is in lax mode.
macro_rules! lax {
    // for `Option`
    ($self:expr, $expr:expr, $err:expr) => {
        match $expr {
            Some(x) => x,
            None if $self.is_lax() => return Ok(vec![]),
            None => return Err($err),
        }
    };
    // for `Option`
    ($self:expr, $expr:expr, $err:expr; continue) => {
        match $expr {
            Some(x) => x,
            None if $self.is_lax() => continue,
            None => return Err($err),
        }
    };
    // for `Option`
    ($self:expr, $expr:expr, $err:expr; break) => {
        match $expr {
            Some(x) => x,
            None if $self.is_lax() => break,
            None => return Err($err),
        }
    };
    // for `Result` in predicate
    ($self:expr, $expr:expr) => {
        match $expr {
            Ok(x) => x,
            Err(e @ Error::NoVariable(_)) => return Err(e),
            Err(_) => return Ok(Truth::Unknown),
        }
    };
}

impl<'a, 'p, T: Json> Evaluator<'a, 'p, T> {
    /// Returns true if the evaluator is in lax mode.
    fn is_lax(&self) -> bool {
        matches!(self.mode, Mode::Lax)
    }

    /// Returns true if the path engine is permitted to stop evaluation early on the first success.
    fn is_first(&self) -> bool {
        self.first && self.is_lax()
    }

    /// Creates a new evaluator with the given current value.
    fn with_current<'b>(&self, current: Current<'b, T>) -> Evaluator<'b, 'p, T>
    where
        'a: 'b,
    {
        Evaluator {
            current,
            root: T::borrow(self.root),
            vars: T::borrow(self.vars),
            array_len: self.array_len,
            mode: self.mode,
            session_tz: self.session_tz,
            session_date: self.session_date,
            object_ids: self.object_ids,
            first: self.first,
            use_tz: self.use_tz,
            silent: self.silent,
        }
    }

    fn all(&self) -> Self {
        Evaluator {
            first: false,
            ..*self
        }
    }

    fn first(&self) -> Self {
        Evaluator {
            first: true,
            ..*self
        }
    }

    /// Returns the value of the given variable.
    fn get_variable(&self, name: &str) -> Result<T::Borrowed<'a>> {
        self.vars
            .as_object()
            // no `vars` input
            .ok_or_else(|| Error::NoVariable(name.into()))?
            .get(name)
            .ok_or_else(|| Error::NoVariable(name.into()))
    }

    /// Evaluates the expression or predicate.
    fn eval_expr_or_predicate(&self, expr: &ExprOrPredicate) -> Result<Vec<EvalItem<'a, T>>> {
        match expr {
            ExprOrPredicate::Expr(expr) => self.eval_expr(expr),
            ExprOrPredicate::Pred(pred) => self
                .eval_predicate(pred)
                .map(|t| vec![EvalItem::owned(t.to_json())]),
        }
    }

    /// Evaluates the predicate.
    fn eval_predicate(&self, pred: &Predicate) -> Result<Truth> {
        match pred {
            Predicate::Compare(op, left, right) => {
                let left = lax!(self, self.all().eval_expr(left));
                let right = lax!(self, self.all().eval_expr(right));

                let mut result = Truth::False;
                // The cross product of these SQL/JSON sequences is formed.
                // Each SQL/JSON item in one SQL/JSON sequence is compared to each item in the other SQL/JSON sequence.
                'product: for r in right.iter() {
                    for l in left.iter() {
                        let res = eval_compare::<T>(
                            *op,
                            l,
                            r,
                            self.use_tz,
                            self.session_tz,
                            self.session_date,
                        )?;
                        if res.is_unknown() && !self.is_lax() {
                            return Ok(Truth::Unknown);
                        }
                        result = result.merge(res);
                        if result.is_true() && self.is_lax() {
                            break 'product;
                        }
                    }
                }
                Ok(result)
            }
            Predicate::Exists(expr) => {
                let set = lax!(self, self.first().eval_expr(expr));
                // If the result of the path expression is an empty SQL/JSON sequence, then result is False.
                // Otherwise, result is True.
                Ok(Truth::from(!set.is_empty()))
            }
            Predicate::And(left, right) => {
                let left = self.eval_predicate(left)?;
                let right = self.eval_predicate(right)?;
                Ok(left.and(right))
            }
            Predicate::Or(left, right) => {
                let left = self.eval_predicate(left)?;
                let right = self.eval_predicate(right)?;
                Ok(left.or(right))
            }
            Predicate::Not(inner) => {
                let inner = self.eval_predicate(inner)?;
                Ok(inner.not())
            }
            Predicate::IsUnknown(inner) => {
                let inner = self.eval_predicate(inner)?;
                Ok(Truth::from(inner.is_unknown()))
            }
            Predicate::StartsWith(expr, prefix) => {
                let set = lax!(self, self.all().eval_expr(expr));
                let prefix = self.eval_value(prefix)?;
                let prefix = prefix.as_json().and_then(JsonRef::as_str).unwrap();
                let mut result = Truth::False;
                for v in set {
                    let res = match v.as_json().and_then(JsonRef::as_str) {
                        Some(s) => s.starts_with(prefix).into(),
                        None => Truth::Unknown,
                    };
                    if res.is_unknown() && !self.is_lax() {
                        return Ok(Truth::Unknown);
                    }
                    result = result.merge(res);
                    if result.is_true() && self.is_lax() {
                        break;
                    }
                }
                Ok(result)
            }
            Predicate::LikeRegex(expr, regex) => {
                let set = lax!(self, self.all().eval_expr(expr));
                let mut result = Truth::False;
                for v in set {
                    let res = match v.as_json().and_then(JsonRef::as_str) {
                        Some(s) => regex.is_match(s).into(),
                        None => Truth::Unknown,
                    };
                    if res.is_unknown() && !self.is_lax() {
                        return Ok(Truth::Unknown);
                    }
                    result = result.merge(res);
                    if result.is_true() && self.is_lax() {
                        break;
                    }
                }
                Ok(result)
            }
        }
    }

    /// Evaluates the expression.
    fn eval_expr(&self, expr: &Expr) -> Result<Vec<EvalItem<'a, T>>> {
        match expr {
            Expr::PathPrimary(primary) => self.eval_path_primary(primary),
            Expr::Accessor(base, op) => {
                let set = self.all().eval_expr(base)?;
                let mut new_set = vec![];
                for v in &set {
                    match v {
                        EvalItem::Json(Cow::Borrowed(value)) => new_set.extend(
                            self.with_current(Current::Json(*value))
                                .eval_accessor_op(op)?,
                        ),
                        EvalItem::Json(Cow::Owned(_)) | EvalItem::Datetime(_) => {
                            let set = self.with_current(v.as_current()).eval_accessor_op(op)?;
                            new_set.extend(set.into_iter().map(EvalItem::into_owned));
                        }
                    }
                    if self.is_first() && !new_set.is_empty() {
                        break;
                    }
                }
                Ok(new_set)
            }
            Expr::UnaryOp(op, expr) => {
                let set = self.eval_expr(expr)?;
                let mut new_set = Vec::with_capacity(set.len());
                let item_skip = self.silent && self.is_lax();
                'outer: for v in set {
                    let Some(v) = v.as_json() else {
                        if item_skip {
                            continue;
                        }
                        return Err(Error::UnaryOperandNotNumeric(*op));
                    };
                    if v.is_array() && self.is_lax() {
                        for v in v.as_array().unwrap().list() {
                            match eval_unary_op(*op, v) {
                                Ok(r) => new_set.push(EvalItem::owned(r)),
                                Err(_) if item_skip => break 'outer,
                                Err(e) => return Err(e),
                            }
                        }
                    } else {
                        match eval_unary_op(*op, v) {
                            Ok(r) => new_set.push(EvalItem::owned(r)),
                            Err(e) if item_skip && e.can_silent() => continue,
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(new_set)
            }
            Expr::BinaryOp(op, left, right) => {
                let left = self.eval_expr(left)?;
                let right = self.eval_expr(right)?;
                if left.len() != 1 {
                    return Err(Error::LeftOperandNotNumeric(*op));
                }
                if right.len() != 1 {
                    return Err(Error::RightOperandNotNumeric(*op));
                }
                // unwrap left if it is an array
                let left = left[0].as_json().ok_or(Error::LeftOperandNotNumeric(*op))?;
                let left = if self.is_lax() {
                    if let Some(array) = left.as_array() {
                        if array.len() != 1 {
                            return Err(Error::LeftOperandNotNumeric(*op));
                        }
                        array.get(0).unwrap()
                    } else {
                        left
                    }
                } else {
                    left
                };
                // unwrap right if it is an array
                let right = right[0]
                    .as_json()
                    .ok_or(Error::RightOperandNotNumeric(*op))?;
                let right = if self.is_lax() {
                    if let Some(array) = right.as_array() {
                        if array.len() != 1 {
                            return Err(Error::RightOperandNotNumeric(*op));
                        }
                        array.get(0).unwrap()
                    } else {
                        right
                    }
                } else {
                    right
                };
                Ok(vec![EvalItem::owned(eval_binary_op(*op, left, right)?)])
            }
        }
    }

    /// Evaluates the path primary.
    fn eval_path_primary(&self, primary: &PathPrimary) -> Result<Vec<EvalItem<'a, T>>> {
        match primary {
            PathPrimary::Root => Ok(vec![EvalItem::borrowed(self.root)]),
            PathPrimary::Current => Ok(vec![self.current.to_item()]),
            PathPrimary::Value(v) => Ok(vec![self.eval_value(v)?]),
            PathPrimary::Last => {
                let len = self
                    .array_len
                    .expect("LAST is allowed only in array subscripts");
                Ok(vec![EvalItem::owned(T::from_i64(len as i64 - 1))])
            }
            PathPrimary::ExprOrPred(expr) => self.eval_expr_or_predicate(expr),
        }
    }

    /// Evaluates the accessor operator.
    fn eval_accessor_op(&self, op: &AccessorOp) -> Result<Vec<EvalItem<'a, T>>> {
        match op {
            AccessorOp::MemberWildcard => self.eval_member_wildcard(),
            AccessorOp::DescendantMemberWildcard(levels) => {
                self.eval_descendant_member_wildcard(levels)
            }
            AccessorOp::ElementWildcard => self.eval_element_wildcard(),
            AccessorOp::Member(name) => self.eval_member(name),
            AccessorOp::Element(indices) => self.eval_element_accessor(indices),
            AccessorOp::FilterExpr(pred) => self.eval_filter_expr(pred),
            AccessorOp::Method(method) => self.eval_method(method),
        }
    }

    fn eval_member_wildcard(&self) -> Result<Vec<EvalItem<'a, T>>> {
        let current = lax!(self, self.current.as_json(), Error::WildcardMemberAccess);
        let set = match current.as_array() {
            Some(array) if self.is_lax() => array.list(),
            _ => vec![current],
        };
        let mut new_set = vec![];
        for v in set {
            let object = lax!(self, v.as_object(), Error::WildcardMemberAccess);
            for v in object.list_value() {
                new_set.push(EvalItem::borrowed(v));
            }
        }
        Ok(new_set)
    }

    fn eval_descendant_member_wildcard(&self, levels: &LevelRange) -> Result<Vec<EvalItem<'a, T>>> {
        let Some(current) = self.current.as_json() else {
            return Ok(if levels.to_range(0).contains(&0) {
                vec![self.current.to_item()]
            } else {
                vec![]
            });
        };
        let mut set = match current.as_array() {
            Some(array) if self.is_lax() => array.list(),
            _ => vec![current],
        };
        // expand all levels
        // level i is set[level_start[i] .. level_start[i+1]]
        let mut level_start = vec![0, set.len()];
        for l in 1..=levels.end() {
            let last_level_range = level_start[l as usize - 1]..level_start[l as usize];
            for i in last_level_range {
                if let Some(object) = set[i].as_object() {
                    set.extend(object.list_value());
                }
            }
            if set.len() == level_start[l as usize] {
                // this level is empty
                break;
            }
            level_start.push(set.len());
        }
        // return the set in level range
        let last_level = level_start.len() - 2;
        let level_range = levels.to_range(last_level);
        let set_range = level_start[level_range.start]..level_start[level_range.end];
        let new_set = set[set_range]
            .iter()
            .cloned()
            .map(EvalItem::borrowed)
            .collect();
        Ok(new_set)
    }

    fn eval_element_wildcard(&self) -> Result<Vec<EvalItem<'a, T>>> {
        let current = self.current.as_json();
        if current.is_none_or(|value| !value.is_array()) && self.is_lax() {
            // wrap the current value into an array
            return Ok(vec![self.current.to_item()]);
        }
        let array = lax!(
            self,
            current.and_then(JsonRef::as_array),
            Error::WildcardArrayAccess
        );
        if self.is_first() && !self.silent {
            return Ok(array.get(0).map(EvalItem::borrowed).into_iter().collect());
        }
        Ok(array.list().into_iter().map(EvalItem::borrowed).collect())
    }

    /// Evaluates the member accessor.
    fn eval_member(&self, name: &str) -> Result<Vec<EvalItem<'a, T>>> {
        let current = lax!(self, self.current.as_json(), Error::MemberAccess);
        let set = match current.as_array() {
            Some(array) if self.is_lax() => array.list(),
            _ => vec![current],
        };
        let mut new_set = vec![];
        for v in set {
            let object = match v.as_object() {
                Some(o) => o,
                None if self.is_lax() => return Ok(vec![]),
                None => return Err(Error::MemberAccess),
            };
            let elem = match object.get(name) {
                Some(e) => e,
                None if self.silent && self.first => continue,
                None if self.is_lax() => return Ok(vec![]),
                None => return Err(Error::NoKey(name.into())),
            };
            new_set.push(EvalItem::borrowed(elem));
        }
        Ok(new_set)
    }

    /// Evaluates the element accessor.
    fn eval_element_accessor(&self, indices: &[ArrayIndex]) -> Result<Vec<EvalItem<'a, T>>> {
        // wrap the scalar value into an array in lax mode
        enum ArrayOrScalar<'a, T: Json + 'a> {
            Array(<T::Borrowed<'a> as JsonRef<'a>>::Array),
            Scalar(Current<'a, T>),
        }
        impl<'a, T: Json> ArrayOrScalar<'a, T> {
            fn len(&self) -> usize {
                match self {
                    Self::Array(array) => array.len(),
                    Self::Scalar(_) => 1,
                }
            }

            fn get(&self, index: usize) -> Option<EvalItem<'a, T>> {
                match self {
                    Self::Array(array) => array.get(index).map(EvalItem::borrowed),
                    Self::Scalar(scalar) if index == 0 => Some(scalar.to_item()),
                    _ => None,
                }
            }
        }
        let array = match self.current.as_json().and_then(JsonRef::as_array) {
            Some(array) => ArrayOrScalar::Array(array),
            None if self.is_lax() => ArrayOrScalar::Scalar(self.current),
            None => return Err(Error::ArrayAccess),
        };
        let mut elems = Vec::with_capacity(indices.len());
        for index in indices {
            let eval_index = |expr: &Expr| {
                // errors in this closure can not be ignored
                let set = Self {
                    // update `array` context
                    array_len: Some(array.len()),
                    ..*self
                }
                .eval_expr(expr)?;
                if set.len() != 1 {
                    return Err(Error::ArrayIndexNotNumeric);
                }
                let number = set[0]
                    .as_json()
                    .ok_or(Error::ArrayIndexNotNumeric)?
                    .as_number()
                    .ok_or(Error::ArrayIndexNotNumeric)?;
                crate::numeric::trunc_to_i32_exact(&number.to_string())
                    .map(i64::from)
                    .ok_or(Error::ArrayIndexOutOfRange)
            };
            match index {
                ArrayIndex::Index(expr) => {
                    let index = eval_index(expr)?;
                    let index =
                        lax!(self, index.try_into().ok(), Error::ArrayIndexOutOfBounds; continue);
                    let elem = lax!(self, array.get(index), Error::ArrayIndexOutOfBounds; continue);
                    elems.push(elem);
                }
                ArrayIndex::Slice(begin, end) => {
                    let begin = eval_index(begin)?;
                    let end = eval_index(end)?;
                    let begin: usize = match begin.try_into() {
                        Ok(i) => i,
                        Err(_) if self.is_lax() => 0,
                        Err(_) => return Err(Error::ArrayIndexOutOfBounds),
                    };
                    let end: usize =
                        lax!(self, end.try_into().ok(), Error::ArrayIndexOutOfBounds; continue);
                    if begin > end && !self.is_lax() {
                        return Err(Error::ArrayIndexOutOfBounds);
                    }
                    for i in begin..=end {
                        let elem = lax!(self, array.get(i), Error::ArrayIndexOutOfBounds; break);
                        elems.push(elem);
                    }
                }
            }
        }
        Ok(elems)
    }

    fn eval_filter_expr(&self, pred: &Predicate) -> Result<Vec<EvalItem<'a, T>>> {
        let set = match self.current.as_json().and_then(JsonRef::as_array) {
            Some(array) if self.is_lax() => array.list().into_iter().map(Current::Json).collect(),
            _ => vec![self.current],
        };
        let mut new_set = vec![];
        for v in set {
            if self.with_current(v).eval_predicate(pred)?.is_true() {
                new_set.push(v.to_item());
                if self.is_first() {
                    break;
                }
            }
        }
        Ok(new_set)
    }

    /// Evaluates the item method.
    fn eval_method(&self, method: &Method) -> Result<Vec<EvalItem<'a, T>>> {
        // unwrap the current value if it is an array
        if let Some(array) = self
            .current
            .as_json()
            .and_then(JsonRef::as_array)
            .filter(|_| self.is_lax() && !matches!(method, Method::Size | Method::Type))
        {
            let mut new_set = vec![];
            for v in array.list() {
                new_set.extend(self.with_current(Current::Json(v)).eval_method(method)?);
            }
            return Ok(new_set);
        }
        match method {
            Method::Type => self.eval_method_type().map(|v| vec![v]),
            Method::Size => self.eval_method_size().map(|v| vec![v]),
            Method::Double => self.eval_method_double().map(|v| vec![v]),
            Method::Ceiling => self.eval_method_ceiling().map(|v| vec![v]),
            Method::Floor => self.eval_method_floor().map(|v| vec![v]),
            Method::Abs => self.eval_method_abs().map(|v| vec![v]),
            Method::Keyvalue => self.eval_method_keyvalue(),
            Method::Datetime { template } => self
                .eval_method_datetime(template.as_deref())
                .map(|v| vec![v]),
            Method::Bigint => self.eval_method_integer::<i64>("bigint").map(|v| vec![v]),
            Method::Integer => self.eval_method_integer::<i32>("integer").map(|v| vec![v]),
            Method::Number => self
                .eval_method_numeric("number", None, None)
                .map(|v| vec![v]),
            Method::Decimal { precision, scale } => self
                .eval_method_numeric("decimal", *precision, *scale)
                .map(|v| vec![v]),
            Method::String => self.eval_method_string().map(|v| vec![v]),
            Method::Boolean => self.eval_method_boolean().map(|v| vec![v]),
            Method::Date => self
                .eval_method_datetime_kind("date", DatetimeKind::Date, None)
                .map(|v| vec![v]),
            Method::Time { precision } => self
                .eval_method_datetime_kind("time", DatetimeKind::Time, *precision)
                .map(|v| vec![v]),
            Method::TimeTz { precision } => self
                .eval_method_datetime_kind("time_tz", DatetimeKind::TimeTz, *precision)
                .map(|v| vec![v]),
            Method::Timestamp { precision } => self
                .eval_method_datetime_kind("timestamp", DatetimeKind::Timestamp, *precision)
                .map(|v| vec![v]),
            Method::TimestampTz { precision } => self
                .eval_method_datetime_kind("timestamp_tz", DatetimeKind::TimestampTz, *precision)
                .map(|v| vec![v]),
        }
    }

    fn eval_method_datetime(&self, template: Option<&str>) -> Result<EvalItem<'a, T>> {
        let input = self
            .current
            .as_json()
            .and_then(JsonRef::as_str)
            .ok_or(Error::DatetimeNotString)?;
        let parsed = match template {
            None => crate::datetime::iso::try_13_formats(input)?,
            Some(t) => crate::datetime::template::parse_apply(input, t)?,
        };
        Ok(EvalItem::Datetime(parsed))
    }

    /// `.date()` / `.time()` / `.time_tz()` / `.timestamp()` / `.timestamp_tz()`.
    ///
    /// Parses with the same ISO list as `.datetime()`, then resolves the parsed kind
    /// against the requested one per PostgreSQL's `executeDateTimeMethod` switch.
    fn eval_method_datetime_kind(
        &self,
        method: &'static str,
        target: crate::datetime::DatetimeKind,
        precision: Option<i64>,
    ) -> Result<EvalItem<'a, T>> {
        let input = self
            .current
            .as_json()
            .and_then(JsonRef::as_str)
            .ok_or(Error::DatetimeMethodNotString(method))?;
        let precision = match precision {
            None => None,
            Some(p) => {
                // numeric_int4_opt_error runs before the clamp, so a value outside int4
                // fails here instead of saturating to the maximum precision.
                let p = i32::try_from(p).map_err(|_| Error::TimePrecisionOutOfRange(method))?;
                // PostgreSQL clamps an in-range but over-large precision with a
                // warning; there is no warning channel here, so clamp silently.
                Some(p.clamp(0, 6) as u8)
            }
        };
        let parsed = crate::datetime::iso::try_13_formats(input)
            .map_err(|_| Error::FormatNotRecognized(method, input.into()))?;
        let cast = crate::datetime::iso::cast_kind(
            parsed,
            target,
            input,
            method,
            self.use_tz,
            self.session_tz,
            self.session_date,
        )?;
        let rounded = match precision {
            None => cast,
            Some(p) => crate::datetime::iso::round_fractional(cast, p)?,
        };
        Ok(EvalItem::Datetime(rounded))
    }

    /// `.bigint()` / `.integer()`.
    ///
    /// Numbers round; strings must be an exact integer. PostgreSQL routes the two
    /// through different C functions, so the JSON type decides the answer: the number
    /// `1.23` yields `1`, while the string `"1.23"` is an error.
    fn eval_method_integer<I>(&self, method: &'static str) -> Result<EvalItem<'a, T>>
    where
        I: TryFrom<i64> + Into<i64>,
    {
        let current = self.current.as_json();
        if let Some(s) = current.and_then(JsonRef::as_str) {
            let parsed = if method == "bigint" {
                crate::numeric::parse_pg_i64(s)
            } else {
                crate::numeric::parse_pg_i32(s).map(i64::from)
            };
            let value = parsed
                .and_then(|value| I::try_from(value).ok())
                .ok_or_else(|| Error::InvalidConversion(s.into(), method, method))?;
            Ok(EvalItem::owned(T::from_i64(value.into())))
        } else if let Some(n) = current.and_then(JsonRef::as_number) {
            let shown = shown_number(&n);
            let value = crate::numeric::round_to_i64_exact(&n.to_string())
                .and_then(|v| I::try_from(v).ok())
                .ok_or_else(|| Error::InvalidConversion(shown.into(), method, method))?;
            Ok(EvalItem::owned(T::from_i64(value.into())))
        } else {
            Err(Error::NumericConversionType(method))
        }
    }

    /// `.number()` and `.decimal([precision [, scale]])`.
    ///
    /// Both canonicalise first, as PostgreSQL does with `numeric_out`; only then is a
    /// typmod applied. Going through `f64` would lose exact integers wider than 53 bits.
    fn eval_method_numeric(
        &self,
        method: &'static str,
        precision: Option<i64>,
        scale: Option<i64>,
    ) -> Result<EvalItem<'a, T>> {
        let current = self.current.as_json();
        let canonical = if let Some(n) = current.and_then(JsonRef::as_number) {
            let raw = n.to_string();
            crate::numeric::canonical(&raw)
                .ok_or_else(|| Error::InvalidConversion(raw.into(), method, "numeric"))?
        } else if let Some(s) = current.and_then(JsonRef::as_str) {
            if crate::numeric::is_numeric_nan_or_inf(s) {
                return Err(Error::NanOrInfinity(method));
            }
            crate::numeric::canonical(s)
                .ok_or_else(|| Error::InvalidConversion(s.into(), method, "numeric"))?
        } else {
            return Err(Error::NumericConversionType(method));
        };

        let Some(precision) = precision else {
            return numeric_value(&canonical, method);
        };
        // The argument is carried as i64 so an out-of-range value reaches evaluation and
        // reports its own error, as PostgreSQL does via numeric_int4_opt_error.
        let precision = i32::try_from(precision).map_err(|_| Error::DecimalPrecisionOutOfRange)?;
        let scale = i32::try_from(scale.unwrap_or(0)).map_err(|_| Error::DecimalScaleOutOfRange)?;
        if !(1..=1000).contains(&precision) {
            return Err(Error::NumericPrecisionOutOfBounds(i64::from(precision)));
        }
        if !(-1000..=1000).contains(&scale) {
            return Err(Error::NumericScaleOutOfBounds(i64::from(scale)));
        }
        let applied = crate::numeric::apply_typmod(&canonical, precision, scale)
            .ok_or_else(|| Error::InvalidConversion(canonical.into(), method, "numeric"))?;
        numeric_value(&applied, method)
    }

    /// `.boolean()`.
    ///
    /// Unlike `.bigint()`/`.integer()` this does not round: PostgreSQL converts through
    /// `int4in`, so `1.23` is an error rather than `1`.
    fn eval_method_boolean(&self) -> Result<EvalItem<'a, T>> {
        let current = self.current.as_json();
        let value = if let Some(b) = current.and_then(JsonRef::as_bool) {
            b
        } else if let Some(n) = current.and_then(JsonRef::as_number) {
            let shown = shown_number(&n);
            let as_int = crate::numeric::numeric_to_i32_exact(&n.to_string())
                .ok_or_else(|| Error::InvalidConversion(shown.into(), "boolean", "boolean"))?;
            as_int != 0
        } else if let Some(s) = current.and_then(JsonRef::as_str) {
            crate::numeric::parse_pg_bool(s)
                .ok_or_else(|| Error::InvalidConversion(s.into(), "boolean", "boolean"))?
        } else {
            return Err(Error::BooleanTypeError);
        };
        Ok(EvalItem::owned(T::bool(value)))
    }

    /// `.string()`.
    fn eval_method_string(&self) -> Result<EvalItem<'a, T>> {
        if let Some(datetime) = self.current.as_datetime() {
            return Ok(EvalItem::owned(T::from_string(&datetime.iso)));
        }
        let current = self.current.as_json().expect("datetime handled above");
        if current.is_string() {
            return Ok(EvalItem::borrowed(current));
        }
        if let Some(n) = current.as_number() {
            return Ok(EvalItem::owned(T::from_string(&shown_number(&n))));
        }
        if let Some(b) = current.as_bool() {
            return Ok(EvalItem::owned(T::from_string(if b {
                "true"
            } else {
                "false"
            })));
        }
        Err(Error::StringTypeError)
    }

    fn eval_method_type(&self) -> Result<EvalItem<'a, T>> {
        if let Some(datetime) = self.current.as_datetime() {
            return Ok(EvalItem::owned(T::from_string(datetime.kind.as_str())));
        }
        let current = self.current.as_json().expect("datetime handled above");
        let s = if current.is_null() {
            "null"
        } else if current.is_bool() {
            "boolean"
        } else if current.is_number() {
            "number"
        } else if current.is_string() {
            "string"
        } else if current.is_array() {
            "array"
        } else if current.is_object() {
            "object"
        } else {
            unreachable!()
        };
        Ok(EvalItem::owned(T::from_string(s)))
    }

    fn eval_method_size(&self) -> Result<EvalItem<'a, T>> {
        let size = if let Some(array) = self.current.as_json().and_then(JsonRef::as_array) {
            // The size of an SQL/JSON array is the number of elements in the array.
            array.len()
        } else if self.is_lax() {
            // The size of an SQL/JSON object or a scalar is 1.
            1
        } else {
            return Err(Error::SizeNotArray);
        };
        Ok(EvalItem::owned(T::from_u64(size as u64)))
    }

    /// PostgreSQL `jpiDouble`. Strings pass through `float8in` and then
    /// `float8_numeric`, while numeric inputs are only checked for float8 range.
    fn eval_method_double(&self) -> Result<EvalItem<'a, T>> {
        let current = self.current.as_json();
        if let Some(s) = current.and_then(JsonRef::as_str) {
            if crate::numeric::is_nan_or_inf(s) {
                return Err(Error::NanOrInfinity("double"));
            }
            let value = crate::numeric::parse_pg_finite_float8(s)
                .ok_or_else(|| Error::InvalidConversion(s.into(), "double", "double precision"))?;
            let canonical = crate::numeric::pg_float8_to_numeric(value)
                .expect("a finite float8 always converts to numeric");
            numeric_value(&canonical, "double")
        } else if let Some(n) = current.and_then(JsonRef::as_number) {
            let raw = n.to_string();
            let canonical = crate::numeric::canonical(&raw).ok_or_else(|| {
                Error::InvalidConversion(raw.into(), "double", "double precision")
            })?;
            crate::numeric::parse_pg_finite_float8(&canonical).ok_or_else(|| {
                Error::InvalidConversion(canonical.into(), "double", "double precision")
            })?;
            Ok(EvalItem::borrowed(
                self.current.as_json().expect("number checked above"),
            ))
        } else {
            Err(Error::NumericConversionType("double"))
        }
    }

    fn eval_method_ceiling(&self) -> Result<EvalItem<'a, T>> {
        let n = self
            .current
            .as_json()
            .and_then(JsonRef::as_number)
            .ok_or(Error::MethodNotNumeric("ceiling"))?;
        Ok(EvalItem::owned(T::from_number(n.ceil()?)))
    }

    fn eval_method_floor(&self) -> Result<EvalItem<'a, T>> {
        let n = self
            .current
            .as_json()
            .and_then(JsonRef::as_number)
            .ok_or(Error::MethodNotNumeric("floor"))?;
        Ok(EvalItem::owned(T::from_number(n.floor()?)))
    }

    fn eval_method_abs(&self) -> Result<EvalItem<'a, T>> {
        let n = self
            .current
            .as_json()
            .and_then(JsonRef::as_number)
            .ok_or(Error::MethodNotNumeric("abs"))?;
        Ok(EvalItem::owned(T::from_number(n.abs()?)))
    }

    fn eval_method_keyvalue(&self) -> Result<Vec<EvalItem<'a, T>>> {
        let object = self
            .current
            .as_json()
            .and_then(JsonRef::as_object)
            .ok_or(Error::KeyValueNotObject)?;
        let id = self.object_ids.borrow_mut().id_for(object.identity());
        let entries: Vec<_> = object.list();
        Ok(entries
            .into_iter()
            .map(|(k, v)| {
                EvalItem::owned(T::object([
                    ("key", T::from_string(k)),
                    ("value", v.to_owned()),
                    ("id", T::from_i64(id)),
                ]))
            })
            .collect())
    }

    /// Evaluates the scalar value.
    fn eval_value(&self, value: &Value) -> Result<EvalItem<'a, T>> {
        Ok(match value {
            Value::Null => EvalItem::owned(T::null()),
            Value::Boolean(b) => EvalItem::owned(T::bool(*b)),
            Value::Number(n) => EvalItem::owned(T::from_number(n.clone())),
            Value::String(s) => EvalItem::owned(T::from_string(s)),
            Value::Variable(v) => EvalItem::borrowed(self.get_variable(v)?),
        })
    }
}

/// Compare two values.
///
/// Return unknown if the values are not comparable.
fn eval_compare<T: Json>(
    op: CompareOp,
    left: &EvalItem<'_, T>,
    right: &EvalItem<'_, T>,
    use_tz: bool,
    session_tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Result<Truth> {
    use CompareOp::*;
    let left_datetime = match left {
        EvalItem::Datetime(value) => Some(value),
        EvalItem::Json(_) => None,
    };
    let right_datetime = match right {
        EvalItem::Datetime(value) => Some(value),
        EvalItem::Json(_) => None,
    };
    if left_datetime.is_some() || right_datetime.is_some() {
        return eval_compare_datetime(
            op,
            left_datetime,
            right_datetime,
            use_tz,
            session_tz,
            session_date,
        );
    }
    let left = left.as_json().expect("datetime handled above");
    let right = right.as_json().expect("datetime handled above");
    // arrays and objects are not comparable
    if left.is_array() || left.is_object() || right.is_array() || right.is_object() {
        return Ok(Truth::Unknown);
    }
    if left.is_null() && right.is_null() {
        return Ok(compare_ord(op, (), ()).into());
    }
    if left.is_null() || right.is_null() {
        return Ok((op == CompareOp::Ne).into());
    }
    if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        return Ok(compare_ord(op, left, right).into());
    }
    if let (Some(left), Some(right)) = (left.as_number(), right.as_number()) {
        return Ok(match op {
            Eq => left.equal(&right)?,
            Ne => !left.equal(&right)?,
            Gt => right.less_than(&left)?,
            Ge => !left.less_than(&right)?,
            Lt => left.less_than(&right)?,
            Le => !right.less_than(&left)?,
        }
        .into());
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(compare_ord(op, left, right).into());
    }
    Ok(Truth::Unknown)
}

fn eval_compare_datetime(
    op: CompareOp,
    left: Option<&ParsedDatetime>,
    right: Option<&ParsedDatetime>,
    use_tz: bool,
    session_tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Result<Truth> {
    use crate::datetime::DatetimeKind as K;
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(Truth::Unknown);
    };
    let (l_iso, l_kind) = (left.iso.as_str(), left.kind);
    let (r_iso, r_kind) = (right.iso.as_str(), right.kind);
    let needs_tz = match (l_kind, r_kind) {
        (a, b) if a == b => false,
        (K::Date, K::Timestamp) | (K::Timestamp, K::Date) => false,
        (K::TimestampTz, _) | (_, K::TimestampTz) => true,
        (K::TimeTz, _) | (_, K::TimeTz) => true,
        (K::Date, K::Time) | (K::Time, K::Date) => return Ok(Truth::Unknown),
        (K::Timestamp, K::Time) | (K::Time, K::Timestamp) => return Ok(Truth::Unknown),
        _ => return Ok(Truth::Unknown),
    };
    if needs_tz && !use_tz {
        let (from, target_kind) = if matches!(l_kind, K::TimestampTz | K::TimeTz) {
            (r_kind, l_kind)
        } else {
            (l_kind, r_kind)
        };
        let has_date = matches!(l_kind, K::Date | K::Timestamp | K::TimestampTz)
            || matches!(r_kind, K::Date | K::Timestamp | K::TimestampTz);
        let to = if has_date {
            "timestamptz"
        } else {
            target_kind.as_tag()
        };
        return Err(Error::DatetimeConvertWithoutTz(
            from.as_tag().into(),
            to.into(),
        ));
    }
    let ord = match compare_datetime_kinds(
        l_iso,
        l_kind,
        r_iso,
        r_kind,
        use_tz,
        session_tz,
        session_date,
    ) {
        Some(o) => o,
        None => return Ok(Truth::Unknown),
    };
    use CompareOp::*;
    Ok(match op {
        Eq => ord.is_eq(),
        Ne => !ord.is_eq(),
        Gt => ord.is_gt(),
        Ge => !ord.is_lt(),
        Lt => ord.is_lt(),
        Le => !ord.is_gt(),
    }
    .into())
}

fn compare_datetime_kinds(
    l_iso: &str,
    l_kind: crate::datetime::DatetimeKind,
    r_iso: &str,
    r_kind: crate::datetime::DatetimeKind,
    use_tz: bool,
    session_tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Option<std::cmp::Ordering> {
    use crate::datetime::DatetimeKind as K;
    if l_kind == r_kind {
        match l_kind {
            K::Date | K::Timestamp => return cmp_date_or_ts(l_iso, l_kind, r_iso, r_kind),
            K::Time => return Some(l_iso.cmp(r_iso)),
            K::TimestampTz => {
                let l_inst = to_instant(l_iso, l_kind, session_tz)?;
                let r_inst = to_instant(r_iso, r_kind, session_tz)?;
                return Some(l_inst.cmp(&r_inst));
            }
            K::TimeTz => {
                return cmp_timetz_pair(l_iso, l_kind, r_iso, r_kind, session_tz, session_date)
            }
        }
    }
    if matches!(
        (l_kind, r_kind),
        (K::Date, K::Timestamp) | (K::Timestamp, K::Date)
    ) {
        return cmp_date_or_ts(l_iso, l_kind, r_iso, r_kind);
    }
    let l_is_time = matches!(l_kind, K::Time | K::TimeTz);
    let r_is_time = matches!(r_kind, K::Time | K::TimeTz);
    let l_is_dated = matches!(l_kind, K::Date | K::Timestamp | K::TimestampTz);
    let r_is_dated = matches!(r_kind, K::Date | K::Timestamp | K::TimestampTz);
    if (l_is_time && r_is_dated) || (l_is_dated && r_is_time) {
        return None;
    }
    // Time <-> TimeTz: PG casts the Time to TimeTz at session TZ then
    // compares as TimeTz (primary by UTC instant, tiebreak by offset).
    if matches!(
        (l_kind, r_kind),
        (K::Time, K::TimeTz) | (K::TimeTz, K::Time) | (K::TimeTz, K::TimeTz) | (K::Time, K::Time)
    ) {
        if !use_tz
            && matches!(
                (l_kind, r_kind),
                (K::Time, K::TimeTz) | (K::TimeTz, K::Time)
            )
        {
            return None;
        }
        return cmp_timetz_pair(l_iso, l_kind, r_iso, r_kind, session_tz, session_date);
    }
    // Date/Timestamp/TimestampTz cross-comparisons: same general rule.
    if !use_tz {
        return None;
    }
    let l_inst = to_instant(l_iso, l_kind, session_tz)?;
    let r_inst = to_instant(r_iso, r_kind, session_tz)?;
    Some(l_inst.cmp(&r_inst))
}

/// Render a number for an error message the way PostgreSQL does, i.e. after
/// `numeric_out`, so `1e1000` is quoted expanded rather than as the source literal.
fn shown_number(n: &Number) -> String {
    let raw = n.to_string();
    crate::numeric::canonical(&raw).unwrap_or(raw)
}

/// Rebuild a JSON number from a canonical decimal string, preserving every digit.
fn numeric_value<'a, T: Json>(value: &str, method: &'static str) -> Result<EvalItem<'a, T>> {
    let n: Number = serde_json::from_str(value)
        .map_err(|_| Error::InvalidConversion(value.into(), method, "numeric"))?;
    Ok(EvalItem::owned(T::from_number(n)))
}

fn cmp_date_or_ts(
    l_iso: &str,
    l_kind: crate::datetime::DatetimeKind,
    r_iso: &str,
    r_kind: crate::datetime::DatetimeKind,
) -> Option<std::cmp::Ordering> {
    use crate::datetime::DatetimeKind as K;
    let (l_date, l_time) = match l_kind {
        K::Date => (crate::datetime::pg::Date::parse(l_iso)?, 0),
        K::Timestamp => {
            let value = crate::datetime::pg::DateTime::parse(l_iso)?;
            (value.date, value.micros_of_day)
        }
        _ => return None,
    };
    let (r_date, r_time) = match r_kind {
        K::Date => (crate::datetime::pg::Date::parse(r_iso)?, 0),
        K::Timestamp => {
            let value = crate::datetime::pg::DateTime::parse(r_iso)?;
            (value.date, value.micros_of_day)
        }
        _ => return None,
    };
    Some((l_date, l_time).cmp(&(r_date, r_time)))
}

fn cmp_timetz_pair(
    l_iso: &str,
    l_kind: crate::datetime::DatetimeKind,
    r_iso: &str,
    r_kind: crate::datetime::DatetimeKind,
    session_tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Option<std::cmp::Ordering> {
    let (l_wall, l_off) = parse_time_pair(l_iso, l_kind, session_tz, session_date)?;
    let (r_wall, r_off) = parse_time_pair(r_iso, r_kind, session_tz, session_date)?;
    let l_utc = l_wall - l_off;
    let r_utc = r_wall - r_off;
    let ord = l_utc.cmp(&r_utc);
    if ord != std::cmp::Ordering::Equal {
        return Some(ord);
    }
    Some(r_off.cmp(&l_off))
}

fn parse_time_pair(
    iso: &str,
    kind: crate::datetime::DatetimeKind,
    session_tz: &jiff::tz::TimeZone,
    session_date: jiff::civil::Date,
) -> Option<(i64, i64)> {
    use crate::datetime::DatetimeKind as K;
    match kind {
        K::Time => {
            let wall = crate::datetime::iso::parse_pg_time_nanos(iso)?;
            let dt = datetime_for_time(session_date, wall)?;
            let offset = crate::datetime::iso::resolve_pg_offset(session_tz, dt).seconds();
            Some((wall, i64::from(offset) * NANOS_PER_SECOND))
        }
        K::TimeTz => {
            let (time_part, off_part) = crate::datetime::iso::split_iso_offset(iso)?;
            let wall = crate::datetime::iso::parse_pg_time_nanos(time_part)?;
            let off = i64::from(parse_offset(off_part)?.seconds()) * NANOS_PER_SECOND;
            Some((wall, off))
        }
        _ => None,
    }
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

fn datetime_for_time(date: jiff::civil::Date, nanos: i64) -> Option<jiff::civil::DateTime> {
    let (date, nanos) = if nanos == 86_400 * NANOS_PER_SECOND {
        (date.tomorrow().ok()?, 0)
    } else {
        (date, nanos)
    };
    let hour = nanos / (3_600 * NANOS_PER_SECOND);
    let minute = (nanos / (60 * NANOS_PER_SECOND)) % 60;
    let second = (nanos / NANOS_PER_SECOND) % 60;
    let subsecond = nanos % NANOS_PER_SECOND;
    Some(date.at(
        i8::try_from(hour).ok()?,
        i8::try_from(minute).ok()?,
        i8::try_from(second).ok()?,
        i32::try_from(subsecond).ok()?,
    ))
}

fn to_instant(
    iso: &str,
    kind: crate::datetime::DatetimeKind,
    session_tz: &jiff::tz::TimeZone,
) -> Option<i128> {
    use crate::datetime::DatetimeKind as K;
    match kind {
        K::Date => crate::datetime::pg::wide_date_instant(iso, session_tz),
        K::Timestamp => crate::datetime::pg::local_instant(iso, session_tz).map(i128::from),
        K::TimestampTz => {
            let (dt_part, off_part) = crate::datetime::iso::split_iso_offset(iso)?;
            let off = parse_offset(off_part)?;
            crate::datetime::pg::fixed_offset_instant(dt_part, off.seconds()).map(i128::from)
        }
        K::Time | K::TimeTz => None,
    }
}

fn parse_offset(s: &str) -> Option<jiff::tz::Offset> {
    crate::datetime::iso::parse_iso_offset(s)
}

/// Evaluate the unary operator.
fn eval_unary_op<T: Json>(op: UnaryOp, value: T::Borrowed<'_>) -> Result<T> {
    let n = value.as_number().ok_or(Error::UnaryOperandNotNumeric(op))?;
    Ok(match op {
        UnaryOp::Plus => value.to_owned(),
        UnaryOp::Minus => T::from_number(n.neg()?),
    })
}

/// Evaluate the binary operator.
fn eval_binary_op<T: Json>(
    op: BinaryOp,
    left: T::Borrowed<'_>,
    right: T::Borrowed<'_>,
) -> Result<T> {
    let left = left.as_number().ok_or(Error::LeftOperandNotNumeric(op))?;
    let right = right.as_number().ok_or(Error::RightOperandNotNumeric(op))?;
    Ok(T::from_number(match op {
        BinaryOp::Add => left.add(&right)?,
        BinaryOp::Sub => left.sub(&right)?,
        BinaryOp::Mul => left.mul(&right)?,
        BinaryOp::Div => left.div(&right)?,
        BinaryOp::Rem => left.rem(&right)?,
    }))
}

/// Compare two values that implement `Ord`.
fn compare_ord<T: Ord>(op: CompareOp, left: T, right: T) -> bool {
    use CompareOp::*;
    match op {
        Eq => left == right,
        Ne => left != right,
        Gt => left > right,
        Ge => left >= right,
        Lt => left < right,
        Le => left <= right,
    }
}

/// Extension methods for `Number`.
pub trait NumberExt: Sized {
    fn equal(&self, other: &Self) -> Result<bool>;
    fn less_than(&self, other: &Self) -> Result<bool>;
    fn neg(&self) -> Result<Self>;
    fn add(&self, other: &Self) -> Result<Self>;
    fn sub(&self, other: &Self) -> Result<Self>;
    fn mul(&self, other: &Self) -> Result<Self>;
    fn div(&self, other: &Self) -> Result<Self>;
    fn rem(&self, other: &Self) -> Result<Self>;
    fn ceil(&self) -> Result<Self>;
    fn floor(&self) -> Result<Self>;
    fn abs(&self) -> Result<Self>;
}

impl NumberExt for Number {
    fn equal(&self, other: &Self) -> Result<bool> {
        Ok(exact_numeric_order(self, other)? == std::cmp::Ordering::Equal)
    }

    fn less_than(&self, other: &Self) -> Result<bool> {
        Ok(exact_numeric_order(self, other)? == std::cmp::Ordering::Less)
    }

    fn neg(&self) -> Result<Self> {
        exact_number(crate::numeric::neg_exact(&self.to_string()))
    }

    fn add(&self, other: &Self) -> Result<Self> {
        exact_number(crate::numeric::add_exact(
            &self.to_string(),
            &other.to_string(),
        ))
    }

    fn sub(&self, other: &Self) -> Result<Self> {
        exact_number(crate::numeric::sub_exact(
            &self.to_string(),
            &other.to_string(),
        ))
    }

    fn mul(&self, other: &Self) -> Result<Self> {
        exact_number(crate::numeric::mul_exact(
            &self.to_string(),
            &other.to_string(),
        ))
    }

    fn div(&self, other: &Self) -> Result<Self> {
        exact_number(crate::numeric::div_exact(
            &self.to_string(),
            &other.to_string(),
        ))
    }

    fn rem(&self, other: &Self) -> Result<Self> {
        exact_number(crate::numeric::rem_exact(
            &self.to_string(),
            &other.to_string(),
        ))
    }

    fn ceil(&self) -> Result<Self> {
        exact_number(crate::numeric::ceil_exact(&self.to_string()))
    }

    fn floor(&self) -> Result<Self> {
        exact_number(crate::numeric::floor_exact(&self.to_string()))
    }

    fn abs(&self) -> Result<Self> {
        exact_number(crate::numeric::abs_exact(&self.to_string()))
    }
}

fn exact_numeric_order(left: &Number, right: &Number) -> Result<std::cmp::Ordering> {
    crate::numeric::compare_exact(&left.to_string(), &right.to_string())
        .ok_or(Error::NumericOverflow)
}

fn exact_number(
    value: std::result::Result<String, crate::numeric::NumericArithmeticError>,
) -> Result<Number> {
    let value = value.map_err(|error| match error {
        crate::numeric::NumericArithmeticError::DivisionByZero => Error::DivisionByZero,
        crate::numeric::NumericArithmeticError::Invalid
        | crate::numeric::NumericArithmeticError::Overflow => Error::NumericOverflow,
    })?;
    serde_json::from_str(&value).map_err(|_| Error::NumericOverflow)
}
