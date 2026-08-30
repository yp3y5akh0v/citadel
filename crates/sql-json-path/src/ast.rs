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

//! The AST of JSON Path.

use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use serde_json::Number;

/// A JSON Path value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPath {
    pub(crate) mode: Mode,
    pub(crate) expr: ExprOrPredicate,
    /// Zone the `_tz` entry points resolve against. `None` is UTC, which is what
    /// PostgreSQL uses when `TimeZone` is unset.
    pub(crate) session_tz: Option<jiff::tz::TimeZone>,
    /// Date used by time-to-timetz casts. PostgreSQL uses the transaction-start
    /// date in the session zone; `None` derives today's date when evaluation starts.
    pub(crate) session_date: Option<jiff::civil::Date>,
}

impl JsonPath {
    /// Whether evaluating this path through a standard (non-`_tz`) entry point
    /// can depend on the session time zone or transaction date.
    ///
    /// In standard mode, datetime parsing and comparison are deterministic or
    /// reject a context-requiring cross-kind operation before reading context.
    /// The sole exception is `.time_tz()`: PostgreSQL's TimestampTz-to-TimeTz
    /// cast resolves the timestamp in the session zone even without `_tz`.
    /// `_tz` callers must classify the entry point itself as dependent.
    pub fn depends_on_session_context_without_tz(&self) -> bool {
        expr_or_predicate_depends_on_session_context(&self.expr)
    }
}

fn expr_or_predicate_depends_on_session_context(expr: &ExprOrPredicate) -> bool {
    match expr {
        ExprOrPredicate::Expr(expr) => expr_depends_on_session_context(expr),
        ExprOrPredicate::Pred(predicate) => predicate_depends_on_session_context(predicate),
    }
}

fn expr_depends_on_session_context(expr: &Expr) -> bool {
    match expr {
        Expr::PathPrimary(primary) => primary_depends_on_session_context(primary),
        Expr::Accessor(base, accessor) => {
            expr_depends_on_session_context(base) || accessor_depends_on_session_context(accessor)
        }
        Expr::UnaryOp(_, expr) => expr_depends_on_session_context(expr),
        Expr::BinaryOp(_, left, right) => {
            expr_depends_on_session_context(left) || expr_depends_on_session_context(right)
        }
    }
}

fn primary_depends_on_session_context(primary: &PathPrimary) -> bool {
    match primary {
        PathPrimary::ExprOrPred(expr) => expr_or_predicate_depends_on_session_context(expr),
        PathPrimary::Root | PathPrimary::Current | PathPrimary::Last | PathPrimary::Value(_) => {
            false
        }
    }
}

fn accessor_depends_on_session_context(accessor: &AccessorOp) -> bool {
    match accessor {
        AccessorOp::Element(indices) => indices.iter().any(|index| match index {
            ArrayIndex::Index(expr) => expr_depends_on_session_context(expr),
            ArrayIndex::Slice(start, end) => {
                expr_depends_on_session_context(start) || expr_depends_on_session_context(end)
            }
        }),
        AccessorOp::FilterExpr(predicate) => predicate_depends_on_session_context(predicate),
        AccessorOp::Method(method) => matches!(method, Method::TimeTz { .. }),
        AccessorOp::MemberWildcard
        | AccessorOp::DescendantMemberWildcard(_)
        | AccessorOp::ElementWildcard
        | AccessorOp::Member(_) => false,
    }
}

fn predicate_depends_on_session_context(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Compare(_, left, right) => {
            expr_depends_on_session_context(left) || expr_depends_on_session_context(right)
        }
        Predicate::Exists(expr) => expr_depends_on_session_context(expr),
        Predicate::And(left, right) | Predicate::Or(left, right) => {
            predicate_depends_on_session_context(left)
                || predicate_depends_on_session_context(right)
        }
        Predicate::Not(predicate) | Predicate::IsUnknown(predicate) => {
            predicate_depends_on_session_context(predicate)
        }
        Predicate::StartsWith(expr, _) | Predicate::LikeRegex(expr, _) => {
            expr_depends_on_session_context(expr)
        }
    }
}

/// The mode of JSON Path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Lax mode converts errors to empty SQL/JSON sequences.
    Lax,
    /// Strict mode raises an error if the data does not strictly adhere to the requirements of a path expression.
    Strict,
}

/// An expression or predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprOrPredicate {
    Expr(Expr),
    Pred(Predicate),
}

/// An expression in JSON Path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// Path primary
    PathPrimary(PathPrimary),
    /// Accessor expression.
    Accessor(Box<Expr>, AccessorOp),
    /// Unary operation.
    UnaryOp(UnaryOp, Box<Expr>),
    /// Binary operation.
    BinaryOp(BinaryOp, Box<Expr>, Box<Expr>),
}

/// A filter expression that evaluates to a truth value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// `==`, `!=`, `<`, `<=`, `>`, `>=` represents the comparison between two values.
    Compare(CompareOp, Box<Expr>, Box<Expr>),
    /// `exists` represents the value exists.
    Exists(Box<Expr>),
    /// `&&` represents logical AND.
    And(Box<Predicate>, Box<Predicate>),
    /// `||` represents logical OR.
    Or(Box<Predicate>, Box<Predicate>),
    /// `!` represents logical NOT.
    Not(Box<Predicate>),
    /// `is unknown` represents the value is unknown.
    IsUnknown(Box<Predicate>),
    /// `starts with` represents the value starts with the given value.
    StartsWith(Box<Expr>, Value),
    /// `like_regex` represents the value matches the given regular expression.
    LikeRegex(Box<Expr>, Box<Regex>),
}

/// A primary expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathPrimary {
    /// `$` represents the root node or element.
    Root,
    /// `@` represents the current node or element being processed in the filter expression.
    Current,
    /// `last` is the size of the array minus 1.
    Last,
    /// Literal value.
    Value(Value),
    /// `(expr)` represents an expression.
    ExprOrPred(Box<ExprOrPredicate>),
}

/// An accessor operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessorOp {
    /// `.*` represents selecting all elements in an object.
    MemberWildcard,
    /// `.**` represents selecting all elements in an object and its sub-objects.
    DescendantMemberWildcard(LevelRange),
    /// `[*]` represents selecting all elements in an array.
    ElementWildcard,
    /// `.<name>` represents selecting element that matched the name in an object, like `$.event`.
    /// The name can also be written as a string literal, allowing the name to contain special characters, like `$." $price"`.
    Member(String),
    /// `[<index1>,<index2>,..]` represents selecting elements specified by the indices in an Array.
    Element(Vec<ArrayIndex>),
    /// `?(<predicate>)` represents filtering elements using the predicate.
    FilterExpr(Box<Predicate>),
    /// `.method()` represents calling a method.
    Method(Method),
}

/// A level range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LevelRange {
    /// none
    All,
    /// `{level}`
    One(Level),
    /// `{start to end}`
    Range(Level, Level),
}

/// A level number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Level {
    N(u32),
    Last,
}

/// An array index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayIndex {
    /// The single number index.
    Index(Expr),
    /// `<start> to <end>` represents the slice of the array.
    Slice(Expr, Expr),
}

/// Represents a scalar value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Number value.
    Number(Number),
    /// UTF-8 string.
    String(String),
    /// Variable
    Variable(String),
}

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// `==` represents left is equal to right.
    Eq,
    /// `!=` and `<>` represents left is not equal to right.
    Ne,
    /// `<` represents left is less than right.
    Lt,
    /// `<=` represents left is less or equal to right.
    Le,
    /// `>` represents left is greater than right.
    Gt,
    /// `>=` represents left is greater than or equal to right.
    Ge,
}

/// A unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// `+` represents plus.
    Plus,
    /// `-` represents minus.
    Minus,
}

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+` represents left plus right.
    Add,
    /// `-` represents left minus right.
    Sub,
    /// `*` represents left multiply right.
    Mul,
    /// `/` represents left divide right.
    Div,
    /// `%` represents left modulo right.
    Rem,
}

/// A item method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Method {
    /// `.type()` returns a character string that names the type of the SQL/JSON item.
    Type,
    /// `.size()` returns the size of an SQL/JSON item.
    Size,
    /// `.double()` converts a string or numeric to an approximate numeric value.
    Double,
    /// `.ceiling()` returns the smallest integer that is greater than or equal to the argument.
    Ceiling,
    /// `.floor()` returns the largest integer that is less than or equal to the argument.
    Floor,
    /// `.abs()` returns the absolute value of the argument.
    Abs,
    /// `.keyvalue()` returns the key-value pairs of an object.
    ///
    /// For example, suppose:
    /// ```json
    /// { who: "Fred", what: 64 }
    /// ```
    /// Then:
    /// ```json
    /// $.keyvalue() =
    /// ( { name: "who",  value: "Fred", id: 9045 },
    ///   { name: "what", value: 64,     id: 9045 }
    /// )
    /// ```
    Keyvalue,
    Datetime {
        template: Option<String>,
    },
    /// `.bigint()` converts a string or numeric to a 64-bit integer, rounding a fraction.
    Bigint,
    /// `.integer()` converts a string or numeric to a 32-bit integer, rounding a fraction.
    Integer,
    /// `.number()` converts a string or numeric to an exact numeric value.
    Number,
    /// `.decimal([precision [, scale]])` converts to numeric under an optional typmod.
    ///
    /// Arguments are held as `i64` because PostgreSQL range-checks them during evaluation,
    /// not while parsing; a value outside `i32` must reach eval to report its own error.
    Decimal {
        precision: Option<i64>,
        scale: Option<i64>,
    },
    /// `.string()` converts a boolean, numeric or datetime to a character string.
    String,
    /// `.boolean()` converts a boolean, string or exact integer to a boolean.
    Boolean,
    /// `.date()` converts a string to a date. Takes no argument.
    Date,
    /// `.time([precision])` converts a string to time without time zone.
    Time {
        precision: Option<i64>,
    },
    /// `.time_tz([precision])` converts a string to time with time zone.
    TimeTz {
        precision: Option<i64>,
    },
    /// `.timestamp([precision])` converts a string to timestamp without time zone.
    Timestamp {
        precision: Option<i64>,
    },
    /// `.timestamp_tz([precision])` converts a string to timestamp with time zone.
    TimestampTz {
        precision: Option<i64>,
    },
}

impl PathPrimary {
    /// If this is a nested path primary, unnest it.
    /// `(primary) => primary`
    pub(crate) fn unnest(self) -> Self {
        match self {
            Self::ExprOrPred(expr) => match *expr {
                ExprOrPredicate::Expr(Expr::PathPrimary(inner)) => inner,
                other => Self::ExprOrPred(Box::new(other)),
            },
            _ => self,
        }
    }
}

impl LevelRange {
    /// Returns the upper bound of the range.
    /// If no upper bound, returns `u32::MAX`.
    pub(crate) fn end(&self) -> u32 {
        match self {
            Self::One(Level::N(n)) => *n,
            Self::Range(_, Level::N(end)) => *end,
            _ => u32::MAX,
        }
    }

    /// Resolve the range with the given `last`.
    ///
    /// # Examples
    ///
    /// ```text
    /// last = 3
    /// .**             => 0..4
    /// .**{1}          => 1..2
    /// .**{1 to 4}     => 1..3
    /// .**{1 to last}  => 1..4
    /// .**{last to 2}  => 3..3
    /// ```
    pub(crate) fn to_range(&self, last: usize) -> std::ops::Range<usize> {
        match self {
            Self::All => 0..last + 1,
            Self::One(level) => {
                level.to_usize(last).min(last + 1)..level.to_usize(last).min(last) + 1
            }
            Self::Range(start, end) => {
                start.to_usize(last).min(last + 1)..end.to_usize(last).min(last) + 1
            }
        }
    }
}

impl Level {
    fn to_usize(&self, last: usize) -> usize {
        match self {
            Self::N(n) => *n as usize,
            Self::Last => last,
        }
    }
}

impl Display for JsonPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.mode == Mode::Strict {
            write!(f, "strict ")?;
        }
        write!(f, "{}", self.expr)
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lax => write!(f, "lax"),
            Self::Strict => write!(f, "strict"),
        }
    }
}

impl Display for ExprOrPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expr(expr) => match expr {
                Expr::BinaryOp(_, _, _) => write!(f, "({})", expr),
                _ => write!(f, "{}", expr),
            },
            Self::Pred(pred) => match pred {
                Predicate::Compare(_, _, _) | Predicate::And(_, _) | Predicate::Or(_, _) => {
                    write!(f, "({})", pred)
                }
                _ => write!(f, "{}", pred),
            },
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare(op, left, right) => write!(f, "{left} {op} {right}"),
            Self::Exists(expr) => write!(f, "exists ({expr})"),
            Self::And(left, right) => {
                match left.as_ref() {
                    Self::Or(_, _) => write!(f, "({left})")?,
                    _ => write!(f, "{left}")?,
                }
                write!(f, " && ")?;
                match right.as_ref() {
                    Self::Or(_, _) => write!(f, "({right})"),
                    _ => write!(f, "{right}"),
                }
            }
            Self::Or(left, right) => write!(f, "{left} || {right}"),
            Self::Not(expr) => write!(f, "!({expr})"),
            Self::IsUnknown(expr) => write!(f, "({expr}) is unknown"),
            Self::StartsWith(expr, v) => write!(f, "{expr} starts with {v}"),
            Self::LikeRegex(expr, regex) => {
                write!(f, "{expr} like_regex \"{}\"", regex.pattern())?;
                if let Some(flags) = regex.flags() {
                    write!(f, " flag \"{flags}\"")?;
                }
                Ok(())
            }
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::PathPrimary(primary) => write!(f, "{primary}"),
            Expr::Accessor(base, op) => {
                match base.as_ref() {
                    Expr::PathPrimary(PathPrimary::Value(Value::Number(_))) => {
                        write!(f, "({base})")?
                    }
                    Expr::PathPrimary(PathPrimary::ExprOrPred(expr)) => match expr.as_ref() {
                        ExprOrPredicate::Expr(Expr::UnaryOp(_, _)) => write!(f, "({base})")?,
                        _ => write!(f, "{base}")?,
                    },
                    _ => write!(f, "{base}")?,
                }
                write!(f, "{op}")?;
                Ok(())
            }
            Expr::UnaryOp(op, expr) => match expr.as_ref() {
                Expr::PathPrimary(_) | Expr::Accessor(_, _) => write!(f, "{op}{expr}"),
                _ => write!(f, "{op}({expr})"),
            },
            Expr::BinaryOp(op, left, right) => write!(f, "{left} {op} {right}"),
        }
    }
}

impl Display for ArrayIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Index(idx) => write!(f, "{idx}"),
            Self::Slice(start, end) => write!(f, "{start} to {end}"),
        }
    }
}

impl Display for PathPrimary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root => write!(f, "$"),
            Self::Current => write!(f, "@"),
            Self::Value(v) => write!(f, "{v}"),
            Self::Last => write!(f, "last"),
            Self::ExprOrPred(expr) => write!(f, "{expr}"),
        }
    }
}

impl Display for AccessorOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemberWildcard => write!(f, ".*"),
            Self::DescendantMemberWildcard(level) => write!(f, ".**{level}"),
            Self::ElementWildcard => write!(f, "[*]"),
            Self::Member(field) => write!(f, ".\"{field}\""),
            Self::Element(indices) => {
                write!(f, "[")?;
                for (i, idx) in indices.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{idx}")?;
                }
                write!(f, "]")
            }
            Self::FilterExpr(expr) => write!(f, "?({expr})"),
            // Argument-carrying methods must each be named here: the catch-all renders
            // `.{method}()` and would silently drop them, breaking path round-tripping.
            Self::Method(method) => match method {
                Method::Datetime { template: Some(t) } => write!(f, ".datetime(\"{t}\")"),
                Method::Decimal {
                    precision: Some(p),
                    scale: Some(s),
                } => write!(f, ".decimal({p},{s})"),
                Method::Decimal {
                    precision: Some(p),
                    scale: None,
                } => write!(f, ".decimal({p})"),
                Method::Time { precision: Some(p) } => write!(f, ".time({p})"),
                Method::TimeTz { precision: Some(p) } => write!(f, ".time_tz({p})"),
                Method::Timestamp { precision: Some(p) } => write!(f, ".timestamp({p})"),
                Method::TimestampTz { precision: Some(p) } => write!(f, ".timestamp_tz({p})"),
                _ => write!(f, ".{method}()"),
            },
        }
    }
}

impl Display for LevelRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => Ok(()),
            Self::One(level) => write!(f, "{{{level}}}"),
            Self::Range(start, end) => write!(f, "{{{start} to {end}}}"),
        }
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::N(n) => write!(f, "{n}"),
            Self::Last => write!(f, "last"),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Number(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "\"{v}\""),
            Self::Variable(v) => write!(f, "$\"{v}\""),
        }
    }
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
        }
    }
}

impl Display for CompareOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "=="),
            Self::Ne => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::Le => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Ge => write!(f, ">="),
        }
    }
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Mul => write!(f, "*"),
            Self::Div => write!(f, "/"),
            Self::Rem => write!(f, "%"),
        }
    }
}

impl Display for Method {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Type => write!(f, "type"),
            Self::Size => write!(f, "size"),
            Self::Double => write!(f, "double"),
            Self::Ceiling => write!(f, "ceiling"),
            Self::Floor => write!(f, "floor"),
            Self::Abs => write!(f, "abs"),
            Self::Keyvalue => write!(f, "keyvalue"),
            Self::Datetime { .. } => write!(f, "datetime"),
            Self::Bigint => write!(f, "bigint"),
            Self::Integer => write!(f, "integer"),
            Self::Number => write!(f, "number"),
            Self::Decimal { .. } => write!(f, "decimal"),
            Self::String => write!(f, "string"),
            Self::Boolean => write!(f, "boolean"),
            Self::Date => write!(f, "date"),
            Self::Time { .. } => write!(f, "time"),
            Self::TimeTz { .. } => write!(f, "time_tz"),
            Self::Timestamp { .. } => write!(f, "timestamp"),
            Self::TimestampTz { .. } => write!(f, "timestamp_tz"),
        }
    }
}

/// A wrapper of `regex::Regex` to combine the pattern and flags.
#[derive(Debug, Clone)]
pub struct Regex {
    regex: regex::Regex,
    flags: String,
}

impl Regex {
    pub(crate) fn with_flags(pattern: &str, flags: Option<String>) -> Result<Self, regex::Error> {
        let translated;
        let mut builder = match flags.as_deref() {
            Some(flags) if flags.contains('q') => regex::RegexBuilder::new(&regex::escape(pattern)),
            _ => {
                translated = translate_pg_regex(pattern);
                regex::RegexBuilder::new(&translated)
            }
        };
        let mut out_flags = String::new();
        if let Some(flags) = flags.as_deref() {
            for c in flags.chars() {
                match c {
                    'q' => {}
                    'i' => {
                        builder.case_insensitive(true);
                    }
                    'm' => {
                        builder.multi_line(true);
                    }
                    's' => {
                        builder.dot_matches_new_line(true);
                    }
                    'x' => {
                        return Err(regex::Error::Syntax(
                            "XQuery \"x\" flag (expanded regular expressions) is not implemented"
                                .to_string(),
                        ))
                    }
                    _ => {
                        return Err(regex::Error::Syntax(format!(
                            "Unrecognized flag character \"{c}\" in LIKE_REGEX predicate."
                        )))
                    }
                };
                // Remove duplicated flags.
                if !out_flags.contains(c) {
                    out_flags.push(c);
                }
            }
        }
        let regex = builder.build()?;
        Ok(Self {
            regex,
            flags: out_flags,
        })
    }

    pub fn pattern(&self) -> &str {
        self.regex.as_str()
    }

    pub fn flags(&self) -> Option<&str> {
        if self.flags.is_empty() {
            None
        } else {
            Some(&self.flags)
        }
    }
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &Self::Target {
        &self.regex
    }
}

fn translate_pg_regex(pat: &str) -> String {
    let mut out = String::with_capacity(pat.len() + 4);
    let mut chars = pat.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push_str("\\\\"),
                Some('b') => out.push_str("\\x08"),
                Some(next) => {
                    out.push('\\');
                    out.push(next);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

impl PartialEq for Regex {
    fn eq(&self, other: &Self) -> bool {
        self.pattern() == other.pattern() && self.flags() == other.flags()
    }
}

impl Eq for Regex {}

#[cfg(test)]
mod dependency_tests {
    use super::JsonPath;

    #[test]
    fn standard_context_independent_paths_remain_independent() {
        for input in [
            "$.account.profile",
            "$.items[*].size()",
            "$ ? (exists (@.enabled))",
            "$.items[1 + 2]",
            "$ ? (@.priority == 1)",
            "$ ? (@.priority == $minimum)",
            "$.created.datetime()",
            "$.created.date()",
            "$.created.time()",
            "$.created.timestamp()",
            "$.created.timestamp_tz()",
            "$ ? (@.left.date() == @.right.date())",
        ] {
            let path = JsonPath::new(input).unwrap();
            assert!(
                !path.depends_on_session_context_without_tz(),
                "standard context-independent path was marked dependent: {input}"
            );
        }
    }

    #[test]
    fn time_tz_method_depends_on_standard_session_context() {
        for input in ["$.created.time_tz()", "$ ? (exists (@.created.time_tz()))"] {
            let path = JsonPath::new(input).unwrap();
            assert!(
                path.depends_on_session_context_without_tz(),
                "dependent path was missed: {input}"
            );
        }
    }
}
