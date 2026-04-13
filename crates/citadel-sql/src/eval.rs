//! Expression evaluator with SQL three-valued logic.

use std::collections::HashMap;

use crate::error::{Result, SqlError};
use crate::parser::{BinOp, Expr, UnaryOp};
use crate::types::{ColumnDef, CompactString, DataType, Value};

pub struct ColumnMap {
    exact: HashMap<String, usize>,
    short: HashMap<String, ShortMatch>,
}

enum ShortMatch {
    Unique(usize),
    Ambiguous,
}

impl ColumnMap {
    pub fn new(columns: &[ColumnDef]) -> Self {
        let mut exact = HashMap::with_capacity(columns.len() * 2);
        let mut short: HashMap<String, ShortMatch> = HashMap::with_capacity(columns.len());

        for (i, col) in columns.iter().enumerate() {
            let lower = col.name.to_ascii_lowercase();
            exact.insert(lower.clone(), i);

            let unqualified = if let Some(dot) = lower.rfind('.') {
                &lower[dot + 1..]
            } else {
                &lower
            };
            short
                .entry(unqualified.to_string())
                .and_modify(|e| *e = ShortMatch::Ambiguous)
                .or_insert(ShortMatch::Unique(i));
        }

        Self { exact, short }
    }

    pub(crate) fn resolve(&self, name: &str) -> Result<usize> {
        if let Some(&idx) = self.exact.get(name) {
            return Ok(idx);
        }
        match self.short.get(name) {
            Some(ShortMatch::Unique(idx)) => Ok(*idx),
            Some(ShortMatch::Ambiguous) => Err(SqlError::AmbiguousColumn(name.to_string())),
            None => Err(SqlError::ColumnNotFound(name.to_string())),
        }
    }

    pub(crate) fn resolve_qualified(&self, table: &str, column: &str) -> Result<usize> {
        let qualified = format!("{table}.{column}");
        if let Some(&idx) = self.exact.get(&qualified) {
            return Ok(idx);
        }
        match self.short.get(column) {
            Some(ShortMatch::Unique(idx)) => Ok(*idx),
            _ => Err(SqlError::ColumnNotFound(format!("{table}.{column}"))),
        }
    }
}

pub fn eval_expr(expr: &Expr, col_map: &ColumnMap, row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column(name) => {
            let idx = col_map.resolve(name)?;
            Ok(row[idx].clone())
        }

        Expr::QualifiedColumn { table, column } => {
            let idx = col_map.resolve_qualified(table, column)?;
            Ok(row[idx].clone())
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, col_map, row)?;
            let rval = eval_expr(right, col_map, row)?;
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, col_map, row)?;
            eval_unary_op(*op, &val)
        }

        Expr::IsNull(e) => {
            let val = eval_expr(e, col_map, row)?;
            Ok(Value::Boolean(val.is_null()))
        }

        Expr::IsNotNull(e) => {
            let val = eval_expr(e, col_map, row)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        Expr::Function { name, args } => eval_scalar_function(name, args, col_map, row),

        Expr::CountStar => Err(SqlError::Unsupported(
            "COUNT(*) in non-aggregate context".into(),
        )),

        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let lhs = eval_expr(e, col_map, row)?;
            eval_in_values(&lhs, list, col_map, row, *negated)
        }

        Expr::InSet {
            expr: e,
            values,
            has_null,
            negated,
        } => {
            let lhs = eval_expr(e, col_map, row)?;
            eval_in_set(&lhs, values, *has_null, *negated)
        }

        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(e, col_map, row)?;
            let lo = eval_expr(low, col_map, row)?;
            let hi = eval_expr(high, col_map, row)?;
            eval_between(&val, &lo, &hi, *negated)
        }

        Expr::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => {
            let val = eval_expr(e, col_map, row)?;
            let pat = eval_expr(pattern, col_map, row)?;
            let esc = escape
                .as_ref()
                .map(|e| eval_expr(e, col_map, row))
                .transpose()?;
            eval_like(&val, &pat, esc.as_ref(), *negated)
        }

        Expr::Case {
            operand,
            conditions,
            else_result,
        } => eval_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            col_map,
            row,
        ),

        Expr::Coalesce(args) => {
            for arg in args {
                let val = eval_expr(arg, col_map, row)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }

        Expr::Cast { expr: e, data_type } => {
            let val = eval_expr(e, col_map, row)?;
            eval_cast(&val, *data_type)
        }

        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => Err(
            SqlError::Unsupported("subquery not materialized (internal error)".into()),
        ),

        Expr::Parameter(n) => Err(SqlError::Parse(format!("unbound parameter ${n}"))),

        Expr::WindowFunction { .. } => Err(SqlError::Unsupported(
            "window functions are only allowed in SELECT columns".into(),
        )),
    }
}

fn eval_binary_op(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    // SQL three-valued logic for AND/OR
    match op {
        BinOp::And => return eval_and(left, right),
        BinOp::Or => return eval_or(left, right),
        _ => {}
    }

    // NULL propagation for all other ops (including || per SQL standard)
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    match op {
        BinOp::Eq => Ok(Value::Boolean(left == right)),
        BinOp::NotEq => Ok(Value::Boolean(left != right)),
        BinOp::Lt => Ok(Value::Boolean(left < right)),
        BinOp::Gt => Ok(Value::Boolean(left > right)),
        BinOp::LtEq => Ok(Value::Boolean(left <= right)),
        BinOp::GtEq => Ok(Value::Boolean(left >= right)),
        BinOp::Add => eval_arithmetic(left, right, i64::checked_add, |a, b| a + b),
        BinOp::Sub => eval_arithmetic(left, right, i64::checked_sub, |a, b| a - b),
        BinOp::Mul => eval_arithmetic(left, right, i64::checked_mul, |a, b| a * b),
        BinOp::Div => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_div, |a, b| a / b)
        }
        BinOp::Mod => {
            match right {
                Value::Integer(0) => return Err(SqlError::DivisionByZero),
                Value::Real(r) if *r == 0.0 => return Err(SqlError::DivisionByZero),
                _ => {}
            }
            eval_arithmetic(left, right, i64::checked_rem, |a, b| a % b)
        }
        BinOp::Concat => {
            let ls = value_to_text(left);
            let rs = value_to_text(right);
            Ok(Value::Text(format!("{ls}{rs}").into()))
        }
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

/// SQL three-valued AND: NULL AND false = false, NULL AND true = NULL
fn eval_and(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
        (Some(true), Some(true)) => Ok(Value::Boolean(true)),
        _ => Ok(Value::Null),
    }
}

/// SQL three-valued OR: NULL OR true = true, NULL OR false = NULL
fn eval_or(left: &Value, right: &Value) -> Result<Value> {
    let l = to_bool_or_null(left)?;
    let r = to_bool_or_null(right)?;
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), Some(false)) => Ok(Value::Boolean(false)),
        _ => Ok(Value::Null),
    }
}

fn to_bool_or_null(val: &Value) -> Result<Option<bool>> {
    match val {
        Value::Boolean(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        Value::Integer(i) => Ok(Some(*i != 0)),
        _ => Err(SqlError::TypeMismatch {
            expected: "BOOLEAN".into(),
            got: format!("{}", val.data_type()),
        }),
    }
}

fn eval_arithmetic(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> Option<i64>,
    real_op: fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op(*a, *b)
            .map(Value::Integer)
            .ok_or(SqlError::IntegerOverflow),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(real_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(real_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(real_op(*a, *b as f64))),
        _ => Err(SqlError::TypeMismatch {
            expected: "numeric".into(),
            got: format!("{} and {}", left.data_type(), right.data_type()),
        }),
    }
}

fn eval_in_values(
    lhs: &Value,
    list: &[Expr],
    col_map: &ColumnMap,
    row: &[Value],
    negated: bool,
) -> Result<Value> {
    if list.is_empty() {
        return Ok(Value::Boolean(negated));
    }
    if lhs.is_null() {
        return Ok(Value::Null);
    }
    let mut has_null = false;
    for item in list {
        let rhs = eval_expr(item, col_map, row)?;
        if rhs.is_null() {
            has_null = true;
        } else if lhs == &rhs {
            return Ok(Value::Boolean(!negated));
        }
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(negated))
    }
}

fn eval_in_set(
    lhs: &Value,
    values: &std::collections::HashSet<Value>,
    has_null: bool,
    negated: bool,
) -> Result<Value> {
    if values.is_empty() && !has_null {
        return Ok(Value::Boolean(negated));
    }
    if lhs.is_null() {
        return Ok(Value::Null);
    }
    if values.contains(lhs) {
        return Ok(Value::Boolean(!negated));
    }
    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(negated))
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(i) => i
                .checked_neg()
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            Value::Real(r) => Ok(Value::Real(-r)),
            _ => Err(SqlError::TypeMismatch {
                expected: "numeric".into(),
                got: format!("{}", val.data_type()),
            }),
        },
        UnaryOp::Not => match val {
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            Value::Integer(i) => Ok(Value::Boolean(*i == 0)),
            _ => Err(SqlError::TypeMismatch {
                expected: "BOOLEAN".into(),
                got: format!("{}", val.data_type()),
            }),
        },
    }
}

fn value_to_text(val: &Value) -> String {
    match val {
        Value::Text(s) => s.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => {
            if r.fract() == 0.0 && r.is_finite() {
                format!("{r:.1}")
            } else {
                format!("{r}")
            }
        }
        Value::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Null => String::new(),
        Value::Blob(b) => {
            let mut s = String::with_capacity(b.len() * 2);
            for byte in b {
                s.push_str(&format!("{byte:02X}"));
            }
            s
        }
    }
}

fn eval_between(val: &Value, low: &Value, high: &Value, negated: bool) -> Result<Value> {
    if val.is_null() || low.is_null() || high.is_null() {
        let ge = if val.is_null() || low.is_null() {
            None
        } else {
            Some(*val >= *low)
        };
        let le = if val.is_null() || high.is_null() {
            None
        } else {
            Some(*val <= *high)
        };

        let result = match (ge, le) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        };

        return match result {
            Some(b) => Ok(Value::Boolean(if negated { !b } else { b })),
            None => Ok(Value::Null),
        };
    }

    let in_range = *val >= *low && *val <= *high;
    Ok(Value::Boolean(if negated { !in_range } else { in_range }))
}

const MAX_LIKE_PATTERN_LEN: usize = 10_000;

fn eval_like(val: &Value, pattern: &Value, escape: Option<&Value>, negated: bool) -> Result<Value> {
    if val.is_null() || pattern.is_null() {
        return Ok(Value::Null);
    }
    let text = match val {
        Value::Text(s) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: val.data_type().to_string(),
            })
        }
    };
    let pat = match pattern {
        Value::Text(s) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: pattern.data_type().to_string(),
            })
        }
    };

    if pat.len() > MAX_LIKE_PATTERN_LEN {
        return Err(SqlError::InvalidValue(format!(
            "LIKE pattern too long ({} chars, max {MAX_LIKE_PATTERN_LEN})",
            pat.len()
        )));
    }

    let esc_char = match escape {
        Some(Value::Text(s)) => {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                SqlError::InvalidValue("ESCAPE must be a single character".into())
            })?;
            if chars.next().is_some() {
                return Err(SqlError::InvalidValue(
                    "ESCAPE must be a single character".into(),
                ));
            }
            Some(c)
        }
        Some(Value::Null) => return Ok(Value::Null),
        Some(_) => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT".into(),
                got: "non-text".into(),
            })
        }
        None => None,
    };

    let matched = like_match(text, pat, esc_char);
    Ok(Value::Boolean(if negated { !matched } else { matched }))
}

fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_impl(&t, &p, 0, 0, escape)
}

fn like_match_impl(
    t: &[char],
    p: &[char],
    mut ti: usize,
    mut pi: usize,
    esc: Option<char>,
) -> bool {
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < t.len() {
        if pi < p.len() {
            if let Some(ec) = esc {
                if p[pi] == ec && pi + 1 < p.len() {
                    pi += 1;
                    let pc_lower = p[pi].to_ascii_lowercase();
                    let tc_lower = t[ti].to_ascii_lowercase();
                    if pc_lower == tc_lower {
                        pi += 1;
                        ti += 1;
                        continue;
                    } else if let Some(sp) = star_pi {
                        pi = sp + 1;
                        star_ti += 1;
                        ti = star_ti;
                        continue;
                    } else {
                        return false;
                    }
                }
            }
            if p[pi] == '%' {
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
                continue;
            }
            if p[pi] == '_' {
                pi += 1;
                ti += 1;
                continue;
            }
            if p[pi].eq_ignore_ascii_case(&t[ti]) {
                pi += 1;
                ti += 1;
                continue;
            }
        }
        if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn eval_case(
    operand: Option<&Expr>,
    conditions: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    col_map: &ColumnMap,
    row: &[Value],
) -> Result<Value> {
    if let Some(op_expr) = operand {
        let op_val = eval_expr(op_expr, col_map, row)?;
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, col_map, row)?;
            if !op_val.is_null() && !cond_val.is_null() && op_val == cond_val {
                return eval_expr(result, col_map, row);
            }
        }
    } else {
        for (cond, result) in conditions {
            let cond_val = eval_expr(cond, col_map, row)?;
            if is_truthy(&cond_val) {
                return eval_expr(result, col_map, row);
            }
        }
    }
    match else_result {
        Some(e) => eval_expr(e, col_map, row),
        None => Ok(Value::Null),
    }
}

fn eval_cast(val: &Value, target: DataType) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match target {
        DataType::Integer => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::Real(r) => Ok(Value::Integer(*r as i64)),
            Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .or_else(|_| s.trim().parse::<f64>().map(|f| Value::Integer(f as i64)))
                .map_err(|_| SqlError::InvalidValue(format!("cannot cast '{s}' to INTEGER"))),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to INTEGER",
                val.data_type()
            ))),
        },
        DataType::Real => match val {
            Value::Real(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Real(*i as f64)),
            Value::Boolean(b) => Ok(Value::Real(if *b { 1.0 } else { 0.0 })),
            Value::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Real)
                .map_err(|_| SqlError::InvalidValue(format!("cannot cast '{s}' to REAL"))),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to REAL",
                val.data_type()
            ))),
        },
        DataType::Text => Ok(Value::Text(value_to_text(val).into())),
        DataType::Boolean => match val {
            Value::Boolean(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Boolean(*i != 0)),
            Value::Text(s) => {
                let lower = s.trim().to_ascii_lowercase();
                match lower.as_str() {
                    "true" | "1" | "yes" | "on" => Ok(Value::Boolean(true)),
                    "false" | "0" | "no" | "off" => Ok(Value::Boolean(false)),
                    _ => Err(SqlError::InvalidValue(format!(
                        "cannot cast '{s}' to BOOLEAN"
                    ))),
                }
            }
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to BOOLEAN",
                val.data_type()
            ))),
        },
        DataType::Blob => match val {
            Value::Blob(_) => Ok(val.clone()),
            Value::Text(s) => Ok(Value::Blob(s.as_bytes().to_vec())),
            _ => Err(SqlError::InvalidValue(format!(
                "cannot cast {} to BLOB",
                val.data_type()
            ))),
        },
        DataType::Null => Ok(Value::Null),
    }
}

fn eval_scalar_function(
    name: &str,
    args: &[Expr],
    col_map: &ColumnMap,
    row: &[Value],
) -> Result<Value> {
    let evaluated: Vec<Value> = args
        .iter()
        .map(|a| eval_expr(a, col_map, row))
        .collect::<Result<Vec<_>>>()?;

    match name {
        "LENGTH" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                _ => Ok(Value::Integer(
                    value_to_text(&evaluated[0]).chars().count() as i64
                )),
            }
        }
        "UPPER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_uppercase())),
                _ => Ok(Value::Text(
                    value_to_text(&evaluated[0]).to_ascii_uppercase().into(),
                )),
            }
        }
        "LOWER" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_ascii_lowercase())),
                _ => Ok(Value::Text(
                    value_to_text(&evaluated[0]).to_ascii_lowercase().into(),
                )),
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if evaluated.len() < 2 || evaluated.len() > 3 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 2 or 3 arguments"
                )));
            }
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let chars: Vec<char> = s.chars().collect();
            let start = match &evaluated[1] {
                Value::Integer(i) => *i,
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "INTEGER".into(),
                        got: evaluated[1].data_type().to_string(),
                    })
                }
            };
            let len = chars.len() as i64;

            let (begin, count) = if evaluated.len() == 3 {
                let cnt = match &evaluated[2] {
                    Value::Integer(i) => *i,
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "INTEGER".into(),
                            got: evaluated[2].data_type().to_string(),
                        })
                    }
                };
                if start >= 1 {
                    let b = (start - 1).min(len) as usize;
                    let c = cnt.max(0) as usize;
                    (b, c)
                } else if start == 0 {
                    let c = (cnt - 1).max(0) as usize;
                    (0usize, c)
                } else {
                    let adjusted_cnt = (cnt + start - 1).max(0) as usize;
                    (0usize, adjusted_cnt)
                }
            } else if start >= 1 {
                let b = (start - 1).min(len) as usize;
                (b, chars.len() - b)
            } else if start == 0 {
                (0usize, chars.len())
            } else {
                let b = (len + start).max(0) as usize;
                (b, chars.len() - b)
            };

            let result: String = chars.iter().skip(begin).take(count).collect();
            Ok(Value::Text(result.into()))
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            if evaluated.is_empty() || evaluated.len() > 2 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 or 2 arguments"
                )));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let trim_chars: Vec<char> = if evaluated.len() == 2 {
                if evaluated[1].is_null() {
                    return Ok(Value::Null);
                }
                value_to_text(&evaluated[1]).chars().collect()
            } else {
                vec![' ']
            };
            let result = match name {
                "TRIM" => s
                    .trim_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                "LTRIM" => s
                    .trim_start_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                "RTRIM" => s
                    .trim_end_matches(|c: char| trim_chars.contains(&c))
                    .to_string(),
                _ => unreachable!(),
            };
            Ok(Value::Text(result.into()))
        }
        "REPLACE" => {
            check_args(name, &evaluated, 3)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&evaluated[0]);
            let from = value_to_text(&evaluated[1]);
            let to = value_to_text(&evaluated[2]);
            if from.is_empty() {
                return Ok(Value::Text(s.into()));
            }
            Ok(Value::Text(s.replace(&from, &to).into()))
        }
        "INSTR" => {
            check_args(name, &evaluated, 2)?;
            if evaluated.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let haystack = value_to_text(&evaluated[0]);
            let needle = value_to_text(&evaluated[1]);
            let pos = haystack
                .find(&needle)
                .map(|i| haystack[..i].chars().count() as i64 + 1)
                .unwrap_or(0);
            Ok(Value::Integer(pos))
        }
        "CONCAT" => {
            if evaluated.is_empty() {
                return Ok(Value::Text(CompactString::default()));
            }
            let mut result = String::new();
            for v in &evaluated {
                match v {
                    Value::Null => {}
                    _ => result.push_str(&value_to_text(v)),
                }
            }
            Ok(Value::Text(result.into()))
        }
        "ABS" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => i
                    .checked_abs()
                    .map(Value::Integer)
                    .ok_or(SqlError::IntegerOverflow),
                Value::Real(r) => Ok(Value::Real(r.abs())),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "ROUND" => {
            if evaluated.is_empty() || evaluated.len() > 2 {
                return Err(SqlError::InvalidValue(
                    "ROUND requires 1 or 2 arguments".into(),
                ));
            }
            if evaluated[0].is_null() {
                return Ok(Value::Null);
            }
            let val = match &evaluated[0] {
                Value::Integer(i) => *i as f64,
                Value::Real(r) => *r,
                _ => {
                    return Err(SqlError::TypeMismatch {
                        expected: "numeric".into(),
                        got: evaluated[0].data_type().to_string(),
                    })
                }
            };
            let places = if evaluated.len() == 2 {
                match &evaluated[1] {
                    Value::Null => return Ok(Value::Null),
                    Value::Integer(i) => *i,
                    _ => {
                        return Err(SqlError::TypeMismatch {
                            expected: "INTEGER".into(),
                            got: evaluated[1].data_type().to_string(),
                        })
                    }
                }
            } else {
                0
            };
            let factor = 10f64.powi(places as i32);
            let rounded = (val * factor).round() / factor;
            Ok(Value::Real(rounded))
        }
        "CEIL" | "CEILING" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Real(r) => Ok(Value::Integer(r.ceil() as i64)),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "FLOOR" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Real(r) => Ok(Value::Integer(r.floor() as i64)),
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "SIGN" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => Ok(Value::Integer(i.signum())),
                Value::Real(r) => {
                    if *r > 0.0 {
                        Ok(Value::Integer(1))
                    } else if *r < 0.0 {
                        Ok(Value::Integer(-1))
                    } else {
                        Ok(Value::Integer(0))
                    }
                }
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "SQRT" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(i) => {
                    if *i < 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real((*i as f64).sqrt()))
                    }
                }
                Value::Real(r) => {
                    if *r < 0.0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real(r.sqrt()))
                    }
                }
                _ => Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: evaluated[0].data_type().to_string(),
                }),
            }
        }
        "RANDOM" => {
            check_args(name, &evaluated, 0)?;
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            use std::time::SystemTime;
            let mut hasher = DefaultHasher::new();
            SystemTime::now().hash(&mut hasher);
            std::thread::current().id().hash(&mut hasher);
            let mut val = hasher.finish() as i64;
            if val == i64::MIN {
                val = i64::MAX;
            }
            Ok(Value::Integer(val))
        }
        "TYPEOF" => {
            check_args(name, &evaluated, 1)?;
            let type_name = match &evaluated[0] {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
                Value::Boolean(_) => "boolean",
            };
            Ok(Value::Text(type_name.into()))
        }
        "MIN" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() {
                return Ok(evaluated[1].clone());
            }
            if evaluated[1].is_null() {
                return Ok(evaluated[0].clone());
            }
            if evaluated[0] <= evaluated[1] {
                Ok(evaluated[0].clone())
            } else {
                Ok(evaluated[1].clone())
            }
        }
        "MAX" => {
            check_args(name, &evaluated, 2)?;
            if evaluated[0].is_null() {
                return Ok(evaluated[1].clone());
            }
            if evaluated[1].is_null() {
                return Ok(evaluated[0].clone());
            }
            if evaluated[0] >= evaluated[1] {
                Ok(evaluated[0].clone())
            } else {
                Ok(evaluated[1].clone())
            }
        }
        "HEX" => {
            check_args(name, &evaluated, 1)?;
            match &evaluated[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let mut s = String::with_capacity(b.len() * 2);
                    for byte in b {
                        s.push_str(&format!("{byte:02X}"));
                    }
                    Ok(Value::Text(s.into()))
                }
                Value::Text(s) => {
                    let mut r = String::with_capacity(s.len() * 2);
                    for byte in s.as_bytes() {
                        r.push_str(&format!("{byte:02X}"));
                    }
                    Ok(Value::Text(r.into()))
                }
                _ => Ok(Value::Text(value_to_text(&evaluated[0]).into())),
            }
        }
        _ => Err(SqlError::Unsupported(format!("scalar function: {name}"))),
    }
}

fn check_args(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        Err(SqlError::InvalidValue(format!(
            "{name} requires {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

pub fn referenced_columns(expr: &Expr, columns: &[ColumnDef]) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_column_refs(expr, columns, &mut indices);
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn collect_column_refs(expr: &Expr, columns: &[ColumnDef], out: &mut Vec<usize>) {
    match expr {
        Expr::Column(name) => {
            for (i, c) in columns.iter().enumerate() {
                if c.name == *name || c.name.ends_with(&format!(".{name}")) {
                    out.push(i);
                    break;
                }
            }
        }
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            if let Some(idx) = columns.iter().position(|c| c.name == qualified) {
                out.push(idx);
            } else {
                let matches: Vec<usize> = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.name == *column)
                    .map(|(i, _)| i)
                    .collect();
                if matches.len() == 1 {
                    out.push(matches[0]);
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_refs(left, columns, out);
            collect_column_refs(right, columns, out);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            collect_column_refs(e, columns, out);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
        }
        Expr::InSubquery { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_column_refs(expr, columns, out);
            for item in list {
                collect_column_refs(item, columns, out);
            }
        }
        Expr::InSet { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_column_refs(expr, columns, out);
            collect_column_refs(low, columns, out);
            collect_column_refs(high, columns, out);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_column_refs(expr, columns, out);
            collect_column_refs(pattern, columns, out);
            if let Some(esc) = escape {
                collect_column_refs(esc, columns, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_column_refs(op, columns, out);
            }
            for (when, then) in conditions {
                collect_column_refs(when, columns, out);
                collect_column_refs(then, columns, out);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, columns, out);
            }
        }
        Expr::Coalesce(args) => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_column_refs(expr, columns, out);
        }
        Expr::WindowFunction { args, spec, .. } => {
            for arg in args {
                collect_column_refs(arg, columns, out);
            }
            for pb in &spec.partition_by {
                collect_column_refs(pb, columns, out);
            }
            for ob in &spec.order_by {
                collect_column_refs(&ob.expr, columns, out);
            }
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_) => {}
    }
}

/// Check if an expression result is truthy (for WHERE/HAVING).
pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Boolean(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Null => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    fn col(name: &str, dt: DataType, nullable: bool, pos: u16) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: dt,
            nullable,
            position: pos,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
        }
    }

    fn test_columns() -> Vec<ColumnDef> {
        vec![
            col("id", DataType::Integer, false, 0),
            col("name", DataType::Text, true, 1),
            col("score", DataType::Real, true, 2),
            col("active", DataType::Boolean, false, 3),
        ]
    }

    fn test_row() -> Vec<Value> {
        vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Real(95.5),
            Value::Boolean(true),
        ]
    }

    #[test]
    fn eval_literal() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::Literal(Value::Integer(42));
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Integer(42));
    }

    #[test]
    fn eval_column_ref() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::Column("name".into());
        assert_eq!(
            eval_expr(&expr, &cm, &row).unwrap(),
            Value::Text("Alice".into())
        );
    }

    #[test]
    fn eval_column_case_insensitive() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::Column("name".into());
        assert_eq!(
            eval_expr(&expr, &cm, &row).unwrap(),
            Value::Text("Alice".into())
        );
    }

    #[test]
    fn eval_arithmetic_int() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Add,
            right: Box::new(Expr::Literal(Value::Integer(10))),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Integer(11));
    }

    #[test]
    fn eval_comparison() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("score".into())),
            op: BinOp::Gt,
            right: Box::new(Expr::Literal(Value::Real(90.0))),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn eval_null_propagation() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = vec![
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Boolean(true),
        ];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("test".into()))),
        };
        assert!(eval_expr(&expr, &cm, &row).unwrap().is_null());
    }

    #[test]
    fn eval_and_three_valued() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = vec![
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Boolean(true),
        ];

        // NULL AND false = false
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::And,
            right: Box::new(Expr::Literal(Value::Boolean(false))),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(false));

        // NULL AND true = NULL
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::And,
            right: Box::new(Expr::Literal(Value::Boolean(true))),
        };
        assert!(eval_expr(&expr, &cm, &row).unwrap().is_null());
    }

    #[test]
    fn eval_or_three_valued() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = vec![
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Boolean(true),
        ];

        // NULL OR true = true
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Or,
            right: Box::new(Expr::Literal(Value::Boolean(true))),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(true));

        // NULL OR false = NULL
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Or,
            right: Box::new(Expr::Literal(Value::Boolean(false))),
        };
        assert!(eval_expr(&expr, &cm, &row).unwrap().is_null());
    }

    #[test]
    fn eval_is_null() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = vec![
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Boolean(true),
        ];
        let expr = Expr::IsNull(Box::new(Expr::Column("name".into())));
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(true));

        let expr = Expr::IsNotNull(Box::new(Expr::Column("id".into())));
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn eval_not() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Column("active".into())),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn eval_neg() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(Expr::Column("id".into())),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Integer(-1));
    }

    #[test]
    fn eval_division_by_zero() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Div,
            right: Box::new(Expr::Literal(Value::Integer(0))),
        };
        assert!(matches!(
            eval_expr(&expr, &cm, &row),
            Err(SqlError::DivisionByZero)
        ));
    }

    #[test]
    fn eval_mixed_numeric() {
        let cols = test_columns();
        let cm = ColumnMap::new(&cols);
        let row = test_row();
        // id (int 1) + score (real 95.5) = real 96.5
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".into())),
            op: BinOp::Add,
            right: Box::new(Expr::Column("score".into())),
        };
        assert_eq!(eval_expr(&expr, &cm, &row).unwrap(), Value::Real(96.5));
    }

    #[test]
    fn is_truthy_values() {
        assert!(is_truthy(&Value::Boolean(true)));
        assert!(!is_truthy(&Value::Boolean(false)));
        assert!(!is_truthy(&Value::Null));
        assert!(is_truthy(&Value::Integer(1)));
        assert!(!is_truthy(&Value::Integer(0)));
    }
}
