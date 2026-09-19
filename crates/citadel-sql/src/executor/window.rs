use std::collections::VecDeque;
use std::ops::Range;

use crate::error::{Result, SqlError};
use crate::eval::{collation_of, eval_expr, operand_collation, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::types::*;

use super::helpers::*;

#[cfg(test)]
thread_local! {
    static WINDOW_KEY_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static WINDOW_PEER_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static WINDOW_AGGREGATE_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[inline]
fn note_window_key_evaluation() {
    #[cfg(test)]
    WINDOW_KEY_EVALUATIONS.with(|count| count.set(count.get() + 1));
}

#[inline]
fn note_window_peer_comparison() {
    #[cfg(test)]
    WINDOW_PEER_COMPARISONS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn take_window_key_evaluations() -> usize {
    WINDOW_KEY_EVALUATIONS.with(|count| count.replace(0))
}

#[cfg(test)]
fn take_window_peer_comparisons() -> usize {
    WINDOW_PEER_COMPARISONS.with(|count| count.replace(0))
}

#[inline]
fn note_window_aggregate_step() {
    #[cfg(test)]
    WINDOW_AGGREGATE_STEPS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn take_window_aggregate_steps() -> usize {
    WINDOW_AGGREGATE_STEPS.with(|count| count.replace(0))
}

pub(super) fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            has_window_function(left) || has_window_function(right)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            has_window_function(left) || has_window_function(right)
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::Cast { expr: e, .. } => has_window_function(e),
        Expr::Function { args, .. } | Expr::Coalesce(args) => args.iter().any(has_window_function),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| has_window_function(e))
                || conditions
                    .iter()
                    .any(|(c, r)| has_window_function(c) || has_window_function(r))
                || else_result.as_ref().is_some_and(|e| has_window_function(e))
        }
        _ => false,
    }
}

pub(super) fn has_any_window_function(stmt: &SelectStmt) -> bool {
    stmt.columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => has_window_function(expr),
        _ => false,
    })
}

/// Extract window functions, replacing with column refs. Returns (rewritten_expr, window_list).
pub(super) fn extract_window_fns(
    expr: &Expr,
    slot_counter: &mut usize,
    extracted: &mut Vec<(String, String, Vec<Expr>, WindowSpec)>,
) -> Expr {
    match expr {
        Expr::WindowFunction { name, args, spec } => {
            let slot_name = format!("__win_{}", *slot_counter);
            *slot_counter += 1;
            extracted.push((slot_name.clone(), name.clone(), args.clone(), spec.clone()));
            Expr::Column(slot_name)
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(extract_window_fns(left, slot_counter, extracted)),
            op: *op,
            right: Box::new(extract_window_fns(right, slot_counter, extracted)),
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(extract_window_fns(left, slot_counter, extracted)),
            right: Box::new(extract_window_fns(right, slot_counter, extracted)),
            negated: *negated,
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(extract_window_fns(e, slot_counter, extracted)),
        },
        Expr::IsNull(e) => Expr::IsNull(Box::new(extract_window_fns(e, slot_counter, extracted))),
        Expr::IsNotNull(e) => {
            Expr::IsNotNull(Box::new(extract_window_fns(e, slot_counter, extracted)))
        }
        Expr::Cast { expr: e, data_type } => Expr::Cast {
            expr: Box::new(extract_window_fns(e, slot_counter, extracted)),
            data_type: *data_type,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| extract_window_fns(a, slot_counter, extracted))
                .collect(),
            distinct: *distinct,
        },
        Expr::Coalesce(args) => Expr::Coalesce(
            args.iter()
                .map(|a| extract_window_fns(a, slot_counter, extracted))
                .collect(),
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(extract_window_fns(e, slot_counter, extracted))),
            conditions: conditions
                .iter()
                .map(|(c, r)| {
                    (
                        extract_window_fns(c, slot_counter, extracted),
                        extract_window_fns(r, slot_counter, extracted),
                    )
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(extract_window_fns(e, slot_counter, extracted))),
        },
        other => other.clone(),
    }
}

/// Resolve default frame per SQL standard.
pub(super) fn resolve_frame(spec: &WindowSpec) -> WindowFrame {
    if let Some(ref frame) = spec.frame {
        return frame.clone();
    }
    if spec.order_by.is_empty() {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::UnboundedFollowing,
        }
    } else {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        }
    }
}

enum ResolvedFrame {
    Rows {
        start: Option<i128>,
        end: Option<i128>,
        sliding: bool,
    },
    Range {
        unbounded_start: bool,
        unbounded_end: bool,
    },
    Ignored,
}

impl ResolvedFrame {
    fn validate_categories(frame: &WindowFrame) -> Result<()> {
        let category = |bound: &WindowFrameBound| match bound {
            WindowFrameBound::UnboundedPreceding => 0,
            WindowFrameBound::Preceding(_) => 1,
            WindowFrameBound::CurrentRow => 2,
            WindowFrameBound::Following(_) => 3,
            WindowFrameBound::UnboundedFollowing => 4,
        };
        if matches!(frame.start, WindowFrameBound::UnboundedFollowing)
            || matches!(frame.end, WindowFrameBound::UnboundedPreceding)
            || category(&frame.start) > category(&frame.end)
        {
            return Err(SqlError::InvalidValue("invalid window frame bounds".into()));
        }
        Ok(())
    }

    fn new(frame: &WindowFrame, cancel: Option<&citadel::CancelToken>) -> Result<Self> {
        Self::validate_categories(frame)?;
        match frame.units {
            WindowFrameUnits::Rows => {
                let offset = |bound: &WindowFrameBound| -> Result<Option<i128>> {
                    match bound {
                        WindowFrameBound::UnboundedPreceding
                        | WindowFrameBound::UnboundedFollowing => Ok(None),
                        WindowFrameBound::CurrentRow => Ok(Some(0)),
                        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                            let mut row_dependent = false;
                            visit_expr(expr, &mut |node| {
                                row_dependent |= match node {
                                    Expr::Column(_)
                                    | Expr::QualifiedColumn { .. }
                                    | Expr::CountStar
                                    | Expr::WindowFunction { .. }
                                    | Expr::ScalarSubquery(_)
                                    | Expr::InSubquery { .. }
                                    | Expr::Exists { .. }
                                    | Expr::Quantified {
                                        right: QuantifiedRhs::Subquery(_),
                                        ..
                                    } => true,
                                    Expr::Function { name, args, .. } => {
                                        super::aggregate::is_aggregate_function(name, args.len())
                                    }
                                    _ => false,
                                };
                            });
                            if row_dependent {
                                return Err(SqlError::InvalidValue(
                                    "ROWS frame offset must not depend on rows or aggregates"
                                        .into(),
                                ));
                            }
                            let value = eval_const_expr_with_cancel(expr, cancel)?;
                            let Value::Integer(count) = value else {
                                return Err(SqlError::InvalidValue(
                                    "ROWS frame offset must be a nonnegative integer".into(),
                                ));
                            };
                            if count < 0 {
                                return Err(SqlError::InvalidValue(
                                    "ROWS frame offset must be a nonnegative integer".into(),
                                ));
                            }
                            Ok(Some(if matches!(bound, WindowFrameBound::Preceding(_)) {
                                -i128::from(count)
                            } else {
                                i128::from(count)
                            }))
                        }
                    }
                };
                Ok(Self::Rows {
                    start: offset(&frame.start)?,
                    end: offset(&frame.end)?,
                    sliding: matches!(
                        frame.start,
                        WindowFrameBound::UnboundedPreceding | WindowFrameBound::Preceding(_)
                    ) && matches!(
                        frame.end,
                        WindowFrameBound::CurrentRow | WindowFrameBound::Following(_)
                    ),
                })
            }
            WindowFrameUnits::Range => {
                if matches!(
                    frame.start,
                    WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
                ) || matches!(
                    frame.end,
                    WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
                ) {
                    return Err(SqlError::Unsupported("RANGE with numeric offset".into()));
                }
                Ok(Self::Range {
                    unbounded_start: matches!(frame.start, WindowFrameBound::UnboundedPreceding),
                    unbounded_end: matches!(frame.end, WindowFrameBound::UnboundedFollowing),
                })
            }
            WindowFrameUnits::Groups => Err(SqlError::Unsupported("GROUPS window frame".into())),
        }
    }

    fn uses_peers(&self) -> bool {
        matches!(self, Self::Range { unbounded_start, unbounded_end } if !unbounded_start || !unbounded_end)
    }

    /// Only explicit unbounded ends (or the unordered default) prove
    /// every output row consumes this same complete partition.
    fn covers_partition(&self) -> bool {
        matches!(
            self,
            Self::Rows {
                start: None,
                end: None,
                ..
            } | Self::Range {
                unbounded_start: true,
                unbounded_end: true
            }
        )
    }

    /// Prefix RANGE frames grow; peer-only RANGE frames are disjoint groups.
    /// Both can add each argument once in its original sorted order.
    fn supports_forward_range(&self) -> bool {
        matches!(
            self,
            Self::Range {
                unbounded_end: false,
                ..
            }
        )
    }

    fn is_rows_prefix(&self) -> bool {
        matches!(self, Self::Rows { start: None, .. })
    }

    fn supports_sliding(&self) -> bool {
        matches!(self, Self::Rows { sliding: true, .. })
    }

    /// Half-open bounds preserve empty frames at either partition boundary.
    fn indices(&self, i: usize, n: usize, peer_bounds: &[(usize, usize)]) -> Range<usize> {
        if n == 0 {
            return 0..0;
        }
        let (start, end) = match self {
            Self::Rows { start, end, .. } => {
                let limit = n as i128;
                let start = start.map_or(0, |offset| (i as i128 + offset).clamp(0, limit) as usize);
                let end = end.map_or(n, |offset| {
                    (i as i128 + offset + 1).clamp(0, limit) as usize
                });
                (start, end)
            }
            Self::Range {
                unbounded_start,
                unbounded_end,
            } => (
                if *unbounded_start {
                    0
                } else {
                    peer_bounds[i].0
                },
                if *unbounded_end {
                    n
                } else {
                    peer_bounds[i].1 + 1
                },
            ),
            Self::Ignored => unreachable!("this function does not consume its frame"),
        };
        start..end.max(start)
    }
}

/// For RANGE frames, index every peer group once. `part_indices` are already in
/// window order, and `keys` are the values used to establish that order.
fn peer_group_bounds(
    part_indices: &[usize],
    keys: &WindowValues,
    order_key_start: usize,
    order_collations: &[Collation],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<(usize, usize)>> {
    let n = part_indices.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    if order_collations.is_empty() {
        if cancel.is_none() {
            return Ok(vec![(0, n - 1); n]);
        }
        let mut bounds = Vec::with_capacity(n);
        for work in 0..n {
            check_cancel_at(cancel, work)?;
            bounds.push((0, n - 1));
        }
        check_cancel(cancel)?;
        return Ok(bounds);
    }

    let mut bounds = vec![(0, 0); n];
    let mut group_start = 0;
    for pos in 1..=n {
        check_cancel_at(cancel, pos)?;
        let group_ended = if pos == n {
            true
        } else {
            note_window_peer_comparison();
            let previous = &keys[part_indices[pos - 1]][order_key_start..];
            let current = &keys[part_indices[pos]][order_key_start..];
            !collated_keys_equal(previous, current, order_collations)
        };
        if group_ended {
            let group_end = pos - 1;
            for (work, bound) in bounds[group_start..pos].iter_mut().enumerate() {
                check_cancel_at(cancel, work)?;
                *bound = (group_start, group_end);
            }
            group_start = pos;
        }
    }
    check_cancel(cancel)?;
    Ok(bounds)
}

fn collated_keys_equal(left: &[Value], right: &[Value], collations: &[Collation]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).enumerate().all(|(i, (a, b))| {
            collations
                .get(i)
                .copied()
                .unwrap_or_default()
                .cmp_value(a, b)
                .is_eq()
        })
}

/// Monotonic deque for sliding MIN/MAX.
/// The caller must establish transitive ordering for the argument domain.
pub(super) struct MonoDeque {
    deque: VecDeque<(usize, Value)>,
    is_min: bool,
    collation: Collation,
}

impl MonoDeque {
    pub(super) fn new(is_min: bool, collation: Collation) -> Self {
        Self {
            deque: VecDeque::new(),
            is_min,
            collation,
        }
    }

    pub(super) fn push(&mut self, idx: usize, val: Value) {
        if val.is_null() {
            return;
        }
        while let Some(back) = self.deque.back() {
            let ordering = self.collation.cmp_value(&val, &back.1);
            let evict = if self.is_min {
                ordering.is_lt()
            } else {
                ordering.is_gt()
            };
            if evict {
                self.deque.pop_back();
            } else {
                break;
            }
        }
        self.deque.push_back((idx, val));
    }

    pub(super) fn pop_expired(&mut self, frame_start: usize) {
        while let Some(front) = self.deque.front() {
            if front.0 < frame_start {
                self.deque.pop_front();
            } else {
                break;
            }
        }
    }

    pub(super) fn current(&self) -> Value {
        self.deque
            .front()
            .map(|(_, v)| v.clone())
            .unwrap_or(Value::Null)
    }
}

/// Removable accumulator for sliding SUM/COUNT/AVG.
pub(super) struct SlidingSum {
    int_sum: i128,
    real_sum: f64,
    real_count: i64,
    count: i64,
}

impl SlidingSum {
    pub(super) fn new() -> Self {
        Self {
            int_sum: 0,
            real_sum: 0.0,
            real_count: 0,
            count: 0,
        }
    }

    pub(super) fn add(&mut self, val: &Value) -> Result<()> {
        match val {
            Value::Integer(i) => {
                self.int_sum += i128::from(*i);
                self.count += 1;
            }
            Value::Real(r) => {
                self.real_sum += r;
                self.real_count += 1;
                self.count += 1;
            }
            Value::Null => {}
            other => {
                return Err(SqlError::TypeMismatch {
                    expected: "numeric".into(),
                    got: other.data_type().to_string(),
                })
            }
        }
        Ok(())
    }

    /// Remove only when the remaining forward sum can be recovered exactly.
    /// A failed removal leaves the accumulator unchanged; the caller must
    /// rebuild the frame because floating addition has no exact inverse.
    #[must_use]
    pub(super) fn try_remove(&mut self, val: &Value) -> bool {
        match val {
            Value::Integer(i) => {
                self.int_sum -= i128::from(*i);
                self.count -= 1;
            }
            Value::Real(_) => {
                if self.real_count != 1 {
                    return false;
                }
                self.real_sum = 0.0;
                self.real_count = 0;
                self.count -= 1;
            }
            _ => {}
        }
        true
    }

    pub(super) fn result_sum(&self) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else if self.real_count > 0 {
            Ok(Value::Real(self.real_sum + self.int_sum as f64))
        } else {
            i64::try_from(self.int_sum)
                .map(Value::Integer)
                .map_err(|_| SqlError::IntegerOverflow)
        }
    }

    pub(super) fn result_avg(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            let total = if self.real_count > 0 {
                self.real_sum + self.int_sum as f64
            } else {
                self.int_sum as f64
            };
            Value::Real(total / self.count as f64)
        }
    }
}

enum WindowAccumulator {
    Count { count: i64, star: bool },
    Sum(SlidingSum),
    Avg(SlidingSum),
}

impl WindowAccumulator {
    fn new(name: &str, arg_count: usize) -> Self {
        match name {
            "COUNT" => Self::Count {
                count: 0,
                star: arg_count == 0,
            },
            "SUM" => Self::Sum(SlidingSum::new()),
            "AVG" => Self::Avg(SlidingSum::new()),
            _ => unreachable!(),
        }
    }

    fn add(&mut self, args: &[Value]) -> Result<()> {
        note_window_aggregate_step();
        match self {
            Self::Count { count, star } => {
                *count += i64::from(*star || !args[0].is_null());
                Ok(())
            }
            Self::Sum(sum) | Self::Avg(sum) => sum.add(&args[0]),
        }
    }

    #[must_use]
    fn try_remove(&mut self, args: &[Value]) -> bool {
        match self {
            Self::Count { count, star } => {
                *count -= i64::from(*star || !args[0].is_null());
                true
            }
            Self::Sum(sum) | Self::Avg(sum) => sum.try_remove(&args[0]),
        }
    }

    fn result(&self) -> Result<Value> {
        match self {
            Self::Count { count, .. } => Ok(Value::Integer(*count)),
            Self::Sum(sum) => sum.result_sum(),
            Self::Avg(sum) => Ok(sum.result_avg()),
        }
    }
}

/// Exact frame-order extrema fold. Equal comparisons retain the first value,
/// including its numeric representation, NaN payload, or collated spelling.
struct WindowExtreme {
    result: Value,
    is_min: bool,
    collation: Collation,
}

impl WindowExtreme {
    fn new(is_min: bool, collation: Collation) -> Self {
        Self {
            result: Value::Null,
            is_min,
            collation,
        }
    }

    fn add(&mut self, value: &Value) {
        note_window_aggregate_step();
        if value.is_null() {
            return;
        }
        let replace = self.result.is_null() || {
            let ordering = self.collation.cmp_value(value, &self.result);
            (self.is_min && ordering.is_lt()) || (!self.is_min && ordering.is_gt())
        };
        if replace {
            self.result = value.clone();
        }
    }

    fn result(&self) -> Value {
        self.result.clone()
    }
}

/// Deque pruning requires transitive comparisons. SQL mixed numeric equality
/// can bridge distinct integers, and NaN compares equal to every numeric value.
/// Array ordering recursively inherits those relations, so arrays use the exact
/// fold. Vector ordering uses total_cmp; other value domains are transitive.
fn supports_monotonic_extrema(
    indices: &[usize],
    arguments: &WindowValues,
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    let mut has_real = false;
    let mut has_inexact_integer = false;
    for (work, &index) in indices.iter().enumerate() {
        check_cancel_at(cancel, work)?;
        match &arguments[index][0] {
            Value::Real(value) if value.is_nan() => return Ok(false),
            Value::Real(_) => has_real = true,
            value @ Value::Integer(_) => {
                has_inexact_integer |= value.strict_coerce(DataType::Real).is_none();
            }
            Value::Array(_) => return Ok(false),
            _ => {}
        }
        if has_real && has_inexact_integer {
            return Ok(false);
        }
    }
    Ok(true)
}

fn window_extreme(
    indices: &[usize],
    arguments: &WindowValues,
    is_min: bool,
    collation: Collation,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    let mut acc = WindowExtreme::new(is_min, collation);
    for (work, &index) in indices.iter().enumerate() {
        check_cancel_at(cancel, work)?;
        acc.add(&arguments[index][0]);
    }
    Ok(acc.result())
}

fn validate_window_args(name: &str, count: usize) -> Result<()> {
    let valid = match name {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" => count == 0,
        "NTILE" | "FIRST_VALUE" | "LAST_VALUE" | "SUM" | "AVG" | "MIN" | "MAX" => count == 1,
        "LAG" | "LEAD" => (1..=3).contains(&count),
        "COUNT" => count <= 1,
        other => return Err(SqlError::Unsupported(format!("window function: {other}"))),
    };
    if !valid {
        return Err(SqlError::Parse(format!(
            "invalid number of arguments for {name}"
        )));
    }
    Ok(())
}

fn uses_window_frame(name: &str) -> bool {
    matches!(
        name,
        "FIRST_VALUE" | "LAST_VALUE" | "SUM" | "COUNT" | "AVG" | "MIN" | "MAX"
    )
}

/// Fixed-width temporary rows, kept contiguous rather than allocating once per row.
/// A zero-width matrix still has logical rows (for functions such as COUNT(*)).
struct WindowValues {
    values: Vec<Value>,
    rows: usize,
    width: usize,
}

impl WindowValues {
    fn with_capacity(rows: usize, width: usize) -> Result<Self> {
        let len = rows
            .checked_mul(width)
            .ok_or_else(|| SqlError::InvalidValue("window temporary size overflow".into()))?;
        Ok(Self {
            values: Vec::with_capacity(len),
            rows,
            width,
        })
    }

    fn nulls(rows: usize, width: usize) -> Result<Self> {
        let mut values = Self::with_capacity(rows, width)?;
        values.values.resize(rows * width, Value::Null);
        Ok(values)
    }

    fn push_row(&mut self, values: impl Iterator<Item = Result<Value>>) -> Result<()> {
        let start = self.values.len();
        for value in values {
            self.values.push(value?);
        }
        debug_assert_eq!(self.values.len() - start, self.width);
        debug_assert!(self.values.len() <= self.rows * self.width);
        Ok(())
    }

    fn row_range(&self, row: usize) -> Range<usize> {
        assert!(row < self.rows, "window row index out of bounds");
        let start = row * self.width;
        start..start + self.width
    }
}

impl std::ops::Index<usize> for WindowValues {
    type Output = [Value];

    fn index(&self, row: usize) -> &Self::Output {
        &self.values[self.row_range(row)]
    }
}

impl std::ops::IndexMut<usize> for WindowValues {
    fn index_mut(&mut self, row: usize) -> &mut Self::Output {
        let range = self.row_range(row);
        &mut self.values[range]
    }
}

pub(super) fn eval_window_select(
    mut rows: Vec<Vec<Value>>,
    ctx: super::SelectCtx<'_>,
) -> Result<ExecutionResult> {
    ctx.check()?;
    let super::SelectCtx {
        columns,
        stmt,
        cancel,
        ..
    } = ctx;
    let mut slot_counter = 0usize;
    let mut all_extracted: Vec<(String, String, Vec<Expr>, WindowSpec)> = Vec::new();
    let mut rewritten_columns: Vec<SelectColumn> = Vec::new();

    for col in &stmt.columns {
        match col {
            SelectColumn::AllColumns => rewritten_columns.push(SelectColumn::AllColumns),
            SelectColumn::AllFromOld => rewritten_columns.push(SelectColumn::AllFromOld),
            SelectColumn::AllFromNew => rewritten_columns.push(SelectColumn::AllFromNew),
            SelectColumn::Expr { expr, alias } => {
                let new_expr = extract_window_fns(expr, &mut slot_counter, &mut all_extracted);
                rewritten_columns.push(SelectColumn::Expr {
                    expr: new_expr,
                    alias: alias.clone(),
                });
            }
        }
    }

    if all_extracted.is_empty() {
        return super::process_select(rows, ctx.predicate_applied(false));
    }

    let frames = all_extracted
        .iter()
        .map(|(_, name, args, spec)| {
            check_cancel(cancel)?;
            let upper_name = name.to_ascii_uppercase();
            validate_window_args(&upper_name, args.len())?;
            let frame = resolve_frame(spec);
            if !uses_window_frame(&upper_name) && matches!(frame.units, WindowFrameUnits::Range) {
                ResolvedFrame::validate_categories(&frame)?;
                Ok(ResolvedFrame::Ignored)
            } else {
                ResolvedFrame::new(&frame, cancel)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if rows.is_empty() {
        let col_names = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::AllColumns => "*".into(),
                SelectColumn::AllFromOld => "old.*".into(),
                SelectColumn::AllFromNew => "new.*".into(),
                SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
                SelectColumn::Expr { expr, .. } => expr_display_name(expr),
            })
            .collect();
        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: vec![],
        }));
    }

    let col_map = ColumnMap::new(columns);
    let slot_collations: Vec<Collation> = all_extracted
        .iter()
        .map(|(_, _, args, _)| {
            args.iter()
                .find_map(collation_of)
                .or_else(|| args.iter().find_map(|arg| operand_collation(arg, &col_map)))
                .unwrap_or_default()
        })
        .collect();
    let num_win = all_extracted.len();
    let mut arg_values: Vec<WindowValues> = Vec::with_capacity(num_win);
    for (window_idx, (_, _, args, _)) in all_extracted.iter().enumerate() {
        check_cancel_at(cancel, window_idx)?;
        let mut per_row = WindowValues::with_capacity(rows.len(), args.len())?;
        for (row_idx, row) in rows.iter().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            per_row.push_row(
                args.iter()
                    .map(|a| eval_expr(a, &EvalCtx::new(&col_map, row).with_cancel(cancel))),
            )?;
        }
        arg_values.push(per_row);
    }

    let n = rows.len();
    let mut row_results = WindowValues::nulls(n, num_win)?;

    for (win_idx, (_, fn_name, args, spec)) in all_extracted.iter().enumerate() {
        check_cancel_at(cancel, win_idx)?;
        let mut sort_keys: Vec<OrderByItem> = Vec::new();
        for pb in &spec.partition_by {
            sort_keys.push(OrderByItem {
                expr: pb.clone(),
                output_name: None,
                output_ordinal: None,
                descending: false,
                nulls_first: Some(true),
            });
        }
        sort_keys.extend(spec.order_by.clone());
        let key_collations: Vec<Collation> = sort_keys
            .iter()
            .map(|key| operand_collation(&key.expr, &col_map).unwrap_or_default())
            .collect();

        let mut indices: Vec<usize> = (0..n).collect();
        let mut keys = WindowValues::with_capacity(n, sort_keys.len())?;
        if !sort_keys.is_empty() {
            for (position, row) in rows.iter().enumerate() {
                check_cancel_at(cancel, position)?;
                note_window_key_evaluation();
                keys.push_row(sort_keys.iter().map(|o| {
                    eval_expr(&o.expr, &EvalCtx::new(&col_map, row).with_cancel(cancel))
                }))?;
            }
        }
        if !sort_keys.is_empty() {
            sort_indices_by(&mut indices, cancel, |a, b| {
                compare_sort_keys(&keys[a], &keys[b], &sort_keys, &key_collations)
            })?;
        }

        let part_count = spec.partition_by.len();
        let partition_collations = &key_collations[..part_count];
        let order_collations = &key_collations[part_count..];
        let mut partitions: Vec<(usize, usize)> = Vec::new();
        let mut part_start = 0;
        for pos in 1..n {
            check_cancel_at(cancel, pos)?;
            let same = part_count == 0
                || collated_keys_equal(
                    &keys[indices[pos - 1]][..part_count],
                    &keys[indices[pos]][..part_count],
                    partition_collations,
                );
            if !same {
                partitions.push((part_start, pos));
                part_start = pos;
            }
        }
        partitions.push((part_start, n));

        let frame = &frames[win_idx];
        let upper_name = fn_name.to_ascii_uppercase();

        for (partition_idx, &(ps, pe)) in partitions.iter().enumerate() {
            check_cancel_at(cancel, partition_idx)?;
            let part_len = pe - ps;
            let part_indices = &indices[ps..pe];
            let uses_frame = uses_window_frame(&upper_name);
            let range_uses_peers = uses_frame && frame.uses_peers();
            let peer_bounds = if range_uses_peers {
                peer_group_bounds(part_indices, &keys, part_count, order_collations, cancel)?
            } else {
                Vec::new()
            };

            match upper_name.as_str() {
                "ROW_NUMBER" => {
                    for (rank, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, rank)?;
                        row_results[orig_idx][win_idx] = Value::Integer(rank as i64 + 1);
                    }
                }
                "RANK" => {
                    if spec.order_by.is_empty() {
                        return Err(SqlError::WindowFunctionRequiresOrderBy("RANK".into()));
                    }
                    let mut rank = 1i64;
                    let mut prev_key: Option<&[Value]> = None;
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, pos)?;
                        let key = &keys[orig_idx][part_count..];
                        if let Some(pk) = prev_key {
                            if !collated_keys_equal(key, pk, order_collations) {
                                rank = pos as i64 + 1;
                            }
                        }
                        row_results[orig_idx][win_idx] = Value::Integer(rank);
                        prev_key = Some(key);
                    }
                }
                "DENSE_RANK" => {
                    if spec.order_by.is_empty() {
                        return Err(SqlError::WindowFunctionRequiresOrderBy("DENSE_RANK".into()));
                    }
                    let mut rank = 1i64;
                    let mut prev_key: Option<&[Value]> = None;
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, pos)?;
                        let key = &keys[orig_idx][part_count..];
                        if let Some(pk) = prev_key {
                            if !collated_keys_equal(key, pk, order_collations) {
                                rank += 1;
                            }
                        }
                        row_results[orig_idx][win_idx] = Value::Integer(rank);
                        prev_key = Some(key);
                    }
                }
                "NTILE" => {
                    let mut first = 0;
                    while first < part_len && arg_values[win_idx][part_indices[first]][0].is_null()
                    {
                        check_cancel_at(cancel, first)?;
                        first += 1;
                    }
                    if first == part_len {
                        continue;
                    }
                    let ntile_n = match &arg_values[win_idx][part_indices[first]][0] {
                        Value::Integer(n) if *n > 0 => {
                            (i128::from(*n).min(part_len as i128)) as usize
                        }
                        _ => {
                            return Err(SqlError::InvalidValue(
                                "NTILE argument must be a positive integer".into(),
                            ))
                        }
                    };
                    let base = part_len / ntile_n;
                    let remainder = part_len % ntile_n;
                    let mut bucket = 1usize;
                    let mut count_in_bucket = 0usize;
                    let bucket_size = |b: usize| -> usize {
                        if b <= remainder {
                            base + 1
                        } else {
                            base
                        }
                    };
                    for (pos, &orig_idx) in part_indices.iter().enumerate().skip(first) {
                        check_cancel_at(cancel, pos)?;
                        row_results[orig_idx][win_idx] = Value::Integer(bucket as i64);
                        count_in_bucket += 1;
                        if count_in_bucket >= bucket_size(bucket) && bucket < ntile_n {
                            bucket += 1;
                            count_in_bucket = 0;
                        }
                    }
                }
                "LAG" | "LEAD" => {
                    let is_lag = upper_name == "LAG";
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, pos)?;
                        let row_args = &arg_values[win_idx][orig_idx];
                        let offset = match row_args.get(1) {
                            None => 1,
                            Some(Value::Integer(offset)) => i128::from(*offset),
                            Some(Value::Null) => continue,
                            Some(other) => {
                                return Err(SqlError::TypeMismatch {
                                    expected: "INTEGER".into(),
                                    got: other.data_type().to_string(),
                                })
                            }
                        };
                        let target = pos as i128 + if is_lag { -offset } else { offset };
                        let val = if (0..part_len as i128).contains(&target) {
                            arg_values[win_idx][part_indices[target as usize]][0].clone()
                        } else {
                            row_args.get(2).cloned().unwrap_or(Value::Null)
                        };
                        row_results[orig_idx][win_idx] = val;
                    }
                }
                "FIRST_VALUE" => {
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, pos)?;
                        let range = frame.indices(pos, part_len, &peer_bounds);
                        if let Some(&source_idx) = part_indices[range].first() {
                            row_results[orig_idx][win_idx] =
                                arg_values[win_idx][source_idx][0].clone();
                        }
                    }
                }
                "LAST_VALUE" => {
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        check_cancel_at(cancel, pos)?;
                        let range = frame.indices(pos, part_len, &peer_bounds);
                        if let Some(&source_idx) = part_indices[range].last() {
                            row_results[orig_idx][win_idx] =
                                arg_values[win_idx][source_idx][0].clone();
                        }
                    }
                }
                "SUM" | "COUNT" | "AVG" => {
                    if frame.covers_partition() {
                        let mut acc = WindowAccumulator::new(&upper_name, args.len());
                        for (work, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, work)?;
                            acc.add(&arg_values[win_idx][orig_idx])?;
                        }
                        let result = acc.result()?;
                        for (work, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, work)?;
                            row_results[orig_idx][win_idx] = result.clone();
                        }
                    } else if frame.supports_forward_range() {
                        let mut acc = WindowAccumulator::new(&upper_name, args.len());
                        let mut previous = 0..0;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let current = frame.indices(pos, part_len, &peer_bounds);
                            if current.start != previous.start {
                                acc = WindowAccumulator::new(&upper_name, args.len());
                            }
                            for (work, add_pos) in
                                (previous.end.max(current.start)..current.end).enumerate()
                            {
                                check_cancel_at(cancel, work)?;
                                acc.add(&arg_values[win_idx][part_indices[add_pos]])?;
                            }
                            row_results[orig_idx][win_idx] = acc.result()?;
                            previous = current;
                        }
                    } else if frame.supports_sliding() {
                        let mut acc = WindowAccumulator::new(&upper_name, args.len());
                        let mut previous = 0..0;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let current = frame.indices(pos, part_len, &peer_bounds);
                            let mut add_start = previous.end.max(current.start);
                            for (work, remove_pos) in
                                (previous.start..current.start.min(previous.end)).enumerate()
                            {
                                check_cancel_at(cancel, work)?;
                                if !acc.try_remove(&arg_values[win_idx][part_indices[remove_pos]]) {
                                    // Expiring a real can lose small terms or leave
                                    // NaN/infinity behind. Rebuild in frame order.
                                    acc = WindowAccumulator::new(&upper_name, args.len());
                                    add_start = current.start;
                                    break;
                                }
                            }
                            for (work, add_pos) in (add_start..current.end).enumerate() {
                                check_cancel_at(cancel, work)?;
                                acc.add(&arg_values[win_idx][part_indices[add_pos]])?;
                            }
                            row_results[orig_idx][win_idx] = acc.result()?;
                            previous = current;
                        }
                    } else {
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let range = frame.indices(pos, part_len, &peer_bounds);
                            let mut acc = WindowAccumulator::new(&upper_name, args.len());
                            for (frame_iteration, fpos) in range.enumerate() {
                                check_cancel_at(cancel, frame_iteration)?;
                                acc.add(&arg_values[win_idx][part_indices[fpos]])?;
                            }
                            row_results[orig_idx][win_idx] = acc.result()?;
                        }
                    }
                }
                "MIN" | "MAX" => {
                    let is_min = upper_name == "MIN";
                    let value_collation = args
                        .first()
                        .and_then(|arg| operand_collation(arg, &col_map))
                        .unwrap_or_default();
                    if frame.covers_partition() {
                        let result = window_extreme(
                            part_indices,
                            &arg_values[win_idx],
                            is_min,
                            value_collation,
                            cancel,
                        )?;
                        for (work, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, work)?;
                            row_results[orig_idx][win_idx] = result.clone();
                        }
                    } else if frame.is_rows_prefix() || frame.supports_forward_range() {
                        // Forward folds retain exact tie representatives without
                        // assuming comparison transitivity. Peer-only frames reset
                        // at each disjoint group; growing prefixes retain state.
                        let mut acc = WindowExtreme::new(is_min, value_collation);
                        let mut previous = 0..0;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let current = frame.indices(pos, part_len, &peer_bounds);
                            if current.start != previous.start {
                                acc = WindowExtreme::new(is_min, value_collation);
                            }
                            for (work, add_pos) in
                                (previous.end.max(current.start)..current.end).enumerate()
                            {
                                check_cancel_at(cancel, work)?;
                                acc.add(&arg_values[win_idx][part_indices[add_pos]][0]);
                            }
                            row_results[orig_idx][win_idx] = acc.result();
                            previous = current;
                        }
                    } else if frame.supports_sliding()
                        && supports_monotonic_extrema(part_indices, &arg_values[win_idx], cancel)?
                    {
                        let mut deque = MonoDeque::new(is_min, value_collation);
                        let mut prev_end = 0;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let range = frame.indices(pos, part_len, &peer_bounds);
                            for (add_iteration, add_pos) in
                                (prev_end.max(range.start)..range.end).enumerate()
                            {
                                check_cancel_at(cancel, add_iteration)?;
                                deque.push(
                                    add_pos,
                                    arg_values[win_idx][part_indices[add_pos]][0].clone(),
                                );
                            }
                            deque.pop_expired(range.start);
                            row_results[orig_idx][win_idx] = deque.current();
                            prev_end = range.end;
                        }
                    } else {
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            check_cancel_at(cancel, pos)?;
                            let range = frame.indices(pos, part_len, &peer_bounds);
                            row_results[orig_idx][win_idx] = window_extreme(
                                &part_indices[range],
                                &arg_values[win_idx],
                                is_min,
                                value_collation,
                                cancel,
                            )?;
                        }
                    }
                }
                other => {
                    return Err(SqlError::Unsupported(format!("window function: {other}")));
                }
            }
        }
    }

    let base_col_count = columns.len();
    let mut extended_columns: Vec<ColumnDef> = columns.to_vec();
    for (i, (slot_name, _, _, _)) in all_extracted.iter().enumerate() {
        extended_columns.push(ColumnDef {
            name: slot_name.clone(),
            data_type: DataType::Null,
            nullable: true,
            position: (base_col_count + i) as u16,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
            is_with_timezone: false,
            generated_expr: None,
            generated_sql: None,
            generated_kind: None,
            collation: slot_collations[i],
        });
    }

    for (row_idx, row) in rows.iter_mut().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        row.extend_from_slice(&row_results[row_idx]);
    }

    ctx.check()?;

    let rewritten_stmt = SelectStmt {
        columns: rewritten_columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        from_subquery: stmt.from_subquery.clone(),
        from_args: stmt.from_args.clone(),
        from_json_table: stmt.from_json_table.clone(),
        joins: stmt.joins.clone(),
        distinct: stmt.distinct,
        where_clause: None,
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by: vec![],
        having: None,
    };

    super::process_select(
        rows,
        super::SelectCtx::new(&extended_columns, &rewritten_stmt, ctx.cancel)
            .predicate_applied(true),
    )
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
