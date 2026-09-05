use crate::encoding::{
    decode_columns, decode_columns_into, decode_composite_key, decode_key_value, decode_pk_integer,
    decode_pk_into, decode_row_into, decode_row_push, encode_composite_key, row_non_pk_count,
    ProjectedOffsetPlan,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, operand_collation, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::types::*;

pub(super) type ReturningRow = (Option<Vec<Value>>, Option<Vec<Value>>);

pub(super) const CANCEL_CHECK_INTERVAL: usize = 256;
const CANCELLABLE_SORT_RUN: usize = 1_024;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct InjectedSortComparatorPanic;

#[cfg(test)]
thread_local! {
    static PANIC_ON_NEXT_SORT_COMPARISON: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) struct InjectSortComparatorPanic {
    previous: bool,
}

#[cfg(test)]
impl Drop for InjectSortComparatorPanic {
    fn drop(&mut self) {
        PANIC_ON_NEXT_SORT_COMPARISON.with(|armed| armed.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn inject_sort_comparator_panic() -> InjectSortComparatorPanic {
    let previous = PANIC_ON_NEXT_SORT_COMPARISON.with(|armed| armed.replace(true));
    InjectSortComparatorPanic { previous }
}

#[cfg(test)]
fn maybe_inject_sort_comparator_panic() {
    PANIC_ON_NEXT_SORT_COMPARISON.with(|armed| {
        if armed.replace(false) {
            std::panic::panic_any(InjectedSortComparatorPanic);
        }
    });
}

#[inline]
pub(super) fn check_cancel(cancel: Option<&citadel::CancelToken>) -> Result<()> {
    if cancel.is_some_and(|token| token.is_cancelled()) {
        Err(SqlError::Storage(citadel_core::Error::Interrupted))
    } else {
        Ok(())
    }
}

#[inline]
pub(super) fn check_cancel_at(
    cancel: Option<&citadel::CancelToken>,
    iteration: usize,
) -> Result<()> {
    let Some(token) = cancel else {
        return Ok(());
    };
    if iteration.is_multiple_of(CANCEL_CHECK_INTERVAL) && token.is_cancelled() {
        return Err(SqlError::Storage(citadel_core::Error::Interrupted));
    }
    Ok(())
}

#[inline]
fn check_sort_cancel(cancel: Option<&citadel::CancelToken>) -> Result<()> {
    check_cancel(cancel)
}

/// Stable-sort row indices without using unwinding as control flow. With no token
/// this stays on the standard-library fast path; with one, cancellation is bounded
/// by `CANCELLABLE_SORT_RUN` and loops poll every `CANCEL_CHECK_INTERVAL`
/// comparisons. Only indices move, so the caller's rows survive a cancellation.
pub(super) fn sort_indices_by(
    indices: &mut [usize],
    cancel: Option<&citadel::CancelToken>,
    mut compare: impl FnMut(usize, usize) -> std::cmp::Ordering,
) -> Result<()> {
    check_sort_cancel(cancel)?;
    if indices.len() < 2 {
        return Ok(());
    }

    if cancel.is_none() {
        indices.sort_by(|&a, &b| {
            #[cfg(test)]
            maybe_inject_sort_comparator_panic();
            compare(a, b)
        });
        return Ok(());
    }

    for run in indices.chunks_mut(CANCELLABLE_SORT_RUN) {
        run.sort_by(|&a, &b| compare(a, b));
        check_sort_cancel(cancel)?;
    }
    if indices.len() <= CANCELLABLE_SORT_RUN {
        return Ok(());
    }

    let len = indices.len();
    let mut scratch = indices.to_vec();
    let mut width = CANCELLABLE_SORT_RUN;
    let mut source_is_indices = true;
    let mut merge_work = 0usize;

    while width < len {
        if source_is_indices {
            merge_index_runs(
                indices,
                &mut scratch,
                width,
                cancel,
                &mut merge_work,
                &mut compare,
            )?;
        } else {
            merge_index_runs(
                &scratch,
                indices,
                width,
                cancel,
                &mut merge_work,
                &mut compare,
            )?;
        }
        source_is_indices = !source_is_indices;
        width = width.saturating_mul(2);
        check_sort_cancel(cancel)?;
    }

    if !source_is_indices {
        indices.copy_from_slice(&scratch);
    }
    Ok(())
}

/// Sort owned materialized values with cancellable index movement. The common
/// no-token path stays on the standard-library sort; the token path does not
/// move a value until the fallible index sort has completed.
pub(crate) fn sort_vec_by<T>(
    mut values: Vec<T>,
    cancel: Option<&citadel::CancelToken>,
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> Result<Vec<T>> {
    check_cancel(cancel)?;
    if cancel.is_none() {
        values.sort_by(compare);
        return Ok(values);
    }
    sort_vec_by_indices(values, cancel, &mut compare)
}

pub(super) fn sort_lists_by_len<T>(
    lists: Vec<Vec<T>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<T>>> {
    sort_vec_by(lists, cancel, |a, b| a.len().cmp(&b.len()))
}

/// As [`sort_vec_by`], retaining the allocation-free unstable std path when
/// cancellation is not installed.
pub(super) fn sort_vec_unstable_by<T>(
    mut values: Vec<T>,
    cancel: Option<&citadel::CancelToken>,
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> Result<Vec<T>> {
    check_cancel(cancel)?;
    if cancel.is_none() {
        values.sort_unstable_by(compare);
        return Ok(values);
    }
    sort_vec_by_indices(values, cancel, &mut compare)
}

fn sort_vec_by_indices<T>(
    values: Vec<T>,
    cancel: Option<&citadel::CancelToken>,
    compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
) -> Result<Vec<T>> {
    let mut indices: Vec<usize> = (0..values.len()).collect();
    sort_indices_by(&mut indices, cancel, |a, b| compare(&values[a], &values[b]))?;
    check_cancel(cancel)?;

    let len = values.len();
    let mut slots: Vec<Option<T>> = values.into_iter().map(Some).collect();
    let mut sorted = Vec::with_capacity(len);
    for (output_idx, source_idx) in indices.into_iter().enumerate() {
        check_cancel_at(cancel, output_idx)?;
        sorted.push(slots[source_idx].take().expect("sort index used once"));
    }
    check_cancel(cancel)?;
    Ok(sorted)
}

fn merge_index_runs(
    source: &[usize],
    destination: &mut [usize],
    width: usize,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
    compare: &mut impl FnMut(usize, usize) -> std::cmp::Ordering,
) -> Result<()> {
    let len = source.len();
    let step = width.saturating_mul(2);
    let mut start = 0;

    while start < len {
        let middle = start.saturating_add(width).min(len);
        let end = start.saturating_add(step).min(len);
        let (mut left, mut right, mut out) = (start, middle, start);

        while left < middle && right < end {
            check_cancel_at(cancel, *work)?;
            *work = work.wrapping_add(1);
            if compare(source[left], source[right]) != std::cmp::Ordering::Greater {
                destination[out] = source[left];
                left += 1;
            } else {
                destination[out] = source[right];
                right += 1;
            }
            out += 1;
        }

        let left_len = middle - left;
        destination[out..out + left_len].copy_from_slice(&source[left..middle]);
        out += left_len;
        destination[out..out + end - right].copy_from_slice(&source[right..end]);
        start = end;
    }
    Ok(())
}

/// Move the selected rows only after the fallible index work has completed.
/// On the token path, cancellation during collection restores every row to its
/// source slot before returning. The final assignment moves only `Vec` handles
/// and is the sort's completion boundary.
fn reorder_rows_by_indices(
    rows: &mut [Vec<Value>],
    indices: &[usize],
    output_len: usize,
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    debug_assert!(output_len <= indices.len());
    if cancel.is_none() {
        let sorted: Vec<Vec<Value>> = indices[..output_len]
            .iter()
            .map(|&source_idx| std::mem::take(&mut rows[source_idx]))
            .collect();
        rows[..output_len]
            .iter_mut()
            .zip(sorted)
            .for_each(|(slot, row)| *slot = row);
        return Ok(());
    }

    let mut selected: Vec<(usize, Vec<Value>)> = Vec::with_capacity(output_len);
    for (output_idx, &source_idx) in indices[..output_len].iter().enumerate() {
        if let Err(err) = check_cancel_at(cancel, output_idx) {
            for (source_idx, row) in selected {
                rows[source_idx] = row;
            }
            return Err(err);
        }
        selected.push((source_idx, std::mem::take(&mut rows[source_idx])));
    }
    if let Err(err) = check_cancel(cancel) {
        for (source_idx, row) in selected {
            rows[source_idx] = row;
        }
        return Err(err);
    }
    for (output_idx, (_, row)) in selected.into_iter().enumerate() {
        rows[output_idx] = row;
    }
    Ok(())
}

/// Keep the smallest `k` indices in the first `k` slots, sorted. The token path
/// uses a fallible three-way introselect, preserving the expected linear-time
/// shape of `select_nth_unstable_by` while bounding adversarial partitions with
/// a checked sort fallback.
pub(super) fn topk_indices_by(
    indices: &mut [usize],
    k: usize,
    cancel: Option<&citadel::CancelToken>,
    mut compare: impl FnMut(usize, usize) -> std::cmp::Ordering,
) -> Result<()> {
    check_sort_cancel(cancel)?;
    if k == 0 || indices.is_empty() {
        return Ok(());
    }
    debug_assert!(k <= indices.len());

    if cancel.is_none() {
        if k < indices.len() {
            indices.select_nth_unstable_by(k - 1, |&a, &b| compare(a, b));
        }
        indices[..k].sort_by(|&a, &b| compare(a, b));
        return Ok(());
    }

    if k < indices.len() {
        select_index_nth(indices, k - 1, cancel, &mut compare)?;
    }
    sort_indices_by(&mut indices[..k], cancel, &mut compare)?;
    check_sort_cancel(cancel)
}

fn select_index_nth(
    indices: &mut [usize],
    nth: usize,
    cancel: Option<&citadel::CancelToken>,
    compare: &mut impl FnMut(usize, usize) -> std::cmp::Ordering,
) -> Result<()> {
    let mut left = 0;
    let mut right = indices.len();
    let log2 = usize::BITS as usize - indices.len().leading_zeros() as usize;
    let mut partition_budget = log2.saturating_mul(2);
    let mut comparison_work = 0usize;

    while right - left > 1 {
        if partition_budget == 0 {
            return sort_indices_by(&mut indices[left..right], cancel, compare);
        }
        partition_budget -= 1;

        let middle = left + (right - left) / 2;
        let pivot = median_index_value(
            indices[left],
            indices[middle],
            indices[right - 1],
            cancel,
            &mut comparison_work,
            compare,
        )?;
        let (mut lower, mut cursor, mut upper) = (left, left, right);

        while cursor < upper {
            check_cancel_at(cancel, comparison_work)?;
            comparison_work = comparison_work.wrapping_add(1);
            match compare(indices[cursor], pivot) {
                std::cmp::Ordering::Less => {
                    indices.swap(lower, cursor);
                    lower += 1;
                    cursor += 1;
                }
                std::cmp::Ordering::Equal => cursor += 1,
                std::cmp::Ordering::Greater => {
                    upper -= 1;
                    indices.swap(cursor, upper);
                }
            }
        }

        if nth < lower {
            right = lower;
        } else if nth >= upper {
            left = upper;
        } else {
            return check_sort_cancel(cancel);
        }
    }
    check_sort_cancel(cancel)
}

fn median_index_value(
    mut a: usize,
    mut b: usize,
    mut c: usize,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
    compare: &mut impl FnMut(usize, usize) -> std::cmp::Ordering,
) -> Result<usize> {
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    if compare(a, b) == std::cmp::Ordering::Greater {
        std::mem::swap(&mut a, &mut b);
    }
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    if compare(b, c) == std::cmp::Ordering::Greater {
        std::mem::swap(&mut b, &mut c);
    }
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    if compare(a, b) == std::cmp::Ordering::Greater {
        std::mem::swap(&mut a, &mut b);
    }
    Ok(b)
}

pub fn drain_deferred_fk_checks(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
    let checks = wtx.take_deferred_fk_checks();
    for chk in checks {
        if wtx.fk_check_cached(&chk.foreign_table, &chk.parent_key) {
            continue;
        }
        let found = wtx
            .table_get(&chk.foreign_table, &chk.parent_key)
            .map_err(SqlError::Storage)?;
        if found.is_none() {
            return Err(SqlError::ForeignKeyViolation(chk.fk_name));
        }
        wtx.mark_fk_verified(&chk.foreign_table, &chk.parent_key);
    }
    Ok(())
}

#[inline]
pub(super) fn coerce_for_column(value: Value, col: &ColumnDef, strict: bool) -> Result<Value> {
    coerce_for_type(value, col.data_type, strict)
}

#[inline]
fn coerce_for_type(value: Value, data_type: DataType, strict: bool) -> Result<Value> {
    let got = value.data_type();
    let coerced = if strict {
        value.strict_coerce(data_type)
    } else {
        value.coerce_into(data_type)
    };
    coerced.ok_or_else(|| SqlError::TypeMismatch {
        expected: data_type.to_string(),
        got: got.to_string(),
    })
}

#[derive(Clone)]
pub(super) enum FastGenEval {
    None,
    /// `(col * mul) + add` over a single Integer column.
    IntColMulAdd {
        col_schema_idx: usize,
        mul: i64,
        add: i64,
    },
    /// `col1 + col2` over two Integer columns.
    IntColAddCol {
        left_idx: usize,
        right_idx: usize,
    },
}

pub(super) fn detect_fast_gen_eval(expr: &Expr, table_schema: &TableSchema) -> FastGenEval {
    let resolve_col_idx = |e: &Expr| -> Option<usize> {
        match e {
            Expr::Column(name) => table_schema.column_index(name),
            Expr::QualifiedColumn { column, .. } => table_schema.column_index(column),
            _ => None,
        }
    };
    let int_lit = |e: &Expr| match e {
        Expr::Literal(Value::Integer(n)) => Some(*n),
        _ => None,
    };

    if let Expr::BinaryOp { left, op, right } = expr {
        match op {
            BinOp::Add => {
                if let (Some(a), Some(b)) = (resolve_col_idx(left), resolve_col_idx(right)) {
                    return FastGenEval::IntColAddCol {
                        left_idx: a,
                        right_idx: b,
                    };
                }
                if let Expr::BinaryOp {
                    left: ml,
                    op: BinOp::Mul,
                    right: mr,
                } = left.as_ref()
                {
                    if let (Some(c), Some(m), Some(a)) =
                        (resolve_col_idx(ml), int_lit(mr), int_lit(right))
                    {
                        return FastGenEval::IntColMulAdd {
                            col_schema_idx: c,
                            mul: m,
                            add: a,
                        };
                    }
                    if let (Some(m), Some(c), Some(a)) =
                        (int_lit(ml), resolve_col_idx(mr), int_lit(right))
                    {
                        return FastGenEval::IntColMulAdd {
                            col_schema_idx: c,
                            mul: m,
                            add: a,
                        };
                    }
                }
            }
            BinOp::Mul => {
                if let (Some(c), Some(m)) = (resolve_col_idx(left), int_lit(right)) {
                    return FastGenEval::IntColMulAdd {
                        col_schema_idx: c,
                        mul: m,
                        add: 0,
                    };
                }
                if let (Some(m), Some(c)) = (int_lit(left), resolve_col_idx(right)) {
                    return FastGenEval::IntColMulAdd {
                        col_schema_idx: c,
                        mul: m,
                        add: 0,
                    };
                }
            }
            _ => {}
        }
    }
    FastGenEval::None
}

#[cfg(test)]
pub(super) fn eval_fast_gen(
    fast: &FastGenEval,
    expr: &Expr,
    partial_row: &[Value],
    col_map: &ColumnMap,
) -> Result<Value> {
    eval_fast_gen_with_cancel(fast, expr, partial_row, col_map, None)
}

pub(super) fn eval_fast_gen_with_cancel(
    fast: &FastGenEval,
    expr: &Expr,
    partial_row: &[Value],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    match fast {
        FastGenEval::IntColMulAdd {
            col_schema_idx,
            mul,
            add,
        } => match partial_row[*col_schema_idx] {
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_mul(*mul).wrapping_add(*add))),
            _ => eval_expr(
                expr,
                &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
            ),
        },
        FastGenEval::IntColAddCol {
            left_idx,
            right_idx,
        } => match (&partial_row[*left_idx], &partial_row[*right_idx]) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_add(*b))),
            _ => eval_expr(
                expr,
                &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
            ),
        },
        FastGenEval::None => eval_expr(
            expr,
            &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
        ),
    }
}

/// Integer overflow returns `IntegerOverflow` (never wraps); non-integer/NULL operands
/// fall back to `eval_expr`, so results are byte-identical.
pub(super) fn eval_fast_gen_checked_with_cancel(
    fast: &FastGenEval,
    expr: &Expr,
    partial_row: &[Value],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    match fast {
        FastGenEval::IntColMulAdd {
            col_schema_idx,
            mul,
            add,
        } => match partial_row[*col_schema_idx] {
            Value::Integer(v) => v
                .checked_mul(*mul)
                .and_then(|p| p.checked_add(*add))
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => eval_expr(
                expr,
                &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
            ),
        },
        FastGenEval::IntColAddCol {
            left_idx,
            right_idx,
        } => match (&partial_row[*left_idx], &partial_row[*right_idx]) {
            (Value::Integer(a), Value::Integer(b)) => a
                .checked_add(*b)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => eval_expr(
                expr,
                &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
            ),
        },
        FastGenEval::None => eval_expr(
            expr,
            &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
        ),
    }
}

pub(super) struct PartialDecodeCtx {
    strict: bool,
    pk_positions: Vec<(usize, usize)>,
    nonpk_targets: Vec<usize>,
    nonpk_schema: Vec<usize>,
    num_cols: usize,
    num_pk_cols: usize,
    remaining_pk: Vec<(usize, usize)>,
    remaining_nonpk_targets: Vec<usize>,
    remaining_nonpk_schema: Vec<usize>,
    nonpk_defaults: Vec<(usize, usize, Value)>,
    remaining_defaults: Vec<(usize, usize, Value)>,
    virtuals_to_eval: Vec<(usize, Expr, DataType, bool, FastGenEval)>,
    col_map: ColumnMap,
    /// Columns this ctx writes; the only ones a reused buffer must clear.
    reset_cols: Vec<usize>,
}

impl PartialDecodeCtx {
    pub(super) fn new(schema: &TableSchema, needed: &[usize]) -> Self {
        Self::new_inner(schema, needed, None, false)
            .expect("non-strict partial decoder construction cannot fail")
    }

    pub(super) fn new_with_cancel(
        schema: &TableSchema,
        needed: &[usize],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Self> {
        Self::new_inner(schema, needed, cancel, true)
    }

    fn new_inner(
        schema: &TableSchema,
        needed: &[usize],
        cancel: Option<&citadel::CancelToken>,
        propagate_default_error: bool,
    ) -> Result<Self> {
        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let mut pk_positions = Vec::new();
        let mut nonpk_targets = Vec::new();
        let mut nonpk_schema = Vec::new();

        let mut expanded_needed: Vec<usize> = needed.to_vec();
        if schema.has_virtual_columns() {
            let mut to_add: rustc_hash::FxHashSet<usize> = rustc_hash::FxHashSet::default();
            for &col in needed {
                let c = &schema.columns[col];
                if matches!(
                    c.generated_kind,
                    Some(crate::parser::GeneratedKind::Virtual)
                ) {
                    let mut refs = Vec::new();
                    super::ddl::collect_column_refs(c.generated_expr.as_ref().unwrap(), &mut refs);
                    for r in refs {
                        if let Some(idx) = schema.column_index(&r) {
                            if !needed.contains(&idx) {
                                to_add.insert(idx);
                            }
                        }
                    }
                }
            }
            for idx in to_add {
                expanded_needed.push(idx);
            }
        }
        let needed: &[usize] = &expanded_needed;

        for &col in needed {
            if let Some(pk_pos) = schema
                .primary_key_columns
                .iter()
                .position(|&i| i as usize == col)
            {
                pk_positions.push((pk_pos, col));
            } else if let Some(nonpk_order) = non_pk.iter().position(|&i| i == col) {
                nonpk_targets.push(enc_pos[nonpk_order] as usize);
                nonpk_schema.push(col);
            }
        }

        let mut paired: Vec<(usize, usize)> = nonpk_targets
            .iter()
            .copied()
            .zip(nonpk_schema.iter().copied())
            .collect();
        paired.sort_by_key(|&(t, _)| t);
        nonpk_targets = paired.iter().map(|&(t, _)| t).collect();
        nonpk_schema = paired.iter().map(|&(_, s)| s).collect();

        let needed_set: rustc_hash::FxHashSet<usize> = needed.iter().copied().collect();
        let mut remaining_pk = Vec::new();
        for (pk_pos, &pk_col) in schema.primary_key_columns.iter().enumerate() {
            if !needed_set.contains(&(pk_col as usize)) {
                remaining_pk.push((pk_pos, pk_col as usize));
            }
        }
        let mut remaining_nonpk_targets = Vec::new();
        let mut remaining_nonpk_schema = Vec::new();
        for (nonpk_order, &col) in non_pk.iter().enumerate() {
            if !needed_set.contains(&col) {
                remaining_nonpk_targets.push(enc_pos[nonpk_order] as usize);
                remaining_nonpk_schema.push(col);
            }
        }

        let mut nonpk_defaults = Vec::new();
        for (&phys_pos, &schema_col) in nonpk_targets.iter().zip(nonpk_schema.iter()) {
            if let Some(ref expr) = schema.columns[schema_col].default_expr {
                match eval_const_expr_with_cancel(expr, cancel) {
                    Ok(val) => nonpk_defaults.push((phys_pos, schema_col, val)),
                    Err(e) if propagate_default_error => return Err(e),
                    Err(_) => {}
                }
            }
        }
        let mut remaining_defaults = Vec::new();
        for (&phys_pos, &schema_col) in remaining_nonpk_targets
            .iter()
            .zip(remaining_nonpk_schema.iter())
        {
            if let Some(ref expr) = schema.columns[schema_col].default_expr {
                match eval_const_expr_with_cancel(expr, cancel) {
                    Ok(val) => remaining_defaults.push((phys_pos, schema_col, val)),
                    Err(e) if propagate_default_error => return Err(e),
                    Err(_) => {}
                }
            }
        }

        let mut virtuals_to_eval = Vec::new();
        if schema.has_virtual_columns() {
            for &col in needed {
                let c = &schema.columns[col];
                if matches!(
                    c.generated_kind,
                    Some(crate::parser::GeneratedKind::Virtual)
                ) {
                    let expr = c.generated_expr.as_ref().unwrap();
                    let fast = detect_fast_gen_eval(expr, schema);
                    virtuals_to_eval.push((col, expr.clone(), c.data_type, c.nullable, fast));
                }
            }
        }

        let mut reset_cols = expanded_needed;
        reset_cols.sort_unstable();
        reset_cols.dedup();

        Ok(Self {
            strict: schema.is_strict(),
            pk_positions,
            nonpk_targets,
            nonpk_schema,
            num_cols: schema.columns.len(),
            num_pk_cols: schema.primary_key_columns.len(),
            remaining_pk,
            remaining_nonpk_targets,
            remaining_nonpk_schema,
            nonpk_defaults,
            remaining_defaults,
            virtuals_to_eval,
            col_map: ColumnMap::new(&schema.columns),
            reset_cols,
        })
    }

    fn materialize_virtuals(
        &self,
        row: &mut [Value],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<()> {
        for (pos, expr, dt, nullable, fast) in &self.virtuals_to_eval {
            let val = eval_fast_gen_checked_with_cancel(fast, expr, row, &self.col_map, cancel)?;
            row[*pos] = if val.is_null() {
                if !*nullable {
                    return Err(SqlError::InvalidValue(format!(
                        "VIRTUAL generated column at position {pos} produced NULL but is NOT NULL"
                    )));
                }
                Value::Null
            } else {
                coerce_for_type(val, *dt, self.strict)?
            };
        }
        Ok(())
    }

    pub(super) fn decode_with_cancel(
        &self,
        key: &[u8],
        value: &[u8],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Vec<Value>> {
        let mut row = Vec::new();
        self.decode_into_with_cancel(key, value, &mut row, cancel)?;
        Ok(row)
    }

    pub(super) fn decode_into_with_cancel(
        &self,
        key: &[u8],
        value: &[u8],
        row: &mut Vec<Value>,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<()> {
        if row.len() != self.num_cols {
            row.clear();
            row.resize(self.num_cols, Value::Null);
        } else {
            for &p in &self.reset_cols {
                row[p] = Value::Null;
            }
        }

        if self.pk_positions.len() == 1 && self.num_pk_cols == 1 {
            let (_, schema_col) = self.pk_positions[0];
            let (v, _) = decode_key_value(key)?;
            row[schema_col] = v;
        } else if !self.pk_positions.is_empty() {
            let mut pk_values = decode_composite_key(key, self.num_pk_cols)?;
            for &(pk_pos, schema_col) in &self.pk_positions {
                row[schema_col] = std::mem::take(&mut pk_values[pk_pos]);
            }
        }

        if !self.nonpk_targets.is_empty() {
            decode_columns_into(value, &self.nonpk_targets, &self.nonpk_schema, row)?;
        }

        if !self.nonpk_defaults.is_empty() {
            let stored = row_non_pk_count(value);
            for (nonpk_idx, schema_col, default) in &self.nonpk_defaults {
                if *nonpk_idx >= stored {
                    row[*schema_col] = default.clone();
                }
            }
        }

        if !self.virtuals_to_eval.is_empty() {
            self.materialize_virtuals(row, cancel)?;
        }

        Ok(())
    }

    pub(super) fn complete(
        &self,
        mut row: Vec<Value>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Vec<Value>> {
        if !self.remaining_pk.is_empty() {
            let mut pk_values = decode_composite_key(key, self.num_pk_cols)?;
            for &(pk_pos, schema_col) in &self.remaining_pk {
                row[schema_col] = std::mem::take(&mut pk_values[pk_pos]);
            }
        }
        if !self.remaining_nonpk_targets.is_empty() {
            let mut values = decode_columns(value, &self.remaining_nonpk_targets)?;
            for (i, &schema_col) in self.remaining_nonpk_schema.iter().enumerate() {
                row[schema_col] = std::mem::take(&mut values[i]);
            }
        }
        if !self.remaining_defaults.is_empty() {
            let stored = row_non_pk_count(value);
            for (nonpk_idx, schema_col, default) in &self.remaining_defaults {
                if *nonpk_idx >= stored {
                    row[*schema_col] = default.clone();
                }
            }
        }
        Ok(row)
    }
}

/// Decodes a projection straight into a right-sized output row. Built only for the common
/// shape (single-column PK, no VIRTUAL or defaulted columns); other shapes fall back.
pub(super) struct ProjectedDecoder {
    single_pk_out: Option<usize>,
    pk_is_integer: bool,
    nonpk_targets: Vec<usize>,
    nonpk_out: Vec<usize>,
    offset_plan: Option<ProjectedOffsetPlan>,
    /// pk at output 0 then non-PK in ascending output==physical order, so the row builds by push.
    monotonic: bool,
    arity: usize,
}

impl ProjectedDecoder {
    pub(super) fn try_new(schema: &TableSchema, idxs: &[usize]) -> Option<Self> {
        if schema.primary_key_columns.len() != 1 {
            return None;
        }
        let pk_col = schema.primary_key_columns[0] as usize;
        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let mut single_pk_out = None;
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        for (out_pos, &col) in idxs.iter().enumerate() {
            let c = &schema.columns[col];
            if matches!(
                c.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                return None;
            }
            if col == pk_col {
                single_pk_out = Some(out_pos);
                continue;
            }
            if c.default_expr.is_some() {
                return None;
            }
            let nonpk_order = non_pk.iter().position(|&i| i == col)?;
            pairs.push((enc_pos[nonpk_order] as usize, out_pos));
        }
        pairs.sort_by_key(|&(t, _)| t);
        // Sized by physical slot count, not live: enc_pos holds physical slots that exceed
        // the live count after a non-trailing DROP COLUMN (dropped slots stay tag 0 -> decline).
        let mut phys_tags = vec![0u8; schema.physical_non_pk_count()];
        for (k, &schema_col) in non_pk.iter().enumerate() {
            phys_tags[enc_pos[k] as usize] = schema.columns[schema_col].data_type.type_tag();
        }
        let offset_plan = ProjectedOffsetPlan::build(&phys_tags, &pairs);
        let monotonic =
            single_pk_out == Some(0) && pairs.iter().enumerate().all(|(i, &(_, o))| o == i + 1);
        Some(Self {
            single_pk_out,
            pk_is_integer: matches!(schema.columns[pk_col].data_type, DataType::Integer),
            nonpk_targets: pairs.iter().map(|&(t, _)| t).collect(),
            nonpk_out: pairs.iter().map(|&(_, o)| o).collect(),
            offset_plan,
            monotonic,
            arity: idxs.len(),
        })
    }

    pub(super) fn decode(&self, key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(self.arity);
        if self.monotonic {
            out.push(self.decode_pk(key)?);
            if let Some(plan) = &self.offset_plan {
                if plan.decode_push(value, &mut out)? {
                    return Ok(out);
                }
                out.truncate(1);
            }
            out.resize(self.arity, Value::Null);
        } else {
            out.resize(self.arity, Value::Null);
            if let Some(j) = self.single_pk_out {
                out[j] = self.decode_pk(key)?;
            }
            if let Some(plan) = &self.offset_plan {
                if plan.decode_into(value, &mut out)? {
                    return Ok(out);
                }
            }
        }
        if !self.nonpk_targets.is_empty() {
            decode_columns_into(value, &self.nonpk_targets, &self.nonpk_out, &mut out)?;
        }
        Ok(out)
    }

    #[inline]
    fn decode_pk(&self, key: &[u8]) -> Result<Value> {
        if self.pk_is_integer {
            Ok(Value::Integer(decode_pk_integer(key)?))
        } else {
            Ok(decode_key_value(key)?.0)
        }
    }
}

pub(crate) fn decode_full_row_with_cancel(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(schema.columns.len());
    decode_full_row_into_with_cancel(schema, key, value, &mut row, cancel)?;
    Ok(row)
}

/// True when a full row can be push-built (single PK at logical 0, no virtual columns,
/// no dropped slots).
pub(crate) fn full_row_push_eligible(schema: &TableSchema) -> bool {
    schema.primary_key_columns.len() == 1
        && schema.primary_key_columns[0] == 0
        && !schema.has_virtual_columns()
        && schema.dropped_non_pk_slots().is_empty()
}

/// Push-build a full `SELECT *` row (pk then each physical cell). Caller gates on
/// `full_row_push_eligible`. `None` when stored count != live schema (pre-column-add row).
pub(crate) fn decode_full_row_push(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
) -> Result<Option<Vec<Value>>> {
    let mut row = Vec::with_capacity(schema.columns.len());
    row.push(decode_key_value(key)?.0);
    if decode_row_push(value, schema.columns.len() - 1, &mut row)? {
        Ok(Some(row))
    } else {
        Ok(None)
    }
}

#[inline]
pub(crate) fn decode_full_row_into_with_cancel(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    row: &mut Vec<Value>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    if row.len() != schema.columns.len() {
        row.clear();
        row.resize(schema.columns.len(), Value::Null);
    } else {
        for v in row.iter_mut() {
            *v = Value::Null;
        }
    }
    decode_pk_into(
        key,
        schema.primary_key_columns.len(),
        row,
        schema.pk_indices(),
    )?;
    let mapping = schema.decode_col_mapping();
    decode_row_into(value, row, mapping)?;
    let stored_count = row_non_pk_count(value);
    if stored_count < mapping.len() {
        for &logical_idx in mapping.iter().skip(stored_count) {
            if logical_idx != usize::MAX {
                if let Some(ref expr) = schema.columns[logical_idx].default_expr {
                    row[logical_idx] = eval_const_expr_with_cancel(expr, cancel)?;
                }
            }
        }
    }
    if schema.has_virtual_columns() {
        materialize_virtual_with_cancel(schema, row, cancel)?;
    }
    Ok(())
}

/// Caller must ensure all non-virtual columns in `row` are already populated.
#[inline]
pub(crate) fn materialize_virtual_with_cancel(
    schema: &TableSchema,
    row: &mut [Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    let col_map = schema.column_map();
    for col in &schema.columns {
        if matches!(
            col.generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(col_map, row).with_cancel(cancel),
            )?;
            let pos = col.position as usize;
            row[pos] = if val.is_null() {
                Value::Null
            } else {
                coerce_for_column(val, col, schema.is_strict())?
            };
        }
    }
    Ok(())
}

pub(super) fn eval_const_expr(expr: &Expr) -> Result<Value> {
    eval_const_expr_with_cancel(expr, None)
}

pub(super) fn eval_const_expr_with_cancel(
    expr: &Expr,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    static EMPTY: std::sync::OnceLock<ColumnMap> = std::sync::OnceLock::new();
    let empty = EMPTY.get_or_init(|| ColumnMap::new(&[]));
    eval_expr(expr, &EvalCtx::new(empty, &[]).with_cancel(cancel))
}

pub(super) fn eval_const_int(expr: &Expr) -> Result<i64> {
    match eval_const_expr(expr)? {
        Value::Integer(i) => Ok(i),
        other => Err(SqlError::TypeMismatch {
            expected: "INTEGER".into(),
            got: other.data_type().to_string(),
        }),
    }
}

pub(super) fn sort_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    validate_order_by_ordinals(order_by, columns.len())?;
    if rows.is_empty() {
        return Ok(());
    }
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        sort_indices_by(&mut indices, cancel, |a, b| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        })?;
    } else if let Some((col_idx, coll)) = try_resolve_collated_flat_sort(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let keys = precompute_collated_keys_with_cancel(rows, col_idx, coll, cancel)?;
        check_sort_cancel(cancel)?;
        sort_indices_by(&mut indices, cancel, |a, b| {
            compare_collated_key(
                &keys[a],
                &keys[b],
                &rows[a][col_idx],
                &rows[b][col_idx],
                desc,
                nulls_first,
            )
        })?;
    } else {
        let keys = extract_sort_keys_with_cancel(rows, order_by, &col_map, cancel)?;
        let collations = sort_key_collations(order_by, &col_map);
        check_sort_cancel(cancel)?;
        sort_indices_by(&mut indices, cancel, |a, b| {
            compare_sort_keys(&keys[a], &keys[b], order_by, &collations)
        })?;
    }
    check_sort_cancel(cancel)?;

    reorder_rows_by_indices(rows, &indices, rows.len(), cancel)
}

/// Sort `rows` by keys taken from the rows they were projected FROM. `SELECT
/// DISTINCT` is the only path that sorts after projecting, so its keys must be
/// extracted first; every other path sorts source rows via [`sort_rows`].
pub(super) fn sort_rows_by_keys(
    rows: &mut [Vec<Value>],
    keys: &[Vec<Value>],
    order_by: &[OrderByItem],
    collations: &[crate::types::Collation],
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    debug_assert_eq!(rows.len(), keys.len(), "a key per row");
    if rows.is_empty() {
        return Ok(());
    }
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    sort_indices_by(&mut indices, cancel, |a, b| {
        compare_sort_keys(&keys[a], &keys[b], order_by, collations)
    })?;
    check_sort_cancel(cancel)?;

    reorder_rows_by_indices(rows, &indices, rows.len(), cancel)
}

pub(super) fn topk_rows_by_keys(
    rows: &mut [Vec<Value>],
    keys: &[Vec<Value>],
    order_by: &[OrderByItem],
    collations: &[crate::types::Collation],
    k: usize,
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    debug_assert_eq!(rows.len(), keys.len(), "a key per row");
    if rows.is_empty() {
        return Ok(());
    }
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    topk_indices_by(&mut indices, k, cancel, |a, b| {
        compare_sort_keys(&keys[a], &keys[b], order_by, collations)
    })?;
    check_sort_cancel(cancel)?;
    reorder_rows_by_indices(rows, &indices, k, cancel)
}

pub(super) fn topk_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
    k: usize,
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    validate_order_by_ordinals(order_by, columns.len())?;
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        topk_indices_by(&mut indices, k, cancel, |a, b| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        })?;
    } else if let Some((col_idx, coll)) = try_resolve_collated_flat_sort(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let keys = precompute_collated_keys_with_cancel(rows, col_idx, coll, cancel)?;
        check_sort_cancel(cancel)?;
        topk_indices_by(&mut indices, k, cancel, |a, b| {
            compare_collated_key(
                &keys[a],
                &keys[b],
                &rows[a][col_idx],
                &rows[b][col_idx],
                desc,
                nulls_first,
            )
        })?;
    } else {
        let keys = extract_sort_keys_with_cancel(rows, order_by, &col_map, cancel)?;
        let collations = sort_key_collations(order_by, &col_map);
        check_sort_cancel(cancel)?;
        topk_indices_by(&mut indices, k, cancel, |a, b| {
            compare_sort_keys(&keys[a], &keys[b], order_by, &collations)
        })?;
    }
    check_sort_cancel(cancel)?;

    reorder_rows_by_indices(rows, &indices, k, cancel)
}

pub(super) fn order_by_uses_projected_output(item: &OrderByItem) -> bool {
    item.output_name.is_some() || item.output_ordinal.is_some()
}

pub(super) fn validate_order_by_ordinals(
    order_by: &[OrderByItem],
    output_width: usize,
) -> Result<()> {
    for item in order_by {
        if let Some(position) = item.output_ordinal {
            if position >= output_width {
                return Err(SqlError::InvalidValue(format!(
                    "ORDER BY position {} out of range",
                    position + 1
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn order_by_output_position(
    item: &OrderByItem,
    output_map: &ColumnMap,
) -> Result<Option<usize>> {
    if let Some(position) = item.output_ordinal {
        validate_order_by_ordinals(std::slice::from_ref(item), output_map.len())?;
        return Ok(Some(position));
    }
    item.output_name
        .as_ref()
        .map(|name| output_map.resolve(&name.to_ascii_lowercase()))
        .transpose()
}

pub(super) fn try_resolve_flat_sort_col(
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Option<usize> {
    if order_by.len() != 1 {
        return None;
    }
    if let Some(idx) = order_by[0].output_ordinal {
        return (col_map.collation_at(idx) == crate::types::Collation::Binary).then_some(idx);
    }
    if let Some(name) = &order_by[0].output_name {
        let idx = col_map.resolve(&name.to_ascii_lowercase()).ok()?;
        return (col_map.collation_at(idx) == crate::types::Collation::Binary).then_some(idx);
    }
    match &order_by[0].expr {
        Expr::Column(name) => {
            let idx = col_map.resolve(&name.to_ascii_lowercase()).ok()?;
            (col_map.collation_at(idx) == crate::types::Collation::Binary).then_some(idx)
        }
        _ => None,
    }
}

pub(super) fn try_resolve_collated_flat_sort(
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Option<(usize, crate::types::Collation)> {
    if order_by.len() != 1 {
        return None;
    }
    if let Some(idx) = order_by[0].output_ordinal {
        let coll = col_map.collation_at(idx);
        return (coll != crate::types::Collation::Binary).then_some((idx, coll));
    }
    if let Some(name) = &order_by[0].output_name {
        let idx = col_map.resolve(&name.to_ascii_lowercase()).ok()?;
        let coll = col_map.collation_at(idx);
        return (coll != crate::types::Collation::Binary).then_some((idx, coll));
    }
    match &order_by[0].expr {
        Expr::Collate { expr: e, collation } => match e.as_ref() {
            Expr::Column(name) => {
                let idx = col_map.resolve(&name.to_ascii_lowercase()).ok()?;
                Some((idx, *collation))
            }
            _ => None,
        },
        Expr::Column(name) => {
            let idx = col_map.resolve(&name.to_ascii_lowercase()).ok()?;
            let coll = col_map.collation_at(idx);
            (coll != crate::types::Collation::Binary).then_some((idx, coll))
        }
        _ => None,
    }
}

pub(super) fn compare_flat_key(
    a: &Value,
    b: &Value,
    desc: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (false, false) => {
            let cmp = a.cmp(b);
            if desc {
                cmp.reverse()
            } else {
                cmp
            }
        }
    }
}

pub(super) enum CollatedKey {
    Null,
    Text(String),
    Other,
}

pub(super) fn precompute_collated_keys(
    rows: &[Vec<Value>],
    col_idx: usize,
    coll: crate::types::Collation,
) -> Vec<CollatedKey> {
    rows.iter()
        .map(|row| match &row[col_idx] {
            Value::Null => CollatedKey::Null,
            Value::Text(s) => match coll {
                crate::types::Collation::Binary => CollatedKey::Text(s.to_string()),
                crate::types::Collation::NoCase => {
                    CollatedKey::Text(s.as_str().to_ascii_lowercase())
                }
                crate::types::Collation::Rtrim => {
                    CollatedKey::Text(s.trim_end_matches(' ').to_string())
                }
            },
            _ => CollatedKey::Other,
        })
        .collect()
}

pub(super) fn precompute_collated_keys_with_cancel(
    rows: &[Vec<Value>],
    col_idx: usize,
    coll: crate::types::Collation,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<CollatedKey>> {
    check_cancel(cancel)?;
    if cancel.is_none() {
        return Ok(precompute_collated_keys(rows, col_idx, coll));
    }
    let mut keys = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        keys.push(match &row[col_idx] {
            Value::Null => CollatedKey::Null,
            Value::Text(s) => match coll {
                crate::types::Collation::Binary => CollatedKey::Text(s.to_string()),
                crate::types::Collation::NoCase => {
                    CollatedKey::Text(s.as_str().to_ascii_lowercase())
                }
                crate::types::Collation::Rtrim => {
                    CollatedKey::Text(s.trim_end_matches(' ').to_string())
                }
            },
            _ => CollatedKey::Other,
        });
    }
    check_cancel(cancel)?;
    Ok(keys)
}

pub(super) fn compare_collated_key(
    a: &CollatedKey,
    b: &CollatedKey,
    fallback_a: &Value,
    fallback_b: &Value,
    desc: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    let ord = match (a, b) {
        (CollatedKey::Null, CollatedKey::Null) => std::cmp::Ordering::Equal,
        (CollatedKey::Null, _) => {
            return if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }
        (_, CollatedKey::Null) => {
            return if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            };
        }
        (CollatedKey::Text(x), CollatedKey::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        _ => fallback_a.cmp(fallback_b),
    };
    if desc {
        ord.reverse()
    } else {
        ord
    }
}

pub(super) fn extract_sort_keys(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Result<Vec<Vec<Value>>> {
    rows.iter()
        .map(|row| {
            order_by
                .iter()
                .map(|item| sort_item_value(item, row, col_map, None))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()
}

pub(super) fn extract_sort_keys_with_cancel(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    check_cancel(cancel)?;
    if cancel.is_none() {
        return extract_sort_keys(rows, order_by, col_map);
    }
    let mut keys = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        keys.push(
            order_by
                .iter()
                .map(|item| sort_item_value(item, row, col_map, cancel))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    check_cancel(cancel)?;
    Ok(keys)
}

fn sort_item_value(
    item: &OrderByItem,
    row: &[Value],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    if let Some(position) = item.output_ordinal {
        return row.get(position).cloned().ok_or_else(|| {
            SqlError::InvalidValue(format!("ORDER BY position {} out of range", position + 1))
        });
    }
    if let Some(name) = &item.output_name {
        let index = col_map.resolve(&name.to_ascii_lowercase())?;
        return row.get(index).cloned().ok_or_else(|| {
            SqlError::InvalidValue(format!("ORDER BY column `{name}` is outside the row"))
        });
    }
    eval_expr(&item.expr, &EvalCtx::new(col_map, row).with_cancel(cancel))
}

pub(super) fn compare_sort_keys(
    a: &[Value],
    b: &[Value],
    order_by: &[OrderByItem],
    collations: &[crate::types::Collation],
) -> std::cmp::Ordering {
    for (i, item) in order_by.iter().enumerate() {
        let nulls_first = item.nulls_first.unwrap_or(!item.descending);
        let ord = match (a[i].is_null(), b[i].is_null()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let coll = collations
                    .get(i)
                    .copied()
                    .unwrap_or(crate::types::Collation::Binary);
                let cmp = if coll != crate::types::Collation::Binary {
                    if let (Value::Text(x), Value::Text(y)) = (&a[i], &b[i]) {
                        coll.cmp_text(x, y)
                    } else {
                        a[i].cmp(&b[i])
                    }
                } else {
                    a[i].cmp(&b[i])
                };
                if item.descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// A column that carries a name, a position and a collation and nothing else: the shape of a
/// projected value, which has no stored column behind it to describe.
pub(crate) fn projected_column(
    name: String,
    position: usize,
    collation: crate::types::Collation,
) -> ColumnDef {
    ColumnDef {
        name,
        data_type: DataType::Null,
        nullable: true,
        position: position as u16,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation,
    }
}

/// The collation a key expression carries: an explicit COLLATE anywhere in it, else a
/// column's own preserved through CAST wrappers. Anything else has none. Grouping,
/// deduplicating and sorting all key expressions by this same rule.
pub(crate) fn expr_collation(expr: &Expr, col_map: &ColumnMap) -> crate::types::Collation {
    operand_collation(expr, col_map).unwrap_or(crate::types::Collation::Binary)
}

pub(super) fn sort_key_collations(
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Vec<crate::types::Collation> {
    order_by
        .iter()
        .map(|item| {
            item.output_ordinal
                .map(|index| col_map.collation_at(index))
                .or_else(|| {
                    item.output_name
                        .as_ref()
                        .and_then(|name| col_map.resolve(&name.to_ascii_lowercase()).ok())
                        .map(|index| col_map.collation_at(index))
                })
                .unwrap_or_else(|| expr_collation(&item.expr, col_map))
        })
        .collect()
}

/// The collation of each PROJECTED column, for deduplicating rows that have already been
/// projected. `*` expands to the source columns, so it contributes one entry each.
pub(crate) fn output_collations(
    select_cols: &[SelectColumn],
    col_map: &ColumnMap,
) -> Vec<crate::types::Collation> {
    let mut out = Vec::with_capacity(select_cols.len());
    for col in select_cols {
        match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                out.extend((0..col_map.len()).map(|i| col_map.collation_at(i)));
            }
            SelectColumn::Expr { expr, .. } => out.push(expr_collation(expr, col_map)),
        }
    }
    out
}

/// A row folded into the key that decides its equality. A row longer than `collations` keeps
/// its extra values as they are, which is the binary comparison they had before.
pub(crate) fn fold_key(row: &[Value], collations: &[crate::types::Collation]) -> Vec<Value> {
    row.iter()
        .enumerate()
        .map(|(i, v)| match collations.get(i) {
            Some(coll) => coll.fold(v.clone()),
            None => v.clone(),
        })
        .collect()
}

/// The set of rows already seen, under the collations that decide when two of them are
/// the same row. When nothing collates the probe borrows the row, so only a surviving
/// row is copied; folding a key for every row would allocate once per duplicate.
pub(crate) struct RowKeys {
    seen: rustc_hash::FxHashSet<Vec<Value>>,
    collations: Vec<crate::types::Collation>,
    folding: bool,
}

impl RowKeys {
    pub(crate) fn new(collations: Vec<crate::types::Collation>) -> Self {
        Self::with_capacity(collations, 0)
    }

    pub(crate) fn with_capacity(collations: Vec<crate::types::Collation>, cap: usize) -> Self {
        let folding = collations
            .iter()
            .any(|c| *c != crate::types::Collation::Binary);
        Self {
            seen: rustc_hash::FxHashSet::with_capacity_and_hasher(cap, Default::default()),
            collations,
            folding,
        }
    }

    /// True the first time this row is seen.
    pub(crate) fn insert(&mut self, row: &[Value]) -> bool {
        if self.folding {
            return self.seen.insert(fold_key(row, &self.collations));
        }
        if self.seen.contains(row) {
            false
        } else {
            self.seen.insert(row.to_vec());
            true
        }
    }

    /// Whether this row was already seen, borrowing it when nothing has to be folded.
    pub(crate) fn contains_row(&self, row: &[Value]) -> bool {
        if self.folding {
            self.seen.contains(&fold_key(row, &self.collations))
        } else {
            self.seen.contains(row)
        }
    }
}

pub(super) fn try_identity_projection_names(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Option<Vec<String>> {
    if select_cols.len() != columns.len() {
        return None;
    }
    let mut names = Vec::with_capacity(columns.len());
    for (i, sc) in select_cols.iter().enumerate() {
        let SelectColumn::Expr { expr, alias } = sc else {
            return None;
        };
        let col_name = columns[i].name.as_str();
        match expr {
            Expr::QualifiedColumn { table, column } => {
                let lt = table.to_ascii_lowercase();
                let lc = column.to_ascii_lowercase();
                let expected_len = lt.len() + 1 + lc.len();
                if col_name.len() != expected_len
                    || col_name.as_bytes().get(lt.len()) != Some(&b'.')
                    || !col_name.starts_with(lt.as_str())
                    || !col_name.ends_with(lc.as_str())
                {
                    return None;
                }
            }
            Expr::Column(name) => {
                let lname = name.to_ascii_lowercase();
                let mut count = 0;
                let mut hit_idx = 0;
                for (j, c) in columns.iter().enumerate() {
                    let cn = c.name.as_str();
                    let hit = cn == lname.as_str()
                        || (cn.len() > lname.len() + 1
                            && cn.as_bytes()[cn.len() - lname.len() - 1] == b'.'
                            && cn.ends_with(lname.as_str()));
                    if hit {
                        if count == 0 {
                            hit_idx = j;
                        }
                        count += 1;
                        if count > 1 {
                            return None;
                        }
                    }
                }
                if count != 1 || hit_idx != i {
                    return None;
                }
            }
            _ => return None,
        }
        names.push(alias.clone().unwrap_or_else(|| expr_display_name(expr)));
    }
    Some(names)
}

pub(super) fn try_build_index_map(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Option<Vec<(String, usize)>> {
    let col_map = ColumnMap::new(columns);
    let mut map = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();
    for sel in select_cols {
        match sel {
            SelectColumn::AllColumns => {
                for col in columns {
                    let idx = col.position as usize;
                    if !seen.insert(idx) {
                        return None;
                    }
                    map.push((col.name.clone(), idx));
                }
            }
            SelectColumn::AllFromOld | SelectColumn::AllFromNew => return None,
            SelectColumn::Expr { expr, alias } => {
                let idx = match expr {
                    Expr::Column(name) => col_map.resolve(name).ok()?,
                    Expr::QualifiedColumn { table, column } => {
                        col_map.resolve_qualified(table, column).ok()?
                    }
                    _ => return None,
                };
                if !seen.insert(idx) {
                    return None;
                }
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                map.push((name, idx));
            }
        }
    }
    Some(map)
}

pub(super) fn project_rows(
    columns: &[ColumnDef],
    select_cols: &[SelectColumn],
    rows: Vec<Vec<Value>>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    project_rows_with_cancel(columns, select_cols, rows, None)
}

pub(super) fn project_rows_with_cancel(
    columns: &[ColumnDef],
    select_cols: &[SelectColumn],
    mut rows: Vec<Vec<Value>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    check_cancel(cancel)?;
    if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::AllColumns) {
        let col_names = columns.iter().map(|c| c.name.clone()).collect();
        return Ok((col_names, rows));
    }

    if let Some(names) = try_identity_projection_names(select_cols, columns) {
        return Ok((names, rows));
    }

    if let Some(map) = try_build_index_map(select_cols, columns) {
        let col_names: Vec<String> = map.iter().map(|(n, _)| n.clone()).collect();
        if map.len() == columns.len() && map.iter().enumerate().all(|(i, &(_, idx))| idx == i) {
            return Ok((col_names, rows));
        }
        if cancel.is_none() {
            let projected = rows
                .iter_mut()
                .map(|row| {
                    map.iter()
                        .map(|&(_, idx)| std::mem::take(&mut row[idx]))
                        .collect()
                })
                .collect();
            return Ok((col_names, projected));
        }
        let mut projected = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.iter_mut().enumerate() {
            check_cancel_at(cancel, row_idx)?;
            projected.push(
                map.iter()
                    .map(|&(_, idx)| std::mem::take(&mut row[idx]))
                    .collect(),
            );
        }
        check_cancel(cancel)?;
        return Ok((col_names, projected));
    }

    let mut col_names = Vec::new();
    type Projector = Box<dyn Fn(&[Value], Option<&citadel::CancelToken>) -> Result<Value>>;
    let mut projectors: Vec<Projector> = Vec::new();
    let col_map = std::sync::Arc::new(ColumnMap::new(columns));

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                for col in columns {
                    let idx = col.position as usize;
                    col_names.push(col.name.clone());
                    projectors.push(Box::new(move |row: &[Value], _| Ok(row[idx].clone())));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let expr = expr.clone();
                let map = col_map.clone();
                projectors.push(Box::new(move |row: &[Value], cancel| {
                    eval_expr(&expr, &EvalCtx::new(&map, row).with_cancel(cancel))
                }));
            }
        }
    }

    if cancel.is_none() {
        let projected = rows
            .iter()
            .map(|row| {
                projectors
                    .iter()
                    .map(|p| p(row, cancel))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok((col_names, projected));
    }

    let mut projected = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        projected.push(
            projectors
                .iter()
                .map(|p| p(row, cancel))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    check_cancel(cancel)?;

    Ok((col_names, projected))
}

pub(super) fn project_returning(
    table_schema: &TableSchema,
    returning: &[SelectColumn],
    rows: &[ReturningRow],
    cancel: Option<&citadel::CancelToken>,
) -> Result<QueryResult> {
    let columns = &table_schema.columns;
    let col_map = table_schema.column_map();

    let mut col_names = Vec::new();
    for sel_col in returning {
        match sel_col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                for c in columns {
                    col_names.push(c.name.clone());
                }
            }
            SelectColumn::Expr { alias: Some(a), .. } => col_names.push(a.clone()),
            SelectColumn::Expr { expr, alias: None } => col_names.push(expr_display_name(expr)),
        }
    }

    let mut out_rows = Vec::with_capacity(rows.len());
    for (old, new) in rows {
        let default_row: &[Value] = new.as_deref().or(old.as_deref()).unwrap_or(&[]);
        let ctx = EvalCtx::with_old_new(col_map, default_row, old.as_deref(), new.as_deref())
            .with_cancel(cancel);

        let mut out = Vec::with_capacity(col_names.len());
        for sel_col in returning {
            match sel_col {
                SelectColumn::AllColumns => {
                    for c in columns {
                        out.push(default_row[c.position as usize].clone());
                    }
                }
                SelectColumn::AllFromOld => match old {
                    Some(r) => {
                        for c in columns {
                            out.push(r[c.position as usize].clone());
                        }
                    }
                    None => {
                        for _ in columns {
                            out.push(Value::Null);
                        }
                    }
                },
                SelectColumn::AllFromNew => match new {
                    Some(r) => {
                        for c in columns {
                            out.push(r[c.position as usize].clone());
                        }
                    }
                    None => {
                        for _ in columns {
                            out.push(Value::Null);
                        }
                    }
                },
                SelectColumn::Expr { expr, .. } => {
                    out.push(eval_expr(expr, &ctx)?);
                }
            }
        }
        out_rows.push(out);
    }

    Ok(QueryResult {
        columns: col_names,
        rows: out_rows,
    })
}

pub(crate) fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{table}.{column}"),
        Expr::Literal(v) => format!("{v}"),
        Expr::CountStar => "COUNT(*)".into(),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
            if *distinct {
                format!("{name}(DISTINCT {})", arg_strs.join(", "))
            } else {
                format!("{name}({})", arg_strs.join(", "))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                expr_display_name(left),
                op_symbol(op),
                expr_display_name(right)
            )
        }
        Expr::WindowFunction { name, args, .. } => {
            if args.is_empty() {
                format!("{name}()")
            } else {
                let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
                format!("{name}({})", arg_strs.join(", "))
            }
        }
        _ => "?".into(),
    }
}

pub(super) fn op_symbol(op: &BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::NotEq => "<>",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::LtEq => "<=",
        BinOp::GtEq => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
        BinOp::Concat => "||",
        BinOp::JsonGet => "->",
        BinOp::JsonGetText => "->>",
        BinOp::JsonPath => "#>",
        BinOp::JsonPathText => "#>>",
        BinOp::JsonContains => "@>",
        BinOp::JsonContainedBy => "<@",
        BinOp::JsonHasKey => "?",
        BinOp::JsonHasAnyKey => "?|",
        BinOp::JsonHasAllKeys => "?&",
        BinOp::JsonDeletePath => "#-",
        BinOp::JsonPathExists => "@?",
        BinOp::JsonPathMatch => "@@",
        BinOp::JsonPathExistsTz => "@?_tz",
        BinOp::JsonPathMatchTz => "@@_tz",
        BinOp::VectorL2 => "<->",
        BinOp::VectorInner => "<#>",
        BinOp::VectorCosine => "<=>",
    }
}

pub(crate) fn build_output_columns(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Vec<ColumnDef> {
    let mut out = Vec::new();
    let col_map = ColumnMap::new(columns);
    for col in select_cols {
        let (name, data_type, collation) = match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                for source in columns {
                    let mut projected =
                        projected_column(source.name.clone(), out.len(), source.collation);
                    projected.data_type = source.data_type;
                    projected.nullable = source.nullable;
                    out.push(projected);
                }
                continue;
            }
            SelectColumn::Expr {
                alias: Some(a),
                expr,
            } => (
                a.clone(),
                infer_expr_type(expr, columns),
                expr_collation(expr, &col_map),
            ),
            SelectColumn::Expr { expr, .. } => (
                expr_display_name(expr),
                infer_expr_type(expr, columns),
                expr_collation(expr, &col_map),
            ),
        };
        let mut projected = projected_column(name, out.len(), collation);
        projected.data_type = data_type;
        out.push(projected);
    }
    out
}

pub(super) fn infer_expr_type(expr: &Expr, columns: &[ColumnDef]) -> DataType {
    match expr {
        Expr::Column(name) => columns
            .iter()
            .find(|c| c.name == *name)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Null),
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            columns
                .iter()
                .find(|c| c.name == qualified)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Null)
        }
        Expr::Literal(v) => v.data_type(),
        Expr::CountStar => DataType::Integer,
        Expr::Function { name, .. } => match name.to_ascii_uppercase().as_str() {
            "COUNT" => DataType::Integer,
            "AVG" => DataType::Real,
            "SUM" | "MIN" | "MAX" => DataType::Null,
            _ => DataType::Null,
        },
        _ => DataType::Null,
    }
}

pub(super) fn encode_index_key_with_schema(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
    schema: &TableSchema,
) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_index_key_into_with_schema(idx, row, pk_values, Some(schema), &mut buf);
    buf
}

pub(super) fn encode_index_key_with_schema_and_cancel(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
    schema: &TableSchema,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<u8>> {
    if idx.is_pure_column_index() {
        return Ok(encode_index_key_with_schema(idx, row, pk_values, schema));
    }
    let mut buf = Vec::new();
    encode_index_key_into_with_schema_and_cancel(
        idx,
        row,
        pk_values,
        Some(schema),
        &mut buf,
        cancel,
    )?;
    Ok(buf)
}

/// If the index has expression keys but `schema` is None, expression results are NULL.
pub(super) fn encode_index_key_into_with_schema(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
    schema: Option<&TableSchema>,
    buf: &mut Vec<u8>,
) {
    buf.clear();
    // Encode straight from `row` (no Vec<Value>); byte-identical to the Expr path.
    if idx.is_pure_column_index() {
        let mut any_null = false;
        for (i, key) in idx.keys.iter().enumerate() {
            let crate::types::IndexKey::Column { idx: col_idx, .. } = key else {
                unreachable!("is_pure_column_index guarantees Column keys")
            };
            let value = &row[*col_idx as usize];
            any_null |= idx.unique && value.is_null();
            encode_index_key_component(value, idx.collation_at(i), buf);
        }
        if !idx.unique || any_null {
            for v in pk_values {
                crate::encoding::encode_key_value_into(v, buf);
            }
        }
        return;
    }
    let key_values = materialize_index_key_values(idx, row, schema);
    let any_null = idx.unique && key_values.iter().any(|v| v.is_null());
    let include_pk = !idx.unique || any_null;
    for (i, value) in key_values.iter().enumerate() {
        encode_index_key_component(value, idx.collation_at(i), buf);
    }
    if include_pk {
        for v in pk_values {
            crate::encoding::encode_key_value_into(v, buf);
        }
    }
}

pub(super) fn encode_index_key_into_with_schema_and_cancel(
    idx: &IndexDef,
    row: &[Value],
    pk_values: &[Value],
    schema: Option<&TableSchema>,
    buf: &mut Vec<u8>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<()> {
    if idx.is_pure_column_index() {
        encode_index_key_into_with_schema(idx, row, pk_values, schema, buf);
        return Ok(());
    }
    buf.clear();
    let key_values = materialize_index_key_values_with_cancel(idx, row, schema, cancel)?;
    let any_null = idx.unique && key_values.iter().any(Value::is_null);
    let include_pk = !idx.unique || any_null;
    for (i, value) in key_values.iter().enumerate() {
        encode_index_key_component(value, idx.collation_at(i), buf);
    }
    if include_pk {
        for value in pk_values {
            crate::encoding::encode_key_value_into(value, buf);
        }
    }
    Ok(())
}

#[inline]
fn encode_index_key_component(value: &Value, coll: crate::types::Collation, buf: &mut Vec<u8>) {
    if coll == crate::types::Collation::Binary {
        crate::encoding::encode_key_value_into(value, buf);
    } else {
        crate::encoding::encode_key_value_collated_into(value, coll, buf);
    }
}

/// Expression eval errors (or missing schema) materialize as `Value::Null` - PG semantics.
pub(super) fn materialize_index_key_values(
    idx: &IndexDef,
    row: &[Value],
    schema: Option<&TableSchema>,
) -> Vec<Value> {
    let col_map = schema.map(|s| s.column_map());
    idx.keys
        .iter()
        .map(|key| match key {
            crate::types::IndexKey::Column { idx: col_idx, .. } => row[*col_idx as usize].clone(),
            crate::types::IndexKey::Expr { expr, .. } => match col_map.as_ref() {
                Some(cm) => {
                    let ctx = crate::eval::EvalCtx::new(cm, row);
                    crate::eval::eval_expr(expr, &ctx).unwrap_or(Value::Null)
                }
                None => Value::Null,
            },
        })
        .collect()
}

pub(super) fn materialize_index_key_values_with_cancel(
    idx: &IndexDef,
    row: &[Value],
    schema: Option<&TableSchema>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Value>> {
    let col_map = schema.map(TableSchema::column_map);
    idx.keys
        .iter()
        .map(|key| match key {
            crate::types::IndexKey::Column { idx: col_idx, .. } => {
                Ok(row[*col_idx as usize].clone())
            }
            crate::types::IndexKey::Expr { expr, .. } => match col_map.as_ref() {
                Some(cm) => crate::eval::eval_expr(
                    expr,
                    &crate::eval::EvalCtx::new(cm, row).with_cancel(cancel),
                ),
                None => Ok(Value::Null),
            },
        })
        .collect()
}

pub(super) fn encode_index_value(idx: &IndexDef, row: &[Value], pk_values: &[Value]) -> Vec<u8> {
    if idx.unique {
        let indexed_values: Vec<Value> = idx
            .column_positions_iter()
            .map(|col_idx| row[col_idx as usize].clone())
            .collect();
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(pk_values);
        }
    }
    vec![]
}

thread_local! {
    static IDX_KEY_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(64));
    static IDX_TABLE_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(64));
}

fn fill_idx_table_name(buf: &mut Vec<u8>, table: &str, idx: &str) {
    buf.clear();
    buf.extend_from_slice(b"__idx_");
    buf.extend_from_slice(table.as_bytes());
    buf.push(b'_');
    buf.extend_from_slice(idx.as_bytes());
}

pub(super) fn insert_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    IDX_KEY_BUF.with(|kb| {
        IDX_TABLE_BUF.with(|tb| {
            let mut key_buf = kb.borrow_mut();
            let mut table_buf = tb.borrow_mut();
            for idx in &table_schema.indices {
                if let Some(cm) = col_map.as_ref() {
                    if !row_matches_partial_with_cancel(idx, row, cm, cancel)? {
                        continue;
                    }
                }
                fill_idx_table_name(&mut table_buf, &table_schema.name, &idx.name);

                if let crate::types::IndexKind::Inverted(inv_kind) = idx.kind {
                    insert_inverted_entries(wtx, idx, inv_kind, row, pk_values, &table_buf)?;
                    continue;
                }

                encode_index_key_into_with_schema_and_cancel(
                    idx,
                    row,
                    pk_values,
                    Some(table_schema),
                    &mut key_buf,
                    cancel,
                )?;
                let value = encode_index_value(idx, row, pk_values);

                let is_new = wtx
                    .table_insert_index(&table_buf, &key_buf, &value)
                    .map_err(SqlError::Storage)?;

                if idx.unique && !is_new {
                    let any_null = idx
                        .column_positions_iter()
                        .any(|c| row[c as usize].is_null());
                    if !any_null {
                        return Err(SqlError::UniqueViolation(idx.name.clone()));
                    }
                }
            }
            Ok(())
        })
    })
}

pub(crate) fn build_inverted_key(entry_bytes: &[u8], row_pk_encoded: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(entry_bytes.len() + 1 + row_pk_encoded.len());
    k.extend_from_slice(entry_bytes);
    k.push(0x1F);
    k.extend_from_slice(row_pk_encoded);
    k
}

pub(crate) fn extract_inverted_entries_with_cancel(
    value: &Value,
    kind: crate::types::InvertedKind,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<u8>>> {
    match kind {
        crate::types::InvertedKind::Gin(ops) => {
            crate::json::extract_gin_entries_with_cancel(value, ops, cancel)
        }
        crate::types::InvertedKind::Fts { config_id } => {
            extract_fts_lexemes(value, config_id, cancel)
        }
        crate::types::InvertedKind::Ann { .. } => Ok(Vec::new()),
    }
}

pub(crate) fn extract_inverted_entries_with_values_and_cancel(
    value: &Value,
    kind: crate::types::InvertedKind,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    match kind {
        crate::types::InvertedKind::Gin(ops) => {
            let keys = crate::json::extract_gin_entries_with_cancel(value, ops, cancel)?;
            if cancel.is_none() {
                return Ok(keys.into_iter().map(|key| (key, Vec::new())).collect());
            }
            let mut entries = Vec::with_capacity(keys.len());
            for (work, key) in keys.into_iter().enumerate() {
                check_cancel_at(cancel, work)?;
                entries.push((key, Vec::new()));
            }
            check_cancel(cancel)?;
            Ok(entries)
        }
        crate::types::InvertedKind::Fts { config_id } => {
            extract_fts_lexemes_with_positions(value, config_id, cancel)
        }
        crate::types::InvertedKind::Ann { .. } => Ok(Vec::new()),
    }
}

fn extract_fts_lexemes(
    value: &Value,
    config_id: u8,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<u8>>> {
    check_cancel(cancel)?;
    let kind = crate::fts::TokenizerKind::from_config_id(config_id)?;
    let mut lexemes: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    match value {
        Value::Null => return Ok(Vec::new()),
        Value::TsVector(bytes) => {
            let (_flags, reader) = crate::fts::TsVectorReader::open(bytes)?;
            for (work, item) in reader.enumerate() {
                check_cancel_at(cancel, work)?;
                let (lex, _positions) = item?;
                lexemes.insert(lex.to_vec());
            }
        }
        Value::Text(s) => {
            for (work, tok) in crate::fts::tokenize_with_cancel(kind, s, cancel)?
                .into_iter()
                .enumerate()
            {
                check_cancel_at(cancel, work)?;
                if tok.stopped || tok.lexeme.is_empty() {
                    continue;
                }
                lexemes.insert(tok.lexeme.into_bytes());
            }
        }
        other => {
            return Err(SqlError::Unsupported(format!(
                "FTS index requires TEXT or TSVECTOR, got {}",
                other.data_type()
            )));
        }
    }
    check_cancel(cancel)?;
    let mut out = Vec::with_capacity(lexemes.len());
    for (work, lexeme) in lexemes.into_iter().enumerate() {
        check_cancel_at(cancel, work)?;
        out.push(lexeme);
    }
    check_cancel(cancel)?;
    Ok(out)
}

fn extract_fts_lexemes_with_positions(
    value: &Value,
    config_id: u8,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    check_cancel(cancel)?;
    let kind = crate::fts::TokenizerKind::from_config_id(config_id)?;
    let mut by_lex: std::collections::BTreeMap<Vec<u8>, Vec<u16>> =
        std::collections::BTreeMap::new();
    match value {
        Value::Null => return Ok(Vec::new()),
        Value::TsVector(bytes) => {
            let (_flags, reader) = crate::fts::TsVectorReader::open(bytes)?;
            for (work, item) in reader.enumerate() {
                check_cancel_at(cancel, work)?;
                let (lex, positions) = item?;
                by_lex.entry(lex.to_vec()).or_default().extend(positions);
            }
        }
        Value::Text(s) => {
            for (work, tok) in crate::fts::tokenize_with_cancel(kind, s, cancel)?
                .into_iter()
                .enumerate()
            {
                check_cancel_at(cancel, work)?;
                if tok.stopped || tok.lexeme.is_empty() {
                    continue;
                }
                let packed = crate::fts::pack_position(tok.position, crate::fts::Weight::D);
                by_lex
                    .entry(tok.lexeme.into_bytes())
                    .or_default()
                    .push(packed);
            }
        }
        other => {
            return Err(SqlError::Unsupported(format!(
                "FTS index requires TEXT or TSVECTOR, got {}",
                other.data_type()
            )));
        }
    }
    let mut out = Vec::with_capacity(by_lex.len());
    for (work, (lex, mut positions)) in by_lex.into_iter().enumerate() {
        check_cancel_at(cancel, work)?;
        positions.sort_unstable();
        positions.dedup();
        let mut value_bytes = Vec::with_capacity(positions.len() * 2);
        for p in positions {
            value_bytes.extend_from_slice(&p.to_le_bytes());
        }
        out.push((lex, value_bytes));
    }
    check_cancel(cancel)?;
    Ok(out)
}

fn insert_inverted_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    idx: &IndexDef,
    kind: crate::types::InvertedKind,
    row: &[Value],
    pk_values: &[Value],
    idx_table: &[u8],
) -> Result<()> {
    let col_idx = idx.column_positions_iter().next().ok_or_else(|| {
        SqlError::Unsupported("inverted index requires at least one column key".into())
    })? as usize;
    let value = &row[col_idx];
    if value.is_null() {
        return Ok(());
    }
    let cancel = wtx.cancel_token().cloned();
    let entries = extract_inverted_entries_with_values_and_cancel(value, kind, cancel.as_ref())?;
    let pk_encoded = crate::encoding::encode_composite_key(pk_values);
    for (entry, val_bytes) in entries {
        let full_key = build_inverted_key(&entry, &pk_encoded);
        wtx.table_insert(idx_table, &full_key, &val_bytes)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

pub(super) fn insert_index_entries_or_fetch(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
    inserted_keys: &mut Vec<(usize, Vec<u8>)>,
) -> Result<Option<usize>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    for (i, idx) in table_schema.indices.iter().enumerate() {
        if let Some(cm) = col_map.as_ref() {
            if !row_matches_partial_with_cancel(idx, row, cm, cancel)? {
                continue;
            }
        }
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key =
            encode_index_key_with_schema_and_cancel(idx, row, pk_values, table_schema, cancel)?;
        let value = encode_index_value(idx, row, pk_values);

        if idx.unique {
            let indexed_values: Vec<Value> = idx
                .column_positions_iter()
                .map(|col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if any_null {
                let is_new = wtx
                    .table_insert(&idx_table, &key, &value)
                    .map_err(SqlError::Storage)?;
                if is_new {
                    inserted_keys.push((i, key));
                }
                continue;
            }
            match wtx
                .table_insert_or_fetch(&idx_table, &key, &value)
                .map_err(SqlError::Storage)?
            {
                citadel_txn::write_txn::InsertOutcome::Inserted => {
                    inserted_keys.push((i, key));
                }
                citadel_txn::write_txn::InsertOutcome::Existed(_) => {
                    return Ok(Some(i));
                }
            }
        } else {
            wtx.table_insert(&idx_table, &key, &value)
                .map_err(SqlError::Storage)?;
            inserted_keys.push((i, key));
        }
    }
    Ok(None)
}

pub(super) fn undo_partial_insert(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    primary_key: &[u8],
    inserted_keys: &[(usize, Vec<u8>)],
) -> Result<()> {
    for (i, key) in inserted_keys.iter().rev() {
        let idx = &table_schema.indices[*i];
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        wtx.table_delete(&idx_table, key)
            .map_err(SqlError::Storage)?;
    }
    wtx.table_delete(table_schema.name.as_bytes(), primary_key)
        .map_err(SqlError::Storage)?;
    Ok(())
}

pub(super) fn delete_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    for idx in &table_schema.indices {
        if let Some(cm) = col_map.as_ref() {
            if !row_matches_partial_with_cancel(idx, row, cm, cancel)? {
                continue;
            }
        }
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        if let crate::types::IndexKind::Inverted(inv_kind) = idx.kind {
            delete_inverted_entries(wtx, idx, inv_kind, row, pk_values, &idx_table)?;
            continue;
        }
        let key =
            encode_index_key_with_schema_and_cancel(idx, row, pk_values, table_schema, cancel)?;
        wtx.table_delete(&idx_table, &key)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

fn delete_inverted_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    idx: &IndexDef,
    kind: crate::types::InvertedKind,
    row: &[Value],
    pk_values: &[Value],
    idx_table: &[u8],
) -> Result<()> {
    let col_idx = idx.column_positions_iter().next().ok_or_else(|| {
        SqlError::Unsupported("inverted index requires at least one column key".into())
    })? as usize;
    let value = &row[col_idx];
    if value.is_null() {
        return Ok(());
    }
    let cancel = wtx.cancel_token().cloned();
    let entries = extract_inverted_entries_with_cancel(value, kind, cancel.as_ref())?;
    let pk_encoded = crate::encoding::encode_composite_key(pk_values);
    for entry in entries {
        let full_key = build_inverted_key(&entry, &pk_encoded);
        wtx.table_delete(idx_table, &full_key)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

pub(super) fn index_columns_changed(
    idx: &IndexDef,
    old_row: &[Value],
    new_row: &[Value],
    schema: &TableSchema,
) -> bool {
    idx.keys.iter().any(|key| match key {
        crate::types::IndexKey::Column { idx: col_idx, .. } => {
            old_row[*col_idx as usize] != new_row[*col_idx as usize]
        }
        crate::types::IndexKey::Expr { expr, .. } => {
            crate::eval::referenced_columns(expr, &schema.columns)
                .into_iter()
                .any(|col_idx| old_row[col_idx] != new_row[col_idx])
        }
    })
}

/// NULL or eval errors -> false (treated as predicate-false).
pub(super) fn row_matches_partial(idx: &IndexDef, row: &[Value], col_map: &ColumnMap) -> bool {
    let Some(expr) = idx.predicate_expr.as_ref() else {
        return true;
    };
    match crate::eval::eval_expr(expr, &EvalCtx::new(col_map, row)) {
        Ok(v) => is_truthy(&v),
        Err(_) => false,
    }
}

pub(super) fn row_matches_partial_with_cancel(
    idx: &IndexDef,
    row: &[Value],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    let Some(expr) = idx.predicate_expr.as_ref() else {
        return Ok(row_matches_partial(idx, row, col_map));
    };
    Ok(is_truthy(&crate::eval::eval_expr(
        expr,
        &EvalCtx::new(col_map, row).with_cancel(cancel),
    )?))
}

pub(super) fn any_partial_index(table_schema: &TableSchema) -> bool {
    table_schema
        .indices
        .iter()
        .any(|idx| idx.predicate_sql.is_some())
}

/// 4-quadrant decision for UPDATE on a partial index.
/// Returns (should_delete_old_entry, should_insert_new_entry).
pub(super) fn partial_idx_update_actions(
    idx: &IndexDef,
    old_row: &[Value],
    new_row: &[Value],
    cols_changed: bool,
    pk_changed: bool,
    col_map: Option<&ColumnMap>,
) -> (bool, bool) {
    let key_changed = cols_changed || pk_changed;
    let Some(cm) = col_map.filter(|_| idx.predicate_expr.is_some()) else {
        return (key_changed, key_changed);
    };
    let old_match = row_matches_partial(idx, old_row, cm);
    let new_match = row_matches_partial(idx, new_row, cm);
    let del = old_match && (key_changed || !new_match);
    let ins = new_match && (key_changed || !old_match);
    (del, ins)
}

pub(super) fn partial_idx_update_actions_with_cancel(
    idx: &IndexDef,
    old_row: &[Value],
    new_row: &[Value],
    cols_changed: bool,
    pk_changed: bool,
    col_map: Option<&ColumnMap>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<(bool, bool)> {
    let key_changed = cols_changed || pk_changed;
    let Some(cm) = col_map.filter(|_| idx.predicate_expr.is_some()) else {
        return Ok(partial_idx_update_actions(
            idx,
            old_row,
            new_row,
            cols_changed,
            pk_changed,
            col_map,
        ));
    };
    let old_match = row_matches_partial_with_cancel(idx, old_row, cm, cancel)?;
    let new_match = row_matches_partial_with_cancel(idx, new_row, cm, cancel)?;
    let del = old_match && (key_changed || !new_match);
    let ins = new_match && (key_changed || !old_match);
    Ok((del, ins))
}

/// Child-row hits from an FK index scan; all key bytes share one arena.
#[derive(Default)]
pub(super) struct FkChildHits {
    arena: Vec<u8>,
    hits: Vec<FkChildHit>,
}

struct FkChildHit {
    key: (u32, u32),
    pk_key_repr: PkKeyRepr,
}

enum PkKeyRepr {
    Suffix(u32),
    Owned((u32, u32)),
}

impl FkChildHits {
    pub fn clear(&mut self) {
        self.arena.clear();
        self.hits.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    fn push(&mut self, key: &[u8], owned_pk: Option<&[u8]>, suffix: u32) {
        let key_off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        let pk_key_repr = match owned_pk {
            Some(pk) => {
                let off = self.arena.len() as u32;
                self.arena.extend_from_slice(pk);
                PkKeyRepr::Owned((off, pk.len() as u32))
            }
            None => PkKeyRepr::Suffix(suffix),
        };
        self.hits.push(FkChildHit {
            key: (key_off, key.len() as u32),
            pk_key_repr,
        });
    }

    fn fk_idx_key(&self, hit: &FkChildHit) -> &[u8] {
        let (off, len) = hit.key;
        &self.arena[off as usize..(off + len) as usize]
    }

    fn pk_key(&self, hit: &FkChildHit) -> &[u8] {
        match hit.pk_key_repr {
            PkKeyRepr::Suffix(s) => &self.fk_idx_key(hit)[s as usize..],
            PkKeyRepr::Owned((off, len)) => &self.arena[off as usize..(off + len) as usize],
        }
    }
}

fn find_cascading_idx<'a>(
    child_schema: &'a TableSchema,
    fk: &ForeignKeySchemaEntry,
) -> Option<&'a IndexDef> {
    child_schema
        .indices
        .iter()
        .find(|idx| idx.columns_vec() == fk.columns)
}

pub(super) fn scan_fk_index_keys(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    child_schema: &TableSchema,
    cascading_idx: &IndexDef,
    parent_pk_key: &[u8],
    out: &mut FkChildHits,
) -> Result<()> {
    let idx_table = TableSchema::index_table_name(&child_schema.name, &cascading_idx.name);
    let unique_no_null = cascading_idx.unique;
    let parent_pk_len = parent_pk_key.len() as u32;
    wtx.table_scan_from(&idx_table, parent_pk_key, |key, value| {
        if !key.starts_with(parent_pk_key) {
            return Ok(false);
        }
        let owned_pk = (unique_no_null && !value.is_empty()).then_some(value);
        out.push(key, owned_pk, parent_pk_len);
        Ok(true)
    })
    .map_err(SqlError::Storage)
}

pub(super) fn cascade_after_parent_delete(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &crate::schema::SchemaManager,
    parent_table: &str,
    deleted_pk_keys: &[Vec<u8>],
) -> Result<()> {
    let mut worklist: Vec<(String, Vec<Vec<u8>>)> =
        vec![(parent_table.to_string(), deleted_pk_keys.to_vec())];
    let mut hits = FkChildHits::default();

    while let Some((cur_table, cur_pks)) = worklist.pop() {
        let child_fks = schema.child_fks_for(&cur_table);
        if child_fks.is_empty() {
            continue;
        }
        for &(child_table, fk) in &child_fks {
            let child_schema = schema.get(child_table).unwrap();
            let cascading_idx = find_cascading_idx(child_schema, fk).ok_or_else(|| {
                SqlError::ForeignKeyViolation(format!(
                    "no index backs the foreign key on '{child_table}' referencing '{cur_table}'"
                ))
            })?;
            hits.clear();
            for parent_pk_key in &cur_pks {
                scan_fk_index_keys(wtx, child_schema, cascading_idx, parent_pk_key, &mut hits)?;
            }
            if hits.is_empty() {
                continue;
            }
            match fk.on_delete {
                crate::parser::ReferentialAction::NoAction
                | crate::parser::ReferentialAction::Restrict => {
                    return Err(SqlError::ForeignKeyViolation(format!(
                        "cannot delete from '{}': referenced by '{}'",
                        cur_table, child_table
                    )));
                }
                crate::parser::ReferentialAction::Cascade => {
                    delete_cascade_hits(wtx, schema, child_schema, cascading_idx, &hits)?;
                    // Skip the pk-key build for a leaf child that can't cascade on.
                    if !schema.child_fks_for(child_table).is_empty() {
                        let pk_keys: Vec<Vec<u8>> =
                            hits.hits.iter().map(|h| hits.pk_key(h).to_vec()).collect();
                        worklist.push((child_table.to_string(), pk_keys));
                    }
                }
                crate::parser::ReferentialAction::SetNull => {
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |_| Value::Null)?;
                }
                crate::parser::ReferentialAction::SetDefault => {
                    let cancel = wtx.cancel_token().cloned();
                    let defaults = fk_defaults(child_schema, fk, cancel.as_ref())?;
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |i| defaults[i].clone())?;
                }
            }
        }
    }
    Ok(())
}

fn delete_cascade_hits(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &crate::schema::SchemaManager,
    child_schema: &TableSchema,
    cascading_idx: &IndexDef,
    hits: &FkChildHits,
) -> Result<()> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let child_table = child_schema.name.as_str();
    let cascading_idx_table = TableSchema::index_table_name(child_table, &cascading_idx.name);
    let cascading_cols = cascading_idx.columns_vec();
    let other_indices: Vec<&IndexDef> = child_schema
        .indices
        .iter()
        .filter(|idx| idx.columns_vec() != cascading_cols)
        .collect();

    let has_after_delete_triggers = schema.triggers_for(child_table).iter().any(|t| {
        t.enabled
            && t.timing == crate::parser::TriggerTiming::After
            && t.granularity == crate::parser::TriggerGranularity::ForEachRow
            && t.events
                .iter()
                .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
    });

    if other_indices.is_empty() && !has_after_delete_triggers {
        for hit in &hits.hits {
            wtx.table_delete(&cascading_idx_table, hits.fk_idx_key(hit))
                .map_err(SqlError::Storage)?;
            wtx.table_delete(child_table.as_bytes(), hits.pk_key(hit))
                .map_err(SqlError::Storage)?;
        }
    } else {
        let rows = fetch_child_rows(wtx, child_schema, hits)?;
        let pk_indices = child_schema.pk_indices();
        let col_map_partial = any_partial_index(child_schema).then(|| child_schema.column_map());
        let other_index_tables: Vec<Vec<u8>> = other_indices
            .iter()
            .map(|idx| TableSchema::index_table_name(child_table, &idx.name))
            .collect();
        let mut pk_values_buf: Vec<Value> = Vec::with_capacity(pk_indices.len());
        let mut idx_key_buf: Vec<u8> = Vec::new();
        for ((pk_key, row), hit) in rows.iter().zip(&hits.hits) {
            wtx.table_delete(&cascading_idx_table, hits.fk_idx_key(hit))
                .map_err(SqlError::Storage)?;
            pk_values_buf.clear();
            pk_values_buf.extend(pk_indices.iter().map(|&j| row[j].clone()));
            for (idx, idx_table) in other_indices.iter().zip(other_index_tables.iter()) {
                if let Some(cm) = col_map_partial {
                    if !row_matches_partial_with_cancel(idx, row, cm, cancel)? {
                        continue;
                    }
                }
                encode_index_key_into_with_schema_and_cancel(
                    idx,
                    row,
                    &pk_values_buf,
                    Some(child_schema),
                    &mut idx_key_buf,
                    cancel,
                )?;
                wtx.table_delete(idx_table, &idx_key_buf)
                    .map_err(SqlError::Storage)?;
            }
            wtx.table_delete(child_table.as_bytes(), pk_key)
                .map_err(SqlError::Storage)?;
            if has_after_delete_triggers {
                super::triggers::fire_row_triggers(
                    wtx,
                    schema,
                    child_table,
                    crate::parser::TriggerTiming::After,
                    super::triggers::FireEvent::Delete,
                    Some(row.clone()),
                    None,
                    &child_schema.columns,
                )?;
            }
        }
    }
    Ok(())
}

fn fetch_child_rows(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    child_schema: &TableSchema,
    hits: &FkChildHits,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let mut rows = Vec::with_capacity(hits.hits.len());
    for hit in &hits.hits {
        let pk = hits.pk_key(hit);
        if let Some(value_bytes) = wtx
            .table_get(child_schema.name.as_bytes(), pk)
            .map_err(SqlError::Storage)?
        {
            let row = decode_full_row_with_cancel(child_schema, pk, &value_bytes, cancel)?;
            rows.push((pk.to_vec(), row));
        }
    }
    Ok(rows)
}

fn fk_defaults(
    child_schema: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Value>> {
    fk.columns
        .iter()
        .map(|&col_idx| -> Result<Value> {
            Ok(
                eval_default_with_cancel(&child_schema.columns[col_idx as usize], cancel)?
                    .unwrap_or(Value::Null),
            )
        })
        .collect()
}

fn set_fk_columns<F: Fn(usize) -> Value>(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    child_schema: &TableSchema,
    fk: &ForeignKeySchemaEntry,
    rows: &[(Vec<u8>, Vec<Value>)],
    value_for: F,
) -> Result<()> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    for (i, &col_idx) in fk.columns.iter().enumerate() {
        let new_val = value_for(i);
        let col = &child_schema.columns[col_idx as usize];
        if matches!(new_val, Value::Null) && !col.nullable {
            return Err(SqlError::NotNullViolation(col.name.clone()));
        }
    }
    let non_pk = child_schema.non_pk_indices();
    let enc_pos = child_schema.encoding_positions();
    let mut value_values: Vec<Value> = vec![Value::Null; non_pk.len()];
    let col_map_partial = any_partial_index(child_schema).then(|| child_schema.column_map());
    let pk_indices = child_schema.pk_indices();
    let table_bytes = child_schema.name.as_bytes();
    for (pk_key, old_row) in rows {
        let mut new_row = old_row.clone();
        for (i, &col_idx) in fk.columns.iter().enumerate() {
            new_row[col_idx as usize] = value_for(i);
        }
        for v in value_values.iter_mut() {
            *v = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            let col = &child_schema.columns[i];
            value_values[enc_pos[j] as usize] = if matches!(
                col.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                Value::Null
            } else {
                new_row[i].clone()
            };
        }
        let new_value = crate::encoding::encode_row(&value_values);
        wtx.table_update_sorted(table_bytes, &[(pk_key.as_slice(), new_value.as_slice())])
            .map_err(SqlError::Storage)?;
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        for idx in &child_schema.indices {
            let cols_changed = index_columns_changed(idx, old_row, &new_row, child_schema);
            let (del, ins) = partial_idx_update_actions_with_cancel(
                idx,
                old_row,
                &new_row,
                cols_changed,
                false,
                col_map_partial,
                cancel,
            )?;
            let idx_table = TableSchema::index_table_name(&child_schema.name, &idx.name);
            if del {
                let old_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    old_row,
                    &pk_values,
                    child_schema,
                    cancel,
                )?;
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
            if ins {
                let new_idx_key = encode_index_key_with_schema_and_cancel(
                    idx,
                    &new_row,
                    &pk_values,
                    child_schema,
                    cancel,
                )?;
                let new_idx_val = encode_index_value(idx, &new_row, &pk_values);
                wtx.table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
            }
        }
    }
    Ok(())
}

fn eval_default_with_cancel(
    col: &ColumnDef,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Option<Value>> {
    let Some(expr) = col.default_expr.as_ref() else {
        return Ok(None);
    };
    let empty_cols: &[ColumnDef] = &[];
    let cm = ColumnMap::new(empty_cols);
    let row: &[Value] = &[];
    crate::eval::eval_expr(expr, &EvalCtx::new(&cm, row).with_cancel(cancel)).map(Some)
}

pub(super) fn cascade_after_parent_update(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &crate::schema::SchemaManager,
    parent_table: &str,
    parent_schema: &TableSchema,
    parent_changes: &[(Vec<u8>, Vec<Value>, Vec<Value>)],
) -> Result<()> {
    let child_fks = schema.child_fks_for(parent_table);
    if child_fks.is_empty() {
        return Ok(());
    }
    let mut hits = FkChildHits::default();

    for &(child_table, fk) in &child_fks {
        let child_schema = schema.get(child_table).unwrap();
        let Some(cascading_idx) = find_cascading_idx(child_schema, fk) else {
            continue;
        };
        let parent_ref_cols: Vec<usize> = fk
            .referred_columns
            .iter()
            .map(|n| parent_schema.column_index(n).unwrap())
            .collect();
        for (old_pk_key, old_parent, new_parent) in parent_changes {
            let changed = parent_ref_cols
                .iter()
                .any(|&j| old_parent[j] != new_parent[j]);
            if !changed {
                continue;
            }
            hits.clear();
            scan_fk_index_keys(wtx, child_schema, cascading_idx, old_pk_key, &mut hits)?;
            if hits.is_empty() {
                continue;
            }
            match fk.on_update {
                crate::parser::ReferentialAction::NoAction
                | crate::parser::ReferentialAction::Restrict => {
                    return Err(SqlError::ForeignKeyViolation(format!(
                        "cannot update PK in '{}': referenced by '{}'",
                        parent_table, child_table
                    )));
                }
                crate::parser::ReferentialAction::Cascade => {
                    let new_fk_vals: Vec<Value> = parent_ref_cols
                        .iter()
                        .map(|&j| new_parent[j].clone())
                        .collect();
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |i| new_fk_vals[i].clone())?;
                }
                crate::parser::ReferentialAction::SetNull => {
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |_| Value::Null)?;
                }
                crate::parser::ReferentialAction::SetDefault => {
                    let cancel = wtx.cancel_token().cloned();
                    let defaults = fk_defaults(child_schema, fk, cancel.as_ref())?;
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |i| defaults[i].clone())?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "helpers_tests.rs"]
mod tests;
