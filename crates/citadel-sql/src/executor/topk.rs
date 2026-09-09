use citadel::CancelToken;

use crate::encoding::{
    decode_composite_key, decode_pk_integer, decode_stored_column_raw, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::ColumnMap;
use crate::parser::*;
use crate::types::*;

use super::aggregate::is_aggregate_expr;
use super::helpers::*;
use super::scan::FastPredicate;
use super::select::resolve_simple_col;
use super::window::has_any_window_function;

enum SortTarget {
    Primary(usize),
    Column(usize),
}

#[derive(Clone, Copy)]
struct SortOrder {
    descending: bool,
    nulls_first: bool,
    collation: Collation,
}

impl SortOrder {
    fn compare_by(
        self,
        left_null: bool,
        right_null: bool,
        non_null: impl FnOnce() -> std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if self.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if self.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let order = non_null();
                if self.descending {
                    order.reverse()
                } else {
                    order
                }
            }
        }
    }

    fn compare(self, left: &Value, right: &Value) -> std::cmp::Ordering {
        self.compare_by(left.is_null(), right.is_null(), || match (left, right) {
            (Value::Text(left), Value::Text(right)) => self.collation.cmp_text(left, right),
            _ => left.cmp(right),
        })
    }

    fn compare_raw(self, left: RawColumn<'_>, right: &Value) -> std::cmp::Ordering {
        if matches!(left, RawColumn::Array(_) | RawColumn::Vector(_)) {
            return self.compare(&left.to_value(), right);
        }
        self.compare_by(
            matches!(left, RawColumn::Null),
            right.is_null(),
            || match (left, right) {
                (RawColumn::Text(left), Value::Text(right)) => self.collation.cmp_text(left, right),
                _ => left
                    .cmp_value(right)
                    .unwrap_or_else(|| left.to_value().cmp(right)),
            },
        )
    }
}

enum SortKey<'a> {
    Borrowed(RawColumn<'a>),
    Owned(Value),
}

impl SortKey<'_> {
    fn compare(&self, right: &Value, order: SortOrder) -> std::cmp::Ordering {
        match self {
            Self::Borrowed(raw) => order.compare_raw(*raw, right),
            Self::Owned(value) => order.compare(value, right),
        }
    }

    fn into_owned(self) -> Value {
        match self {
            Self::Borrowed(raw) => raw.to_value(),
            Self::Owned(value) => value,
        }
    }
}

struct Candidate {
    sort_key: Value,
    raw_key: Vec<u8>,
    raw_value: Vec<u8>,
    order: SortOrder,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        #[cfg(test)]
        tick_topk_sort_cancel();
        self.order.compare(&self.sort_key, &other.sort_key)
    }
}

pub(super) struct TopKScanPlan {
    sort_target: SortTarget,
    default_expr: Option<Expr>,
    fast_pred: Option<FastPredicate>,
    num_pk_cols: usize,
    pk_is_int: bool,
    descending: bool,
    nulls_first: bool,
    keep: usize,
    collation: crate::types::Collation,
}

fn topk_simple_sort_column(expr: &Expr, col_map: &ColumnMap) -> Option<(usize, Option<Collation>)> {
    let (expr, explicit_collation) = match expr {
        Expr::Collate { expr, collation } => (expr.as_ref(), Some(*collation)),
        other => (other, None),
    };
    resolve_simple_col(expr, col_map).map(|index| (index, explicit_collation))
}

fn topk_ordinal_sort_column(
    columns: &[SelectColumn],
    mut position: usize,
    schema: &TableSchema,
    col_map: &ColumnMap,
) -> Option<(usize, Option<Collation>)> {
    for column in columns {
        match column {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                if position < schema.columns.len() {
                    return Some((position, None));
                }
                position -= schema.columns.len();
            }
            SelectColumn::Expr { expr, .. } => {
                if position == 0 {
                    return topk_simple_sort_column(expr, col_map);
                }
                position -= 1;
            }
        }
    }
    None
}

#[cfg(test)]
thread_local! {
    static TOPK_SORT_CANCEL_HOOK: std::cell::RefCell<Option<(CancelToken, usize, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn arm_topk_sort_cancel(token: CancelToken, after_comparisons: usize) {
    TOPK_SORT_CANCEL_HOOK.with(|hook| {
        *hook.borrow_mut() = Some((token, after_comparisons, 0));
    });
}

#[cfg(test)]
fn tick_topk_sort_cancel() {
    TOPK_SORT_CANCEL_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        if let Some((token, cancel_after, comparisons)) = hook.as_mut() {
            *comparisons += 1;
            if *comparisons == *cancel_after {
                token.cancel();
            }
        }
    });
}

#[cfg(test)]
pub(super) fn take_topk_sort_comparisons() -> usize {
    TOPK_SORT_CANCEL_HOOK.with(|hook| {
        hook.borrow_mut()
            .take()
            .map_or(0, |(_, _, comparisons)| comparisons)
    })
}

impl TopKScanPlan {
    pub(super) fn try_new(stmt: &SelectStmt, schema: &TableSchema) -> Result<Option<Self>> {
        if stmt.order_by.len() != 1
            || stmt.limit.is_none()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
            || !stmt.joins.is_empty()
            || stmt.distinct
        {
            return Ok(None);
        }

        if has_any_window_function(stmt) {
            return Ok(None);
        }

        let has_aggregates = stmt.columns.iter().any(|c| match c {
            SelectColumn::Expr { expr, .. } => is_aggregate_expr(expr),
            _ => false,
        });
        if has_aggregates {
            return Ok(None);
        }

        let fast_pred = match &stmt.where_clause {
            None => None,
            Some(expr) => {
                let Some(predicate) = FastPredicate::try_new(expr, schema) else {
                    return Ok(None);
                };
                if !matches!(
                    crate::planner::plan_select_inverted(schema, &stmt.where_clause),
                    crate::planner::ScanPlan::SeqScan
                ) {
                    return Ok(None);
                }
                Some(predicate)
            }
        };
        let ob = &stmt.order_by[0];
        let col_map = schema.column_map();
        let resolved = if let Some(position) = ob.output_ordinal {
            topk_ordinal_sort_column(&stmt.columns, position, schema, col_map)
        } else {
            topk_simple_sort_column(&ob.expr, col_map)
        };
        let (col_idx, explicit_coll) = match resolved {
            Some(resolved) => resolved,
            None => return Ok(None),
        };
        // Virtual generated columns are stored as NULL placeholders; the
        // raw-bytes scan cannot compute them.
        if matches!(
            schema.columns[col_idx].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) || schema.columns[col_idx]
            .default_expr
            .as_ref()
            .is_some_and(|expr| crate::parser::volatile_function_in_expr(expr).is_some())
        {
            return Ok(None);
        }
        let collation = explicit_coll.unwrap_or_else(|| schema.columns[col_idx].collation);

        let non_pk = schema.non_pk_indices();
        let enc_pos_arr = schema.encoding_positions();
        let sort_target = if let Some(pk_pos) = schema
            .primary_key_columns
            .iter()
            .position(|&i| i as usize == col_idx)
        {
            SortTarget::Primary(pk_pos)
        } else {
            let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
            SortTarget::Column(enc_pos_arr[nonpk_order] as usize)
        };

        let limit = eval_row_count(stmt.limit.as_ref().unwrap())?;
        let offset = stmt
            .offset
            .as_ref()
            .map(eval_row_count)
            .transpose()?
            .unwrap_or(0);
        let keep = if limit == 0 {
            0
        } else {
            limit.saturating_add(offset)
        };

        Ok(Some(Self {
            sort_target,
            default_expr: schema.columns[col_idx].default_expr.clone(),
            fast_pred,
            num_pk_cols: schema.primary_key_columns.len(),
            pk_is_int: schema.primary_key_columns.len() == 1
                && schema.columns[schema.primary_key_columns[0] as usize].data_type
                    == DataType::Integer,
            descending: ob.descending,
            nulls_first: ob.nulls_first.unwrap_or(!ob.descending),
            keep,
            collation,
        }))
    }

    pub(super) fn execute_scan(
        &self,
        schema: &TableSchema,
        stmt: &SelectStmt,
        cancel: Option<&CancelToken>,
        scan: impl FnOnce(
            &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> std::result::Result<(), citadel::Error>,
    ) -> Result<ExecutionResult> {
        check_cancel(cancel)?;
        let order = SortOrder {
            descending: self.descending,
            nulls_first: self.nulls_first,
            collation: self.collation,
        };
        let mut visited = 0;
        let mut accepts = |key: &[u8], value: &[u8]| -> Result<bool> {
            check_cancel_at(cancel, visited)?;
            visited += 1;
            self.fast_pred
                .as_ref()
                .map_or(Ok(true), |predicate| predicate.matches_raw(key, value))
        };
        let k = self.keep;
        if k == 0 {
            return finish_topk(schema, stmt, Vec::new(), cancel);
        }
        let decoder = SelectRowDecoder::new(schema, stmt, cancel)?;
        // Primary-tree order == output order: keep the first k, skip the heap.
        if matches!(self.sort_target, SortTarget::Primary(0))
            && !self.descending
            && self.collation == crate::types::Collation::Binary
        {
            let mut firsts = Vec::new();
            let mut scan_err = None;
            scan(&mut |key, value| {
                match accepts(key, value) {
                    Ok(true) => {}
                    Ok(false) => return true,
                    Err(error) => {
                        scan_err = Some(error);
                        return false;
                    }
                }
                firsts.push((key.to_vec(), value.to_vec()));
                firsts.len() < k
            })
            .map_err(SqlError::Storage)?;
            if let Some(error) = scan_err {
                return Err(error);
            }
            let mut rows: Vec<Vec<Value>> = Vec::with_capacity(firsts.len());
            for (row_idx, (key, value)) in firsts.iter().enumerate() {
                check_cancel_at(cancel, row_idx)?;
                rows.push(decoder.decode(key, value, cancel)?);
            }
            return finish_topk(schema, stmt, rows, cancel);
        }
        let mut heap = std::collections::BinaryHeap::<Candidate>::new();
        let mut scan_err: Option<SqlError> = None;

        scan(&mut |key, value| {
            match accepts(key, value) {
                Ok(true) => {}
                Ok(false) => return true,
                Err(error) => {
                    scan_err = Some(error);
                    return false;
                }
            }
            let sort_key = match self.read_sort_key(key, value, cancel) {
                Ok(key) => key,
                Err(error) => {
                    scan_err = Some(error);
                    return false;
                }
            };
            if heap.len() >= k {
                if let Some(top) = heap.peek() {
                    if sort_key.compare(&top.sort_key, order) != std::cmp::Ordering::Less {
                        return true;
                    }
                }
            }
            let candidate = Candidate {
                sort_key: sort_key.into_owned(),
                raw_key: key.to_vec(),
                raw_value: value.to_vec(),
                order,
            };
            if heap.len() < k {
                heap.push(candidate);
            } else if let Some(mut top) = heap.peek_mut() {
                *top = candidate;
            }
            true
        })
        .map_err(SqlError::Storage)?;

        if let Some(e) = scan_err {
            return Err(e);
        }

        let winners = sort_vec_by(heap.into_vec(), cancel, |a, b| a.cmp(b))?;

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(winners.len());
        for (winner_idx, w) in winners.iter().enumerate() {
            check_cancel_at(cancel, winner_idx)?;
            rows.push(decoder.decode(&w.raw_key, &w.raw_value, cancel)?);
        }

        finish_topk(schema, stmt, rows, cancel)
    }

    fn read_sort_key<'a>(
        &self,
        key: &'a [u8],
        value: &'a [u8],
        cancel: Option<&CancelToken>,
    ) -> Result<SortKey<'a>> {
        Ok(match self.sort_target {
            SortTarget::Primary(index) if self.pk_is_int && index == 0 => {
                SortKey::Owned(Value::Integer(decode_pk_integer(key)?))
            }
            SortTarget::Primary(index) => {
                let mut values = decode_composite_key(key, self.num_pk_cols)?;
                SortKey::Owned(std::mem::take(&mut values[index]))
            }
            SortTarget::Column(index) => match decode_stored_column_raw(value, index)? {
                Some(raw) => SortKey::Borrowed(raw),
                None => SortKey::Owned(
                    self.default_expr
                        .as_ref()
                        .map(|expr| eval_const_expr_with_cancel(expr, cancel))
                        .transpose()?
                        .unwrap_or(Value::Null),
                ),
            },
        })
    }
}

fn finish_topk(
    schema: &TableSchema,
    stmt: &SelectStmt,
    mut rows: Vec<Vec<Value>>,
    cancel: Option<&CancelToken>,
) -> Result<ExecutionResult> {
    if let Some(ref offset_expr) = stmt.offset {
        let offset = eval_row_count(offset_expr)?;
        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }
    }
    if let Some(ref limit_expr) = stmt.limit {
        let limit = eval_row_count(limit_expr)?;
        rows.truncate(limit);
    }

    let (col_names, projected) =
        project_rows_with_cancel(&schema.columns, &stmt.columns, rows, cancel)?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: col_names,
        rows: projected,
    }))
}

#[cfg(test)]
#[path = "topk_tests.rs"]
mod tests;
