use std::collections::VecDeque;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, ColumnMap};
use crate::parser::*;
use crate::types::*;

use super::helpers::*;

// ── Window functions ────────────────────────────────────────────────

pub(super) fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
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
        Expr::Function { name, args } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| extract_window_fns(a, slot_counter, extracted))
                .collect(),
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

/// Convert frame bounds to (start_idx, end_idx) for ROWS frames.
pub(super) fn rows_frame_indices(
    frame: &WindowFrame,
    i: usize,
    n: usize,
) -> Result<(usize, usize)> {
    let start = match &frame.start {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(e) => {
            let k = eval_const_int(e)? as usize;
            i.saturating_sub(k)
        }
        WindowFrameBound::CurrentRow => i,
        WindowFrameBound::Following(e) => {
            let k = eval_const_int(e)? as usize;
            (i + k).min(n - 1)
        }
        WindowFrameBound::UnboundedFollowing => n - 1,
    };
    let end = match &frame.end {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(e) => {
            let k = eval_const_int(e)? as usize;
            i.saturating_sub(k)
        }
        WindowFrameBound::CurrentRow => i,
        WindowFrameBound::Following(e) => {
            let k = eval_const_int(e)? as usize;
            (i + k).min(n - 1)
        }
        WindowFrameBound::UnboundedFollowing => n - 1,
    };
    Ok((start, end.min(n - 1)))
}

/// For RANGE frames, find peer group boundaries (rows with same ORDER BY key).
pub(super) fn find_peer_range(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
    i: usize,
) -> (usize, usize) {
    let key: Vec<Value> = order_by
        .iter()
        .map(|o| eval_expr(&o.expr, col_map, &rows[i]).unwrap_or(Value::Null))
        .collect();
    let mut start = i;
    while start > 0 {
        let prev_key: Vec<Value> = order_by
            .iter()
            .map(|o| eval_expr(&o.expr, col_map, &rows[start - 1]).unwrap_or(Value::Null))
            .collect();
        if prev_key != key {
            break;
        }
        start -= 1;
    }
    let mut end = i;
    while end + 1 < rows.len() {
        let next_key: Vec<Value> = order_by
            .iter()
            .map(|o| eval_expr(&o.expr, col_map, &rows[end + 1]).unwrap_or(Value::Null))
            .collect();
        if next_key != key {
            break;
        }
        end += 1;
    }
    (start, end)
}

/// Resolve frame indices for a given row position within a partition.
pub(super) fn frame_indices(
    frame: &WindowFrame,
    i: usize,
    n: usize,
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Result<(usize, usize)> {
    match frame.units {
        WindowFrameUnits::Rows => rows_frame_indices(frame, i, n),
        WindowFrameUnits::Range => {
            // For RANGE, only UNBOUNDED and CURRENT ROW are supported
            let start = match &frame.start {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::CurrentRow => find_peer_range(rows, order_by, col_map, i).0,
                _ => return Err(SqlError::Unsupported("RANGE with numeric offset".into())),
            };
            let end = match &frame.end {
                WindowFrameBound::UnboundedFollowing => n - 1,
                WindowFrameBound::CurrentRow => find_peer_range(rows, order_by, col_map, i).1,
                _ => return Err(SqlError::Unsupported("RANGE with numeric offset".into())),
            };
            Ok((start, end))
        }
        WindowFrameUnits::Groups => Err(SqlError::Unsupported("GROUPS window frame".into())),
    }
}

// Monotonic deque for O(1) amortized sliding MIN/MAX
pub(super) struct MonoDeque {
    deque: VecDeque<(usize, Value)>,
    is_min: bool,
}

impl MonoDeque {
    pub(super) fn new(is_min: bool) -> Self {
        Self {
            deque: VecDeque::new(),
            is_min,
        }
    }

    pub(super) fn push(&mut self, idx: usize, val: Value) {
        if val.is_null() {
            return;
        }
        while let Some(back) = self.deque.back() {
            let evict = if self.is_min {
                val <= back.1
            } else {
                val >= back.1
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

// Removable accumulator for O(1) sliding SUM/COUNT/AVG
pub(super) struct SlidingSum {
    int_sum: i64,
    real_sum: f64,
    has_real: bool,
    count: i64,
}

impl SlidingSum {
    pub(super) fn new() -> Self {
        Self {
            int_sum: 0,
            real_sum: 0.0,
            has_real: false,
            count: 0,
        }
    }

    pub(super) fn add(&mut self, val: &Value) {
        match val {
            Value::Integer(i) => {
                self.int_sum += i;
                self.count += 1;
            }
            Value::Real(r) => {
                self.real_sum += r;
                self.has_real = true;
                self.count += 1;
            }
            _ => {}
        }
    }

    pub(super) fn remove(&mut self, val: &Value) {
        match val {
            Value::Integer(i) => {
                self.int_sum -= i;
                self.count -= 1;
            }
            Value::Real(r) => {
                self.real_sum -= r;
                self.count -= 1;
            }
            _ => {}
        }
    }

    pub(super) fn result_sum(&self) -> Value {
        if self.count == 0 && !self.has_real {
            Value::Null
        } else if self.has_real {
            Value::Real(self.real_sum + self.int_sum as f64)
        } else {
            Value::Integer(self.int_sum)
        }
    }

    pub(super) fn result_count(&self) -> Value {
        Value::Integer(self.count)
    }

    pub(super) fn result_avg(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            let total = if self.has_real {
                self.real_sum + self.int_sum as f64
            } else {
                self.int_sum as f64
            };
            Value::Real(total / self.count as f64)
        }
    }
}

pub(super) fn eval_window_select(
    columns: &[ColumnDef],
    mut rows: Vec<Vec<Value>>,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    if rows.is_empty() {
        let col_names = stmt
            .columns
            .iter()
            .map(|c| match c {
                SelectColumn::AllColumns => "*".into(),
                SelectColumn::Expr { alias: Some(a), .. } => a.clone(),
                SelectColumn::Expr { expr, .. } => expr_display_name(expr),
            })
            .collect();
        return Ok(ExecutionResult::Query(QueryResult {
            columns: col_names,
            rows: vec![],
        }));
    }

    // 1. Extract window functions from SELECT columns
    let mut slot_counter = 0usize;
    let mut all_extracted: Vec<(String, String, Vec<Expr>, WindowSpec)> = Vec::new();
    let mut rewritten_columns: Vec<SelectColumn> = Vec::new();

    for col in &stmt.columns {
        match col {
            SelectColumn::AllColumns => rewritten_columns.push(SelectColumn::AllColumns),
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
        return super::process_select(columns, rows, stmt, false);
    }

    // 2. Pre-evaluate window function argument expressions per row
    let col_map = ColumnMap::new(columns);
    let num_win = all_extracted.len();
    let mut arg_values: Vec<Vec<Vec<Value>>> = Vec::with_capacity(num_win);
    for (_, _, args, _) in &all_extracted {
        let mut per_row = Vec::with_capacity(rows.len());
        for row in &rows {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval_expr(a, &col_map, row).unwrap_or(Value::Null))
                .collect();
            per_row.push(vals);
        }
        arg_values.push(per_row);
    }

    // 3. Group window functions by (partition_by, order_by) for sort sharing
    let n = rows.len();
    let mut row_results: Vec<Vec<Value>> = (0..n).map(|_| vec![Value::Null; num_win]).collect();

    for (win_idx, (_, fn_name, _, spec)) in all_extracted.iter().enumerate() {
        // Sort rows by (partition_by, order_by) for this window spec
        let mut sort_keys: Vec<OrderByItem> = Vec::new();
        for pb in &spec.partition_by {
            sort_keys.push(OrderByItem {
                expr: pb.clone(),
                descending: false,
                nulls_first: Some(true),
            });
        }
        sort_keys.extend(spec.order_by.clone());

        // Build index array for this sort
        let mut indices: Vec<usize> = (0..n).collect();
        if !sort_keys.is_empty() {
            let keys: Vec<Vec<Value>> = indices
                .iter()
                .map(|&i| {
                    sort_keys
                        .iter()
                        .map(|o| eval_expr(&o.expr, &col_map, &rows[i]).unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
            indices.sort_by(|&a, &b| compare_sort_keys(&keys[a], &keys[b], &sort_keys));
        }

        // Identify partition boundaries
        let part_count = spec.partition_by.len();
        let mut partitions: Vec<(usize, usize)> = Vec::new();
        let mut part_start = 0;
        for pos in 1..n {
            let mut same = true;
            if part_count > 0 {
                for p in 0..part_count {
                    let prev = eval_expr(&spec.partition_by[p], &col_map, &rows[indices[pos - 1]])
                        .unwrap_or(Value::Null);
                    let cur = eval_expr(&spec.partition_by[p], &col_map, &rows[indices[pos]])
                        .unwrap_or(Value::Null);
                    if prev != cur {
                        same = false;
                        break;
                    }
                }
            }
            if !same {
                partitions.push((part_start, pos));
                part_start = pos;
            }
        }
        partitions.push((part_start, n));

        let frame = resolve_frame(spec);
        let upper_name = fn_name.to_ascii_uppercase();

        // Evaluate per partition
        for &(ps, pe) in &partitions {
            let part_len = pe - ps;
            let part_indices = &indices[ps..pe];

            match upper_name.as_str() {
                "ROW_NUMBER" => {
                    for (rank, &orig_idx) in part_indices.iter().enumerate() {
                        row_results[orig_idx][win_idx] = Value::Integer(rank as i64 + 1);
                    }
                }
                "RANK" => {
                    if spec.order_by.is_empty() {
                        return Err(SqlError::WindowFunctionRequiresOrderBy("RANK".into()));
                    }
                    let mut rank = 1i64;
                    let mut prev_key: Option<Vec<Value>> = None;
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        let key: Vec<Value> = spec
                            .order_by
                            .iter()
                            .map(|o| {
                                eval_expr(&o.expr, &col_map, &rows[orig_idx]).unwrap_or(Value::Null)
                            })
                            .collect();
                        if let Some(ref pk) = prev_key {
                            if &key != pk {
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
                    let mut prev_key: Option<Vec<Value>> = None;
                    for &orig_idx in part_indices {
                        let key: Vec<Value> = spec
                            .order_by
                            .iter()
                            .map(|o| {
                                eval_expr(&o.expr, &col_map, &rows[orig_idx]).unwrap_or(Value::Null)
                            })
                            .collect();
                        if let Some(ref pk) = prev_key {
                            if &key != pk {
                                rank += 1;
                            }
                        }
                        row_results[orig_idx][win_idx] = Value::Integer(rank);
                        prev_key = Some(key);
                    }
                }
                "NTILE" => {
                    let ntile_n = if arg_values[win_idx][0].is_empty() {
                        return Err(SqlError::Parse("NTILE requires one argument".into()));
                    } else {
                        match &arg_values[win_idx][part_indices[0]][0] {
                            Value::Integer(n) if *n > 0 => *n as usize,
                            _ => {
                                return Err(SqlError::InvalidValue(
                                    "NTILE argument must be a positive integer".into(),
                                ))
                            }
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
                    for &orig_idx in part_indices {
                        row_results[orig_idx][win_idx] = Value::Integer(bucket as i64);
                        count_in_bucket += 1;
                        if count_in_bucket >= bucket_size(bucket) && bucket < ntile_n {
                            bucket += 1;
                            count_in_bucket = 0;
                        }
                    }
                }
                "LAG" | "LEAD" => {
                    let offset = if arg_values[win_idx][0].len() >= 2 {
                        match &arg_values[win_idx][0][1] {
                            Value::Integer(n) => *n as usize,
                            _ => 1,
                        }
                    } else {
                        1
                    };
                    let default_val = if arg_values[win_idx][0].len() >= 3 {
                        arg_values[win_idx][0][2].clone()
                    } else {
                        Value::Null
                    };
                    let is_lag = upper_name == "LAG";
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        let target_pos = if is_lag {
                            if pos >= offset {
                                Some(pos - offset)
                            } else {
                                None
                            }
                        } else if pos + offset < part_len {
                            Some(pos + offset)
                        } else {
                            None
                        };
                        let val = match target_pos {
                            Some(tp) => arg_values[win_idx][part_indices[tp]][0].clone(),
                            None => default_val.clone(),
                        };
                        row_results[orig_idx][win_idx] = val;
                    }
                }
                "FIRST_VALUE" => {
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        let (fs, _) = frame_indices(
                            &frame,
                            pos,
                            part_len,
                            &part_indices
                                .iter()
                                .map(|&i| rows[i].clone())
                                .collect::<Vec<_>>(),
                            &spec.order_by,
                            &col_map,
                        )?;
                        let source_idx = part_indices[fs];
                        row_results[orig_idx][win_idx] = arg_values[win_idx][source_idx][0].clone();
                    }
                }
                "LAST_VALUE" => {
                    for (pos, &orig_idx) in part_indices.iter().enumerate() {
                        let (_, fe) = frame_indices(
                            &frame,
                            pos,
                            part_len,
                            &part_indices
                                .iter()
                                .map(|&i| rows[i].clone())
                                .collect::<Vec<_>>(),
                            &spec.order_by,
                            &col_map,
                        )?;
                        let source_idx = part_indices[fe];
                        row_results[orig_idx][win_idx] = arg_values[win_idx][source_idx][0].clone();
                    }
                }
                "SUM" | "COUNT" | "AVG" => {
                    let is_count_star = upper_name == "COUNT" && arg_values[win_idx][0].is_empty();
                    // Check if we can use sliding window optimization (ROWS frame)
                    if matches!(frame.units, WindowFrameUnits::Rows)
                        && matches!(
                            frame.start,
                            WindowFrameBound::UnboundedPreceding | WindowFrameBound::Preceding(_)
                        )
                        && matches!(
                            frame.end,
                            WindowFrameBound::CurrentRow | WindowFrameBound::Following(_)
                        )
                    {
                        // Sliding accumulator
                        let mut acc = SlidingSum::new();
                        let mut prev_start = 0usize;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            let (fs, fe) = rows_frame_indices(&frame, pos, part_len)?;
                            // Remove expired rows
                            while prev_start < fs {
                                if is_count_star {
                                    acc.count -= 1;
                                } else {
                                    acc.remove(&arg_values[win_idx][part_indices[prev_start]][0]);
                                }
                                prev_start += 1;
                            }
                            // Add new rows (from previous end+1 to current end)
                            let add_from = if pos == 0 {
                                fs
                            } else {
                                let (_, prev_fe) = rows_frame_indices(&frame, pos - 1, part_len)?;
                                prev_fe + 1
                            };
                            for add_pos in add_from..=fe {
                                if is_count_star {
                                    acc.count += 1;
                                } else {
                                    acc.add(&arg_values[win_idx][part_indices[add_pos]][0]);
                                }
                            }
                            row_results[orig_idx][win_idx] = match upper_name.as_str() {
                                "SUM" => acc.result_sum(),
                                "COUNT" => acc.result_count(),
                                "AVG" => acc.result_avg(),
                                _ => unreachable!(),
                            };
                        }
                    } else {
                        // Fallback: recompute per row
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            let part_rows: Vec<Vec<Value>> =
                                part_indices.iter().map(|&i| rows[i].clone()).collect();
                            let (fs, fe) = frame_indices(
                                &frame,
                                pos,
                                part_len,
                                &part_rows,
                                &spec.order_by,
                                &col_map,
                            )?;
                            let mut acc = SlidingSum::new();
                            for fpos in fs..=fe {
                                if is_count_star {
                                    acc.count += 1;
                                } else {
                                    acc.add(&arg_values[win_idx][part_indices[fpos]][0]);
                                }
                            }
                            row_results[orig_idx][win_idx] = match upper_name.as_str() {
                                "SUM" => acc.result_sum(),
                                "COUNT" => acc.result_count(),
                                "AVG" => acc.result_avg(),
                                _ => unreachable!(),
                            };
                        }
                    }
                }
                "MIN" | "MAX" => {
                    let is_min = upper_name == "MIN";
                    if matches!(frame.units, WindowFrameUnits::Rows)
                        && matches!(
                            frame.start,
                            WindowFrameBound::UnboundedPreceding | WindowFrameBound::Preceding(_)
                        )
                        && matches!(
                            frame.end,
                            WindowFrameBound::CurrentRow | WindowFrameBound::Following(_)
                        )
                    {
                        // Monotonic deque O(N)
                        let mut deque = MonoDeque::new(is_min);
                        let mut prev_end: Option<usize> = None;
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            let (fs, fe) = rows_frame_indices(&frame, pos, part_len)?;
                            // Add new elements
                            let add_from = prev_end.map(|pe| pe + 1).unwrap_or(fs);
                            for add_pos in add_from..=fe {
                                deque.push(
                                    add_pos,
                                    arg_values[win_idx][part_indices[add_pos]][0].clone(),
                                );
                            }
                            deque.pop_expired(fs);
                            row_results[orig_idx][win_idx] = deque.current();
                            prev_end = Some(fe);
                        }
                    } else {
                        // Fallback
                        for (pos, &orig_idx) in part_indices.iter().enumerate() {
                            let part_rows: Vec<Vec<Value>> =
                                part_indices.iter().map(|&i| rows[i].clone()).collect();
                            let (fs, fe) = frame_indices(
                                &frame,
                                pos,
                                part_len,
                                &part_rows,
                                &spec.order_by,
                                &col_map,
                            )?;
                            let mut result = Value::Null;
                            for fpos in fs..=fe {
                                let v = &arg_values[win_idx][part_indices[fpos]][0];
                                if !v.is_null() {
                                    result = match result {
                                        Value::Null => v.clone(),
                                        ref cur => {
                                            if (is_min && v < cur) || (!is_min && v > cur) {
                                                v.clone()
                                            } else {
                                                cur.clone()
                                            }
                                        }
                                    };
                                }
                            }
                            row_results[orig_idx][win_idx] = result;
                        }
                    }
                }
                other => {
                    return Err(SqlError::Unsupported(format!("window function: {other}")));
                }
            }
        }
    }

    // 4. Extend rows with window results
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
        });
    }

    for (row_idx, row) in rows.iter_mut().enumerate() {
        row.extend_from_slice(&row_results[row_idx]);
    }

    // 5. Build rewritten statement for final processing
    let rewritten_stmt = SelectStmt {
        columns: rewritten_columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        joins: stmt.joins.clone(),
        distinct: stmt.distinct,
        where_clause: None, // already applied
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by: vec![],
        having: None,
    };

    super::process_select(&extended_columns, rows, &rewritten_stmt, true)
}
