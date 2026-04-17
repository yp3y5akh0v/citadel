use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_composite_key, decode_key_value, decode_pk_integer,
    encode_composite_key, row_non_pk_count, RawColumn,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap};
use crate::parser::*;
use crate::planner::{self, ScanPlan};
use crate::types::*;

use super::helpers::*;

/// Check PK range conditions. Returns: 0 = match, 1 = below lower (skip), 2 = above upper (stop).
fn check_pk_range(pk_val: &Value, range_conds: &[(BinOp, Value)]) -> u8 {
    for (op, bound) in range_conds {
        match op {
            BinOp::Lt if pk_val >= bound => return 2,
            BinOp::LtEq if pk_val > bound => return 2,
            BinOp::Gt if pk_val <= bound => return 1,
            BinOp::GtEq if pk_val < bound => return 1,
            _ => {}
        }
    }
    0
}

pub(super) fn extract_pk_key(
    idx_key: &[u8],
    idx_value: &[u8],
    is_unique: bool,
    num_index_cols: usize,
    num_pk_cols: usize,
) -> Result<Vec<u8>> {
    if is_unique && !idx_value.is_empty() {
        Ok(idx_value.to_vec())
    } else {
        let total_cols = num_index_cols + num_pk_cols;
        let all_values = decode_composite_key(idx_key, total_cols)?;
        let pk_values = &all_values[num_index_cols..];
        Ok(encode_composite_key(pk_values))
    }
}

pub(super) fn check_range_conditions(
    idx_key: &[u8],
    num_prefix_cols: usize,
    range_conds: &[(BinOp, Value)],
    num_index_cols: usize,
) -> Result<RangeCheck> {
    if range_conds.is_empty() {
        return Ok(RangeCheck::Match);
    }

    let num_to_decode = num_prefix_cols + 1;
    if num_to_decode > num_index_cols {
        return Ok(RangeCheck::Match);
    }

    // Decode just enough columns to check the range column
    let mut pos = 0;
    for _ in 0..num_prefix_cols {
        let (_, n) = decode_key_value(&idx_key[pos..])?;
        pos += n;
    }
    let (range_val, _) = decode_key_value(&idx_key[pos..])?;

    let mut exceeds_upper = false;
    let mut below_lower = false;

    for (op, val) in range_conds {
        match op {
            BinOp::Lt if range_val >= *val => exceeds_upper = true,
            BinOp::LtEq if range_val > *val => exceeds_upper = true,
            BinOp::Gt if range_val <= *val => below_lower = true,
            BinOp::GtEq if range_val < *val => below_lower = true,
            _ => {}
        }
    }

    if exceeds_upper {
        Ok(RangeCheck::ExceedsUpper)
    } else if below_lower {
        Ok(RangeCheck::BelowLower)
    } else {
        Ok(RangeCheck::Match)
    }
}

pub(super) enum RangeCheck {
    Match,
    BelowLower,
    ExceedsUpper,
}

/// Collect rows via ReadTxn using the scan plan.
pub(super) fn collect_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;

    match plan {
        ScanPlan::SeqScan => {
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));

            if let Some(ref pred) = simple_pred {
                let mut rtx = db.begin_read();
                let entry_count =
                    rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
                let mut rows = Vec::with_capacity(entry_count / 4);
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
                    match pred.matches_raw(key, value) {
                        Ok(true) => match decode_full_row(table_schema, key, value) {
                            Ok(row) => rows.push(row),
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
                            }
                        },
                        Ok(false) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                    scan_err.is_none() && limit.map_or(true, |n| rows.len() < n)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                return Ok((rows, true));
            }

            let mut rtx = db.begin_read();
            let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
            let capacity = if where_clause.is_some() {
                entry_count / 4
            } else {
                entry_count
            };
            let mut rows = Vec::with_capacity(capacity);
            let mut scan_err: Option<SqlError> = None;

            let col_map = ColumnMap::new(columns);
            let partial_ctx = where_clause.as_ref().and_then(|expr| {
                let needed = referenced_columns(expr, columns);
                if needed.len() < columns.len() {
                    Some(PartialDecodeCtx::new(table_schema, &needed))
                } else {
                    None
                }
            });

            rtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                match (&where_clause, &partial_ctx) {
                    (Some(expr), Some(ctx)) => match ctx.decode(key, value) {
                        Ok(partial) => match eval_expr(expr, &col_map, &partial) {
                            Ok(val) if is_truthy(&val) => match ctx.complete(partial, key, value) {
                                Ok(row) => rows.push(row),
                                Err(e) => scan_err = Some(e),
                            },
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    (Some(expr), None) => match decode_full_row(table_schema, key, value) {
                        Ok(row) => match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    _ => match decode_full_row(table_schema, key, value) {
                        Ok(row) => rows.push(row),
                        Err(e) => scan_err = Some(e),
                    },
                }
                let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                Ok(keep_going)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    if let Some(ref expr) = where_clause {
                        let col_map = ColumnMap::new(columns);
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => Ok((vec![row], true)),
                            _ => Ok((vec![], true)),
                        }
                    } else {
                        Ok((vec![row], false))
                    }
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
        } => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            let col_map = ColumnMap::new(columns);
            rtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => {
                        let keep = match &where_clause {
                            Some(expr) => {
                                eval_expr(expr, &col_map, &row).is_ok_and(|v| is_truthy(&v))
                            }
                            None => true,
                        };
                        if keep {
                            rows.push(row);
                        }
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none() && limit.map_or(true, |n| rows.len() < n))
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, true))
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let col_map = ColumnMap::new(columns);
            for pk_key in &pk_keys {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    let row = decode_full_row(table_schema, pk_key, &value)?;
                    if let Some(ref expr) = where_clause {
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            _ => {}
                        }
                    } else {
                        rows.push(row);
                    }
                }
            }
            Ok((rows, where_clause.is_some()))
        }
    }
}

/// Collect rows via WriteTxn using the scan plan.
pub(super) fn collect_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
    limit: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;
    let columns = &table_schema.columns;

    match plan {
        ScanPlan::SeqScan => {
            let simple_pred = where_clause
                .as_ref()
                .and_then(|expr| try_simple_predicate(expr, table_schema));

            if let Some(ref pred) = simple_pred {
                let mut rows = Vec::new();
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                    match pred.matches_raw(key, value) {
                        Ok(true) => match decode_full_row(table_schema, key, value) {
                            Ok(row) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                        },
                        Ok(false) => {}
                        Err(e) => scan_err = Some(e),
                    }
                    let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                    Ok(keep_going)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
                return Ok((rows, true));
            }

            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;

            let col_map = ColumnMap::new(columns);
            let partial_ctx = where_clause.as_ref().and_then(|expr| {
                let needed = referenced_columns(expr, columns);
                if needed.len() < columns.len() {
                    Some(PartialDecodeCtx::new(table_schema, &needed))
                } else {
                    None
                }
            });

            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                match (&where_clause, &partial_ctx) {
                    (Some(expr), Some(ctx)) => match ctx.decode(key, value) {
                        Ok(partial) => match eval_expr(expr, &col_map, &partial) {
                            Ok(val) if is_truthy(&val) => match ctx.complete(partial, key, value) {
                                Ok(row) => rows.push(row),
                                Err(e) => scan_err = Some(e),
                            },
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    (Some(expr), None) => match decode_full_row(table_schema, key, value) {
                        Ok(row) => match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            Err(e) => scan_err = Some(e),
                            _ => {}
                        },
                        Err(e) => scan_err = Some(e),
                    },
                    _ => match decode_full_row(table_schema, key, value) {
                        Ok(row) => rows.push(row),
                        Err(e) => scan_err = Some(e),
                    },
                }
                let keep_going = scan_err.is_none() && limit.map_or(true, |n| rows.len() < n);
                Ok(keep_going)
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, where_clause.is_some()))
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    if let Some(ref expr) = where_clause {
                        let col_map = ColumnMap::new(columns);
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => Ok((vec![row], true)),
                            _ => Ok((vec![], true)),
                        }
                    } else {
                        Ok((vec![row], false))
                    }
                }
                None => Ok((vec![], true)),
            }
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            let col_map = ColumnMap::new(columns);
            wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => {
                        let keep = match &where_clause {
                            Some(expr) => {
                                eval_expr(expr, &col_map, &row).is_ok_and(|v| is_truthy(&v))
                            }
                            None => true,
                        };
                        if keep {
                            rows.push(row);
                        }
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none() && limit.map_or(true, |n| rows.len() < n))
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok((rows, true))
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            let col_map = ColumnMap::new(columns);
            for pk_key in &pk_keys {
                if let Some(value) = wtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    let row = decode_full_row(table_schema, pk_key, &value)?;
                    if let Some(ref expr) = where_clause {
                        match eval_expr(expr, &col_map, &row) {
                            Ok(val) if is_truthy(&val) => rows.push(row),
                            _ => {}
                        }
                    } else {
                        rows.push(row);
                    }
                }
            }
            Ok((rows, where_clause.is_some()))
        }
    }
}

/// Collect (encoded_key, full_row) pairs via ReadTxn. Used by UPDATE/DELETE.
pub(super) fn collect_keyed_rows_read(
    db: &Database,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            let mut rtx = db.begin_read();
            match rtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
        } => {
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            let mut scan_err: Option<SqlError> = None;
            rtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();
            {
                let mut rtx = db.begin_read();
                let mut scan_err: Option<SqlError> = None;
                rtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }
            let mut rows = Vec::new();
            let mut rtx = db.begin_read();
            for pk_key in &pk_keys {
                if let Some(value) = rtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((
                        pk_key.clone(),
                        decode_full_row(table_schema, pk_key, &value)?,
                    ));
                }
            }
            Ok(rows)
        }
    }
}

/// Collect (encoded_key, full_row) pairs via WriteTxn using the scan plan.
pub(super) fn collect_keyed_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    where_clause: &Option<Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let plan = planner::plan_select(table_schema, where_clause);
    let lower_name = &table_schema.name;

    match plan {
        ScanPlan::SeqScan => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => scan_err = Some(e),
                }
                Ok(())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(&pk_values);
            match wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                Some(value) => {
                    let row = decode_full_row(table_schema, &key, &value)?;
                    Ok(vec![(key, row)])
                }
                None => Ok(vec![]),
            }
        }

        ScanPlan::PkRangeScan {
            ref start_key,
            ref range_conds,
            num_pk_cols,
        } => {
            let mut rows = Vec::new();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let pk_vals = match decode_composite_key(key, num_pk_cols) {
                    Ok(v) => v,
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                };
                match check_pk_range(&pk_vals[0], range_conds) {
                    2 => return Ok(false),
                    1 => return Ok(true),
                    _ => {}
                }
                match decode_full_row(table_schema, key, value) {
                    Ok(row) => rows.push((key.to_vec(), row)),
                    Err(e) => {
                        scan_err = Some(e);
                        return Ok(false);
                    }
                }
                Ok(scan_err.is_none())
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
            Ok(rows)
        }

        ScanPlan::IndexScan {
            idx_table,
            prefix,
            num_prefix_cols,
            range_conds,
            is_unique,
            index_columns,
            ..
        } => {
            let num_pk_cols = table_schema.primary_key_columns.len();
            let num_index_cols = index_columns.len();
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();

            {
                let mut scan_err: Option<SqlError> = None;
                wtx.table_scan_from(&idx_table, &prefix, |key, value| {
                    if !key.starts_with(&prefix) {
                        return Ok(false);
                    }
                    match check_range_conditions(key, num_prefix_cols, &range_conds, num_index_cols)
                    {
                        Ok(RangeCheck::ExceedsUpper) => return Ok(false),
                        Ok(RangeCheck::BelowLower) => return Ok(true),
                        Ok(RangeCheck::Match) => {}
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    match extract_pk_key(key, value, is_unique, num_index_cols, num_pk_cols) {
                        Ok(pk) => pk_keys.push(pk),
                        Err(e) => {
                            scan_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
                .map_err(SqlError::Storage)?;
                if let Some(e) = scan_err {
                    return Err(e);
                }
            }

            let mut rows = Vec::new();
            for pk_key in &pk_keys {
                if let Some(value) = wtx
                    .table_get(lower_name.as_bytes(), pk_key)
                    .map_err(SqlError::Storage)?
                {
                    rows.push((
                        pk_key.clone(),
                        decode_full_row(table_schema, pk_key, &value)?,
                    ));
                }
            }
            Ok(rows)
        }
    }
}

pub(super) struct SimplePredicate {
    is_pk: bool,
    pk_pos: usize,
    nonpk_idx: usize,
    op: BinOp,
    literal: Value,
    num_pk_cols: usize,
    precomputed_int: Option<i64>,
    default_int: Option<i64>,
    default_val: Option<Value>,
}

impl SimplePredicate {
    pub(super) fn matches_raw(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if let Some(target) = self.precomputed_int {
            return Ok(self.match_nonpk_int_inline(value, target));
        }
        let raw = if self.is_pk {
            if self.num_pk_cols == 1 {
                RawColumn::Integer(decode_pk_integer(key)?)
            } else {
                let pk = decode_composite_key(key, self.num_pk_cols)?;
                match &pk[self.pk_pos] {
                    Value::Integer(i) => RawColumn::Integer(*i),
                    Value::Real(r) => RawColumn::Real(*r),
                    Value::Boolean(b) => RawColumn::Boolean(*b),
                    _ => {
                        return Ok(raw_matches_op_value(
                            &pk[self.pk_pos],
                            self.op,
                            &self.literal,
                        ))
                    }
                }
            }
        } else if self.nonpk_idx >= row_non_pk_count(value) {
            return Ok(match &self.default_val {
                Some(d) => raw_matches_op_value(d, self.op, &self.literal),
                None => false,
            });
        } else {
            decode_column_raw(value, self.nonpk_idx)?
        };
        Ok(raw_matches_op(&raw, self.op, &self.literal))
    }

    #[inline(always)]
    fn match_nonpk_int_inline(&self, data: &[u8], target: i64) -> bool {
        let col_count = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;

        if self.nonpk_idx >= col_count {
            return match self.default_int {
                Some(v) => match self.op {
                    BinOp::Eq => v == target,
                    BinOp::NotEq => v != target,
                    BinOp::Lt => v < target,
                    BinOp::Gt => v > target,
                    BinOp::LtEq => v <= target,
                    BinOp::GtEq => v >= target,
                    _ => false,
                },
                None => false,
            };
        }

        let bm_bytes = col_count.div_ceil(8);

        // NULL -> false (SQL NULL semantics)
        if data[2 + self.nonpk_idx / 8] & (1 << (self.nonpk_idx % 8)) != 0 {
            return false;
        }

        let mut pos = 2 + bm_bytes;

        // Skip preceding non-null columns by reading their length
        for col in 0..self.nonpk_idx {
            if data[2 + col / 8] & (1 << (col % 8)) == 0 {
                let len = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
                pos += 5 + len;
            }
        }

        // Read i64 directly: skip type_tag(1) + len(4), read 8 bytes
        let v = i64::from_le_bytes(data[pos + 5..pos + 13].try_into().unwrap());

        match self.op {
            BinOp::Eq => v == target,
            BinOp::NotEq => v != target,
            BinOp::Lt => v < target,
            BinOp::Gt => v > target,
            BinOp::LtEq => v <= target,
            BinOp::GtEq => v >= target,
            _ => false,
        }
    }
}

pub(super) fn try_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    let (col_name, op, literal) = match expr {
        Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(name), Expr::Literal(lit)) => (name.as_str(), *op, lit),
            (Expr::Literal(lit), Expr::Column(name)) => (name.as_str(), flip_cmp_op(*op)?, lit),
            _ => return None,
        },
        _ => return None,
    };

    if !matches!(
        op,
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::Gt | BinOp::LtEq | BinOp::GtEq
    ) {
        return None;
    }

    let col_idx = schema.column_index(col_name)?;
    let non_pk = schema.non_pk_indices();

    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&i| i as usize == col_idx)
    {
        Some(SimplePredicate {
            is_pk: true,
            pk_pos,
            nonpk_idx: 0,
            op,
            literal: literal.clone(),
            num_pk_cols: schema.primary_key_columns.len(),
            precomputed_int: None,
            default_int: None,
            default_val: None,
        })
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx)?;
        let nonpk_idx = schema.encoding_positions()[nonpk_order] as usize;
        let precomputed_int = match literal {
            Value::Integer(i) => Some(*i),
            _ => None,
        };
        let default_val = schema.columns[col_idx]
            .default_expr
            .as_ref()
            .and_then(|expr| eval_const_expr(expr).ok());
        let default_int = default_val.as_ref().and_then(|v| match v {
            Value::Integer(i) => Some(*i),
            _ => None,
        });
        Some(SimplePredicate {
            is_pk: false,
            pk_pos: 0,
            nonpk_idx,
            op,
            literal: literal.clone(),
            num_pk_cols: schema.primary_key_columns.len(),
            precomputed_int,
            default_int,
            default_val,
        })
    }
}

pub(super) fn flip_cmp_op(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Eq => Some(BinOp::Eq),
        BinOp::NotEq => Some(BinOp::NotEq),
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::LtEq => Some(BinOp::GtEq),
        BinOp::GtEq => Some(BinOp::LtEq),
        _ => None,
    }
}

pub(super) fn raw_matches_op(raw: &RawColumn, op: BinOp, literal: &Value) -> bool {
    // SQL NULL semantics: any comparison involving NULL yields NULL (falsy)
    if matches!(raw, RawColumn::Null) || literal.is_null() {
        return false;
    }
    match op {
        BinOp::Eq => raw.eq_value(literal),
        BinOp::NotEq => !raw.eq_value(literal),
        BinOp::Lt => raw.cmp_value(literal) == Some(std::cmp::Ordering::Less),
        BinOp::Gt => raw.cmp_value(literal) == Some(std::cmp::Ordering::Greater),
        BinOp::LtEq => raw
            .cmp_value(literal)
            .is_some_and(|o| o != std::cmp::Ordering::Greater),
        BinOp::GtEq => raw
            .cmp_value(literal)
            .is_some_and(|o| o != std::cmp::Ordering::Less),
        _ => false,
    }
}

pub(super) fn raw_matches_op_value(val: &Value, op: BinOp, literal: &Value) -> bool {
    match op {
        BinOp::Eq => val == literal,
        BinOp::NotEq => val != literal && !val.is_null(),
        BinOp::Lt => val < literal,
        BinOp::Gt => val > literal,
        BinOp::LtEq => val <= literal,
        BinOp::GtEq => val >= literal,
        _ => false,
    }
}
