use std::collections::HashMap;

use citadel::Database;

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::*;
use super::scan::*;

// ── JOIN execution ──────────────────────────────────────────────────

pub(super) fn resolve_table_name<'a>(
    schema: &'a SchemaManager,
    name: &str,
) -> Result<&'a TableSchema> {
    schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))
}

pub(super) fn build_joined_columns(tables: &[(String, &TableSchema)]) -> Vec<ColumnDef> {
    let mut result = Vec::new();
    let mut pos: u16 = 0;

    for (alias, schema) in tables {
        for col in &schema.columns {
            result.push(ColumnDef {
                name: format!("{}.{}", alias.to_ascii_lowercase(), col.name),
                data_type: col.data_type,
                nullable: col.nullable,
                position: pos,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            });
            pos += 1;
        }
    }

    result
}

pub(super) fn extract_equi_join_keys(
    on_expr: &Expr,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    fn flatten<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                flatten(left, out);
                flatten(right, out);
            }
            _ => out.push(e),
        }
    }
    let mut conjuncts = Vec::new();
    flatten(on_expr, &mut conjuncts);

    for expr in conjuncts {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = expr
        {
            if let (Some(l_idx), Some(r_idx)) = (
                resolve_col_idx(left, combined_cols),
                resolve_col_idx(right, combined_cols),
            ) {
                if l_idx < outer_col_count && r_idx >= outer_col_count {
                    pairs.push((l_idx, r_idx - outer_col_count));
                } else if r_idx < outer_col_count && l_idx >= outer_col_count {
                    pairs.push((r_idx, l_idx - outer_col_count));
                }
            }
        }
    }

    pairs
}

pub(super) fn resolve_col_idx(expr: &Expr, columns: &[ColumnDef]) -> Option<usize> {
    match expr {
        Expr::Column(name) => {
            let matches: Vec<usize> = columns
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    c.name == *name
                        || (c.name.len() > name.len()
                            && c.name.as_bytes()[c.name.len() - name.len() - 1] == b'.'
                            && c.name.ends_with(name.as_str()))
                })
                .map(|(i, _)| i)
                .collect();
            if matches.len() == 1 {
                Some(matches[0])
            } else {
                None
            }
        }
        Expr::QualifiedColumn { table, column } => {
            let qualified = format!("{table}.{column}");
            columns.iter().position(|c| c.name == qualified)
        }
        _ => None,
    }
}

pub(super) fn hash_key(row: &[Value], col_indices: &[usize]) -> Vec<Value> {
    col_indices.iter().map(|&i| row[i].clone()).collect()
}

pub(super) fn count_conjuncts(expr: &Expr) -> usize {
    match expr {
        Expr::BinaryOp {
            op: BinOp::And,
            left,
            right,
        } => count_conjuncts(left) + count_conjuncts(right),
        _ => 1,
    }
}

pub(super) fn combine_row(outer: &[Value], inner: &[Value], cap: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(cap);
    combined.extend(outer.iter().cloned());
    combined.extend(inner.iter().cloned());
    combined
}

pub(super) struct CombineProjection {
    slots: Vec<(usize, bool)>,
}

pub(super) fn combine_row_projected(
    outer: &[Value],
    inner: &[Value],
    proj: &CombineProjection,
) -> Vec<Value> {
    proj.slots
        .iter()
        .map(|&(idx, is_inner)| {
            if is_inner {
                inner[idx].clone()
            } else {
                outer[idx].clone()
            }
        })
        .collect()
}

pub(super) fn build_combine_projection(
    needed_combined: &[usize],
    outer_col_count: usize,
) -> CombineProjection {
    CombineProjection {
        slots: needed_combined
            .iter()
            .map(|&ci| {
                if ci < outer_col_count {
                    (ci, false)
                } else {
                    (ci - outer_col_count, true)
                }
            })
            .collect(),
    }
}

pub(super) fn build_projected_columns(
    full_cols: &[ColumnDef],
    needed_combined: &[usize],
) -> Vec<ColumnDef> {
    needed_combined
        .iter()
        .enumerate()
        .map(|(new_pos, &old_pos)| {
            let orig = &full_cols[old_pos];
            ColumnDef {
                name: orig.name.clone(),
                data_type: orig.data_type,
                nullable: orig.nullable,
                position: new_pos as u16,
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_integer_join(
    outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join_type: &JoinType,
    outer_key_col: usize,
    inner_key_col: usize,
    outer_col_count: usize,
    inner_col_count: usize,
    outer_is_sorted: bool,
    projection: Option<&CombineProjection>,
) -> std::result::Result<Vec<Vec<Value>>, Vec<Vec<Value>>> {
    let cap = projection.map_or(outer_col_count + inner_col_count, |p| p.slots.len());

    if outer_is_sorted && matches!(join_type, JoinType::Inner | JoinType::Cross) {
        let mut sorted_inner: Vec<(i64, usize)> = Vec::with_capacity(inner_rows.len());
        let mut needs_sort = false;
        let mut prev = i64::MIN;
        for (i, r) in inner_rows.iter().enumerate() {
            match r[inner_key_col] {
                Value::Integer(k) => {
                    if k < prev {
                        needs_sort = true;
                    }
                    prev = k;
                    sorted_inner.push((k, i));
                }
                Value::Null => {}
                _ => return Err(outer_rows),
            }
        }
        if needs_sort {
            sorted_inner.sort_unstable_by_key(|&(k, _)| k);
        }

        let mut result = Vec::with_capacity(outer_rows.len());
        let mut j = 0;
        for mut outer in outer_rows {
            let ok = match outer[outer_key_col] {
                Value::Integer(i) => i,
                _ => continue,
            };
            while j < sorted_inner.len() && sorted_inner[j].0 < ok {
                j += 1;
            }
            let mut kk = j;
            while kk < sorted_inner.len() && sorted_inner[kk].0 == ok {
                let is_last = kk + 1 >= sorted_inner.len() || sorted_inner[kk + 1].0 != ok;
                let inner = &inner_rows[sorted_inner[kk].1];
                if let Some(proj) = projection {
                    if is_last {
                        result.push(
                            proj.slots
                                .iter()
                                .map(|&(idx, is_inner)| {
                                    if is_inner {
                                        inner[idx].clone()
                                    } else {
                                        std::mem::take(&mut outer[idx])
                                    }
                                })
                                .collect(),
                        );
                    } else {
                        result.push(combine_row_projected(&outer, inner, proj));
                    }
                } else if is_last {
                    outer.extend(inner.iter().cloned());
                    result.push(outer);
                    break;
                } else {
                    result.push(combine_row(&outer, inner, cap));
                }
                kk += 1;
            }
        }
        return Ok(result);
    }

    let mut inner_map: HashMap<i64, Vec<usize>> = HashMap::with_capacity(inner_rows.len());
    for (idx, inner) in inner_rows.iter().enumerate() {
        match &inner[inner_key_col] {
            Value::Integer(k) => inner_map.entry(*k).or_default().push(idx),
            Value::Null => {}
            _ => return Err(outer_rows),
        }
    }

    let mut result = Vec::with_capacity(inner_rows.len());

    match join_type {
        JoinType::Inner | JoinType::Cross => {
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
        }
        JoinType::Left => {
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                        continue;
                    }
                }
                if let Some(proj) = projection {
                    let null_inner = vec![Value::Null; inner_col_count];
                    result.push(combine_row_projected(&outer, &null_inner, proj));
                } else {
                    outer.resize(cap, Value::Null);
                    result.push(outer);
                }
            }
        }
        JoinType::Right => {
            let mut inner_matched = vec![false; inner_rows.len()];
            for mut outer in outer_rows {
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                                inner_matched[idx] = true;
                            }
                            let last_idx = *indices.last().unwrap();
                            inner_matched[last_idx] = true;
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
            for (j, inner) in inner_rows.iter().enumerate() {
                if !inner_matched[j] {
                    if let Some(proj) = projection {
                        let null_outer = vec![Value::Null; outer_col_count];
                        result.push(combine_row_projected(&null_outer, inner, proj));
                    } else {
                        let mut padded = Vec::with_capacity(cap);
                        padded.resize(outer_col_count, Value::Null);
                        padded.extend(inner.iter().cloned());
                        result.push(padded);
                    }
                }
            }
        }
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_join_step(
    mut outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
    inner_col_count: usize,
    outer_pk_col: Option<usize>,
    projection: Option<&CombineProjection>,
) -> Vec<Vec<Value>> {
    let equi_pairs = join
        .on_clause
        .as_ref()
        .map(|on| extract_equi_join_keys(on, combined_cols, outer_col_count))
        .unwrap_or_default();

    let is_pure_equi = join.on_clause.as_ref().map_or(true, |on| {
        !equi_pairs.is_empty() && count_conjuncts(on) == equi_pairs.len()
    });

    let effective_proj = if is_pure_equi { projection } else { None };

    if equi_pairs.len() == 1 && is_pure_equi {
        let (outer_key_col, inner_key_col) = equi_pairs[0];
        let outer_is_sorted = outer_pk_col == Some(outer_key_col);
        match try_integer_join(
            outer_rows,
            inner_rows,
            &join.join_type,
            outer_key_col,
            inner_key_col,
            outer_col_count,
            inner_col_count,
            outer_is_sorted,
            effective_proj,
        ) {
            Ok(result) => return result,
            Err(rows) => outer_rows = rows,
        }
    }

    let outer_key_cols: Vec<usize> = equi_pairs.iter().map(|&(o, _)| o).collect();
    let inner_key_cols: Vec<usize> = equi_pairs.iter().map(|&(_, i)| i).collect();

    let mut inner_map: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for (idx, inner) in inner_rows.iter().enumerate() {
        inner_map
            .entry(hash_key(inner, &inner_key_cols))
            .or_default()
            .push(idx);
    }

    let cap = effective_proj.map_or(outer_col_count + inner_col_count, |p| p.slots.len());
    let mut result = Vec::new();

    if is_pure_equi {
        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
            }
            JoinType::Left => {
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                            }
                            let last_idx = *indices.last().unwrap();
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    } else if let Some(proj) = effective_proj {
                        let null_inner = vec![Value::Null; inner_col_count];
                        result.push(combine_row_projected(&outer, &null_inner, proj));
                    } else {
                        outer.resize(cap, Value::Null);
                        result.push(outer);
                    }
                }
            }
            JoinType::Right => {
                let mut inner_matched = vec![false; inner_rows.len()];
                for mut outer in outer_rows {
                    let key = hash_key(&outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        if let Some(proj) = effective_proj {
                            for &idx in indices {
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                                inner_matched[idx] = true;
                            }
                            let last_idx = *indices.last().unwrap();
                            inner_matched[last_idx] = true;
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        if let Some(proj) = effective_proj {
                            let null_outer = vec![Value::Null; outer_col_count];
                            result.push(combine_row_projected(&null_outer, inner, proj));
                        } else {
                            let mut padded = Vec::with_capacity(cap);
                            padded.resize(outer_col_count, Value::Null);
                            padded.extend(inner.iter().cloned());
                            result.push(padded);
                        }
                    }
                }
            }
        }
    } else {
        let combined_map = ColumnMap::new(combined_cols);
        let on_matches = |combined: &[Value]| -> bool {
            match join.on_clause {
                Some(ref on_expr) => eval_expr(on_expr, &combined_map, combined)
                    .map(|v| is_truthy(&v))
                    .unwrap_or(false),
                None => true,
            }
        };

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                            }
                        }
                    }
                }
            }
            JoinType::Left => {
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    let mut matched = false;
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                                matched = true;
                            }
                        }
                    }
                    if !matched {
                        let mut padded = Vec::with_capacity(cap);
                        padded.extend(outer.iter().cloned());
                        padded.resize(cap, Value::Null);
                        result.push(padded);
                    }
                }
            }
            JoinType::Right => {
                let mut inner_matched = vec![false; inner_rows.len()];
                for outer in &outer_rows {
                    let key = hash_key(outer, &outer_key_cols);
                    if let Some(indices) = inner_map.get(&key) {
                        for &idx in indices {
                            let combined = combine_row(outer, &inner_rows[idx], cap);
                            if on_matches(&combined) {
                                result.push(combined);
                                inner_matched[idx] = true;
                            }
                        }
                    }
                }
                for (j, inner) in inner_rows.iter().enumerate() {
                    if !inner_matched[j] {
                        let mut padded = Vec::with_capacity(cap);
                        padded.resize(outer_col_count, Value::Null);
                        padded.extend(inner.iter().cloned());
                        result.push(padded);
                    }
                }
            }
        }
    }

    result
}

pub(super) fn table_alias_or_name(name: &str, alias: &Option<String>) -> String {
    match alias {
        Some(a) => a.to_ascii_lowercase(),
        None => name.to_ascii_lowercase(),
    }
}

pub(super) fn collect_all_rows_raw(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    let lower_name = &table_schema.name;
    let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match decode_full_row(table_schema, key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return false;
            }
        }
        true
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(rows)
}

pub(super) fn collect_all_rows_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
) -> Result<Vec<Vec<Value>>> {
    collect_rows_write(wtx, table_schema, &None, None).map(|(rows, _)| rows)
}

pub(super) fn has_ambiguous_bare_ref(expr: &Expr, columns: &[ColumnDef]) -> bool {
    match expr {
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            columns
                .iter()
                .filter(|c| c.name == lower || c.name.ends_with(&format!(".{lower}")))
                .count()
                > 1
        }
        Expr::BinaryOp { left, right, .. } => {
            has_ambiguous_bare_ref(left, columns) || has_ambiguous_bare_ref(right, columns)
        }
        Expr::UnaryOp { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            has_ambiguous_bare_ref(inner, columns)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            args.iter().any(|a| has_ambiguous_bare_ref(a, columns))
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            has_ambiguous_bare_ref(e, columns)
                || has_ambiguous_bare_ref(low, columns)
                || has_ambiguous_bare_ref(high, columns)
        }
        Expr::InList { expr: e, list, .. } => {
            has_ambiguous_bare_ref(e, columns)
                || list.iter().any(|a| has_ambiguous_bare_ref(a, columns))
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            has_ambiguous_bare_ref(e, columns)
                || has_ambiguous_bare_ref(pattern, columns)
                || escape
                    .as_ref()
                    .is_some_and(|esc| has_ambiguous_bare_ref(esc, columns))
        }
        Expr::Cast { expr: inner, .. } => has_ambiguous_bare_ref(inner, columns),
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| has_ambiguous_bare_ref(o, columns))
                || conditions.iter().any(|(w, t)| {
                    has_ambiguous_bare_ref(w, columns) || has_ambiguous_bare_ref(t, columns)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| has_ambiguous_bare_ref(e, columns))
        }
        _ => false,
    }
}

pub(super) struct JoinColumnPlan {
    per_table: Vec<Vec<usize>>,
    output_combined: Vec<usize>,
}

pub(super) fn compute_join_needed_columns(
    stmt: &SelectStmt,
    tables: &[(String, &TableSchema)],
) -> Option<JoinColumnPlan> {
    for sel in &stmt.columns {
        if matches!(sel, SelectColumn::AllColumns) {
            return None;
        }
    }

    let combined_cols = build_joined_columns(tables);

    for sel in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = sel {
            if has_ambiguous_bare_ref(expr, &combined_cols) {
                return None;
            }
        }
    }

    let mut output_combined: Vec<usize> = Vec::new();
    for sel in &stmt.columns {
        if let SelectColumn::Expr { expr, .. } = sel {
            output_combined.extend(referenced_columns(expr, &combined_cols));
        }
    }
    if let Some(w) = &stmt.where_clause {
        output_combined.extend(referenced_columns(w, &combined_cols));
    }
    for ob in &stmt.order_by {
        output_combined.extend(referenced_columns(&ob.expr, &combined_cols));
    }
    for gb in &stmt.group_by {
        output_combined.extend(referenced_columns(gb, &combined_cols));
    }
    if let Some(h) = &stmt.having {
        output_combined.extend(referenced_columns(h, &combined_cols));
    }
    output_combined.sort_unstable();
    output_combined.dedup();

    let mut needed_combined = output_combined.clone();
    for join in &stmt.joins {
        if let Some(on_expr) = &join.on_clause {
            needed_combined.extend(referenced_columns(on_expr, &combined_cols));
        }
    }
    needed_combined.sort_unstable();
    needed_combined.dedup();

    let mut offsets = Vec::with_capacity(tables.len() + 1);
    offsets.push(0usize);
    for (_, s) in tables {
        offsets.push(offsets.last().unwrap() + s.columns.len());
    }

    let mut per_table: Vec<Vec<usize>> = tables.iter().map(|_| Vec::new()).collect();
    for &ci in &needed_combined {
        for (t, _) in tables.iter().enumerate() {
            let start = offsets[t];
            let end = offsets[t + 1];
            if ci >= start && ci < end {
                per_table[t].push(ci - start);
                break;
            }
        }
    }

    Some(JoinColumnPlan {
        per_table,
        output_combined,
    })
}

pub(super) fn collect_rows_partial(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    table_schema: &TableSchema,
    needed: &[usize],
) -> Result<Vec<Vec<Value>>> {
    if needed.is_empty() || needed.len() == table_schema.columns.len() {
        return collect_all_rows_raw(rtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new(table_schema, needed);
    let lower_name = &table_schema.name;
    let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match ctx.decode(key, value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                scan_err = Some(e);
                return false;
            }
        }
        true
    })
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }
    Ok(rows)
}

pub(super) fn collect_rows_partial_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    needed: &[usize],
) -> Result<Vec<Vec<Value>>> {
    if needed.is_empty() || needed.len() == table_schema.columns.len() {
        return collect_all_rows_write(wtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new(table_schema, needed);
    let lower_name = &table_schema.name;
    let entry_count = wtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
        match ctx.decode(key, value) {
            Ok(row) => rows.push(row),
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
    Ok(rows)
}

pub(super) fn exec_select_join(
    db: &Database,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        all_tables.push((inner_alias, inner_schema));
    }
    let (needed_per_table, output_combined) = match compute_join_needed_columns(stmt, &all_tables) {
        Some(plan) => (Some(plan.per_table), Some(plan.output_combined)),
        None => (None, None),
    };

    let mut rtx = db.begin_read();
    let mut outer_rows = match &needed_per_table {
        Some(n) if !n.is_empty() => collect_rows_partial(&mut rtx, from_schema, &n[0])?,
        _ => collect_all_rows_raw(&mut rtx, from_schema)?,
    };

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    let mut last_combined_cols: Option<Vec<ColumnDef>> = None;
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => {
                collect_rows_partial(&mut rtx, inner_schema, &n[ji + 1])?
            }
            _ => collect_all_rows_raw(&mut rtx, inner_schema)?,
        };

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        let is_last = ji == num_joins - 1;
        let proj = if is_last {
            output_combined
                .as_ref()
                .map(|oc| build_combine_projection(oc, outer_col_count))
        } else {
            None
        };

        outer_rows = exec_join_step(
            outer_rows,
            &inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
        );
        last_combined_cols = Some(combined_cols);
        tables.push((inner_alias, inner_schema));
        cur_outer_pk_col = None;
    }
    drop(rtx);

    let joined_cols = last_combined_cols.unwrap_or_else(|| build_joined_columns(&tables));
    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&joined_cols, oc);
            return super::process_select(&projected_cols, outer_rows, stmt, false);
        }
    }
    super::process_select(&joined_cols, outer_rows, stmt, false)
}

pub(super) fn exec_select_join_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    for join in &stmt.joins {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        all_tables.push((inner_alias, inner_schema));
    }
    let (needed_per_table, output_combined) = match compute_join_needed_columns(stmt, &all_tables) {
        Some(plan) => (Some(plan.per_table), Some(plan.output_combined)),
        None => (None, None),
    };

    let mut outer_rows = match &needed_per_table {
        Some(n) if !n.is_empty() => collect_rows_partial_write(wtx, from_schema, &n[0])?,
        _ => collect_all_rows_write(wtx, from_schema)?,
    };

    let mut tables: Vec<(String, &TableSchema)> = vec![(from_alias.clone(), from_schema)];
    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = resolve_table_name(schema, &join.table.name)?;
        let inner_alias = table_alias_or_name(&join.table.name, &join.table.alias);
        let inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => {
                collect_rows_partial_write(wtx, inner_schema, &n[ji + 1])?
            }
            _ => collect_all_rows_write(wtx, inner_schema)?,
        };

        let mut preview_tables = tables.clone();
        preview_tables.push((inner_alias.clone(), inner_schema));
        let combined_cols = build_joined_columns(&preview_tables);

        let outer_col_count = if outer_rows.is_empty() {
            tables.iter().map(|(_, s)| s.columns.len()).sum()
        } else {
            outer_rows[0].len()
        };
        let inner_col_count = inner_schema.columns.len();

        let is_last = ji == num_joins - 1;
        let proj = if is_last {
            output_combined
                .as_ref()
                .map(|oc| build_combine_projection(oc, outer_col_count))
        } else {
            None
        };

        outer_rows = exec_join_step(
            outer_rows,
            &inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
        );
        tables.push((inner_alias, inner_schema));
        cur_outer_pk_col = None;
    }

    let joined_cols = build_joined_columns(&tables);
    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&joined_cols, oc);
            return super::process_select(&projected_cols, outer_rows, stmt, false);
        }
    }
    super::process_select(&joined_cols, outer_rows, stmt, false)
}
