use crate::encoding::{
    decode_columns, decode_columns_into, decode_composite_key, decode_key_value, decode_pk_into,
    decode_row_into, encode_composite_key, row_non_pk_count,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::types::*;

pub(super) type ReturningRow = (Option<Vec<Value>>, Option<Vec<Value>>);

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
    let got = value.data_type();
    let coerced = if strict {
        value.strict_coerce(col.data_type)
    } else {
        value.coerce_into(col.data_type)
    };
    coerced.ok_or_else(|| SqlError::TypeMismatch {
        expected: col.data_type.to_string(),
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

pub(super) fn eval_fast_gen(
    fast: &FastGenEval,
    expr: &Expr,
    partial_row: &[Value],
    col_map: &ColumnMap,
) -> Result<Value> {
    match fast {
        FastGenEval::IntColMulAdd {
            col_schema_idx,
            mul,
            add,
        } => match partial_row[*col_schema_idx] {
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_mul(*mul).wrapping_add(*add))),
            _ => eval_expr(expr, &EvalCtx::new(col_map, partial_row)),
        },
        FastGenEval::IntColAddCol {
            left_idx,
            right_idx,
        } => match (&partial_row[*left_idx], &partial_row[*right_idx]) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_add(*b))),
            _ => eval_expr(expr, &EvalCtx::new(col_map, partial_row)),
        },
        FastGenEval::None => eval_expr(expr, &EvalCtx::new(col_map, partial_row)),
    }
}

pub(super) struct PartialDecodeCtx {
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
    virtuals_to_eval: Vec<(usize, Expr, DataType, bool)>,
    col_map: ColumnMap,
}

impl PartialDecodeCtx {
    pub(super) fn new(schema: &TableSchema, needed: &[usize]) -> Self {
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
                if let Ok(val) = eval_const_expr(expr) {
                    nonpk_defaults.push((phys_pos, schema_col, val));
                }
            }
        }
        let mut remaining_defaults = Vec::new();
        for (&phys_pos, &schema_col) in remaining_nonpk_targets
            .iter()
            .zip(remaining_nonpk_schema.iter())
        {
            if let Some(ref expr) = schema.columns[schema_col].default_expr {
                if let Ok(val) = eval_const_expr(expr) {
                    remaining_defaults.push((phys_pos, schema_col, val));
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
                    virtuals_to_eval.push((
                        col,
                        c.generated_expr.as_ref().unwrap().clone(),
                        c.data_type,
                        c.nullable,
                    ));
                }
            }
        }

        Self {
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
        }
    }

    fn materialize_virtuals(&self, row: &mut [Value]) -> Result<()> {
        for (pos, expr, dt, nullable) in &self.virtuals_to_eval {
            let val = eval_expr(expr, &EvalCtx::new(&self.col_map, row))?;
            row[*pos] = if val.is_null() {
                if !*nullable {
                    return Err(SqlError::InvalidValue(format!(
                        "VIRTUAL generated column at position {pos} produced NULL but is NOT NULL"
                    )));
                }
                Value::Null
            } else {
                let got = val.data_type();
                val.coerce_into(*dt).ok_or_else(|| SqlError::TypeMismatch {
                    expected: dt.to_string(),
                    got: got.to_string(),
                })?
            };
        }
        Ok(())
    }

    pub(super) fn decode(&self, key: &[u8], value: &[u8]) -> Result<Vec<Value>> {
        let mut row = vec![Value::Null; self.num_cols];

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
            decode_columns_into(value, &self.nonpk_targets, &self.nonpk_schema, &mut row)?;
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
            self.materialize_virtuals(&mut row)?;
        }

        Ok(row)
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

pub(crate) fn decode_full_row(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(schema.columns.len());
    decode_full_row_into(schema, key, value, &mut row)?;
    Ok(row)
}

#[inline]
pub(crate) fn decode_full_row_into(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    row: &mut Vec<Value>,
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
    let stored_count = row_non_pk_count(value);
    decode_row_into(value, row, mapping)?;
    if stored_count < mapping.len() {
        for &logical_idx in mapping.iter().skip(stored_count) {
            if logical_idx != usize::MAX {
                if let Some(ref expr) = schema.columns[logical_idx].default_expr {
                    row[logical_idx] = eval_const_expr(expr)?;
                }
            }
        }
    }
    if schema.has_virtual_columns() {
        materialize_virtual(schema, row)?;
    }
    Ok(())
}

/// Caller must ensure all non-virtual columns in `row` are already populated.
#[inline]
pub(crate) fn materialize_virtual(schema: &TableSchema, row: &mut [Value]) -> Result<()> {
    let col_map = ColumnMap::new(&schema.columns);
    for col in &schema.columns {
        if matches!(
            col.generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(&col_map, row),
            )?;
            let pos = col.position as usize;
            row[pos] = if val.is_null() {
                Value::Null
            } else {
                let got_type = val.data_type();
                val.coerce_into(col.data_type)
                    .ok_or_else(|| SqlError::TypeMismatch {
                        expected: col.data_type.to_string(),
                        got: got_type.to_string(),
                    })?
            };
        }
    }
    Ok(())
}

pub(super) fn eval_const_expr(expr: &Expr) -> Result<Value> {
    static EMPTY: std::sync::OnceLock<ColumnMap> = std::sync::OnceLock::new();
    let empty = EMPTY.get_or_init(|| ColumnMap::new(&[]));
    eval_expr(expr, &EvalCtx::new(empty, &[]))
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
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        indices.sort_by(|&a, &b| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        });
    } else if let Some((col_idx, coll)) = try_resolve_collated_flat_sort(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let keys = precompute_collated_keys(rows, col_idx, coll);
        indices.sort_by(|&a, &b| {
            compare_collated_key(
                &keys[a],
                &keys[b],
                &rows[a][col_idx],
                &rows[b][col_idx],
                desc,
                nulls_first,
            )
        });
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        let collations = sort_key_collations(order_by, &col_map);
        indices.sort_by(|&a, &b| compare_sort_keys(&keys[a], &keys[b], order_by, &collations));
    }

    let sorted: Vec<Vec<Value>> = indices
        .iter()
        .map(|&i| std::mem::take(&mut rows[i]))
        .collect();
    rows.iter_mut()
        .zip(sorted)
        .for_each(|(slot, row)| *slot = row);
    Ok(())
}

pub(super) fn topk_rows(
    rows: &mut [Vec<Value>],
    order_by: &[OrderByItem],
    columns: &[ColumnDef],
    k: usize,
) -> Result<()> {
    let col_map = ColumnMap::new(columns);
    let mut indices: Vec<usize> = (0..rows.len()).collect();

    if let Some(col_idx) = try_resolve_flat_sort_col(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let cmp = |&a: &usize, &b: &usize| {
            compare_flat_key(&rows[a][col_idx], &rows[b][col_idx], desc, nulls_first)
        };
        indices.select_nth_unstable_by(k - 1, cmp);
        indices[..k].sort_by(cmp);
    } else if let Some((col_idx, coll)) = try_resolve_collated_flat_sort(order_by, &col_map) {
        let desc = order_by[0].descending;
        let nulls_first = order_by[0].nulls_first.unwrap_or(!desc);
        let keys = precompute_collated_keys(rows, col_idx, coll);
        let cmp = |&a: &usize, &b: &usize| {
            compare_collated_key(
                &keys[a],
                &keys[b],
                &rows[a][col_idx],
                &rows[b][col_idx],
                desc,
                nulls_first,
            )
        };
        indices.select_nth_unstable_by(k - 1, cmp);
        indices[..k].sort_by(cmp);
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        let collations = sort_key_collations(order_by, &col_map);
        let cmp =
            |&a: &usize, &b: &usize| compare_sort_keys(&keys[a], &keys[b], order_by, &collations);
        indices.select_nth_unstable_by(k - 1, cmp);
        indices[..k].sort_by(cmp);
    }

    let sorted: Vec<Vec<Value>> = indices[..k]
        .iter()
        .map(|&i| std::mem::take(&mut rows[i]))
        .collect();
    rows[..k]
        .iter_mut()
        .zip(sorted)
        .for_each(|(slot, row)| *slot = row);
    Ok(())
}

pub(super) fn try_resolve_flat_sort_col(
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Option<usize> {
    if order_by.len() != 1 {
        return None;
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
) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            order_by
                .iter()
                .map(|item| {
                    eval_expr(&item.expr, &EvalCtx::new(col_map, row)).unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect()
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

pub(super) fn sort_key_collations(
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Vec<crate::types::Collation> {
    order_by
        .iter()
        .map(|item| match &item.expr {
            Expr::Collate { collation, .. } => *collation,
            Expr::Column(name) => col_map
                .resolve(&name.to_ascii_lowercase())
                .ok()
                .map(|i| col_map.collation_at(i))
                .unwrap_or(crate::types::Collation::Binary),
            _ => crate::types::Collation::Binary,
        })
        .collect()
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
    mut rows: Vec<Vec<Value>>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
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

    let mut col_names = Vec::new();
    type Projector = Box<dyn Fn(&[Value]) -> Result<Value>>;
    let mut projectors: Vec<Projector> = Vec::new();
    let col_map = std::sync::Arc::new(ColumnMap::new(columns));

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                for col in columns {
                    let idx = col.position as usize;
                    col_names.push(col.name.clone());
                    projectors.push(Box::new(move |row: &[Value]| Ok(row[idx].clone())));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let expr = expr.clone();
                let map = col_map.clone();
                projectors.push(Box::new(move |row: &[Value]| {
                    eval_expr(&expr, &EvalCtx::new(&map, row))
                }));
            }
        }
    }

    let projected = rows
        .iter()
        .map(|row| {
            projectors
                .iter()
                .map(|p| p(row))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((col_names, projected))
}

pub(super) fn project_returning(
    table_schema: &TableSchema,
    returning: &[SelectColumn],
    rows: &[ReturningRow],
) -> Result<QueryResult> {
    let columns = &table_schema.columns;
    let col_map = ColumnMap::new(columns);

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
        let ctx = EvalCtx::with_old_new(&col_map, default_row, old.as_deref(), new.as_deref());

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
    for (i, col) in select_cols.iter().enumerate() {
        let (name, data_type) = match col {
            SelectColumn::AllColumns | SelectColumn::AllFromOld | SelectColumn::AllFromNew => {
                (format!("col{i}"), DataType::Null)
            }
            SelectColumn::Expr {
                alias: Some(a),
                expr,
            } => (a.clone(), infer_expr_type(expr, columns)),
            SelectColumn::Expr { expr, .. } => {
                (expr_display_name(expr), infer_expr_type(expr, columns))
            }
        };
        out.push(ColumnDef {
            name,
            data_type,
            nullable: true,
            position: i as u16,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
            is_with_timezone: false,
            generated_expr: None,
            generated_sql: None,
            generated_kind: None,
            collation: crate::types::Collation::Binary,
        });
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

#[inline]
fn encode_index_key_component(value: &Value, coll: crate::types::Collation, buf: &mut Vec<u8>) {
    if coll == crate::types::Collation::Binary {
        crate::encoding::encode_key_value_into(value, buf);
    } else {
        crate::encoding::encode_key_value_collated_into(value, coll, buf);
    }
}

/// Expression eval errors (or missing schema) materialize as `Value::Null` — PG semantics.
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
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    IDX_KEY_BUF.with(|kb| {
        IDX_TABLE_BUF.with(|tb| {
            let mut key_buf = kb.borrow_mut();
            let mut table_buf = tb.borrow_mut();
            for idx in &table_schema.indices {
                if let Some(cm) = col_map.as_ref() {
                    if !row_matches_partial(idx, row, cm) {
                        continue;
                    }
                }
                fill_idx_table_name(&mut table_buf, &table_schema.name, &idx.name);

                if let crate::types::IndexKind::Inverted(inv_kind) = idx.kind {
                    insert_inverted_entries(wtx, idx, inv_kind, row, pk_values, &table_buf)?;
                    continue;
                }

                encode_index_key_into_with_schema(
                    idx,
                    row,
                    pk_values,
                    Some(table_schema),
                    &mut key_buf,
                );
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

pub(crate) fn extract_inverted_entries(
    value: &Value,
    kind: crate::types::InvertedKind,
) -> Result<Vec<Vec<u8>>> {
    match kind {
        crate::types::InvertedKind::Gin(ops) => crate::json::extract_gin_entries(value, ops),
        crate::types::InvertedKind::Fts { config_id } => extract_fts_lexemes(value, config_id),
        crate::types::InvertedKind::Ann { .. } => Ok(Vec::new()),
    }
}

pub(crate) fn extract_inverted_entries_with_values(
    value: &Value,
    kind: crate::types::InvertedKind,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    match kind {
        crate::types::InvertedKind::Gin(ops) => {
            let keys = crate::json::extract_gin_entries(value, ops)?;
            Ok(keys.into_iter().map(|k| (k, Vec::new())).collect())
        }
        crate::types::InvertedKind::Fts { config_id } => {
            extract_fts_lexemes_with_positions(value, config_id)
        }
        crate::types::InvertedKind::Ann { .. } => Ok(Vec::new()),
    }
}

fn extract_fts_lexemes(value: &Value, config_id: u8) -> Result<Vec<Vec<u8>>> {
    let kind = crate::fts::TokenizerKind::from_config_id(config_id)?;
    let mut lexemes: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    match value {
        Value::Null => return Ok(Vec::new()),
        Value::TsVector(bytes) => {
            let (_flags, reader) = crate::fts::TsVectorReader::open(bytes)?;
            for item in reader {
                let (lex, _positions) = item?;
                lexemes.insert(lex.to_vec());
            }
        }
        Value::Text(s) => {
            for tok in crate::fts::tokenize(kind, s) {
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
    Ok(lexemes.into_iter().collect())
}

fn extract_fts_lexemes_with_positions(
    value: &Value,
    config_id: u8,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let kind = crate::fts::TokenizerKind::from_config_id(config_id)?;
    let mut by_lex: std::collections::BTreeMap<Vec<u8>, Vec<u16>> =
        std::collections::BTreeMap::new();
    match value {
        Value::Null => return Ok(Vec::new()),
        Value::TsVector(bytes) => {
            let (_flags, reader) = crate::fts::TsVectorReader::open(bytes)?;
            for item in reader {
                let (lex, positions) = item?;
                by_lex.entry(lex.to_vec()).or_default().extend(positions);
            }
        }
        Value::Text(s) => {
            for tok in crate::fts::tokenize(kind, s) {
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
    for (lex, mut positions) in by_lex {
        positions.sort_unstable();
        positions.dedup();
        let mut value_bytes = Vec::with_capacity(positions.len() * 2);
        for p in positions {
            value_bytes.extend_from_slice(&p.to_le_bytes());
        }
        out.push((lex, value_bytes));
    }
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
    let entries = extract_inverted_entries_with_values(value, kind)?;
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
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    for (i, idx) in table_schema.indices.iter().enumerate() {
        if let Some(cm) = col_map.as_ref() {
            if !row_matches_partial(idx, row, cm) {
                continue;
            }
        }
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key_with_schema(idx, row, pk_values, table_schema);
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
    let col_map = any_partial_index(table_schema).then(|| table_schema.column_map());
    for idx in &table_schema.indices {
        if let Some(cm) = col_map.as_ref() {
            if !row_matches_partial(idx, row, cm) {
                continue;
            }
        }
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        if let crate::types::IndexKind::Inverted(inv_kind) = idx.kind {
            delete_inverted_entries(wtx, idx, inv_kind, row, pk_values, &idx_table)?;
            continue;
        }
        let key = encode_index_key_with_schema(idx, row, pk_values, table_schema);
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
    let entries = extract_inverted_entries(value, kind)?;
    let pk_encoded = crate::encoding::encode_composite_key(pk_values);
    for entry in entries {
        let full_key = build_inverted_key(&entry, &pk_encoded);
        wtx.table_delete(idx_table, &full_key)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

pub(super) fn index_columns_changed(idx: &IndexDef, old_row: &[Value], new_row: &[Value]) -> bool {
    idx.column_positions_iter()
        .any(|col_idx| old_row[col_idx as usize] != new_row[col_idx as usize])
}

/// NULL or eval errors → false (treated as predicate-false).
pub(super) fn row_matches_partial(idx: &IndexDef, row: &[Value], col_map: &ColumnMap) -> bool {
    let Some(expr) = idx.predicate_expr.as_ref() else {
        return true;
    };
    match crate::eval::eval_expr(expr, &EvalCtx::new(col_map, row)) {
        Ok(v) => is_truthy(&v),
        Err(_) => false,
    }
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

pub(super) struct FkChildHit {
    pub fk_idx_key: Vec<u8>,
    pk_key_repr: PkKeyRepr,
}

enum PkKeyRepr {
    Suffix(u32),
    Owned(Vec<u8>),
}

impl FkChildHit {
    pub fn pk_key(&self) -> &[u8] {
        match &self.pk_key_repr {
            PkKeyRepr::Suffix(off) => &self.fk_idx_key[*off as usize..],
            PkKeyRepr::Owned(v) => v,
        }
    }

    pub fn into_pk_key(self) -> Vec<u8> {
        match self.pk_key_repr {
            PkKeyRepr::Suffix(off) => self.fk_idx_key[off as usize..].to_vec(),
            PkKeyRepr::Owned(v) => v,
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
    out: &mut Vec<FkChildHit>,
) -> Result<()> {
    let idx_table = TableSchema::index_table_name(&child_schema.name, &cascading_idx.name);
    let unique_no_null = cascading_idx.unique;
    let parent_pk_len = parent_pk_key.len() as u32;
    wtx.table_scan_from(&idx_table, parent_pk_key, |key, value| {
        if !key.starts_with(parent_pk_key) {
            return Ok(false);
        }
        let pk_key_repr = if unique_no_null && !value.is_empty() {
            PkKeyRepr::Owned(value.to_vec())
        } else {
            PkKeyRepr::Suffix(parent_pk_len)
        };
        out.push(FkChildHit {
            fk_idx_key: key.to_vec(),
            pk_key_repr,
        });
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
            let mut hits: Vec<FkChildHit> = Vec::new();
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
                            hits.into_iter().map(|h| h.into_pk_key()).collect();
                        worklist.push((child_table.to_string(), pk_keys));
                    }
                }
                crate::parser::ReferentialAction::SetNull => {
                    let rows = fetch_child_rows(wtx, child_schema, &hits)?;
                    set_fk_columns(wtx, child_schema, fk, &rows, |_| Value::Null)?;
                }
                crate::parser::ReferentialAction::SetDefault => {
                    let defaults = fk_defaults(child_schema, fk);
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
    hits: &[FkChildHit],
) -> Result<()> {
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
        for hit in hits {
            wtx.table_delete(&cascading_idx_table, &hit.fk_idx_key)
                .map_err(SqlError::Storage)?;
            wtx.table_delete(child_table.as_bytes(), hit.pk_key())
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
        for ((pk_key, row), hit) in rows.iter().zip(hits) {
            wtx.table_delete(&cascading_idx_table, &hit.fk_idx_key)
                .map_err(SqlError::Storage)?;
            pk_values_buf.clear();
            pk_values_buf.extend(pk_indices.iter().map(|&j| row[j].clone()));
            for (idx, idx_table) in other_indices.iter().zip(other_index_tables.iter()) {
                if let Some(cm) = col_map_partial {
                    if !row_matches_partial(idx, row, cm) {
                        continue;
                    }
                }
                encode_index_key_into_with_schema(
                    idx,
                    row,
                    &pk_values_buf,
                    Some(child_schema),
                    &mut idx_key_buf,
                );
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
    hits: &[FkChildHit],
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let mut rows = Vec::with_capacity(hits.len());
    for hit in hits {
        let pk = hit.pk_key();
        if let Some(value_bytes) = wtx
            .table_get(child_schema.name.as_bytes(), pk)
            .map_err(SqlError::Storage)?
        {
            let row = decode_full_row(child_schema, pk, &value_bytes)?;
            rows.push((pk.to_vec(), row));
        }
    }
    Ok(rows)
}

fn fk_defaults(child_schema: &TableSchema, fk: &ForeignKeySchemaEntry) -> Vec<Value> {
    fk.columns
        .iter()
        .map(|&col_idx| {
            eval_default(&child_schema.columns[col_idx as usize]).unwrap_or(Value::Null)
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
            let cols_changed = index_columns_changed(idx, old_row, &new_row);
            let (del, ins) = partial_idx_update_actions(
                idx,
                old_row,
                &new_row,
                cols_changed,
                false,
                col_map_partial,
            );
            let idx_table = TableSchema::index_table_name(&child_schema.name, &idx.name);
            if del {
                let old_idx_key =
                    encode_index_key_with_schema(idx, old_row, &pk_values, child_schema);
                wtx.table_delete(&idx_table, &old_idx_key)
                    .map_err(SqlError::Storage)?;
            }
            if ins {
                let new_idx_key =
                    encode_index_key_with_schema(idx, &new_row, &pk_values, child_schema);
                let new_idx_val = encode_index_value(idx, &new_row, &pk_values);
                wtx.table_insert(&idx_table, &new_idx_key, &new_idx_val)
                    .map_err(SqlError::Storage)?;
            }
        }
    }
    Ok(())
}

fn eval_default(col: &ColumnDef) -> Option<Value> {
    let expr = col.default_expr.as_ref()?;
    let empty_cols: &[ColumnDef] = &[];
    let cm = ColumnMap::new(empty_cols);
    let row: &[Value] = &[];
    crate::eval::eval_expr(expr, &EvalCtx::new(&cm, row)).ok()
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
            let mut hits: Vec<FkChildHit> = Vec::new();
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
                    let defaults = fk_defaults(child_schema, fk);
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
