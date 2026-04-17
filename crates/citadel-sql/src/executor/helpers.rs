use crate::encoding::{
    decode_columns, decode_columns_into, decode_composite_key, decode_key_value, decode_pk_into,
    decode_row_into, encode_composite_key, row_non_pk_count,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, ColumnMap};
use crate::parser::*;
use crate::types::*;

// ── Helpers ─────────────────────────────────────────────────────────

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
}

impl PartialDecodeCtx {
    pub(super) fn new(schema: &TableSchema, needed: &[usize]) -> Self {
        let non_pk = schema.non_pk_indices();
        let enc_pos = schema.encoding_positions();
        let mut pk_positions = Vec::new();
        let mut nonpk_targets = Vec::new();
        let mut nonpk_schema = Vec::new();

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

        let needed_set: std::collections::HashSet<usize> = needed.iter().copied().collect();
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
        }
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

pub(super) fn decode_full_row(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<Value>> {
    let mut row = vec![Value::Null; schema.columns.len()];
    decode_pk_into(
        key,
        schema.primary_key_columns.len(),
        &mut row,
        schema.pk_indices(),
    )?;
    let mapping = schema.decode_col_mapping();
    let stored_count = row_non_pk_count(value);
    decode_row_into(value, &mut row, mapping)?;
    // Fill defaults for columns added after this row was written
    if stored_count < mapping.len() {
        for &logical_idx in mapping.iter().skip(stored_count) {
            if logical_idx != usize::MAX {
                if let Some(ref expr) = schema.columns[logical_idx].default_expr {
                    row[logical_idx] = eval_const_expr(expr)?;
                }
            }
        }
    }
    Ok(row)
}

/// Evaluate a constant expression (no column references).
pub(super) fn eval_const_expr(expr: &Expr) -> Result<Value> {
    static EMPTY: std::sync::OnceLock<ColumnMap> = std::sync::OnceLock::new();
    let empty = EMPTY.get_or_init(|| ColumnMap::new(&[]));
    eval_expr(expr, empty, &[])
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
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        indices.sort_by(|&a, &b| compare_sort_keys(&keys[a], &keys[b], order_by));
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
    } else {
        let keys = extract_sort_keys(rows, order_by, &col_map);
        let cmp = |&a: &usize, &b: &usize| compare_sort_keys(&keys[a], &keys[b], order_by);
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
        Expr::Column(name) => col_map.resolve(&name.to_ascii_lowercase()).ok(),
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

pub(super) fn extract_sort_keys(
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    col_map: &ColumnMap,
) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            order_by
                .iter()
                .map(|item| eval_expr(&item.expr, col_map, row).unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

pub(super) fn compare_sort_keys(
    a: &[Value],
    b: &[Value],
    order_by: &[OrderByItem],
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
                let cmp = a[i].cmp(&b[i]);
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

pub(super) fn try_build_index_map(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Option<Vec<(String, usize)>> {
    let col_map = ColumnMap::new(columns);
    let mut map = Vec::new();
    let mut seen = std::collections::HashSet::new();
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
    // Fast path: SELECT * - zero clones
    if select_cols.len() == 1 && matches!(select_cols[0], SelectColumn::AllColumns) {
        let col_names = columns.iter().map(|c| c.name.clone()).collect();
        return Ok((col_names, rows));
    }

    // Fast path: all simple column refs - use mem::take, zero clones
    if let Some(map) = try_build_index_map(select_cols, columns) {
        let col_names: Vec<String> = map.iter().map(|(n, _)| n.clone()).collect();
        // Identity: columns already in the right order - return as-is
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

    // Fallback: expression evaluation (requires cloning)
    let mut col_names = Vec::new();
    type Projector = Box<dyn Fn(&[Value]) -> Result<Value>>;
    let mut projectors: Vec<Projector> = Vec::new();
    let col_map = std::sync::Arc::new(ColumnMap::new(columns));

    for sel_col in select_cols {
        match sel_col {
            SelectColumn::AllColumns => {
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
                projectors.push(Box::new(move |row: &[Value]| eval_expr(&expr, &map, row)));
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

pub(super) fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{table}.{column}"),
        Expr::Literal(v) => format!("{v}"),
        Expr::CountStar => "COUNT(*)".into(),
        Expr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(expr_display_name).collect();
            format!("{name}({})", arg_strs.join(", "))
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
    }
}

pub(super) fn build_output_columns(
    select_cols: &[SelectColumn],
    columns: &[ColumnDef],
) -> Vec<ColumnDef> {
    let mut out = Vec::new();
    for (i, col) in select_cols.iter().enumerate() {
        let (name, data_type) = match col {
            SelectColumn::AllColumns => (format!("col{i}"), DataType::Null),
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

// ── Index helpers ────────────────────────────────────────────────────

pub(super) fn encode_index_key(idx: &IndexDef, row: &[Value], pk_values: &[Value]) -> Vec<u8> {
    let indexed_values: Vec<Value> = idx
        .columns
        .iter()
        .map(|&col_idx| row[col_idx as usize].clone())
        .collect();

    if idx.unique {
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(&indexed_values);
        }
    }

    let mut all_values = indexed_values;
    all_values.extend_from_slice(pk_values);
    encode_composite_key(&all_values)
}

pub(super) fn encode_index_value(idx: &IndexDef, row: &[Value], pk_values: &[Value]) -> Vec<u8> {
    if idx.unique {
        let indexed_values: Vec<Value> = idx
            .columns
            .iter()
            .map(|&col_idx| row[col_idx as usize].clone())
            .collect();
        let any_null = indexed_values.iter().any(|v| v.is_null());
        if !any_null {
            return encode_composite_key(pk_values);
        }
    }
    vec![]
}

pub(super) fn insert_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        let value = encode_index_value(idx, row, pk_values);

        let is_new = wtx
            .table_insert(&idx_table, &key, &value)
            .map_err(SqlError::Storage)?;

        if idx.unique && !is_new {
            let indexed_values: Vec<Value> = idx
                .columns
                .iter()
                .map(|&col_idx| row[col_idx as usize].clone())
                .collect();
            let any_null = indexed_values.iter().any(|v| v.is_null());
            if !any_null {
                return Err(SqlError::UniqueViolation(idx.name.clone()));
            }
        }
    }
    Ok(())
}

pub(super) fn delete_index_entries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    row: &[Value],
    pk_values: &[Value],
) -> Result<()> {
    for idx in &table_schema.indices {
        let idx_table = TableSchema::index_table_name(&table_schema.name, &idx.name);
        let key = encode_index_key(idx, row, pk_values);
        wtx.table_delete(&idx_table, &key)
            .map_err(SqlError::Storage)?;
    }
    Ok(())
}

pub(super) fn index_columns_changed(idx: &IndexDef, old_row: &[Value], new_row: &[Value]) -> bool {
    idx.columns
        .iter()
        .any(|&col_idx| old_row[col_idx as usize] != new_row[col_idx as usize])
}
