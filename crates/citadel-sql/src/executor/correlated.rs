use citadel_txn::read_txn::ReadView;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::encoding::{decode_column_raw, decode_composite_key, decode_pk_integer};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::{check_cancel, check_cancel_at, decode_full_row_with_cancel};
use super::join::{KeyedRowIndex, KeyedRows};
use super::CteContext;

#[path = "correlated_bind.rs"]
mod binding;

#[path = "correlated_apply.rs"]
mod apply;

pub(super) use apply::RowExpressions;
pub(super) use binding::{bind_outer_query, OuterScope};

/// Unlike the conjunct-only decorrelator, mutation predicates may contain a
/// correlated query under OR, CASE, or another expression.
pub(super) fn mutation_has_correlated_where(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    predicate: &Option<Expr>,
    ctx: &CorrelationCtx<'_>,
    schema: &SchemaManager,
) -> Result<bool> {
    let Some(predicate) = predicate else {
        return Ok(false);
    };
    mutation_has_correlated_expr(wtx, predicate, ctx, schema)
}

/// Resolve nested names against their local scopes before identifying a capture
/// of the row being mutated. SET and WHERE must use the same binding rules.
pub(super) fn mutation_has_correlated_expr(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    expr: &Expr,
    ctx: &CorrelationCtx<'_>,
    schema: &SchemaManager,
) -> Result<bool> {
    if !super::dml::has_subquery(expr) {
        return Ok(false);
    }
    Ok(super::dml::has_conditional_subquery(expr)
        || binding::bind_predicate(
            wtx,
            schema,
            &CteContext::default(),
            &mut expr.clone(),
            ctx,
            None,
        )?)
}

fn complete_exists_semijoin(
    predicate: &Expr,
    ctx: &CorrelationCtx<'_>,
    schema: &SchemaManager,
) -> bool {
    flatten_and_exprs(predicate).into_iter().all(|conjunct| {
        if !super::dml::has_subquery(conjunct) {
            return true;
        }
        let Expr::Exists {
            subquery: query, ..
        } = conjunct
        else {
            return false;
        };
        if !query.joins.is_empty()
            || query.from_subquery.is_some()
            || query.from_args.is_some()
            || query.from_json_table.is_some()
            || !query.group_by.is_empty()
            || query.having.is_some()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
            || query.where_clause.as_ref().is_some_and(calls_volatile)
            || !query.columns.iter().all(|column| {
                matches!(
                    column,
                    SelectColumn::Expr {
                        expr: Expr::Literal(_),
                        ..
                    }
                )
            })
        {
            return false;
        }
        let Some(inner) = schema.get(&query.from) else {
            return false;
        };
        let Some(where_clause) = &query.where_clause else {
            return false;
        };
        let (pairs, _) =
            extract_correlation_predicates(where_clause, ctx, inner, query.from_alias.as_deref());
        if pairs.is_empty() {
            return false;
        }
        let (inner_where, residual) = strip_correlation_predicates(
            &query.where_clause,
            ctx,
            inner,
            query.from_alias.as_deref(),
        );
        residual.is_empty() && !inner_where.as_ref().is_some_and(super::dml::has_subquery)
    })
}

/// Run the subqueries in `expr` that do not read the target row; keep the
/// ones that do, for each row to bind.
pub(super) fn materialize_closed_subqueries(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    ctes: &CteContext,
    expr: &Expr,
    ctx: &CorrelationCtx<'_>,
) -> Result<Expr> {
    if super::dml::has_conditional_subquery(expr) {
        return Ok(expr.clone());
    }
    super::dml::materialize_expr_selective(expr, &mut |query| {
        let mut candidate = Expr::ScalarSubquery(Box::new(query.clone()));
        if binding::bind_predicate(wtx, schema, ctes, &mut candidate, ctx, None)? {
            Ok(None)
        } else {
            super::dml::exec_subquery_write(wtx, schema, query, ctes).map(Some)
        }
    })
}

/// UPDATE SET expressions with correlated or conditional subqueries. Query
/// results are shared by captured values; conditional branches are evaluated
/// against the current target row.
pub(super) struct SetRowBinder<'db> {
    names: Vec<String>,
    deferred: Vec<bool>,
    columns: ColumnMap,
    expressions: RowExpressions,
    snapshot: Option<citadel_txn::read_txn::StatementReadTxn<'db>>,
}

impl<'db> SetRowBinder<'db> {
    pub(super) fn new(
        schema: &SchemaManager,
        ctes: &CteContext,
        assignments: &[(String, Expr)],
        ctx: &CorrelationCtx<'_>,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Option<Self>> {
        if !assignments
            .iter()
            .any(|(_, expr)| super::dml::has_subquery(expr))
        {
            return Ok(None);
        }
        let deferred: Vec<bool> = assignments
            .iter()
            .map(|(_, expr)| super::dml::has_subquery(expr))
            .collect();
        let outer = OuterScope::single(
            &ctx.outer_schema.name,
            ctx.outer_alias,
            &ctx.outer_schema.columns,
        );
        let expressions = RowExpressions::new(
            schema,
            ctes,
            assignments.iter().map(|(_, expr)| expr.clone()).collect(),
            outer,
            ctx.outer_schema.columns.len(),
            cancel,
        )?;
        Ok(Some(Self {
            names: assignments.iter().map(|(name, _)| name.clone()).collect(),
            deferred,
            columns: ColumnMap::new(&ctx.outer_schema.columns),
            expressions,
            snapshot: None,
        }))
    }

    /// A trigger or cascade can change a later selected row. Its conditional
    /// SET expression must see that current row, while a newly demanded closed
    /// subquery still reads the statement's original pending-write snapshot.
    pub(super) fn prepare_refresh(
        &mut self,
        wtx: &citadel_txn::write_txn::WriteTxn<'db>,
        schema: &SchemaManager,
        table: &TableSchema,
    ) -> Result<bool> {
        if self.expressions.captures_outer()
            || (!super::triggers::has_update_triggers(schema, &table.name)
                && schema.child_fks_for(&table.name).is_empty()
                && table.foreign_keys.is_empty())
        {
            return Ok(false);
        }
        self.snapshot = Some(wtx.read_snapshot().map_err(SqlError::Storage)?);
        Ok(true)
    }

    /// The mutation engine invokes this immediately before evaluating the row's
    /// assignments, and before changing any row. Resolve only demanded branches.
    pub(super) fn bind(
        &mut self,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        schema: &SchemaManager,
        ctes: &CteContext,
        row: &[Value],
    ) -> Result<Vec<(String, Expr)>> {
        let cancel = wtx.cancel_token().cloned();
        let ctx = EvalCtx::new(&self.columns, row).with_cancel(cancel.as_ref());
        let snapshot = &mut self.snapshot;
        self.names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let expr = if self.deferred[index] {
                    Expr::Literal(self.expressions.eval(
                        index,
                        schema,
                        ctes,
                        &ctx,
                        &mut |query| match snapshot.as_mut() {
                            Some(snapshot) => super::dml::exec_subquery_with_read(
                                &mut snapshot.view(),
                                schema,
                                query,
                                ctes,
                            ),
                            None => super::dml::exec_subquery_write(wtx, schema, query, ctes),
                        },
                    )?)
                } else {
                    self.expressions.expressions[index].clone()
                };
                Ok((name.clone(), expr))
            })
            .collect()
    }
}

pub(super) fn calls_volatile(expr: &Expr) -> bool {
    let mut volatile = false;
    crate::parser::visit_expr(expr, &mut |node| {
        if let Expr::Function { name, args, .. } = node {
            volatile |= crate::eval::is_volatile_function_expr(&name.to_ascii_uppercase(), args);
        }
    });
    volatile
}

/// Keep physical row locators attached while filtering. Only a complete simple
/// EXISTS equijoin may use a semijoin; other predicates bind each outer row in
/// its lexical query scopes and execute against this same writer. `ctes` are
/// the CTEs visible to the filtered statement.
#[allow(clippy::too_many_arguments)]
pub(super) fn filter_mutation_correlated_rows<T>(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    ctes: &CteContext,
    predicate: &Option<Expr>,
    ctx: &CorrelationCtx<'_>,
    rows: &mut Vec<T>,
    values: impl Fn(&T) -> &[Value],
) -> Result<Option<Expr>> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let predicate = materialize_closed_subqueries(wtx, schema, ctes, predicate, ctx)?;
    if complete_exists_semijoin(&predicate, ctx, schema) {
        return exists_semijoin_write(wtx, schema, &Some(predicate.clone()), ctx, rows, values);
    }
    let cancel = wtx.cancel_token().cloned();
    let columns = ctx.outer_schema.column_map();
    let outer = OuterScope::single(
        &ctx.outer_schema.name,
        ctx.outer_alias,
        &ctx.outer_schema.columns,
    );
    let expressions = RowExpressions::new(
        schema,
        ctes,
        vec![predicate],
        outer,
        ctx.outer_schema.columns.len(),
        cancel.as_ref(),
    )?;
    retain_cancellable(rows, cancel.as_ref(), |item| {
        let ctx = EvalCtx::new(columns, values(item)).with_cancel(cancel.as_ref());
        Ok(is_truthy(&expressions.eval(
            0,
            schema,
            ctes,
            &ctx,
            &mut |query| super::dml::exec_subquery_write(wtx, schema, query, ctes),
        )?))
    })?;
    Ok(None)
}

/// A correlated IN subquery's rows, found by an outer row's correlation values
/// as `=` compares them.
pub(super) struct InRows {
    /// Distinct complete tuples, including rows whose selected value is NULL.
    /// Every index below addresses this immutable store.
    tuples: Vec<Vec<Value>>,
    /// The distinct correlation values of every row.
    groups: KeyedRowIndex,
    /// The distinct correlation values of the rows selecting NULL.
    nulls: KeyedRowIndex,
    /// The distinct correlation values, then selected value, of the rows
    /// selecting a value.
    values: KeyedRowIndex,
}

/// Deduplication must retain type distinctions used by SQL coercion. In
/// particular, equal INTEGER/REAL values do not compare alike to a DATE.
struct InTupleKey<T>(T);

impl<T: AsRef<[Value]>> PartialEq for InTupleKey<T> {
    fn eq(&self, other: &Self) -> bool {
        let (left, right) = (self.0.as_ref(), other.0.as_ref());
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(a, b)| a.data_type() == b.data_type() && a == b)
    }
}

impl<T: AsRef<[Value]>> Eq for InTupleKey<T> {}

impl<T: AsRef<[Value]>> std::hash::Hash for InTupleKey<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let values = self.0.as_ref();
        values.len().hash(state);
        for value in values {
            value.data_type().type_tag().hash(state);
            value.hash(state);
        }
    }
}

impl InRows {
    fn from_distinct_tuples(
        tuples: Vec<Vec<Value>>,
        key_collations: &[Collation],
        value_collation: Collation,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Self> {
        let width = key_collations.len();
        let mut seen_groups = FxHashSet::default();
        let mut groups = Vec::new();
        let mut nulls = Vec::new();
        for (index, row) in tuples.iter().enumerate() {
            check_cancel_at(cancel, index)?;
            if seen_groups.insert(InTupleKey(&row[..width])) {
                groups.push(index);
            }
            if row[width].is_null() {
                nulls.push(index);
            }
        }
        drop(seen_groups);
        // NULL selected values cannot match the value index. Keep its capacity
        // proportional to indexed tuples, especially for an all-NULL RHS.
        let selected = if nulls.is_empty() {
            None
        } else {
            let mut selected = Vec::with_capacity(tuples.len() - nulls.len());
            for (index, row) in tuples.iter().enumerate() {
                check_cancel_at(cancel, index)?;
                if !row[width].is_null() {
                    selected.push(index);
                }
            }
            Some(selected)
        };
        let mut keys: Vec<_> = key_collations.iter().copied().enumerate().collect();
        let groups = KeyedRowIndex::build(&tuples, &keys, Some(groups), cancel)?;
        let nulls = KeyedRowIndex::build(&tuples, &keys, Some(nulls), cancel)?;
        keys.push((width, value_collation));
        let values = KeyedRowIndex::build(&tuples, &keys, selected, cancel)?;
        check_cancel(cancel)?;
        Ok(Self {
            tuples,
            groups,
            nulls,
            values,
        })
    }

    /// Whether `in_value IN (subquery)`, or NOT IN when `negated`, holds for the
    /// outer row whose correlation values are `key`. NULL does not hold.
    /// `in_value` runs only when the subquery has rows for the key.
    fn passes(
        &self,
        key: &mut Vec<Value>,
        negated: bool,
        cancel: Option<&citadel::CancelToken>,
        in_value: impl FnOnce() -> Result<Value>,
    ) -> Result<bool> {
        if !self.groups.contains(&self.tuples, key, cancel)? {
            // The subquery is empty for this row: NULL NOT IN (empty) is true,
            // unlike NULL NOT IN (nonempty).
            return Ok(negated);
        }
        let in_value = in_value()?;
        if in_value.is_null() {
            return Ok(false);
        }
        // The caller reuses this correlation key across rows. Appending the
        // selected value avoids cloning every correlation value for the probe;
        // restore the key even if comparison or cancellation returns an error.
        key.push(in_value);
        let found = self.values.contains(&self.tuples, key, cancel);
        key.pop();
        if found? {
            return Ok(!negated);
        }
        Ok(negated && !self.nulls.contains(&self.tuples, key, cancel)?)
    }
}

/// The inner column each correlation pair reads, with the collation its `=`
/// compares under.
fn inner_keys(
    corr_pairs: &[CorrEqPair],
    inner_schema: &TableSchema,
) -> Result<Vec<(usize, Collation)>> {
    corr_pairs
        .iter()
        .map(|pair| {
            let column = inner_schema
                .column_index(&pair.inner_col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(pair.inner_col_name.clone()))?;
            Ok((column, pair.collation))
        })
        .collect()
}

/// Key positions `0..` of rows that begin with the correlation values.
fn leading_keys(keys: &[(usize, Collation)]) -> Vec<(usize, Collation)> {
    keys.iter()
        .enumerate()
        .map(|(position, &(_, collation))| (position, collation))
        .collect()
}

/// A row's values at `columns`, in that order.
fn values_at(row: &[Value], columns: &[usize]) -> Vec<Value> {
    columns.iter().map(|&column| row[column].clone()).collect()
}

/// The distinct correlation values of `rows`, in no particular order. A
/// repeated key never changes whether one equals an outer row's.
fn distinct_keys(
    rows: &[Vec<Value>],
    keys: &[(usize, Collation)],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let columns: Vec<usize> = keys.iter().map(|&(column, _)| column).collect();
    let mut distinct = FxHashSet::default();
    for (row_idx, row) in rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        distinct.insert(values_at(row, &columns));
    }
    Ok(distinct.into_iter().collect())
}

fn in_subquery_value_collation(
    subquery: &SelectStmt,
    inner_schema: &TableSchema,
) -> Result<Collation> {
    let expr = match &subquery.columns[0] {
        SelectColumn::Expr { expr, .. } => expr,
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let index = in_subquery_value_column_index(expr, inner_schema)?;
    let col_map = inner_schema.column_map();
    Ok(crate::eval::operand_collation(expr, col_map)
        .unwrap_or(inner_schema.columns[index].collation))
}

fn in_subquery_value_column_index(expr: &Expr, inner_schema: &TableSchema) -> Result<usize> {
    let name = match expr {
        Expr::Column(name) => name,
        Expr::QualifiedColumn { column, .. } => column,
        Expr::Collate { expr, .. } => {
            return in_subquery_value_column_index(expr, inner_schema);
        }
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    inner_schema
        .column_index(name)
        .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))
}

fn retain_cancellable<T>(
    values: &mut Vec<T>,
    cancel: Option<&citadel::CancelToken>,
    mut keep: impl FnMut(&T) -> Result<bool>,
) -> Result<()> {
    if cancel.is_none() {
        let mut error = None;
        values.retain(|item| {
            if error.is_some() {
                return false;
            }
            match keep(item) {
                Ok(keep) => keep,
                Err(err) => {
                    error = Some(err);
                    false
                }
            }
        });
        return error.map_or(Ok(()), Err);
    }

    let original = std::mem::take(values);
    values.reserve(original.len());
    for (item_idx, item) in original.into_iter().enumerate() {
        check_cancel_at(cancel, item_idx)?;
        if keep(&item)? {
            values.push(item);
        }
    }
    check_cancel(cancel)
}

fn any_cancellable<T>(
    values: &[T],
    cancel: Option<&citadel::CancelToken>,
    mut predicate: impl FnMut(&T) -> Result<bool>,
) -> Result<bool> {
    for (item_idx, item) in values.iter().enumerate() {
        check_cancel_at(cancel, item_idx)?;
        if predicate(item)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn handle_correlated_select_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut [Vec<Value>],
    row_width: &mut usize,
) -> Result<SelectStmt> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let mut new_columns = Vec::new();
    // For each hashed subquery, its value for every row.
    let mut hashed: Vec<Vec<Value>> = Vec::new();
    let mut corr_col_idx = *row_width;

    for col in &stmt.columns {
        match col {
            SelectColumn::Expr {
                expr: written @ Expr::ScalarSubquery(sub),
                alias,
            } => {
                if is_correlated_subquery(sub, ctx, schema) {
                    let inner_name = sub.from.to_ascii_lowercase();
                    if let Some(inner_schema) = schema.get(&inner_name) {
                        let (corr_pairs, _) = extract_correlation_predicates(
                            sub.where_clause
                                .as_ref()
                                .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                            ctx,
                            inner_schema,
                            sub.from_alias.as_deref(),
                        );
                        let shape = hashable_scalar(sub, inner_schema).filter(|_| {
                            !corr_pairs.is_empty()
                                && !has_residual_correlation(sub, ctx, inner_schema)
                        });
                        let values = match shape {
                            Some(shape) => {
                                let by_key = decorrelate_scalar_with_read(
                                    rtx,
                                    schema,
                                    sub,
                                    &corr_pairs,
                                    ctx,
                                    &shape,
                                )?;
                                hashed_scalar_values(&by_key, rows, &corr_pairs, &shape, cancel)?
                            }
                            None => None,
                        };
                        if let Some(values) = values {
                            hashed.push(values);
                            let slot = Expr::InputRef {
                                index: corr_col_idx,
                                collation: None,
                            };
                            new_columns.push(SelectColumn::Expr {
                                alias: super::helpers::written_alias(alias, written, &slot),
                                expr: slot,
                            });
                            corr_col_idx += 1;
                            continue;
                        }
                    }
                }
                new_columns.push(col.clone());
            }
            _ => new_columns.push(col.clone()),
        }
    }

    if hashed.is_empty() {
        return Ok(stmt.clone());
    }

    *row_width = corr_col_idx;

    for (row_idx, row) in rows.iter_mut().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        row.extend(hashed.iter().map(|values| values[row_idx].clone()));
    }

    check_cancel(cancel)?;

    Ok(SelectStmt {
        columns: new_columns,
        from: stmt.from.clone(),
        from_alias: stmt.from_alias.clone(),
        from_subquery: stmt.from_subquery.clone(),
        from_args: stmt.from_args.clone(),
        from_json_table: stmt.from_json_table.clone(),
        joins: stmt.joins.clone(),
        distinct: stmt.distinct,
        where_clause: stmt.where_clause.clone(),
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
    })
}

pub(super) fn resolve_inner_schema_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<TableSchema> {
    if let Some(ts) = schema.get(name) {
        return Ok(ts.clone());
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = super::exec_view_with_read(rtx, schema, vd)?;
        return super::build_view_schema(name, &qr);
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

pub(super) fn resolve_inner_schema_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    name: &str,
) -> Result<TableSchema> {
    if let Some(ts) = schema.get(name) {
        return Ok(ts.clone());
    }
    if let Some(vd) = schema.get_view(name) {
        let qr = super::exec_view_write(wtx, schema, vd)?;
        return super::build_view_schema(name, &qr);
    }
    Err(SqlError::TableNotFound(name.to_string()))
}

/// Context for correlation detection — carries outer table info.
pub(super) struct CorrelationCtx<'a> {
    pub(super) outer_schema: &'a TableSchema,
    pub(super) outer_alias: Option<&'a str>,
}

impl<'a> CorrelationCtx<'a> {
    fn outer_name(&self) -> &str {
        &self.outer_schema.name
    }

    fn matches_outer(&self, table_part: &str) -> bool {
        table_part == self.outer_name()
            || self
                .outer_alias
                .is_some_and(|a| a.eq_ignore_ascii_case(table_part))
    }
}

pub(super) fn resolves_in(name: &str, schema: &TableSchema) -> bool {
    let lower = name.to_ascii_lowercase();
    schema.columns.iter().any(|c| c.name == lower)
}

/// A column an expression reads, lowercased. A qualifier may itself contain a
/// dot, as `information_schema.tables` does.
#[derive(Debug, PartialEq)]
pub(super) struct ColumnName {
    pub(super) table: Option<String>,
    pub(super) column: String,
}

pub(super) fn collect_column_names(expr: &Expr, out: &mut Vec<ColumnName>) {
    match expr {
        Expr::Column(name) => out.push(ColumnName {
            table: None,
            column: name.to_ascii_lowercase(),
        }),
        Expr::QualifiedColumn { table, column } => out.push(ColumnName {
            table: Some(table.to_ascii_lowercase()),
            column: column.to_ascii_lowercase(),
        }),
        Expr::BinaryOp { left, right, .. } => {
            collect_column_names(left, out);
            collect_column_names(right, out);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::Cast { expr: e, .. }
        | Expr::Collate { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::Function { args, filter, .. } => {
            for a in args {
                collect_column_names(a, out);
            }
            if let Some(filter) = filter {
                collect_column_names(filter, out);
            }
        }
        Expr::Coalesce(args) | Expr::ArrayLiteral(args) => {
            for a in args {
                collect_column_names(a, out);
            }
        }
        Expr::InList { expr: e, list, .. } => {
            collect_column_names(e, out);
            for item in list {
                collect_column_names(item, out);
            }
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_column_names(e, out);
            collect_column_names(low, out);
            collect_column_names(high, out);
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_column_names(left, out);
            collect_column_names(right, out);
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            collect_column_names(e, out);
            collect_column_names(pattern, out);
            if let Some(escape) = escape {
                collect_column_names(escape, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_column_names(op, out);
            }
            for (c, r) in conditions {
                collect_column_names(c, out);
                collect_column_names(r, out);
            }
            if let Some(el) = else_result {
                collect_column_names(el, out);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            for a in args {
                collect_column_names(a, out);
            }
            for p in &spec.partition_by {
                collect_column_names(p, out);
            }
            for o in &spec.order_by {
                collect_column_names(&o.expr, out);
            }
            if let Some(frame) = &spec.frame {
                for bound in [&frame.start, &frame.end] {
                    match bound {
                        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                            collect_column_names(expr, out);
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::InSubquery { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::InSet { expr: e, .. } => {
            collect_column_names(e, out);
        }
        Expr::Quantified { left, right, .. } => {
            collect_column_names(left, out);
            if let QuantifiedRhs::Array(expr) = right {
                collect_column_names(expr, out);
            }
        }
        _ => {}
    }
}

/// Whether subqueries must run at expression evaluation: either they capture
/// the source row or occur beneath conditional evaluation.
/// `ctes` are the CTEs visible to `stmt`.
pub(super) fn requires_subquery_runtime(
    schema: &SchemaManager,
    ctes: &CteContext,
    stmt: &SelectStmt,
    outer: &OuterScope,
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    let columns = stmt.columns.iter().filter_map(|column| match column {
        SelectColumn::Expr { expr, .. } => Some(expr),
        _ => None,
    });
    let clauses = columns
        .chain(&stmt.where_clause)
        .chain(&stmt.group_by)
        .chain(&stmt.having)
        .chain(stmt.order_by.iter().map(|item| &item.expr))
        .chain(stmt.joins.iter().filter_map(|join| join.on_clause.as_ref()));
    for expr in clauses {
        if super::dml::has_conditional_subquery(expr)
            || expr_captures_outer(schema, ctes, expr, outer, cancel)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn expr_captures_outer(
    schema: &SchemaManager,
    ctes: &CteContext,
    expr: &Expr,
    outer: &OuterScope,
    cancel: Option<&citadel::CancelToken>,
) -> Result<bool> {
    if !super::dml::has_subquery(expr) {
        return Ok(false);
    }
    Ok(binding::bind_outer(schema, ctes, &mut expr.clone(), outer, None, cancel)?.is_some())
}

/// Finish a SELECT over its source rows: subqueries that read a row run per
/// row, closed subqueries run once. `ctes` are the CTEs visible to `stmt`.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_captured_select(
    schema: &SchemaManager,
    ctes: &CteContext,
    stmt: SelectStmt,
    outer: &OuterScope,
    rows: Vec<Vec<Value>>,
    columns: Vec<ColumnDef>,
    row_width: usize,
    cancel: Option<&citadel::CancelToken>,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<super::CteRows>,
) -> Result<ExecutionResult> {
    apply::finish_subqueries(
        schema, ctes, stmt, outer, rows, columns, row_width, cancel, exec_sub,
    )
}

/// JOIN subqueries must be closed. Conditional ones retain their plans for
/// demand evaluation by the join predicate; other closed queries run here.
pub(super) fn materialize_join_conditions(
    schema: &SchemaManager,
    ctes: &CteContext,
    stmt: &mut SelectStmt,
    outer: &OuterScope,
    cancel: Option<&citadel::CancelToken>,
    exec_sub: &mut dyn FnMut(&SelectStmt) -> Result<super::CteRows>,
) -> Result<()> {
    for join in &mut stmt.joins {
        let Some(condition) = &mut join.on_clause else {
            continue;
        };
        if !super::dml::has_subquery(condition) {
            continue;
        }
        if expr_captures_outer(schema, ctes, condition, outer, cancel)? {
            return Err(SqlError::Unsupported(
                "a subquery in a JOIN condition that reads a joined row".into(),
            ));
        }
        if !super::dml::has_conditional_subquery(condition) {
            *condition = super::dml::materialize_expr(condition, exec_sub)?;
        }
    }
    Ok(())
}

/// Hash decorrelation reproduces a subquery only when it reads one source
/// through conjuncts, with no nested query, grouping, ordering or limit, and
/// no volatile call, which each outer row's run evaluates anew.
fn hashable_shape(query: &SelectStmt) -> bool {
    query.joins.is_empty()
        && query.from_subquery.is_none()
        && query.from_args.is_none()
        && query.from_json_table.is_none()
        && query.group_by.is_empty()
        && query.having.is_none()
        && query.order_by.is_empty()
        && query.limit.is_none()
        && query.offset.is_none()
        && !query
            .where_clause
            .as_ref()
            .is_some_and(|expr| super::dml::has_subquery(expr) || calls_volatile(expr))
        && query.columns.iter().all(|column| match column {
            SelectColumn::Expr { expr, .. } => {
                !super::dml::has_subquery(expr) && !calls_volatile(expr)
            }
            _ => true,
        })
}

/// An aggregate projection yields a row even for no input, so EXISTS over it
/// is not a membership test.
fn hashable_exists(query: &SelectStmt) -> bool {
    hashable_shape(query)
        && !query.columns.iter().any(|column| match column {
            SelectColumn::Expr { expr, .. } => crate::parser::is_aggregate_expr(expr),
            _ => false,
        })
}

/// The IN path hashes the values of one column of a base table.
fn hashable_in(schema: &SchemaManager, query: &SelectStmt, inner_schema: &TableSchema) -> bool {
    schema.get(&query.from.to_ascii_lowercase()).is_some()
        && hashable_shape(query)
        && query.columns.len() == 1
        && matches!(&query.columns[0], SelectColumn::Expr { expr, .. }
            if in_subquery_value_column_index(expr, inner_schema).is_ok())
}

/// How hash decorrelation answers a scalar subquery for one outer key.
pub(super) enum HashedScalar {
    /// One aggregate call: its value over the key's rows, and this value for a
    /// key no inner row matches.
    Aggregate(Value),
    /// One plain value: the key's row, NULL for no row, and an error for more
    /// than one.
    Row,
}

/// The scalar subqueries hash decorrelation reproduces: one value computed
/// from the subquery's own source. DISTINCT could merge rows a plain value
/// must count, so it keeps a plain value on the per-row path.
fn hashable_scalar(query: &SelectStmt, inner_schema: &TableSchema) -> Option<HashedScalar> {
    if !hashable_shape(query) || !projects_own_columns(query, inner_schema) {
        return None;
    }
    let [SelectColumn::Expr { expr, .. }] = query.columns.as_slice() else {
        return None;
    };
    match expr {
        Expr::CountStar => Some(HashedScalar::Aggregate(Value::Integer(0))),
        Expr::Function { name, args, .. }
            if crate::parser::is_aggregate_function(name, args.len())
                && !args.iter().any(crate::parser::is_aggregate_expr) =>
        {
            Some(HashedScalar::Aggregate(
                if name.eq_ignore_ascii_case("count") {
                    Value::Integer(0)
                } else {
                    Value::Null
                },
            ))
        }
        expr if !query.distinct && !crate::parser::is_aggregate_expr(expr) => {
            Some(HashedScalar::Row)
        }
        _ => None,
    }
}

/// Whether every column the subquery projects belongs to its own source.
fn projects_own_columns(query: &SelectStmt, inner_schema: &TableSchema) -> bool {
    let own = query.from_alias.as_deref().unwrap_or(&query.from);
    let mut names = Vec::new();
    for column in &query.columns {
        if let SelectColumn::Expr { expr, .. } = column {
            collect_column_names(expr, &mut names);
        }
    }
    names.iter().all(|name| match &name.table {
        Some(table) => table.eq_ignore_ascii_case(own) && resolves_in(&name.column, inner_schema),
        None => resolves_in(&name.column, inner_schema),
    })
}

/// A hashed scalar subquery's value for each of `outer_rows`, or None when an
/// aggregate's key equals several groups: `=` converts between their values,
/// so no one group holds the aggregate and the rows need the per-row path.
/// `by_key` holds the correlation values, then the value, of each group or row.
fn hashed_scalar_values(
    by_key: &KeyedRows,
    outer_rows: &[Vec<Value>],
    corr_pairs: &[CorrEqPair],
    shape: &HashedScalar,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Option<Vec<Value>>> {
    let outer_columns: Vec<usize> = corr_pairs.iter().map(|p| p.outer_col_idx).collect();
    let mut values = Vec::with_capacity(outer_rows.len());
    for (row_idx, row) in outer_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let found = by_key.matching(&values_at(row, &outer_columns), cancel)?;
        values.push(match (found.as_slice(), shape) {
            ([], HashedScalar::Aggregate(empty)) => empty.clone(),
            ([], HashedScalar::Row) => Value::Null,
            ([one], _) => one[corr_pairs.len()].clone(),
            (_, HashedScalar::Row) => return Err(SqlError::SubqueryMultipleRows),
            (_, HashedScalar::Aggregate(_)) => return Ok(None),
        });
    }
    Ok(Some(values))
}

/// Correlation conjuncts other than equalities, which only the EXISTS path
/// evaluates per outer row.
fn has_residual_correlation(
    query: &SelectStmt,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
) -> bool {
    let (_, residual) = strip_correlation_predicates(
        &query.where_clause,
        ctx,
        inner_schema,
        query.from_alias.as_deref(),
    );
    !residual.is_empty()
}

/// Check if a subquery references outer columns not in the inner table.
pub(super) fn is_correlated_subquery(
    subquery: &SelectStmt,
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema.get(&inner_name);
    if inner_schema.is_none() && schema.get_view(&inner_name).is_none() {
        return false;
    }
    let inner_alias = subquery
        .from_alias
        .as_deref()
        .map(|a| a.to_ascii_lowercase());

    let mut col_names = Vec::new();
    if let Some(ref w) = subquery.where_clause {
        collect_column_names(w, &mut col_names);
    }
    for col in &subquery.columns {
        if let SelectColumn::Expr { expr, .. } = col {
            collect_column_names(expr, &mut col_names);
        }
    }

    for name in &col_names {
        if let Some(table) = &name.table {
            if *table == inner_alias.as_deref().unwrap_or(&inner_name) {
                continue;
            }
            if ctx.matches_outer(table) && resolves_in(&name.column, ctx.outer_schema) {
                return true;
            }
        } else if let Some(is) = inner_schema {
            if !resolves_in(&name.column, is) && resolves_in(&name.column, ctx.outer_schema) {
                return true;
            }
        }
    }
    false
}

/// A correlation equality predicate: outer_col = inner_col
pub(super) struct CorrEqPair {
    outer_col_idx: usize,
    inner_col_name: String,
    /// Collation of the syntactic left operand of the extracted `=` predicate.
    collation: Collation,
}

/// Extract equality correlation predicates. Returns (pairs, remaining inner-only WHERE).
pub(super) fn extract_correlation_predicates(
    where_clause: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> (Vec<CorrEqPair>, Option<Expr>) {
    let conjuncts = flatten_and_exprs(where_clause);
    let mut corr_pairs = Vec::new();
    let mut remaining = Vec::new();

    for conj in conjuncts {
        if let Some(pair) = try_extract_corr_eq(conj, ctx, inner_schema, inner_alias) {
            corr_pairs.push(pair);
        } else {
            remaining.push(conj.clone());
        }
    }

    let remaining_expr = if remaining.is_empty() {
        None
    } else {
        let mut combined = remaining.remove(0);
        for r in remaining {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Some(combined)
    };

    (corr_pairs, remaining_expr)
}

pub(super) fn flatten_and_exprs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = flatten_and_exprs(left);
            v.extend(flatten_and_exprs(right));
            v
        }
        _ => vec![expr],
    }
}

/// Try to extract a correlation equality from an expression like `t2.x = t1.x` or `inner_col = outer_col`.
pub(super) fn try_extract_corr_eq(
    expr: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> Option<CorrEqPair> {
    let (left, right) = match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };

    if let Some(pair) = try_match_corr_pair(left, right, ctx, inner_schema, inner_alias, true) {
        return Some(pair);
    }
    try_match_corr_pair(right, left, ctx, inner_schema, inner_alias, false)
}

pub(super) fn try_match_corr_pair(
    maybe_outer: &Expr,
    maybe_inner: &Expr,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
    outer_is_left: bool,
) -> Option<CorrEqPair> {
    let outer_col = match maybe_outer {
        Expr::QualifiedColumn { table, column } => {
            let t = table.to_ascii_lowercase();
            if ctx.matches_outer(&t)
                && !t.eq_ignore_ascii_case(inner_alias.unwrap_or(&inner_schema.name))
            {
                column.to_ascii_lowercase()
            } else {
                return None;
            }
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if resolves_in(&lower, inner_schema) || !resolves_in(&lower, ctx.outer_schema) {
                return None;
            }
            lower
        }
        _ => return None,
    };

    let inner_col = match maybe_inner {
        Expr::QualifiedColumn { table, column } => {
            let t = table.to_ascii_lowercase();
            if t.eq_ignore_ascii_case(inner_alias.unwrap_or(&inner_schema.name)) {
                column.to_ascii_lowercase()
            } else {
                return None;
            }
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if !resolves_in(&lower, inner_schema) {
                return None;
            }
            lower
        }
        _ => return None,
    };

    let outer_col_idx = ctx.outer_schema.column_index(&outer_col)?;
    let inner_col_idx = inner_schema.column_index(&inner_col)?;
    let collation = if outer_is_left {
        ctx.outer_schema.columns[outer_col_idx].collation
    } else {
        inner_schema.columns[inner_col_idx].collation
    };

    Some(CorrEqPair {
        outer_col_idx,
        inner_col_name: inner_col,
        collation,
    })
}

/// Strip exact correlation equalities from WHERE, preserving inner-only and
/// outer-dependent residual predicates with their original scopes.
pub(super) fn strip_correlation_predicates(
    where_clause: &Option<Expr>,
    ctx: &CorrelationCtx,
    inner_schema: &TableSchema,
    inner_alias: Option<&str>,
) -> (Option<Expr>, Vec<Expr>) {
    let w = match where_clause {
        Some(w) => w,
        None => return (None, vec![]),
    };
    let conjuncts = flatten_and_exprs(w);
    let mut inner_only: Vec<Expr> = Vec::new();
    let mut non_eq_corr: Vec<Expr> = Vec::new();

    for c in conjuncts {
        if try_extract_corr_eq(c, ctx, inner_schema, inner_alias).is_some() {
            // Remove only the exact qualified inner/outer equalities used as
            // hash keys. Equal column spellings do not establish correlation.
            continue;
        }
        let mut refs = Vec::new();
        collect_column_names(c, &mut refs);
        let refs_outer = refs.iter().any(|name| match &name.table {
            Some(table) => {
                ctx.matches_outer(table)
                    && !table.eq_ignore_ascii_case(inner_alias.unwrap_or(&inner_schema.name))
            }
            None => {
                !resolves_in(&name.column, inner_schema)
                    && resolves_in(&name.column, ctx.outer_schema)
            }
        });
        if refs_outer {
            non_eq_corr.push(c.clone());
        } else {
            inner_only.push(c.clone());
        }
    }

    let inner_where = if inner_only.is_empty() {
        None
    } else {
        let mut combined = inner_only.remove(0);
        for c in inner_only {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(c),
            };
        }
        Some(combined)
    };

    (inner_where, non_eq_corr)
}

/// Replace outer column references in an expression with literal values from the outer row.
pub(super) fn bind_outer_values_in_expr(
    expr: &Expr,
    outer_row: &[Value],
    outer_col_map: &ColumnMap,
    inner_col_map: &ColumnMap,
    ctx: &CorrelationCtx,
) -> Expr {
    let bind =
        |expr: &Expr| bind_outer_values_in_expr(expr, outer_row, outer_col_map, inner_col_map, ctx);
    match expr {
        Expr::QualifiedColumn { table, column } => {
            if ctx.matches_outer(&table.to_ascii_lowercase()) {
                if let Ok(idx) = outer_col_map.resolve(&column.to_ascii_lowercase()) {
                    return Expr::BoundColumn {
                        value: outer_row[idx].clone(),
                        collation: outer_col_map.collation_at(idx),
                    };
                }
            }
            expr.clone()
        }
        Expr::Column(name) => {
            let lower = name.to_ascii_lowercase();
            if matches!(
                inner_col_map.resolve(&lower),
                Err(SqlError::ColumnNotFound(_))
            ) {
                if let Ok(idx) = outer_col_map.resolve(&lower) {
                    return Expr::BoundColumn {
                        value: outer_row[idx].clone(),
                        collation: outer_col_map.collation_at(idx),
                    };
                }
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(bind(left)),
            op: *op,
            right: Box::new(bind(right)),
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind(e)),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(bind(expr))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(bind(expr))),
        Expr::Function {
            name,
            args,
            distinct,
            filter,
        } => Expr::Function {
            name: name.clone(),
            args: args.iter().map(bind).collect(),
            distinct: *distinct,
            filter: filter.as_deref().map(|filter| Box::new(bind(filter))),
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(bind(expr)),
            subquery: subquery.clone(),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(bind(expr)),
            list: list.iter().map(bind).collect(),
            negated: *negated,
        },
        Expr::InSet {
            expr,
            values,
            families,
            has_null,
            negated,
            collation,
        } => Expr::InSet {
            expr: Box::new(bind(expr)),
            values: values.clone(),
            families: *families,
            has_null: *has_null,
            negated: *negated,
            collation: *collation,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(bind(expr)),
            low: Box::new(bind(low)),
            high: Box::new(bind(high)),
            negated: *negated,
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(bind(left)),
            right: Box::new(bind(right)),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Like {
            expr: Box::new(bind(expr)),
            pattern: Box::new(bind(pattern)),
            escape: escape.as_ref().map(|expr| Box::new(bind(expr))),
            negated: *negated,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            operand: operand.as_ref().map(|expr| Box::new(bind(expr))),
            conditions: conditions
                .iter()
                .map(|(condition, result)| (bind(condition), bind(result)))
                .collect(),
            else_result: else_result.as_ref().map(|expr| Box::new(bind(expr))),
        },
        Expr::Coalesce(args) => Expr::Coalesce(args.iter().map(bind).collect()),
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(bind(expr)),
            data_type: *data_type,
        },
        Expr::WindowFunction { name, args, spec } => {
            let mut spec = spec.clone();
            spec.partition_by = spec.partition_by.iter().map(bind).collect();
            for item in &mut spec.order_by {
                item.expr = bind(&item.expr);
            }
            if let Some(frame) = &mut spec.frame {
                for bound in [&mut frame.start, &mut frame.end] {
                    match bound {
                        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                            **expr = bind(expr);
                        }
                        _ => {}
                    }
                }
            }
            Expr::WindowFunction {
                name: name.clone(),
                args: args.iter().map(bind).collect(),
                spec,
            }
        }
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: Box::new(bind(expr)),
            collation: *collation,
        },
        Expr::ArrayLiteral(values) => Expr::ArrayLiteral(values.iter().map(bind).collect()),
        Expr::Quantified {
            left,
            op,
            quantifier,
            right,
        } => Expr::Quantified {
            left: Box::new(bind(left)),
            op: *op,
            quantifier: *quantifier,
            right: match right {
                QuantifiedRhs::Subquery(subquery) => QuantifiedRhs::Subquery(subquery.clone()),
                QuantifiedRhs::Array(expr) => QuantifiedRhs::Array(Box::new(bind(expr))),
            },
        },
        Expr::InputRef { .. }
        | Expr::BoundColumn { .. }
        | Expr::Literal(_)
        | Expr::CountStar
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Parameter(_)
        | Expr::TypedNullRecord(_) => expr.clone(),
    }
}

/// The inner rows of a correlated EXISTS, found by an outer row's correlation
/// values, and its correlation conjuncts other than those equalities, which a
/// found row must also satisfy. Without such conjuncts a row holds only its
/// correlation values.
pub(super) struct ExistsRows {
    rows: KeyedRows,
    non_eq_predicates: Vec<Expr>,
    inner_schema: TableSchema,
}

impl ExistsRows {
    /// Whether deciding a match reads the outer row beyond its correlation
    /// values.
    fn reads_outer_row(&self) -> bool {
        !self.non_eq_predicates.is_empty()
    }

    /// Whether one of `candidates`, the rows correlated to `outer_row`,
    /// satisfies every other correlation conjunct.
    fn satisfied_by(
        &self,
        candidates: &[&[Value]],
        outer_row: &[Value],
        outer_col_map: &ColumnMap,
        ctx: &CorrelationCtx,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<bool> {
        if candidates.is_empty() {
            return Ok(false);
        }
        let inner_col_map = self.inner_schema.column_map();
        // The binding varies only with the outer row, so bind once for it.
        let bound: Vec<_> = self
            .non_eq_predicates
            .iter()
            .map(|pred| {
                bind_outer_values_in_expr(pred, outer_row, outer_col_map, inner_col_map, ctx)
            })
            .collect();
        any_cancellable(candidates, cancel, |inner_row| {
            for predicate in &bound {
                if !is_truthy(&eval_expr(
                    predicate,
                    &EvalCtx::new(inner_col_map, inner_row).with_cancel(cancel),
                )?) {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }
}

pub(super) fn decorrelate_exists_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<ExistsRows> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();

    let (inner_schema_owned, inner_rows) = if let Some(ts) = schema.get(&inner_name) {
        let (inner_where, _) = strip_correlation_predicates(
            &subquery.where_clause,
            ctx,
            ts,
            subquery.from_alias.as_deref(),
        );
        let (rows, _) = super::collect_rows_with_read(rtx, ts, &inner_where, None)?;
        (ts.clone(), rows)
    } else if let Some(vd) = schema.get_view(&inner_name) {
        let vqr = super::exec_view_with_read(rtx, schema, vd)?;
        let vs = super::build_view_schema(&inner_name, &vqr)?;
        let (inner_where, _) = strip_correlation_predicates(
            &subquery.where_clause,
            ctx,
            &vs,
            subquery.from_alias.as_deref(),
        );
        let col_map = ColumnMap::new(&vs.columns);
        let rows: Vec<Vec<Value>> = if let Some(ref w) = inner_where {
            let mut filtered = Vec::new();
            for (row_idx, row) in vqr.result.rows.into_iter().enumerate() {
                check_cancel_at(cancel, row_idx)?;
                if is_truthy(&eval_expr(
                    w,
                    &EvalCtx::new(&col_map, &row).with_cancel(cancel),
                )?) {
                    filtered.push(row);
                }
            }
            filtered
        } else {
            vqr.result.rows
        };
        (vs, rows)
    } else {
        return Err(SqlError::TableNotFound(subquery.from.clone()));
    };
    let inner_schema = &inner_schema_owned;

    let (_, non_eq) = strip_correlation_predicates(
        &subquery.where_clause,
        ctx,
        inner_schema,
        subquery.from_alias.as_deref(),
    );

    let keys = inner_keys(corr_pairs, inner_schema)?;
    let rows = if non_eq.is_empty() {
        let key_rows = distinct_keys(&inner_rows, &keys, cancel)?;
        KeyedRows::build(key_rows, &leading_keys(&keys), cancel)?
    } else {
        KeyedRows::build(inner_rows, &keys, cancel)?
    };
    check_cancel(cancel)?;
    Ok(ExistsRows {
        rows,
        non_eq_predicates: non_eq,
        inner_schema: inner_schema.clone(),
    })
}

/// Decorrelate IN/NOT IN subquery: its rows, found by correlation values and
/// by the selected value, which compares under `value_collation`.
pub(super) fn decorrelate_in_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
    value_collation: Collation,
) -> Result<InRows> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;

    let in_expr = match &subquery.columns[0] {
        SelectColumn::Expr { expr, .. } => expr,
        _ => return Err(SqlError::Unsupported("complex IN subquery column".into())),
    };
    let in_col_idx = in_subquery_value_column_index(in_expr, inner_schema)?;

    let (inner_where, _non_eq) = strip_correlation_predicates(
        &subquery.where_clause,
        ctx,
        inner_schema,
        subquery.from_alias.as_deref(),
    );
    let (inner_rows, _) = super::collect_rows_with_read(rtx, inner_schema, &inner_where, None)?;

    let keys = inner_keys(corr_pairs, inner_schema)?;
    let key_columns: Vec<usize> = keys.iter().map(|&(column, _)| column).collect();
    // Keep complete tuples once. The prefix and NULL indexes use row IDs into
    // this same storage rather than owning copies of the correlation values.
    let mut tuples = FxHashSet::default();
    for (row_idx, row) in inner_rows.iter().enumerate() {
        check_cancel_at(cancel, row_idx)?;
        let mut tuple = Vec::with_capacity(keys.len() + 1);
        tuple.extend(key_columns.iter().map(|&column| row[column].clone()));
        tuple.push(row[in_col_idx].clone());
        tuples.insert(InTupleKey(tuple));
    }
    let key_collations: Vec<_> = keys.iter().map(|&(_, collation)| collation).collect();
    InRows::from_distinct_tuples(
        tuples.into_iter().map(|tuple| tuple.0).collect(),
        &key_collations,
        value_collation,
        cancel,
    )
}

/// Decorrelate scalar subquery: each group of an aggregate, or each row of a
/// plain value, as its correlation values then the value, found by the
/// correlation values.
pub(super) fn decorrelate_scalar_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
    shape: &HashedScalar,
) -> Result<KeyedRows> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;

    let corr_col_names: Vec<String> = corr_pairs
        .iter()
        .map(|p| p.inner_col_name.clone())
        .collect();

    // An aggregate is computed per key; a plain value keeps every row so a key
    // with more than one row is caught.
    let group_by: Vec<Expr> = match shape {
        HashedScalar::Aggregate(_) => corr_col_names
            .iter()
            .map(|name| Expr::Column(name.clone()))
            .collect(),
        HashedScalar::Row => Vec::new(),
    };

    let (inner_where, _non_eq) = strip_correlation_predicates(
        &subquery.where_clause,
        ctx,
        inner_schema,
        subquery.from_alias.as_deref(),
    );

    let mut select_cols: Vec<SelectColumn> = corr_col_names
        .iter()
        .map(|name| SelectColumn::Expr {
            expr: Expr::Column(name.clone()),
            alias: None,
        })
        .collect();
    select_cols.extend(subquery.columns.clone());

    let rewritten = SelectStmt {
        columns: select_cols,
        from: subquery.from.clone(),
        from_alias: subquery.from_alias.clone(),
        from_subquery: subquery.from_subquery.clone(),
        from_args: subquery.from_args.clone(),
        from_json_table: subquery.from_json_table.clone(),
        joins: vec![],
        distinct: false,
        where_clause: inner_where,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by,
        having: None,
    };

    let empty_ctes = CteContext::default();
    let qr = match super::exec_select_with_read(rtx, schema, &rewritten, &empty_ctes)? {
        ExecutionResult::Query(qr) => qr,
        _ => return Err(SqlError::Plan("expected Query result".into())),
    };
    let keys: Vec<(usize, Collation)> = corr_pairs
        .iter()
        .enumerate()
        .map(|(position, pair)| (position, pair.collation))
        .collect();
    KeyedRows::build(qr.rows, &keys, cancel)
}

// Write-transaction variants below — same logic, use collect_rows_write.

pub(super) fn decorrelate_exists_write(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    subquery: &SelectStmt,
    corr_pairs: &[CorrEqPair],
    ctx: &CorrelationCtx,
) -> Result<KeyedRows> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let inner_name = subquery.from.to_ascii_lowercase();
    let inner_schema = schema
        .get(&inner_name)
        .ok_or_else(|| SqlError::TableNotFound(subquery.from.clone()))?;
    let (inner_where, _non_eq) = strip_correlation_predicates(
        &subquery.where_clause,
        ctx,
        inner_schema,
        subquery.from_alias.as_deref(),
    );
    let (inner_rows, _) = super::collect_rows_write(wtx, inner_schema, &inner_where, None)?;
    let keys = inner_keys(corr_pairs, inner_schema)?;
    let key_rows = distinct_keys(&inner_rows, &keys, cancel)?;
    let rows = KeyedRows::build(key_rows, &leading_keys(&keys), cancel)?;
    check_cancel(cancel)?;
    Ok(rows)
}

/// The semijoin for a predicate `complete_exists_semijoin` accepts: each
/// EXISTS conjunct filters `rows` by its hashed correlation keys, and the
/// conjuncts without a subquery are returned for the caller to evaluate.
fn exists_semijoin_write<T>(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    where_clause: &Option<Expr>,
    ctx: &CorrelationCtx,
    rows: &mut Vec<T>,
    row_values: impl Fn(&T) -> &[Value],
) -> Result<Option<Expr>> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let where_clause = match where_clause {
        Some(w) => w,
        None => return Ok(None),
    };
    let conjuncts = flatten_and_exprs(where_clause);
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();

    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, negated } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema = resolve_inner_schema_write(
                        wtx,
                        schema,
                        &subquery.from.to_ascii_lowercase(),
                    )?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let inner_rows =
                        decorrelate_exists_write(wtx, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    retain_cancellable(rows, cancel, |item| {
                        let key = values_at(row_values(item), &outer_col_indices);
                        Ok(inner_rows.contains(&key, cancel)? != is_negated)
                    })?;
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            _ => remaining_conjuncts.push(conj.clone()),
        }
    }

    check_cancel(cancel)?;
    if remaining_conjuncts.is_empty() {
        Ok(None)
    } else {
        let mut combined = remaining_conjuncts.remove(0);
        for r in remaining_conjuncts {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Ok(Some(combined))
    }
}

/// Check if a WHERE clause has any correlated subquery (top-level AND conjuncts).
pub(super) fn has_correlated_where(
    where_clause: &Option<Expr>,
    ctx: &CorrelationCtx,
    schema: &SchemaManager,
) -> bool {
    let w = match where_clause {
        Some(w) => w,
        None => return false,
    };
    let conjuncts = flatten_and_exprs(w);
    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, .. } | Expr::InSubquery { subquery, .. }
                if is_correlated_subquery(subquery, ctx, schema) =>
            {
                return true;
            }
            Expr::BinaryOp { left, right, .. } => {
                if let Expr::ScalarSubquery(sub) = left.as_ref() {
                    if is_correlated_subquery(sub, ctx, schema) {
                        return true;
                    }
                }
                if let Expr::ScalarSubquery(sub) = right.as_ref() {
                    if is_correlated_subquery(sub, ctx, schema) {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

/// Decorrelate + partial-decode scan: only fully decode rows matching correlation.
pub(super) fn build_and_scan_correlated_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    outer_schema: &TableSchema,
    ctx: &CorrelationCtx,
) -> Result<(Vec<Vec<Value>>, Option<Expr>)> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => {
            let (rows, _) = super::collect_rows_with_read(rtx, outer_schema, &None, None)?;
            return Ok((rows, None));
        }
    };

    let conjuncts = flatten_and_exprs(where_clause);
    let mut exists_filters: Vec<ExistsFilter> = Vec::new();
    let mut in_filters: Vec<InFilter> = Vec::new();
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();
    let outer_col_map = ColumnMap::new(&outer_schema.columns);

    for conj in &conjuncts {
        match conj {
            Expr::Exists { subquery, negated }
                if hashable_exists(subquery) && is_correlated_subquery(subquery, ctx, schema) =>
            {
                let inner_schema = resolve_inner_schema_with_read(
                    rtx,
                    schema,
                    &subquery.from.to_ascii_lowercase(),
                )?;
                let (corr_pairs, _) = extract_correlation_predicates(
                    subquery
                        .where_clause
                        .as_ref()
                        .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                    ctx,
                    &inner_schema,
                    subquery.from_alias.as_deref(),
                );
                if corr_pairs.is_empty() {
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let result = decorrelate_exists_with_read(rtx, schema, subquery, &corr_pairs, ctx)?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                exists_filters.push(ExistsFilter {
                    result,
                    outer_col_indices,
                    negated: *negated,
                });
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } if is_correlated_subquery(subquery, ctx, schema) => {
                let inner_schema = resolve_inner_schema_with_read(
                    rtx,
                    schema,
                    &subquery.from.to_ascii_lowercase(),
                )?;
                let (corr_pairs, _) = extract_correlation_predicates(
                    subquery
                        .where_clause
                        .as_ref()
                        .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                    ctx,
                    &inner_schema,
                    subquery.from_alias.as_deref(),
                );
                if corr_pairs.is_empty()
                    || !hashable_in(schema, subquery, &inner_schema)
                    || has_residual_correlation(subquery, ctx, &inner_schema)
                {
                    remaining_conjuncts.push((*conj).clone());
                    continue;
                }
                let selected_collation = in_subquery_value_collation(subquery, &inner_schema)?;
                let value_collation = crate::eval::operand_collation(expr, &outer_col_map)
                    .unwrap_or(selected_collation);
                let rows = decorrelate_in_with_read(
                    rtx,
                    schema,
                    subquery,
                    &corr_pairs,
                    ctx,
                    value_collation,
                )?;
                let outer_col_indices: Vec<usize> =
                    corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                in_filters.push(InFilter {
                    rows,
                    outer_col_indices,
                    in_expr: (**expr).clone(),
                    negated: *negated,
                });
            }
            _ => remaining_conjuncts.push((*conj).clone()),
        }
    }

    // If no optimizable filters, fall back to generic path
    if exists_filters.is_empty() && in_filters.is_empty() {
        let (mut rows, _) = super::collect_rows_with_read(rtx, outer_schema, &None, None)?;
        let remaining = handle_correlated_where_with_read(rtx, schema, stmt, ctx, &mut rows)?;
        return Ok((rows, remaining));
    }

    let lower = &outer_schema.name;
    let num_pk_cols = outer_schema.primary_key_columns.len();
    let non_pk = outer_schema.non_pk_indices();
    let enc_pos = outer_schema.encoding_positions();
    // Pre-compute how to extract each needed outer column from raw bytes
    let mut needed_raw: Vec<(usize, RawColTarget)> = Vec::new();
    for ef in &exists_filters {
        for &oci in &ef.outer_col_indices {
            if !needed_raw.iter().any(|(idx, _)| *idx == oci) {
                needed_raw.push((oci, raw_col_target(oci, outer_schema, non_pk, enc_pos)));
            }
        }
    }
    for inf in &in_filters {
        for &oci in &inf.outer_col_indices {
            if !needed_raw.iter().any(|(idx, _)| *idx == oci) {
                needed_raw.push((oci, raw_col_target(oci, outer_schema, non_pk, enc_pos)));
            }
        }
    }

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut scan_err: Option<SqlError> = None;

    let mut col_vals: Vec<(usize, Value)> = Vec::with_capacity(needed_raw.len());
    let key_capacity = exists_filters
        .iter()
        .map(|filter| filter.outer_col_indices.len())
        .chain(
            in_filters
                .iter()
                .map(|filter| filter.outer_col_indices.len()),
        )
        .max()
        .unwrap_or(0);
    let mut corr_key = Vec::with_capacity(key_capacity + usize::from(!in_filters.is_empty()));

    rtx.table_scan_raw(lower.as_bytes(), |key, value| {
        // Extract only the correlation columns from raw bytes (fast partial decode)
        col_vals.clear();
        for &(col_idx, ref target) in &needed_raw {
            let val = match extract_raw_value(key, value, target, num_pk_cols) {
                Ok(v) => v,
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            };
            col_vals.push((col_idx, val));
        }
        // A filter decodes the whole row only when it has to read more than
        // the correlation values.
        let mut decoded_row: Option<Vec<Value>> = None;
        let passes = (|| -> Result<bool> {
            for ef in &exists_filters {
                partial_values_into(&col_vals, &ef.outer_col_indices, &mut corr_key);
                let found = if ef.result.reads_outer_row() {
                    let candidates = ef.result.rows.matching(&corr_key, cancel)?;
                    !candidates.is_empty() && {
                        let row = decode_once(&mut decoded_row, outer_schema, key, value, cancel)?;
                        ef.result
                            .satisfied_by(&candidates, row, &outer_col_map, ctx, cancel)?
                    }
                } else {
                    ef.result.rows.contains(&corr_key, cancel)?
                };
                if ef.negated == found {
                    return Ok(false);
                }
            }
            for inf in &in_filters {
                partial_values_into(&col_vals, &inf.outer_col_indices, &mut corr_key);
                let passes = inf.rows.passes(&mut corr_key, inf.negated, cancel, || {
                    let row = decode_once(&mut decoded_row, outer_schema, key, value, cancel)?;
                    eval_expr(
                        &inf.in_expr,
                        &EvalCtx::new(&outer_col_map, row).with_cancel(cancel),
                    )
                })?;
                if !passes {
                    return Ok(false);
                }
            }
            Ok(true)
        })();
        match passes {
            Ok(false) => true,
            Ok(true) => {
                // Reuse a full decode a filter performed, or decode once now for
                // the output.
                let row = match decoded_row {
                    Some(row) => row,
                    None => match decode_full_row_with_cancel(outer_schema, key, value, cancel) {
                        Ok(row) => row,
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                };
                rows.push(row);
                true
            }
            Err(e) => {
                scan_err = Some(e);
                false
            }
        }
    })
    .map_err(SqlError::Storage)?;

    if let Some(e) = scan_err {
        return Err(e);
    }

    check_cancel(cancel)?;

    let remaining = if remaining_conjuncts.is_empty() {
        None
    } else {
        Some(
            remaining_conjuncts
                .into_iter()
                .reduce(|a, b| Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinOp::And,
                    right: Box::new(b),
                })
                .unwrap(),
        )
    };
    Ok((rows, remaining))
}

enum RawColTarget {
    Pk(usize),    // PK position
    NonPk(usize), // Physical encoding position
}

fn raw_col_target(
    col_idx: usize,
    schema: &TableSchema,
    non_pk: &[usize],
    enc_pos: &[u16],
) -> RawColTarget {
    if let Some(pk_pos) = schema
        .primary_key_columns
        .iter()
        .position(|&c| c as usize == col_idx)
    {
        RawColTarget::Pk(pk_pos)
    } else {
        let nonpk_order = non_pk.iter().position(|&i| i == col_idx).unwrap();
        RawColTarget::NonPk(enc_pos[nonpk_order] as usize)
    }
}

fn extract_raw_value(
    key: &[u8],
    value: &[u8],
    target: &RawColTarget,
    num_pk_cols: usize,
) -> Result<Value> {
    match target {
        RawColTarget::Pk(pk_pos) => {
            if num_pk_cols == 1 && *pk_pos == 0 {
                Ok(Value::Integer(decode_pk_integer(key)?))
            } else {
                let pk = decode_composite_key(key, num_pk_cols)?;
                Ok(pk[*pk_pos].clone())
            }
        }
        RawColTarget::NonPk(idx) => decode_column_raw(value, *idx)?.to_value(),
    }
}

/// The values at `columns` of a row decoded only at its correlation columns.
fn partial_values_into(decoded: &[(usize, Value)], columns: &[usize], key: &mut Vec<Value>) {
    key.clear();
    key.extend(columns.iter().map(|&column| {
        decoded
            .iter()
            .find(|(index, _)| *index == column)
            .unwrap()
            .1
            .clone()
    }));
}

/// The whole row at `key`/`value`, decoded on first use.
fn decode_once<'r>(
    decoded: &'r mut Option<Vec<Value>>,
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<&'r [Value]> {
    if decoded.is_none() {
        *decoded = Some(decode_full_row_with_cancel(schema, key, value, cancel)?);
    }
    Ok(decoded.as_deref().unwrap())
}

struct ExistsFilter {
    result: ExistsRows,
    outer_col_indices: Vec<usize>,
    negated: bool,
}

struct InFilter {
    rows: InRows,
    outer_col_indices: Vec<usize>,
    in_expr: Expr,
    negated: bool,
}

pub(super) fn handle_correlated_where_with_read(
    rtx: &mut ReadView<'_, '_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctx: &CorrelationCtx,
    rows: &mut Vec<Vec<Value>>,
) -> Result<Option<Expr>> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    check_cancel(cancel)?;
    let where_clause = match &stmt.where_clause {
        Some(w) => w,
        None => return Ok(None),
    };

    let conjuncts = flatten_and_exprs(where_clause);
    let mut remaining_conjuncts: Vec<Expr> = Vec::new();

    for conj in conjuncts {
        match conj {
            Expr::Exists { subquery, negated } => {
                if hashable_exists(subquery) && is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema = resolve_inner_schema_with_read(
                        rtx,
                        schema,
                        &subquery.from.to_ascii_lowercase(),
                    )?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty() {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let exists =
                        decorrelate_exists_with_read(rtx, schema, subquery, &corr_pairs, ctx)?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let outer_col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    let is_negated = *negated;
                    retain_cancellable(rows, cancel, |outer_row| {
                        let key = values_at(outer_row, &outer_col_indices);
                        let found = if exists.reads_outer_row() {
                            let candidates = exists.rows.matching(&key, cancel)?;
                            exists.satisfied_by(
                                &candidates,
                                outer_row,
                                &outer_col_map,
                                ctx,
                                cancel,
                            )?
                        } else {
                            exists.rows.contains(&key, cancel)?
                        };
                        Ok(found != is_negated)
                    })?;
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            Expr::InSubquery {
                expr: in_expr,
                subquery,
                negated,
            } => {
                if is_correlated_subquery(subquery, ctx, schema) {
                    let inner_schema = resolve_inner_schema_with_read(
                        rtx,
                        schema,
                        &subquery.from.to_ascii_lowercase(),
                    )?;
                    let (corr_pairs, _) = extract_correlation_predicates(
                        subquery
                            .where_clause
                            .as_ref()
                            .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                        ctx,
                        &inner_schema,
                        subquery.from_alias.as_deref(),
                    );
                    if corr_pairs.is_empty()
                        || !hashable_in(schema, subquery, &inner_schema)
                        || has_residual_correlation(subquery, ctx, &inner_schema)
                    {
                        remaining_conjuncts.push(conj.clone());
                        continue;
                    }
                    let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                    let selected_collation = in_subquery_value_collation(subquery, &inner_schema)?;
                    let value_collation = crate::eval::operand_collation(in_expr, &col_map)
                        .unwrap_or(selected_collation);
                    let in_rows = decorrelate_in_with_read(
                        rtx,
                        schema,
                        subquery,
                        &corr_pairs,
                        ctx,
                        value_collation,
                    )?;
                    let outer_col_indices: Vec<usize> =
                        corr_pairs.iter().map(|p| p.outer_col_idx).collect();
                    let is_negated = *negated;
                    let mut key = Vec::with_capacity(outer_col_indices.len() + 1);
                    retain_cancellable(rows, cancel, |row| {
                        key.clear();
                        key.extend(outer_col_indices.iter().map(|&column| row[column].clone()));
                        in_rows.passes(&mut key, is_negated, cancel, || {
                            eval_expr(in_expr, &EvalCtx::new(&col_map, row).with_cancel(cancel))
                        })
                    })?;
                } else {
                    remaining_conjuncts.push(conj.clone());
                }
            }
            _ => {
                // Check for scalar subquery comparisons: col > (SELECT ...)
                let mut handled = false;
                if let Expr::BinaryOp { left, op, right } = conj {
                    if let Expr::ScalarSubquery(sub) = right.as_ref() {
                        let inner_schema = schema
                            .get(&sub.from.to_ascii_lowercase())
                            .filter(|_| is_correlated_subquery(sub, ctx, schema));
                        if let Some(inner_schema) = inner_schema {
                            let (corr_pairs, _) = extract_correlation_predicates(
                                sub.where_clause
                                    .as_ref()
                                    .unwrap_or(&Expr::Literal(Value::Boolean(true))),
                                ctx,
                                inner_schema,
                                sub.from_alias.as_deref(),
                            );
                            let shape = hashable_scalar(sub, inner_schema).filter(|_| {
                                !corr_pairs.is_empty()
                                    && !has_residual_correlation(sub, ctx, inner_schema)
                            });
                            let values = match shape {
                                Some(shape) => {
                                    let by_key = decorrelate_scalar_with_read(
                                        rtx,
                                        schema,
                                        sub,
                                        &corr_pairs,
                                        ctx,
                                        &shape,
                                    )?;
                                    hashed_scalar_values(
                                        &by_key,
                                        rows,
                                        &corr_pairs,
                                        &shape,
                                        cancel,
                                    )?
                                }
                                None => None,
                            };
                            if let Some(values) = values {
                                let col_map = ColumnMap::new(&ctx.outer_schema.columns);
                                // One value for each row, in the order `rows` holds them.
                                let mut position = 0;
                                retain_cancellable(rows, cancel, |row| {
                                    let value = values[position].clone();
                                    position += 1;
                                    // The left operand keeps its column collation.
                                    let cmp_expr = Expr::BinaryOp {
                                        left: left.clone(),
                                        op: *op,
                                        right: Box::new(Expr::Literal(value)),
                                    };
                                    Ok(is_truthy(&eval_expr(
                                        &cmp_expr,
                                        &EvalCtx::new(&col_map, row).with_cancel(cancel),
                                    )?))
                                })?;
                                handled = true;
                            }
                        }
                    }
                }
                if !handled {
                    remaining_conjuncts.push(conj.clone());
                }
            }
        }
    }

    check_cancel(cancel)?;
    if remaining_conjuncts.is_empty() {
        Ok(None)
    } else {
        let mut combined = remaining_conjuncts.remove(0);
        for r in remaining_conjuncts {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinOp::And,
                right: Box::new(r),
            };
        }
        Ok(Some(combined))
    }
}

#[cfg(test)]
#[path = "correlated_tests.rs"]
mod tests;
