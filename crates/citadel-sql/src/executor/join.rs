use citadel_txn::read_txn::ReadTxn;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use std::hash::{Hash, Hasher};

use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, referenced_columns, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::helpers::*;
use super::scan::*;

mod keys;
use keys::ProbeTable;

/// JOIN sources currently support relations and derived tables. Function
/// arguments must not be discarded by resolving only a same-named relation.
pub(super) fn validate_join_sources(stmt: &SelectStmt) -> Result<()> {
    if let Some(join) = stmt.joins.iter().find(|join| join.table.args.is_some()) {
        return Err(SqlError::Unsupported(format!(
            "table function on the right side of JOIN: {}",
            join.table.name
        )));
    }
    Ok(())
}

/// Amortize cancellation loads across CPU-only join work: building a probe map
/// and expanding matches can outlast the scan that fed them.
const JOIN_CANCEL_INTERVAL: usize = 256;
type IntegerJoinAttempt = std::result::Result<Vec<Vec<Value>>, Vec<Vec<Value>>>;

struct JoinCancel<'a> {
    token: Option<&'a citadel::CancelToken>,
    until_check: usize,
}

impl<'a> JoinCancel<'a> {
    fn new(token: Option<&'a citadel::CancelToken>) -> Result<Self> {
        let guard = Self {
            token,
            until_check: JOIN_CANCEL_INTERVAL,
        };
        guard.check()?;
        Ok(guard)
    }

    #[inline]
    fn work(&mut self) -> Result<()> {
        self.until_check -= 1;
        if self.until_check == 0 {
            self.until_check = JOIN_CANCEL_INTERVAL;
            self.check()?;
        }
        Ok(())
    }

    #[inline]
    fn check(&self) -> Result<()> {
        match self.token {
            Some(token) => token.check().map_err(SqlError::Storage),
            None => Ok(()),
        }
    }
}

pub(super) fn resolve_table_name<'a>(
    schema: &'a SchemaManager,
    name: &str,
) -> Result<&'a TableSchema> {
    schema
        .get(name)
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))
}

pub(super) fn build_joined_columns(tables: &[(String, &TableSchema)]) -> Vec<ColumnDef> {
    let total: usize = tables.iter().map(|(_, s)| s.columns.len()).sum();
    let mut result = Vec::with_capacity(total);
    for entry in tables {
        extend_joined_columns(&mut result, entry);
    }
    result
}

pub(super) fn extend_joined_columns(out: &mut Vec<ColumnDef>, table: &(String, &TableSchema)) {
    let (alias, schema) = table;
    out.reserve(schema.columns.len());
    let alias_lc = alias.to_ascii_lowercase();
    for (pos, col) in (out.len() as u16..).zip(schema.columns.iter()) {
        let mut name = String::with_capacity(alias_lc.len() + 1 + col.name.len());
        name.push_str(&alias_lc);
        name.push('.');
        name.push_str(&col.name);
        out.push(ColumnDef {
            name,
            data_type: col.data_type,
            nullable: col.nullable,
            position: pos,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
            is_with_timezone: false,
            generated_expr: None,
            generated_sql: None,
            generated_kind: None,
            collation: col.collation,
        });
    }
}

pub(super) fn extract_equi_join_keys(
    on_expr: &Expr,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
) -> Vec<KeyPair> {
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
                    pairs.push(KeyPair {
                        outer: l_idx,
                        inner: r_idx - outer_col_count,
                        left_is_outer: true,
                    });
                } else if r_idx < outer_col_count && l_idx >= outer_col_count {
                    pairs.push(KeyPair {
                        outer: r_idx,
                        inner: l_idx - outer_col_count,
                        left_is_outer: false,
                    });
                }
            }
        }
    }

    pairs
}

pub(super) fn resolve_col_idx(expr: &Expr, columns: &[ColumnDef]) -> Option<usize> {
    match expr {
        Expr::Column(name) => {
            let mut found: Option<usize> = None;
            for (i, c) in columns.iter().enumerate() {
                let matches = c.name == *name
                    || (c.name.len() > name.len()
                        && c.name.as_bytes()[c.name.len() - name.len() - 1] == b'.'
                        && c.name.ends_with(name.as_str()));
                if matches {
                    if found.is_some() {
                        return None;
                    }
                    found = Some(i);
                }
            }
            found
        }
        Expr::QualifiedColumn { table, column } => {
            let total_len = table.len() + 1 + column.len();
            for (i, c) in columns.iter().enumerate() {
                if c.name.len() == total_len
                    && c.name.as_bytes()[table.len()] == b'.'
                    && c.name.starts_with(table.as_str())
                    && c.name.ends_with(column.as_str())
                {
                    return Some(i);
                }
            }
            None
        }
        _ => None,
    }
}

/// Candidate hash only; bucket members still require equality verification.
pub(super) fn join_key_hash(
    row: &[Value],
    col_indices: &[usize],
    key_colls: &[crate::types::Collation],
) -> u64 {
    let mut state = FxHasher::default();
    col_indices.len().hash(&mut state);
    for (k, &i) in col_indices.iter().enumerate() {
        hash_join_value(
            &row[i],
            key_colls.get(k).copied().unwrap_or_default(),
            &mut state,
        );
    }
    state.finish()
}

fn hash_join_value(value: &Value, collation: Collation, state: &mut FxHasher) {
    if let Value::Interval {
        months,
        days,
        micros,
    } = value
    {
        8u8.hash(state);
        crate::datetime::interval_to_total_micros(*months, *days, *micros).hash(state);
    } else {
        match collation {
            Collation::NoCase | Collation::Rtrim => collation.fold(value.clone()).hash(state),
            Collation::Binary => value.hash(state),
        }
    }
}

/// The collation an equi-join key compares under: the syntactic left operand's, including
/// BINARY. An explicit COLLATE is not a bare column pair and never reaches this lane.
pub(super) fn equi_key_collations(
    pairs: &[KeyPair],
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
) -> Vec<crate::types::Collation> {
    pairs
        .iter()
        .map(|pair| {
            if pair.left_is_outer {
                combined_cols[pair.outer].collation
            } else {
                combined_cols[outer_col_count + pair.inner].collation
            }
        })
        .collect()
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

/// The equality keys of one join step: which columns pair up, whether the ON clause is
/// nothing but those equalities, and the collation each pair compares under.
pub(super) struct EquiJoin {
    pairs: Vec<KeyPair>,
    /// Every ON conjunct is one of `pairs`; otherwise evaluate the full ON expression.
    pure: bool,
    key_colls: Vec<crate::types::Collation>,
}

/// One `outer.x = inner.y` of a join condition, as positions in the combined row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct KeyPair {
    pub(super) outer: usize,
    pub(super) inner: usize,
    /// Hashing always stores outer/inner positions, so retain which one was written on the
    /// left of `=`: SQL collation precedence is syntactic, not join-order precedence.
    pub(super) left_is_outer: bool,
}

impl EquiJoin {
    fn new(
        pairs: Vec<KeyPair>,
        pure: bool,
        combined_cols: &[ColumnDef],
        outer_col_count: usize,
    ) -> Self {
        let key_colls = equi_key_collations(&pairs, combined_cols, outer_col_count);
        Self {
            pairs,
            pure,
            key_colls,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.pairs.len()
    }

    pub(super) fn is_pure(&self) -> bool {
        self.pure
    }

    /// The single pair of a one-key join, which the integer lanes specialize on.
    fn only_pair(&self) -> KeyPair {
        self.pairs[0]
    }

    fn inner_cols(&self) -> Vec<usize> {
        self.pairs.iter().map(|pair| pair.inner).collect()
    }

    fn keys_match(&self, outer: &[Value], inner: &[Value]) -> Result<bool> {
        for (pair, &collation) in self.pairs.iter().zip(&self.key_colls) {
            if !crate::eval::collated_eq(&outer[pair.outer], &inner[pair.inner], Some(collation))? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn has_integer_keys(
    rows: &[Vec<Value>],
    column: usize,
    cancel: &mut JoinCancel<'_>,
) -> Result<bool> {
    if cancel.token.is_none() {
        return Ok(rows
            .iter()
            .all(|row| matches!(row[column], Value::Integer(_) | Value::Null)));
    }
    for row in rows {
        cancel.work()?;
        if !matches!(row[column], Value::Integer(_) | Value::Null) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn compute_equi_join_meta(
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
) -> EquiJoin {
    let pairs = join
        .on_clause
        .as_ref()
        .map(|on| extract_equi_join_keys(on, combined_cols, outer_col_count))
        .unwrap_or_default();
    let pure = join
        .on_clause
        .as_ref()
        .is_none_or(|on| !pairs.is_empty() && count_conjuncts(on) == pairs.len());
    EquiJoin::new(pairs, pure, combined_cols, outer_col_count)
}

pub(super) fn combine_row(outer: &[Value], inner: &[Value], cap: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(cap);
    combined.extend(outer.iter().cloned());
    combined.extend(inner.iter().cloned());
    combined
}

pub(super) struct CombineProjection {
    slots: Vec<(usize, bool)>,
    unique_sources: bool,
    outer_prefix: Option<usize>,
}

impl CombineProjection {
    fn take_row(&self, outer: &mut [Value], inner: &mut [Value]) -> Vec<Value> {
        if !self.unique_sources {
            return combine_row_projected(outer, inner, self);
        }
        self.slots
            .iter()
            .map(|&(index, is_inner)| {
                std::mem::take(if is_inner {
                    &mut inner[index]
                } else {
                    &mut outer[index]
                })
            })
            .collect()
    }

    fn finish_outer(&self, mut outer: Vec<Value>, inner: Option<&[Value]>) -> Vec<Value> {
        if self.slots.is_empty() {
            return Vec::new();
        }
        if let Some(prefix) = self.outer_prefix {
            if (self.slots.len()..=self.slots.len().saturating_mul(2)).contains(&outer.capacity()) {
                for destination in 0..prefix {
                    let source = self.slots[destination].0;
                    if destination != source {
                        let value = std::mem::take(&mut outer[source]);
                        outer[destination] = value;
                    }
                }
                outer.truncate(prefix);
                for &(index, _) in &self.slots[prefix..] {
                    outer.push(inner.map_or(Value::Null, |row| row[index].clone()));
                }
                return outer;
            }
        }
        self.slots
            .iter()
            .map(|&(index, is_inner)| {
                if is_inner {
                    inner.map_or(Value::Null, |row| row[index].clone())
                } else if self.unique_sources {
                    std::mem::take(&mut outer[index])
                } else {
                    outer[index].clone()
                }
            })
            .collect()
    }

    fn unmatched_inner(&self, inner: &[Value]) -> Vec<Value> {
        self.slots
            .iter()
            .map(|&(index, is_inner)| {
                if is_inner {
                    inner[index].clone()
                } else {
                    Value::Null
                }
            })
            .collect()
    }
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
    let mut seen = FxHashSet::default();
    let slots: Vec<_> = needed_combined
        .iter()
        .map(|&column| {
            if column < outer_col_count {
                (column, false)
            } else {
                (column - outer_col_count, true)
            }
        })
        .collect();
    let prefix = slots.iter().take_while(|&&(_, inner)| !inner).count();
    let outer_prefix = (slots[prefix..].iter().all(|&(_, inner)| inner)
        && slots[..prefix].windows(2).all(|pair| pair[0].0 < pair[1].0))
    .then_some(prefix);
    CombineProjection {
        unique_sources: needed_combined.iter().all(|&column| seen.insert(column)),
        slots,
        outer_prefix,
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
                is_with_timezone: false,
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                collation: orig.collation,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn try_integer_join(
    outer_rows: Vec<Vec<Value>>,
    inner_rows: &mut [Vec<Value>],
    join_type: &JoinType,
    outer_key_col: usize,
    inner_key_col: usize,
    outer_col_count: usize,
    inner_col_count: usize,
    outer_is_sorted: bool,
    projection: Option<&CombineProjection>,
    cancel: &mut JoinCancel<'_>,
) -> Result<IntegerJoinAttempt> {
    let cap = projection.map_or(outer_col_count + inner_col_count, |p| p.slots.len());

    if outer_is_sorted && matches!(join_type, JoinType::Inner | JoinType::Cross) {
        let mut prev = i64::MIN;
        let mut sorted = true;
        let mut has_null = false;
        for r in inner_rows.iter() {
            cancel.work()?;
            match r[inner_key_col] {
                Value::Integer(k) => {
                    if k < prev {
                        sorted = false;
                    }
                    prev = k;
                }
                Value::Null => {
                    has_null = true;
                }
                _ => return Ok(Err(outer_rows)),
            }
        }

        let mut result = Vec::with_capacity(outer_rows.len());

        if sorted && !has_null {
            // mem::take leaves empty Vecs at consumed positions, so
            // `j` must move past them before the next outer revisits.
            let key_at = |idx: usize, inner_rows: &[Vec<Value>]| -> i64 {
                match inner_rows[idx][inner_key_col] {
                    Value::Integer(k) => k,
                    _ => unreachable!(),
                }
            };
            let mut j = 0;
            for mut outer in outer_rows {
                cancel.work()?;
                let ok = match outer[outer_key_col] {
                    Value::Integer(i) => i,
                    _ => continue,
                };
                while j < inner_rows.len() && key_at(j, inner_rows) < ok {
                    cancel.work()?;
                    j += 1;
                }
                let mut kk = j;
                while kk < inner_rows.len() && key_at(kk, inner_rows) == ok {
                    cancel.work()?;
                    let is_last = kk + 1 >= inner_rows.len() || key_at(kk + 1, inner_rows) != ok;
                    if let Some(proj) = projection {
                        if is_last {
                            let mut inner = std::mem::take(&mut inner_rows[kk]);
                            result.push(proj.take_row(&mut outer, &mut inner));
                        } else {
                            let inner = &inner_rows[kk];
                            result.push(combine_row_projected(&outer, inner, proj));
                        }
                    } else if is_last {
                        let inner = std::mem::take(&mut inner_rows[kk]);
                        outer.extend(inner);
                        result.push(outer);
                        kk += 1;
                        break;
                    } else {
                        let inner = &inner_rows[kk];
                        result.push(combine_row(&outer, inner, cap));
                    }
                    kk += 1;
                }
                j = kk;
            }
            cancel.check()?;
            return Ok(Ok(result));
        }

        let mut aux: Vec<(i64, usize)> = Vec::with_capacity(inner_rows.len());
        for (i, r) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if let Value::Integer(k) = r[inner_key_col] {
                aux.push((k, i));
            }
        }
        if !sorted {
            aux = sort_vec_unstable_by(aux, cancel.token, |a, b| a.0.cmp(&b.0))?;
        }

        let mut j = 0;
        for mut outer in outer_rows {
            cancel.work()?;
            let ok = match outer[outer_key_col] {
                Value::Integer(i) => i,
                _ => continue,
            };
            while j < aux.len() && aux[j].0 < ok {
                cancel.work()?;
                j += 1;
            }
            let mut kk = j;
            while kk < aux.len() && aux[kk].0 == ok {
                cancel.work()?;
                let is_last = kk + 1 >= aux.len() || aux[kk + 1].0 != ok;
                let inner_idx = aux[kk].1;
                if let Some(proj) = projection {
                    if is_last {
                        let mut inner = std::mem::take(&mut inner_rows[inner_idx]);
                        result.push(proj.take_row(&mut outer, &mut inner));
                    } else {
                        let inner = &inner_rows[inner_idx];
                        result.push(combine_row_projected(&outer, inner, proj));
                    }
                } else if is_last {
                    let inner = std::mem::take(&mut inner_rows[inner_idx]);
                    outer.extend(inner);
                    result.push(outer);
                    break;
                } else {
                    let inner = &inner_rows[inner_idx];
                    result.push(combine_row(&outer, inner, cap));
                }
                kk += 1;
            }
        }
        cancel.check()?;
        return Ok(Ok(result));
    }

    let mut inner_map: FxHashMap<i64, Vec<usize>> =
        FxHashMap::with_capacity_and_hasher(inner_rows.len(), Default::default());
    for (idx, inner) in inner_rows.iter().enumerate() {
        cancel.work()?;
        match &inner[inner_key_col] {
            Value::Integer(k) => inner_map.entry(*k).or_default().push(idx),
            Value::Null => {}
            _ => return Ok(Err(outer_rows)),
        }
    }

    let mut result = Vec::with_capacity(inner_rows.len());

    match join_type {
        JoinType::Inner | JoinType::Cross => {
            for mut outer in outer_rows {
                cancel.work()?;
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                cancel.work()?;
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                cancel.work()?;
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
                cancel.work()?;
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                cancel.work()?;
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                cancel.work()?;
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
                cancel.work()?;
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        if let Some(proj) = projection {
                            for &idx in indices {
                                cancel.work()?;
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                cancel.work()?;
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
                cancel.work()?;
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
        JoinType::FullOuter => {
            let mut inner_matched = vec![false; inner_rows.len()];
            for mut outer in outer_rows {
                cancel.work()?;
                let mut matched = false;
                if let Value::Integer(k) = outer[outer_key_col] {
                    if let Some(indices) = inner_map.get(&k) {
                        matched = true;
                        if let Some(proj) = projection {
                            for &idx in indices {
                                cancel.work()?;
                                result.push(combine_row_projected(&outer, &inner_rows[idx], proj));
                                inner_matched[idx] = true;
                            }
                        } else {
                            for &idx in &indices[..indices.len() - 1] {
                                cancel.work()?;
                                result.push(combine_row(&outer, &inner_rows[idx], cap));
                                inner_matched[idx] = true;
                            }
                            let last_idx = *indices.last().unwrap();
                            inner_matched[last_idx] = true;
                            outer.extend(inner_rows[last_idx].iter().cloned());
                            result.push(outer);
                            continue;
                        }
                    }
                }
                if !matched {
                    if let Some(proj) = projection {
                        let null_inner = vec![Value::Null; inner_col_count];
                        result.push(combine_row_projected(&outer, &null_inner, proj));
                    } else {
                        outer.resize(cap, Value::Null);
                        result.push(outer);
                    }
                }
            }
            for (j, inner) in inner_rows.iter().enumerate() {
                cancel.work()?;
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

    cancel.check()?;
    Ok(Ok(result))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_join_step(
    mut outer_rows: Vec<Vec<Value>>,
    inner_rows: &mut [Vec<Value>],
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
    inner_col_count: usize,
    outer_pk_col: Option<usize>,
    projection: Option<&CombineProjection>,
    equi: &EquiJoin,
    token: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let mut cancel = JoinCancel::new(token)?;
    let effective_proj = if equi.is_pure() { projection } else { None };

    if equi.len() == 1
        && equi.is_pure()
        && has_integer_keys(&outer_rows, equi.only_pair().outer, &mut cancel)?
    {
        let pair = equi.only_pair();
        let outer_is_sorted = outer_pk_col == Some(pair.outer);
        match try_integer_join(
            outer_rows,
            inner_rows,
            &join.join_type,
            pair.outer,
            pair.inner,
            outer_col_count,
            inner_col_count,
            outer_is_sorted,
            effective_proj,
            &mut cancel,
        )? {
            Ok(result) => {
                cancel.check()?;
                return Ok(result);
            }
            Err(rows) => outer_rows = rows,
        }
    }

    let buckets = ProbeTable::build(inner_rows, equi, &mut cancel)?;
    exec_hash_join(
        outer_rows,
        inner_rows,
        join,
        combined_cols,
        outer_col_count,
        inner_col_count,
        effective_proj,
        equi,
        &buckets,
        &mut cancel,
    )
}

#[allow(clippy::too_many_arguments)]
fn exec_hash_join(
    outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
    inner_col_count: usize,
    projection: Option<&CombineProjection>,
    equi: &EquiJoin,
    buckets: &ProbeTable,
    cancel: &mut JoinCancel<'_>,
) -> Result<Vec<Vec<Value>>> {
    let preserve_outer = matches!(join.join_type, JoinType::Left | JoinType::FullOuter);
    let preserve_inner = matches!(join.join_type, JoinType::Right | JoinType::FullOuter);
    let mut inner_matched = if preserve_inner {
        vec![false; inner_rows.len()]
    } else {
        Vec::new()
    };
    let cap = outer_col_count + inner_col_count;
    let combined_map = ColumnMap::new(combined_cols);
    let capacity_hint = outer_rows.len();
    let mut result = Vec::new();
    let mut emit = |row| {
        if result.capacity() == 0 {
            result.reserve_exact(capacity_hint);
        }
        result.push(row);
    };

    for mut outer in outer_rows {
        cancel.work()?;
        let mut matched = false;
        let mut pending = None;
        let comparison;
        let indices = match buckets.cached_candidates(&outer, equi) {
            Some(indices) => indices,
            None => {
                comparison = buckets.comparison_candidates(&outer, equi, inner_rows, cancel)?;
                comparison.as_ref()
            }
        };
        for &index in indices {
            cancel.work()?;
            let inner = &inner_rows[index];
            if equi.is_pure() {
                if !equi.keys_match(&outer, inner)? {
                    continue;
                }
                if let Some(previous) = pending.replace(index) {
                    let inner = &inner_rows[previous];
                    emit(if let Some(projection) = projection {
                        combine_row_projected(&outer, inner, projection)
                    } else {
                        combine_row(&outer, inner, cap)
                    });
                }
            } else {
                let combined = combine_row(&outer, inner, cap);
                if let Some(on) = &join.on_clause {
                    if !is_truthy(&eval_expr(
                        on,
                        &EvalCtx::new(&combined_map, &combined).with_cancel(cancel.token),
                    )?) {
                        continue;
                    }
                }
                emit(combined);
            }
            matched = true;
            if preserve_inner {
                inner_matched[index] = true;
            }
        }
        if let Some(last) = pending {
            if let Some(projection) = projection {
                emit(projection.finish_outer(outer, Some(&inner_rows[last])));
            } else {
                outer.extend(inner_rows[last].iter().cloned());
                emit(outer);
            }
        } else if !matched && preserve_outer {
            if let Some(projection) = projection {
                emit(projection.finish_outer(outer, None));
            } else {
                outer.resize(cap, Value::Null);
                emit(outer);
            }
        }
    }

    if preserve_inner {
        for (index, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if !inner_matched[index] {
                if let Some(projection) = projection {
                    emit(projection.unmatched_inner(inner));
                } else {
                    let mut padded = Vec::with_capacity(cap);
                    padded.resize(outer_col_count, Value::Null);
                    padded.extend(inner.iter().cloned());
                    emit(padded);
                }
            }
        }
    }
    cancel.check()?;
    Ok(result)
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
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let lower_name = &table_schema.name;
    let entry_count = rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0) as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match decode_full_row_with_cancel(table_schema, key, value, cancel) {
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
            let lower_bytes = lower.as_bytes();
            columns
                .iter()
                .filter(|c| {
                    c.name == lower
                        || (c.name.len() > lower.len()
                            && c.name.as_bytes()[c.name.len() - lower.len() - 1] == b'.'
                            && c.name.as_bytes().ends_with(lower_bytes))
                })
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
        Expr::IsDistinctFrom { left, right, .. } => {
            has_ambiguous_bare_ref(left, columns) || has_ambiguous_bare_ref(right, columns)
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
    pub(super) per_table: Vec<Vec<usize>>,
    pub(super) output_combined: Vec<usize>,
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
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    if needed.len() == table_schema.columns.len() {
        return collect_all_rows_raw(rtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new_with_cancel(table_schema, needed, cancel)?;
    collect_rows_partial_with_ctx(rtx, table_schema, &ctx, None, cancel)
}

pub(super) fn collect_rows_partial_with_ctx(
    rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    table_schema: &TableSchema,
    ctx: &PartialDecodeCtx,
    cached_count: Option<&std::sync::OnceLock<u64>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let lower_name = &table_schema.name;
    let entry_count = match cached_count {
        Some(cell) => {
            *cell.get_or_init(|| rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0))
        }
        None => rtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0),
    } as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    rtx.table_scan_raw(lower_name.as_bytes(), |key, value| {
        match ctx.decode_with_cancel(key, value, cancel) {
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    if needed.len() == table_schema.columns.len() {
        return collect_all_rows_write(wtx, table_schema);
    }
    let ctx = PartialDecodeCtx::new_with_cancel(table_schema, needed, cancel)?;
    collect_rows_partial_write_with_ctx(wtx, table_schema, &ctx, None, cancel)
}

pub(super) fn collect_rows_partial_write_with_ctx(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_schema: &TableSchema,
    ctx: &PartialDecodeCtx,
    cached_count: Option<&std::sync::OnceLock<u64>>,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let lower_name = &table_schema.name;
    let entry_count = match cached_count {
        Some(cell) => {
            *cell.get_or_init(|| wtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0))
        }
        None => wtx.table_entry_count(lower_name.as_bytes()).unwrap_or(0),
    } as usize;
    let mut rows = Vec::with_capacity(entry_count);
    let mut scan_err: Option<SqlError> = None;
    wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
        match ctx.decode_with_cancel(key, value, cancel) {
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

pub(super) fn exec_select_join_with_read(
    rtx: &mut ReadTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let cancel = rtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = Vec::with_capacity(stmt.joins.len() + 1);
    all_tables.push((from_alias, from_schema));
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
        Some(n) if !n.is_empty() => collect_rows_partial(rtx, from_schema, &n[0])?,
        _ => collect_all_rows_raw(rtx, from_schema)?,
    };

    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    let mut combined_cols: Vec<ColumnDef> = build_joined_columns(&all_tables[..1]);
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = all_tables[ji + 1].1;
        let mut inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => collect_rows_partial(rtx, inner_schema, &n[ji + 1])?,
            _ => collect_all_rows_raw(rtx, inner_schema)?,
        };

        extend_joined_columns(&mut combined_cols, &all_tables[ji + 1]);

        let outer_col_count = if outer_rows.is_empty() {
            all_tables[..ji + 1]
                .iter()
                .map(|(_, s)| s.columns.len())
                .sum()
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

        let equi = compute_equi_join_meta(join, &combined_cols, outer_col_count);
        outer_rows = exec_join_step(
            outer_rows,
            &mut inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
            &equi,
            cancel,
        )?;
        cur_outer_pk_col = None;
    }

    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&combined_cols, oc);
            return super::process_select(
                outer_rows,
                super::SelectCtx::new(&projected_cols, stmt, cancel),
            );
        }
    }
    super::process_select(
        outer_rows,
        super::SelectCtx::new(&combined_cols, stmt, cancel),
    )
}

pub(super) fn exec_select_join_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
) -> Result<ExecutionResult> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let from_schema = resolve_table_name(schema, &stmt.from)?;
    let from_alias = table_alias_or_name(&stmt.from, &stmt.from_alias);

    let mut all_tables: Vec<(String, &TableSchema)> = Vec::with_capacity(stmt.joins.len() + 1);
    all_tables.push((from_alias, from_schema));
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

    let mut cur_outer_pk_col: Option<usize> = if from_schema.primary_key_columns.len() == 1 {
        Some(from_schema.primary_key_columns[0] as usize)
    } else {
        None
    };

    let num_joins = stmt.joins.len();
    let mut combined_cols: Vec<ColumnDef> = build_joined_columns(&all_tables[..1]);
    for (ji, join) in stmt.joins.iter().enumerate() {
        let inner_schema = all_tables[ji + 1].1;
        let mut inner_rows = match &needed_per_table {
            Some(n) if ji + 1 < n.len() => {
                collect_rows_partial_write(wtx, inner_schema, &n[ji + 1])?
            }
            _ => collect_all_rows_write(wtx, inner_schema)?,
        };

        extend_joined_columns(&mut combined_cols, &all_tables[ji + 1]);

        let outer_col_count = if outer_rows.is_empty() {
            all_tables[..ji + 1]
                .iter()
                .map(|(_, s)| s.columns.len())
                .sum()
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

        let equi = compute_equi_join_meta(join, &combined_cols, outer_col_count);
        outer_rows = exec_join_step(
            outer_rows,
            &mut inner_rows,
            join,
            &combined_cols,
            outer_col_count,
            inner_col_count,
            cur_outer_pk_col,
            proj.as_ref(),
            &equi,
            cancel,
        )?;
        cur_outer_pk_col = None;
    }

    if let Some(ref oc) = output_combined {
        let actual_width = outer_rows.first().map_or(0, |r| r.len());
        if actual_width == oc.len() {
            let projected_cols = build_projected_columns(&combined_cols, oc);
            return super::process_select(
                outer_rows,
                super::SelectCtx::new(&projected_cols, stmt, cancel),
            );
        }
    }
    super::process_select(
        outer_rows,
        super::SelectCtx::new(&combined_cols, stmt, cancel),
    )
}

#[allow(clippy::too_many_arguments)]
fn try_integer_join_borrowed(
    outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join_type: &JoinType,
    outer_key_col: usize,
    inner_key_col: usize,
    outer_col_count: usize,
    inner_col_count: usize,
    outer_is_sorted: bool,
    projection: Option<&CombineProjection>,
    cancel: &mut JoinCancel<'_>,
) -> Result<IntegerJoinAttempt> {
    let cap = projection.map_or(outer_col_count + inner_col_count, |p| p.slots.len());

    if outer_is_sorted && matches!(join_type, JoinType::Inner | JoinType::Cross) {
        let mut prev = i64::MIN;
        let mut sorted = true;
        let mut has_null = false;
        for r in inner_rows.iter() {
            cancel.work()?;
            match r[inner_key_col] {
                Value::Integer(k) => {
                    if k < prev {
                        sorted = false;
                    }
                    prev = k;
                }
                Value::Null => {
                    has_null = true;
                }
                _ => return Ok(Err(outer_rows)),
            }
        }

        let mut result = Vec::with_capacity(outer_rows.len());

        if sorted && !has_null {
            let key_at = |idx: usize, rows: &[Vec<Value>]| -> i64 {
                match rows[idx][inner_key_col] {
                    Value::Integer(k) => k,
                    _ => unreachable!(),
                }
            };
            let mut j = 0;
            for mut outer in outer_rows {
                cancel.work()?;
                let ok = match outer[outer_key_col] {
                    Value::Integer(i) => i,
                    _ => continue,
                };
                while j < inner_rows.len() && key_at(j, inner_rows) < ok {
                    cancel.work()?;
                    j += 1;
                }
                let mut kk = j;
                while kk < inner_rows.len() && key_at(kk, inner_rows) == ok {
                    cancel.work()?;
                    let inner = &inner_rows[kk];
                    let is_last = kk + 1 >= inner_rows.len() || key_at(kk + 1, inner_rows) != ok;
                    if is_last {
                        if let Some(projection) = projection {
                            result.push(projection.finish_outer(outer, Some(inner)));
                        } else {
                            outer.extend(inner.iter().cloned());
                            result.push(outer);
                        }
                        kk += 1;
                        break;
                    }
                    if let Some(proj) = projection {
                        result.push(combine_row_projected(&outer, inner, proj));
                    } else {
                        result.push(combine_row(&outer, inner, cap));
                    }
                    kk += 1;
                }
                j = kk;
            }
            cancel.check()?;
            return Ok(Ok(result));
        }

        let mut aux: Vec<(i64, usize)> = Vec::with_capacity(inner_rows.len());
        for (i, r) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if let Value::Integer(k) = r[inner_key_col] {
                aux.push((k, i));
            }
        }
        if !sorted {
            aux = sort_vec_unstable_by(aux, cancel.token, |a, b| a.0.cmp(&b.0))?;
        }

        let mut j = 0;
        for mut outer in outer_rows {
            cancel.work()?;
            let ok = match outer[outer_key_col] {
                Value::Integer(i) => i,
                _ => continue,
            };
            while j < aux.len() && aux[j].0 < ok {
                cancel.work()?;
                j += 1;
            }
            let mut kk = j;
            while kk < aux.len() && aux[kk].0 == ok {
                cancel.work()?;
                let inner_idx = aux[kk].1;
                let inner = &inner_rows[inner_idx];
                let is_last = kk + 1 >= aux.len() || aux[kk + 1].0 != ok;
                if is_last {
                    if let Some(projection) = projection {
                        result.push(projection.finish_outer(outer, Some(inner)));
                    } else {
                        outer.extend(inner.iter().cloned());
                        result.push(outer);
                    }
                    break;
                }
                if let Some(proj) = projection {
                    result.push(combine_row_projected(&outer, inner, proj));
                } else {
                    result.push(combine_row(&outer, inner, cap));
                }
                kk += 1;
            }
        }
        cancel.check()?;
        return Ok(Ok(result));
    }

    let mut inner_map: FxHashMap<i64, Vec<usize>> =
        FxHashMap::with_capacity_and_hasher(inner_rows.len(), Default::default());
    for (idx, inner) in inner_rows.iter().enumerate() {
        cancel.work()?;
        match &inner[inner_key_col] {
            Value::Integer(k) => inner_map.entry(*k).or_default().push(idx),
            Value::Null => {}
            _ => return Ok(Err(outer_rows)),
        }
    }

    let joined = integer_join_with_map(
        outer_rows,
        join_type,
        &IntJoinCtx {
            inner_rows,
            inner_map: &inner_map,
            outer_key_col,
            outer_col_count,
            inner_col_count,
            projection,
        },
        cancel,
    )?;
    Ok(Ok(joined))
}

pub(super) struct IntJoinCtx<'a> {
    inner_rows: &'a [Vec<Value>],
    inner_map: &'a FxHashMap<i64, Vec<usize>>,
    outer_key_col: usize,
    outer_col_count: usize,
    inner_col_count: usize,
    projection: Option<&'a CombineProjection>,
}

fn integer_join_with_map(
    outer_rows: Vec<Vec<Value>>,
    join_type: &JoinType,
    ctx: &IntJoinCtx<'_>,
    cancel: &mut JoinCancel<'_>,
) -> Result<Vec<Vec<Value>>> {
    let IntJoinCtx {
        inner_rows,
        inner_map,
        outer_key_col,
        outer_col_count,
        inner_col_count,
        projection,
    } = *ctx;
    let cap = outer_col_count + inner_col_count;
    let preserve_outer = matches!(join_type, JoinType::Left | JoinType::FullOuter);
    let preserve_inner = matches!(join_type, JoinType::Right | JoinType::FullOuter);
    let mut inner_matched = if preserve_inner {
        vec![false; inner_rows.len()]
    } else {
        Vec::new()
    };
    let mut result = Vec::with_capacity(outer_rows.len());

    for mut outer in outer_rows {
        cancel.work()?;
        let indices = match outer[outer_key_col] {
            Value::Integer(key) => inner_map.get(&key).map_or(&[][..], Vec::as_slice),
            _ => &[],
        };
        if let Some((&last, preceding)) = indices.split_last() {
            for &index in preceding {
                cancel.work()?;
                let inner = &inner_rows[index];
                result.push(if let Some(projection) = projection {
                    combine_row_projected(&outer, inner, projection)
                } else {
                    combine_row(&outer, inner, cap)
                });
                if preserve_inner {
                    inner_matched[index] = true;
                }
            }
            cancel.work()?;
            if preserve_inner {
                inner_matched[last] = true;
            }
            if let Some(projection) = projection {
                result.push(projection.finish_outer(outer, Some(&inner_rows[last])));
            } else {
                outer.extend(inner_rows[last].iter().cloned());
                result.push(outer);
            }
        } else if preserve_outer {
            if let Some(projection) = projection {
                result.push(projection.finish_outer(outer, None));
            } else {
                outer.resize(cap, Value::Null);
                result.push(outer);
            }
        }
    }
    if preserve_inner {
        for (index, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if !inner_matched[index] {
                if let Some(projection) = projection {
                    result.push(projection.unmatched_inner(inner));
                } else {
                    let mut padded = Vec::with_capacity(cap);
                    padded.resize(outer_col_count, Value::Null);
                    padded.extend(inner.iter().cloned());
                    result.push(padded);
                }
            }
        }
    }
    cancel.check()?;
    Ok(result)
}

/// Probe side of a pure-equi join step, cached per commit generation.
pub(super) enum ProbeIndex {
    Int(FxHashMap<i64, Vec<usize>>),
    Generic(ProbeTable),
    None,
}

pub(super) fn build_probe_index(
    inner_rows: &[Vec<Value>],
    equi: &EquiJoin,
    token: Option<&citadel::CancelToken>,
) -> Result<ProbeIndex> {
    let mut cancel = JoinCancel::new(token)?;
    if !equi.is_pure() || equi.is_empty() {
        return Ok(ProbeIndex::None);
    }
    if equi.len() == 1 {
        // Int-lane parity: NULL keys skipped, any non-integer key disqualifies.
        let inner_key_col = equi.only_pair().inner;
        let mut map: FxHashMap<i64, Vec<usize>> =
            FxHashMap::with_capacity_and_hasher(inner_rows.len(), Default::default());
        let mut all_int = true;
        for (idx, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            match &inner[inner_key_col] {
                Value::Integer(k) => map.entry(*k).or_default().push(idx),
                Value::Null => {}
                _ => {
                    all_int = false;
                    break;
                }
            }
        }
        if all_int {
            cancel.check()?;
            return Ok(ProbeIndex::Int(map));
        }
    }
    Ok(ProbeIndex::Generic(ProbeTable::build(
        inner_rows,
        equi,
        &mut cancel,
    )?))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_join_step_borrowed(
    mut outer_rows: Vec<Vec<Value>>,
    inner_rows: &[Vec<Value>],
    join: &JoinClause,
    combined_cols: &[ColumnDef],
    outer_col_count: usize,
    inner_col_count: usize,
    outer_pk_col: Option<usize>,
    projection: Option<&CombineProjection>,
    equi: &EquiJoin,
    probe: Option<&ProbeIndex>,
    token: Option<&citadel::CancelToken>,
) -> Result<Vec<Vec<Value>>> {
    let mut cancel = JoinCancel::new(token)?;
    let effective_proj = if equi.is_pure() { projection } else { None };

    if equi.len() == 1
        && equi.is_pure()
        && has_integer_keys(&outer_rows, equi.only_pair().outer, &mut cancel)?
    {
        let pair = equi.only_pair();
        if let Some(ProbeIndex::Int(map)) = probe {
            return integer_join_with_map(
                outer_rows,
                &join.join_type,
                &IntJoinCtx {
                    inner_rows,
                    inner_map: map,
                    outer_key_col: pair.outer,
                    outer_col_count,
                    inner_col_count,
                    projection: effective_proj,
                },
                &mut cancel,
            );
        }
        let outer_is_sorted = outer_pk_col == Some(pair.outer);
        match try_integer_join_borrowed(
            outer_rows,
            inner_rows,
            &join.join_type,
            pair.outer,
            pair.inner,
            outer_col_count,
            inner_col_count,
            outer_is_sorted,
            effective_proj,
            &mut cancel,
        )? {
            Ok(result) => {
                cancel.check()?;
                return Ok(result);
            }
            Err(rows) => outer_rows = rows,
        }
    }

    let built;
    let buckets = match probe {
        Some(ProbeIndex::Generic(buckets)) => buckets,
        _ => {
            built = ProbeTable::build(inner_rows, equi, &mut cancel)?;
            &built
        }
    };
    exec_hash_join(
        outer_rows,
        inner_rows,
        join,
        combined_cols,
        outer_col_count,
        inner_col_count,
        effective_proj,
        equi,
        buckets,
        &mut cancel,
    )
}

#[cfg(test)]
#[path = "join_tests.rs"]
mod tests;
