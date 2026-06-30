use std::cell::RefCell;

use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_column_with_offset, decode_composite_key, decode_pk_integer,
    encode_composite_key, encode_row, patch_at_offset, patch_column_in_place, patch_row_column,
};
use crate::error::{Result, SqlError};
use crate::eval::{eval_expr, is_truthy, ColumnMap, EvalCtx};
use crate::parser::*;
use crate::schema::SchemaManager;
use crate::types::*;

use super::compile::CompiledPlan;
use super::correlated::*;
use super::dml::*;
use super::helpers::*;
use super::scan::*;
use super::select::*;
use super::view::*;
use super::CteContext;

struct UpdateBufs {
    partial_row: Vec<Value>,
    patch_buf: Vec<u8>,
    offsets: Vec<usize>,
    kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    patched: Vec<(Vec<u8>, Vec<u8>)>,
}

impl UpdateBufs {
    fn new() -> Self {
        Self {
            partial_row: Vec::new(),
            patch_buf: Vec::with_capacity(256),
            offsets: Vec::new(),
            kv_pairs: Vec::new(),
            patched: Vec::new(),
        }
    }
}

thread_local! {
    static UPDATE_SCRATCH: RefCell<UpdateBufs> = RefCell::new(UpdateBufs::new());
}

fn with_update_scratch<R>(f: impl FnOnce(&mut UpdateBufs) -> R) -> R {
    UPDATE_SCRATCH.with(|slot| f(&mut slot.borrow_mut()))
}

pub struct CompiledUpdate {
    table_name_lower: String,
    is_view: bool,
    has_correlated_where: bool,
    has_subquery: bool,
    can_fast_path: bool,
    fast: Option<CompiledFastPath>,
}

struct CompiledFastPath {
    num_pk_cols: usize,
    num_columns: usize,
    single_int_pk: bool,
    targets: Vec<CompiledTarget>,
    scan_plan: crate::planner::ScanPlan,
    pk_idx_cache: Vec<usize>,
    col_map: ColumnMap,
    range_bounds_i64: Option<Vec<(BinOp, i64)>>,
    gen_targets: Vec<GenColPatch>,
    gen_extra_cols: Vec<(usize, usize)>,
    pk_lookup_fast: Option<PkLookupFast>,
}

#[derive(Clone)]
enum PkLookupSource {
    Literal(Value),
    Parameter(usize),
}

#[derive(Clone)]
struct PkLookupFast {
    source: PkLookupSource,
}

#[derive(Clone)]
struct GenColPatch {
    schema_idx: usize,
    phys_idx: usize,
    expr: Expr,
    col: ColumnDef,
    fast_eval: FastGenEval,
}

enum FastEval {
    None,
    IntAdd(i64),
    IntSub(i64),
    IntMul(i64),
    IntSet(i64),
    IntAddParam(usize),
    IntSubParam(usize),
    IntMulParam(usize),
    IntSetParam(usize),
}

struct CompiledTarget {
    schema_idx: usize,
    phys_idx: usize,
    expr: Expr,
    col: ColumnDef,
    fast_eval: FastEval,
}

fn detect_fast_eval(expr: &Expr, col_name: &str) -> FastEval {
    let lower = col_name.to_ascii_lowercase();
    match expr {
        Expr::Literal(Value::Integer(n)) => FastEval::IntSet(*n),
        Expr::Parameter(n) => FastEval::IntSetParam(*n),
        Expr::BinaryOp { left, op, right } => {
            let col_match =
                |e: &Expr| matches!(e, Expr::Column(c) if c.to_ascii_lowercase() == lower);
            let int_lit = |e: &Expr| match e {
                Expr::Literal(Value::Integer(n)) => Some(*n),
                _ => None,
            };
            let param_ref = |e: &Expr| match e {
                Expr::Parameter(n) => Some(*n),
                _ => None,
            };
            if col_match(left) {
                if let Some(n) = int_lit(right) {
                    return match op {
                        BinOp::Add => FastEval::IntAdd(n),
                        BinOp::Sub => FastEval::IntSub(n),
                        BinOp::Mul => FastEval::IntMul(n),
                        _ => FastEval::None,
                    };
                }
                if let Some(n) = param_ref(right) {
                    return match op {
                        BinOp::Add => FastEval::IntAddParam(n),
                        BinOp::Sub => FastEval::IntSubParam(n),
                        BinOp::Mul => FastEval::IntMulParam(n),
                        _ => FastEval::None,
                    };
                }
            }
            if col_match(right) {
                if let Some(n) = int_lit(left) {
                    return match op {
                        BinOp::Add => FastEval::IntAdd(n),
                        BinOp::Mul => FastEval::IntMul(n),
                        _ => FastEval::None,
                    };
                }
                if let Some(n) = param_ref(left) {
                    return match op {
                        BinOp::Add => FastEval::IntAddParam(n),
                        BinOp::Mul => FastEval::IntMulParam(n),
                        _ => FastEval::None,
                    };
                }
            }
            FastEval::None
        }
        _ => FastEval::None,
    }
}

fn detect_pk_lookup_fast(
    where_clause: &Option<Expr>,
    table_schema: &TableSchema,
) -> Option<PkLookupFast> {
    let pk = &table_schema.primary_key_columns;
    if pk.len() != 1 {
        return None;
    }
    let pk_idx = pk[0] as usize;
    let pk_name = table_schema.columns[pk_idx].name.to_ascii_lowercase();
    let where_expr = where_clause.as_ref()?;
    let (left, right) = match where_expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };
    let col_matches = |e: &Expr| match e {
        Expr::Column(name) => name.to_ascii_lowercase() == pk_name,
        Expr::QualifiedColumn { column, .. } => column.to_ascii_lowercase() == pk_name,
        _ => false,
    };
    let extract_source = |e: &Expr| match e {
        Expr::Literal(v) => Some(PkLookupSource::Literal(v.clone())),
        Expr::Parameter(n) => Some(PkLookupSource::Parameter(*n)),
        _ => None,
    };
    let source = if col_matches(left) {
        extract_source(right)?
    } else if col_matches(right) {
        extract_source(left)?
    } else {
        return None;
    };
    Some(PkLookupFast { source })
}

fn resolve_int_param(n: usize) -> Option<i64> {
    match crate::eval::resolve_scoped_param(n).ok()? {
        Value::Integer(v) => Some(v),
        _ => None,
    }
}

fn compute_gen_col_targets(
    table_schema: &TableSchema,
    set_target_schema_indices: &[usize],
    pk_indices: &[usize],
) -> (Vec<GenColPatch>, Vec<(usize, usize)>) {
    let stored_gen_cols: Vec<&ColumnDef> = table_schema
        .columns
        .iter()
        .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
        .collect();
    if stored_gen_cols.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let mut gen_targets = Vec::with_capacity(stored_gen_cols.len());
    for c in &stored_gen_cols {
        let schema_idx = c.position as usize;
        let nonpk_order = non_pk.iter().position(|&i| i == schema_idx).unwrap();
        let phys_idx = enc_pos[nonpk_order] as usize;
        let expr = c.generated_expr.clone().unwrap();
        let fast_eval = detect_fast_gen_eval(&expr, table_schema);
        gen_targets.push(GenColPatch {
            schema_idx,
            phys_idx,
            expr,
            col: (*c).clone(),
            fast_eval,
        });
    }

    let mut needed_names: Vec<String> = Vec::new();
    for gp in &gen_targets {
        super::ddl::collect_column_refs(&gp.expr, &mut needed_names);
    }

    let mut needed_indices: Vec<usize> = Vec::new();
    for name in &needed_names {
        if let Some(idx) = table_schema.column_index(name) {
            if !needed_indices.contains(&idx) {
                needed_indices.push(idx);
            }
        }
    }

    let mut gen_eval_decode_cols: Vec<(usize, usize)> = Vec::new();
    for &schema_idx in &needed_indices {
        // Single-column UPDATE: the set-target's new value is live in partial_row, skip re-decode.
        // Multi-column SET re-decodes (RHS evaluates against the original, unmutated row).
        if pk_indices.contains(&schema_idx)
            || (set_target_schema_indices.len() == 1
                && set_target_schema_indices.contains(&schema_idx))
        {
            continue;
        }
        if let Some(nonpk_order) = non_pk.iter().position(|&i| i == schema_idx) {
            let phys_idx = enc_pos[nonpk_order] as usize;
            gen_eval_decode_cols.push((schema_idx, phys_idx));
        }
    }

    (gen_targets, gen_eval_decode_cols)
}

enum RangeStatus {
    Hit,
    Skip,
    Stop,
    Err,
}

fn range_in_bounds(
    key: &[u8],
    single_int_pk: bool,
    num_pk_cols: usize,
    range_conds: &[(BinOp, Value)],
    out_err: &mut Option<SqlError>,
) -> RangeStatus {
    let pk_val = if single_int_pk {
        match decode_pk_integer(key) {
            Ok(v) => Value::Integer(v),
            Err(e) => {
                *out_err = Some(e);
                return RangeStatus::Err;
            }
        }
    } else {
        match decode_composite_key(key, num_pk_cols) {
            Ok(mut vs) => vs.remove(0),
            Err(e) => {
                *out_err = Some(e);
                return RangeStatus::Err;
            }
        }
    };
    for (op, bound) in range_conds {
        match op {
            BinOp::Lt if &pk_val >= bound => return RangeStatus::Stop,
            BinOp::LtEq if &pk_val > bound => return RangeStatus::Stop,
            BinOp::Gt if &pk_val <= bound => return RangeStatus::Skip,
            BinOp::GtEq if &pk_val < bound => return RangeStatus::Skip,
            _ => {}
        }
    }
    RangeStatus::Hit
}

fn is_fixed_width_type(dt: DataType) -> bool {
    matches!(
        dt,
        DataType::Integer
            | DataType::Real
            | DataType::Boolean
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Interval
    )
}

fn pk_range_patch_safe(set_cols: &[ColumnDef], gen_cols: &[ColumnDef]) -> bool {
    set_cols
        .iter()
        .chain(gen_cols.iter())
        .all(|c| !c.nullable && is_fixed_width_type(c.data_type))
}

fn coerce_gen_value(val: Value, col: &ColumnDef) -> Result<Value> {
    if val.is_null() {
        if !col.nullable {
            return Err(SqlError::NotNullViolation(col.name.clone()));
        }
        Ok(Value::Null)
    } else {
        let got_type = val.data_type();
        val.coerce_into(col.data_type)
            .ok_or_else(|| SqlError::TypeMismatch {
                expected: col.data_type.to_string(),
                got: got_type.to_string(),
            })
    }
}

fn apply_gen_col_patches_slice(
    value: &mut [u8],
    partial_row: &mut [Value],
    gen_targets: &[GenColPatch],
    gen_extra_cols: &[(usize, usize)],
    col_map: &ColumnMap,
    patch_buf: &mut Vec<u8>,
) -> Result<()> {
    if gen_targets.is_empty() {
        return Ok(());
    }
    for &(schema_idx, phys_idx) in gen_extra_cols {
        partial_row[schema_idx] = decode_column_raw(value, phys_idx)?.to_value();
    }
    for gp in gen_targets {
        let raw = eval_fast_gen(&gp.fast_eval, &gp.expr, partial_row, col_map)?;
        let coerced = coerce_gen_value(raw, &gp.col)?;
        partial_row[gp.schema_idx] = coerced.clone();
        if !patch_column_in_place(value, gp.phys_idx, &coerced)? {
            patch_row_column(value, gp.phys_idx, &coerced, patch_buf)?;
            value[..patch_buf.len()].copy_from_slice(patch_buf);
        }
    }
    Ok(())
}

fn apply_gen_col_patches_vec(
    value: &mut Vec<u8>,
    partial_row: &mut [Value],
    gen_targets: &[GenColPatch],
    gen_extra_cols: &[(usize, usize)],
    col_map: &ColumnMap,
    patch_buf: &mut Vec<u8>,
) -> Result<()> {
    if gen_targets.is_empty() {
        return Ok(());
    }
    for &(schema_idx, phys_idx) in gen_extra_cols {
        partial_row[schema_idx] = decode_column_raw(value, phys_idx)?.to_value();
    }
    for gp in gen_targets {
        let raw = eval_fast_gen(&gp.fast_eval, &gp.expr, partial_row, col_map)?;
        let coerced = coerce_gen_value(raw, &gp.col)?;
        partial_row[gp.schema_idx] = coerced.clone();
        if !patch_column_in_place(value, gp.phys_idx, &coerced)? {
            patch_row_column(value, gp.phys_idx, &coerced, patch_buf)?;
            std::mem::swap(value, patch_buf);
        }
    }
    Ok(())
}

impl CompiledUpdate {
    pub fn try_compile(schema: &SchemaManager, stmt: &UpdateStmt) -> Result<Option<Self>> {
        compile_update_impl(schema, stmt).map(Some)
    }
}

impl CompiledPlan for CompiledUpdate {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
        txn: super::compile::ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult> {
        let upd = match stmt {
            Statement::Update(u) => u,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledUpdate received non-UPDATE statement".into(),
                ))
            }
        };
        use super::compile::ActiveTxnRef;
        match txn {
            ActiveTxnRef::None => {
                with_update_scratch(|bufs| exec_update_compiled(db, schema, upd, self, bufs))
            }
            ActiveTxnRef::Read(_) => Err(SqlError::Unsupported(
                "cannot execute mutating statement inside a read-only transaction".into(),
            )),
            ActiveTxnRef::Write(outer) => with_update_scratch(|bufs| {
                exec_update_in_txn_compiled(outer, schema, upd, self, bufs)
            }),
        }
    }
}

fn compile_update_impl(schema: &SchemaManager, stmt: &UpdateStmt) -> Result<CompiledUpdate> {
    let user_name = stmt.table.to_ascii_lowercase();
    let is_view = schema.get_view(&user_name).is_some();
    if is_view {
        return Ok(CompiledUpdate {
            table_name_lower: user_name,
            is_view: true,
            has_correlated_where: false,
            has_subquery: false,
            can_fast_path: false,
            fast: None,
        });
    }

    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    // Storage name (post-TEMP-alias resolution); used by wtx.table_* calls below.
    let table_name_lower = table_schema.name.clone();

    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    let has_correlated = has_correlated_where(&stmt.where_clause, &corr_ctx, schema);
    let has_sub = update_has_subquery(stmt);

    if has_correlated || has_sub {
        return Ok(CompiledUpdate {
            table_name_lower,
            is_view: false,
            has_correlated_where: has_correlated,
            has_subquery: has_sub,
            can_fast_path: false,
            fast: None,
        });
    }

    let pk_indices = table_schema.pk_indices();
    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });
    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&table_name_lower).is_empty();
    let can_fast_path = !pk_changed_by_set
        && !has_fk
        && !has_indices
        && !has_child_fk
        && !table_schema.has_checks();

    let fast = if can_fast_path {
        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let num_pk_cols = table_schema.primary_key_columns.len();

        let mut targets = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let schema_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let col = &table_schema.columns[schema_idx];
            if col.generated_kind.is_some() {
                return Err(SqlError::CannotUpdateGeneratedColumn(col.name.clone()));
            }
            let nonpk_order = non_pk
                .iter()
                .position(|&i| i == schema_idx)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let phys_idx = enc_pos[nonpk_order] as usize;
            let fast_eval = detect_fast_eval(expr, col_name);
            targets.push(CompiledTarget {
                schema_idx,
                phys_idx,
                expr: expr.clone(),
                col: col.clone(),
                fast_eval,
            });
        }

        let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
        let single_int_pk = num_pk_cols == 1
            && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                == DataType::Integer;

        let range_bounds_i64 = if single_int_pk {
            if let crate::planner::ScanPlan::PkRangeScan {
                ref range_conds, ..
            } = plan
            {
                let bounds: Vec<(BinOp, i64)> = range_conds
                    .iter()
                    .filter_map(|(op, val)| match val {
                        Value::Integer(i) => Some((*op, *i)),
                        _ => None,
                    })
                    .collect();
                if bounds.len() == range_conds.len() {
                    Some(bounds)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let set_target_indices: Vec<usize> = targets.iter().map(|t| t.schema_idx).collect();
        let (gen_targets, gen_extra_cols) =
            compute_gen_col_targets(table_schema, &set_target_indices, pk_indices);
        let pk_lookup_fast = detect_pk_lookup_fast(&stmt.where_clause, table_schema);

        Some(CompiledFastPath {
            num_pk_cols,
            num_columns: table_schema.columns.len(),
            single_int_pk,
            targets,
            scan_plan: plan,
            pk_idx_cache: pk_indices.to_vec(),
            col_map: ColumnMap::new(&table_schema.columns),
            range_bounds_i64,
            gen_targets,
            gen_extra_cols,
            pk_lookup_fast,
        })
    } else {
        None
    };

    Ok(CompiledUpdate {
        table_name_lower,
        is_view: false,
        has_correlated_where: false,
        has_subquery: false,
        can_fast_path,
        fast,
    })
}

fn exec_update_compiled(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
    compiled: &CompiledUpdate,
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    if compiled.is_view {
        // exec_update handles INSTEAD OF view dispatch (or returns CannotModifyView).
        return exec_update(db, schema, stmt);
    }
    if compiled.has_correlated_where
        || compiled.has_subquery
        || !compiled.can_fast_path
        || stmt.returning.is_some()
    {
        return exec_update(db, schema, stmt);
    }

    let fast = compiled.fast.as_ref().unwrap();
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    // Mark + purge the persisted segment in the same txn, like every DML path.
    schema.mark_dml(&compiled.table_name_lower);
    super::ann_persist::purge_segment(&mut wtx, &compiled.table_name_lower)?;

    if let crate::planner::ScanPlan::PkRangeScan {
        ref start_key,
        ref range_conds,
        ..
    } = fast.scan_plan
    {
        bufs.partial_row.clear();
        bufs.partial_row.resize(fast.num_columns, Value::Null);
        bufs.offsets.clear();
        bufs.offsets.resize(fast.targets.len(), usize::MAX);

        let count = wtx.table_update_range(
            compiled.table_name_lower.as_bytes(),
            start_key,
            |key, value| {
                if let Some(ref bounds) = fast.range_bounds_i64 {
                    let pk = decode_pk_integer(key)?;
                    for &(op, bound) in bounds {
                        match op {
                            BinOp::Lt if pk >= bound => return Ok(None),
                            BinOp::LtEq if pk > bound => return Ok(None),
                            BinOp::Gt if pk <= bound => return Ok(Some(false)),
                            BinOp::GtEq if pk < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    bufs.partial_row[fast.pk_idx_cache[0]] = Value::Integer(pk);
                } else if fast.single_int_pk {
                    let pk = decode_pk_integer(key)?;
                    let pk_val = Value::Integer(pk);
                    for (op, bound) in range_conds {
                        match op {
                            BinOp::Lt if &pk_val >= bound => return Ok(None),
                            BinOp::LtEq if &pk_val > bound => return Ok(None),
                            BinOp::Gt if &pk_val <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_val < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    bufs.partial_row[fast.pk_idx_cache[0]] = pk_val;
                } else {
                    let pk_vals = decode_composite_key(key, fast.num_pk_cols)?;
                    for (op, bound) in range_conds {
                        match op {
                            BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                            BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                            BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    for (i, &pi) in fast.pk_idx_cache.iter().enumerate() {
                        bufs.partial_row[pi] = pk_vals[i].clone();
                    }
                }
                for (i, target) in fast.targets.iter().enumerate() {
                    let (raw, off) = decode_column_with_offset(value, target.phys_idx)?;
                    bufs.partial_row[target.schema_idx] = raw.to_value();
                    bufs.offsets[i] = off;
                }
                for (i, target) in fast.targets.iter().enumerate() {
                    let generic_eval = || {
                        eval_expr(
                            &target.expr,
                            &EvalCtx::new(&fast.col_map, &bufs.partial_row),
                        )
                    };
                    let new_val = match target.fast_eval {
                        FastEval::IntAdd(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_add(n))
                            } else {
                                generic_eval()?
                            }
                        }
                        FastEval::IntSub(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_sub(n))
                            } else {
                                generic_eval()?
                            }
                        }
                        FastEval::IntMul(n) => {
                            if let Value::Integer(v) = bufs.partial_row[target.schema_idx] {
                                Value::Integer(v.wrapping_mul(n))
                            } else {
                                generic_eval()?
                            }
                        }
                        FastEval::IntSet(n) => Value::Integer(n),
                        FastEval::IntAddParam(p) => {
                            match (resolve_int_param(p), &bufs.partial_row[target.schema_idx]) {
                                (Some(n), Value::Integer(v)) => Value::Integer(v.wrapping_add(n)),
                                _ => generic_eval()?,
                            }
                        }
                        FastEval::IntSubParam(p) => {
                            match (resolve_int_param(p), &bufs.partial_row[target.schema_idx]) {
                                (Some(n), Value::Integer(v)) => Value::Integer(v.wrapping_sub(n)),
                                _ => generic_eval()?,
                            }
                        }
                        FastEval::IntMulParam(p) => {
                            match (resolve_int_param(p), &bufs.partial_row[target.schema_idx]) {
                                (Some(n), Value::Integer(v)) => Value::Integer(v.wrapping_mul(n)),
                                _ => generic_eval()?,
                            }
                        }
                        FastEval::IntSetParam(p) => match resolve_int_param(p) {
                            Some(n) => Value::Integer(n),
                            None => generic_eval()?,
                        },
                        FastEval::None => generic_eval()?,
                    };
                    let coerced = if new_val.is_null() {
                        if !target.col.nullable {
                            return Err(SqlError::NotNullViolation(target.col.name.clone()));
                        }
                        Value::Null
                    } else {
                        let got_type = new_val.data_type();
                        new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                            SqlError::TypeMismatch {
                                expected: target.col.data_type.to_string(),
                                got: got_type.to_string(),
                            }
                        })?
                    };
                    if !patch_at_offset(value, bufs.offsets[i], &coerced)?
                        && !patch_column_in_place(value, target.phys_idx, &coerced)?
                    {
                        patch_row_column(value, target.phys_idx, &coerced, &mut bufs.patch_buf)?;
                        value[..bufs.patch_buf.len()].copy_from_slice(&bufs.patch_buf);
                        for off in bufs.offsets.iter_mut().skip(i + 1) {
                            *off = usize::MAX;
                        }
                    }
                    if fast.targets.len() == 1 {
                        bufs.partial_row[target.schema_idx] = coerced;
                    }
                }
                apply_gen_col_patches_slice(
                    value,
                    &mut bufs.partial_row,
                    &fast.gen_targets,
                    &fast.gen_extra_cols,
                    &fast.col_map,
                    &mut bufs.patch_buf,
                )?;
                Ok(Some(true))
            },
        )?;

        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    drop(wtx);
    exec_update(db, schema, stmt)
}

pub(super) fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(
            schema,
            &user_name,
            super::triggers::FireEvent::Update {
                changed_columns: &[],
            },
        ) {
            let aliases = view_def.column_aliases.clone();
            let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
            let r =
                exec_instead_of_view_update_in_txn(&mut wtx, schema, &user_name, &aliases, stmt)?;
            wtx.commit().map_err(SqlError::Storage)?;
            return Ok(r);
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    if schema.get_matview(&user_name).is_some() {
        return Err(SqlError::CannotModifyView(format!(
            "materialized view '{}' is read-only — use REFRESH MATERIALIZED VIEW",
            stmt.table
        )));
    }
    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    schema.mark_dml(&table_schema.name);
    // Use storage name (post-TEMP-alias resolution) for all wtx.* storage calls below.
    let lower_name = table_schema.name.clone();
    let strict = table_schema.is_strict();

    // Correlated subquery in UPDATE WHERE: check BEFORE materialization.
    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let select_stmt = SelectStmt {
            columns: vec![SelectColumn::AllColumns],
            from: stmt.table.clone(),
            from_alias: None,
            from_subquery: None,
            from_args: None,
            from_json_table: None,
            joins: vec![],
            distinct: false,
            where_clause: stmt.where_clause.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            group_by: vec![],
            having: None,
        };
        let (mut rows, _) = collect_rows_read(db, table_schema, &None, None)?;
        let remaining =
            handle_correlated_where_read(db, schema, &select_stmt, &corr_ctx, &mut rows)?;

        if let Some(ref w) = remaining {
            let col_map = ColumnMap::new(&table_schema.columns);
            rows.retain(|row| match eval_expr(w, &EvalCtx::new(&col_map, row)) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_col = &table_schema.columns[pk_indices[0]].name;
        let in_set: rustc_hash::FxHashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_col.clone())),
                values: in_set,
                has_null: false,
                negated: false,
            })
        };

        let rewritten = UpdateStmt {
            table: stmt.table.clone(),
            assignments: stmt.assignments.clone(),
            where_clause: new_where,
            returning: stmt.returning.clone(),
        };
        return exec_update(db, schema, &rewritten);
    }

    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &CteContext::default())
        })?;
        &materialized
    } else {
        stmt
    };

    let col_map = ColumnMap::new(&table_schema.columns);
    let pk_indices = table_schema.pk_indices();

    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });

    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&lower_name).is_empty();
    let has_stored_generated = table_schema
        .columns
        .iter()
        .any(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)));
    // Fast paths skip the trigger-firing site at the slow path's tail. Gate on
    // "no UPDATE triggers" so AFTER UPDATE row triggers always run.
    let has_update_triggers = schema.triggers_for(&table_schema.name).iter().any(|t| {
        t.enabled
            && (t.timing == crate::parser::TriggerTiming::After
                || t.timing == crate::parser::TriggerTiming::Before)
            && t.events
                .iter()
                .any(|e| matches!(e, crate::parser::TriggerEvent::Update(_)))
    });
    if !pk_changed_by_set
        && !has_fk
        && !has_indices
        && !has_child_fk
        && !has_update_triggers
        && !table_schema.has_checks()
        && stmt.returning.is_none()
    {
        let non_pk = table_schema.non_pk_indices();
        let enc_pos = table_schema.encoding_positions();
        let num_pk_cols = table_schema.primary_key_columns.len();

        struct AssignTarget {
            schema_idx: usize,
            phys_idx: usize,
            expr: Expr,
            col: ColumnDef,
        }
        let mut targets: Vec<AssignTarget> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let schema_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let col = &table_schema.columns[schema_idx];
            if col.generated_kind.is_some() {
                return Err(SqlError::CannotUpdateGeneratedColumn(col.name.clone()));
            }
            let nonpk_order = non_pk
                .iter()
                .position(|&i| i == schema_idx)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let phys_idx = enc_pos[nonpk_order] as usize;
            targets.push(AssignTarget {
                schema_idx,
                phys_idx,
                expr: expr.clone(),
                col: col.clone(),
            });
        }

        let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
        let single_int_pk = num_pk_cols == 1
            && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                == DataType::Integer;

        let pk_indices_vec = table_schema.pk_indices().to_vec();
        let set_target_indices: Vec<usize> = targets.iter().map(|t| t.schema_idx).collect();
        let (gen_targets, gen_extra_cols) =
            compute_gen_col_targets(table_schema, &set_target_indices, &pk_indices_vec);

        let set_cols: Vec<ColumnDef> = targets.iter().map(|t| t.col.clone()).collect();
        let gen_cols: Vec<ColumnDef> = gen_targets.iter().map(|g| g.col.clone()).collect();
        let patch_safe = pk_range_patch_safe(&set_cols, &gen_cols);

        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
        super::ann_persist::purge_segment(&mut wtx, &lower_name)?;

        // `value: &mut [u8]` can't grow; nullable/variable-width fall through.
        if let (
            true,
            crate::planner::ScanPlan::PkRangeScan {
                start_key,
                range_conds,
                ..
            },
        ) = (patch_safe, &plan)
        {
            let range_conds = range_conds.clone();
            let mut partial_row = vec![Value::Null; table_schema.columns.len()];
            let pk_idx_cache = table_schema.pk_indices().to_vec();
            let mut patch_buf: Vec<u8> = Vec::with_capacity(256);

            let count =
                wtx.table_update_range(lower_name.as_bytes(), start_key, |key, value| {
                    if single_int_pk {
                        let pk_int = Value::Integer(decode_pk_integer(key)?);
                        for (op, bound) in &range_conds {
                            match op {
                                BinOp::Lt if &pk_int >= bound => return Ok(None),
                                BinOp::LtEq if &pk_int > bound => return Ok(None),
                                BinOp::Gt if &pk_int <= bound => return Ok(Some(false)),
                                BinOp::GtEq if &pk_int < bound => return Ok(Some(false)),
                                _ => {}
                            }
                        }
                    } else {
                        let pk_vals = decode_composite_key(key, num_pk_cols)?;
                        for (op, bound) in &range_conds {
                            match op {
                                BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                                BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                                BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                                BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                                _ => {}
                            }
                        }
                    }

                    if single_int_pk {
                        partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
                    } else {
                        let pk_vals = decode_composite_key(key, num_pk_cols)?;
                        for (i, &pi) in pk_idx_cache.iter().enumerate() {
                            partial_row[pi] = pk_vals[i].clone();
                        }
                    }
                    for target in &targets {
                        partial_row[target.schema_idx] =
                            decode_column_raw(value, target.phys_idx)?.to_value();
                    }
                    for target in &targets {
                        let new_val =
                            eval_expr(&target.expr, &EvalCtx::new(&col_map, &partial_row))?;
                        let coerced = if new_val.is_null() {
                            if !target.col.nullable {
                                return Err(SqlError::NotNullViolation(target.col.name.clone()));
                            }
                            Value::Null
                        } else {
                            let got_type = new_val.data_type();
                            new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                                SqlError::TypeMismatch {
                                    expected: target.col.data_type.to_string(),
                                    got: got_type.to_string(),
                                }
                            })?
                        };
                        if !patch_column_in_place(value, target.phys_idx, &coerced)? {
                            patch_row_column(value, target.phys_idx, &coerced, &mut patch_buf)?;
                            value[..patch_buf.len()].copy_from_slice(&patch_buf);
                        }
                        if targets.len() == 1 {
                            partial_row[target.schema_idx] = coerced;
                        }
                    }
                    apply_gen_col_patches_slice(
                        value,
                        &mut partial_row,
                        &gen_targets,
                        &gen_extra_cols,
                        &col_map,
                        &mut patch_buf,
                    )?;
                    Ok(Some(true))
                })?;

            wtx.commit().map_err(SqlError::Storage)?;
            return Ok(ExecutionResult::RowsAffected(count));
        }

        let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            match &plan {
                crate::planner::ScanPlan::PkLookup { pk_values } => {
                    let key = crate::encoding::encode_composite_key(pk_values);
                    if let Some(value) = wtx
                        .table_get(lower_name.as_bytes(), &key)
                        .map_err(SqlError::Storage)?
                    {
                        kv_pairs.push((key, value));
                    }
                }
                crate::planner::ScanPlan::PkRangeScan {
                    start_key,
                    range_conds,
                    ..
                } => {
                    let range_conds = range_conds.clone();
                    let mut scan_err: Option<SqlError> = None;
                    wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                        let in_range = range_in_bounds(
                            key,
                            single_int_pk,
                            num_pk_cols,
                            &range_conds,
                            &mut scan_err,
                        );
                        match in_range {
                            RangeStatus::Stop => Ok(false),
                            RangeStatus::Skip => Ok(true),
                            RangeStatus::Hit => {
                                kv_pairs.push((key.to_vec(), value.to_vec()));
                                Ok(true)
                            }
                            RangeStatus::Err => Ok(false),
                        }
                    })
                    .map_err(SqlError::Storage)?;
                    if let Some(e) = scan_err {
                        return Err(e);
                    }
                }
                _ => {
                    wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                        kv_pairs.push((key.to_vec(), value.to_vec()));
                        Ok(())
                    })
                    .map_err(SqlError::Storage)?;
                }
            }
        }

        let mut patch_buf: Vec<u8> = Vec::with_capacity(256);
        let mut partial_row = vec![Value::Null; table_schema.columns.len()];
        let pk_idx_cache = table_schema.pk_indices().to_vec();
        let mut patched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(kv_pairs.len());

        for (key, raw_value) in &mut kv_pairs {
            if matches!(plan, crate::planner::ScanPlan::SeqScan) {
                if let Some(ref w) = stmt.where_clause {
                    let row = decode_full_row(table_schema, key, raw_value)?;
                    if !eval_expr(w, &EvalCtx::new(&col_map, &row)).is_ok_and(|v| is_truthy(&v)) {
                        continue;
                    }
                }
            }
            if single_int_pk {
                partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
            } else {
                let pk_vals = decode_composite_key(key, num_pk_cols)?;
                for (i, &pi) in pk_idx_cache.iter().enumerate() {
                    partial_row[pi] = pk_vals[i].clone();
                }
            }
            for target in &targets {
                partial_row[target.schema_idx] =
                    decode_column_raw(raw_value, target.phys_idx)?.to_value();
            }
            for target in &targets {
                let new_val = eval_expr(&target.expr, &EvalCtx::new(&col_map, &partial_row))?;
                let coerced = if new_val.is_null() {
                    if !target.col.nullable {
                        return Err(SqlError::NotNullViolation(target.col.name.clone()));
                    }
                    Value::Null
                } else {
                    let got_type = new_val.data_type();
                    new_val.coerce_into(target.col.data_type).ok_or_else(|| {
                        SqlError::TypeMismatch {
                            expected: target.col.data_type.to_string(),
                            got: got_type.to_string(),
                        }
                    })?
                };
                if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                    patch_row_column(raw_value, target.phys_idx, &coerced, &mut patch_buf)?;
                    std::mem::swap(raw_value, &mut patch_buf);
                }
                if targets.len() == 1 {
                    partial_row[target.schema_idx] = coerced;
                }
            }
            apply_gen_col_patches_vec(
                raw_value,
                &mut partial_row,
                &gen_targets,
                &gen_extra_cols,
                &col_map,
                &mut patch_buf,
            )?;
            patched.push((std::mem::take(key), std::mem::take(raw_value)));
        }

        if !patched.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = patched
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            wtx.table_update_sorted(lower_name.as_bytes(), &refs)
                .map_err(SqlError::Storage)?;
        }
        let count = patched.len() as u64;
        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => {
                eval_expr(where_expr, &EvalCtx::new(&col_map, row)).is_ok_and(|v| is_truthy(&v))
            }
            None => true,
        })
        .collect();

    if matching_rows.is_empty() {
        if let Some(returning_cols) = stmt.returning.as_ref() {
            let qr = super::helpers::project_returning(table_schema, returning_cols, &[])?;
            return Ok(ExecutionResult::Query(qr));
        }
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let mut changes: Vec<UpdateChange> = Vec::new();

    let stored_gen_cols: Vec<&ColumnDef> = if has_stored_generated {
        table_schema
            .columns
            .iter()
            .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
            .collect()
    } else {
        Vec::new()
    };
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let mut value_values = vec![Value::Null; phys_count];

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();

        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let col = &table_schema.columns[col_idx];
            if col.generated_kind.is_some() {
                return Err(SqlError::CannotUpdateGeneratedColumn(col.name.clone()));
            }
            let new_val = eval_expr(expr, &EvalCtx::new(&col_map, row))?;

            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                super::helpers::coerce_for_column(new_val, col, strict)?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            new_row[col_idx] = coerced;
        }

        for col in &stored_gen_cols {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(&col_map, &new_row),
            )?;
            let pos = col.position as usize;
            new_row[pos] = if val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
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

        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &EvalCtx::new(&col_map, &new_row))?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &EvalCtx::new(&col_map, &new_row))?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        for v in value_values.iter_mut() {
            *v = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            let col = &table_schema.columns[i];
            value_values[enc_pos[j] as usize] = if matches!(
                col.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                Value::Null
            } else {
                new_row[i].clone()
            };
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed: pk_changed_by_set,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        let mut new_keys: rustc_hash::FxHashSet<Vec<u8>> = rustc_hash::FxHashSet::default();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    super::ann_persist::purge_segment(&mut wtx, &lower_name)?;

    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                if fk.deferrable && fk.initially_deferred {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table).to_string();
                    wtx.defer_fk_check(citadel_txn::write_txn::DeferredFkCheck {
                        fk_name: name,
                        foreign_table: fk.foreign_table.as_bytes().to_vec(),
                        parent_key: fk_key,
                    });
                    continue;
                }
                if !wtx.fk_check_cached(fk.foreign_table.as_bytes(), &fk_key) {
                    let found = wtx
                        .table_get(fk.foreign_table.as_bytes(), &fk_key)
                        .map_err(SqlError::Storage)?;
                    if found.is_none() {
                        let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                        return Err(SqlError::ForeignKeyViolation(name.to_string()));
                    }
                    wtx.mark_fk_verified(fk.foreign_table.as_bytes(), &fk_key);
                }
            }
        }
    }

    if !schema.child_fks_for(&lower_name).is_empty() {
        let parent_changes: Vec<(Vec<u8>, Vec<Value>, Vec<Value>)> = changes
            .iter()
            .map(|c| {
                let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
                (
                    encode_composite_key(&old_pk),
                    c.old_row.clone(),
                    c.new_row.clone(),
                )
            })
            .collect();
        cascade_after_parent_update(&mut wtx, schema, &lower_name, table_schema, &parent_changes)?;
    }

    let before_update_triggers: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Update(_)))
        })
        .cloned()
        .collect();
    if !before_update_triggers.is_empty() {
        let changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
        for c in &changes {
            super::triggers::fire_row_triggers(
                &mut wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Update {
                    changed_columns: &changed_cols,
                },
                Some(c.old_row.clone()),
                Some(c.new_row.clone()),
                &table_schema.columns,
            )?;
        }
    }

    let stmt_changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
    let stmt_old_rows: Vec<Vec<Value>> = changes.iter().map(|c| c.old_row.clone()).collect();
    let stmt_new_rows: Vec<Vec<Value>> = changes.iter().map(|c| c.new_row.clone()).collect();
    super::triggers::fire_statement_triggers(
        &mut wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::Before,
        super::triggers::FireEvent::Update {
            changed_columns: &stmt_changed_cols,
        },
        &table_schema.columns,
        &stmt_old_rows,
        &stmt_new_rows,
    )?;

    let col_map_partial = any_partial_index(table_schema).then(|| table_schema.column_map());

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            let cols_changed = index_columns_changed(idx, &c.old_row, &c.new_row);
            let (del, _) = partial_idx_update_actions(
                idx,
                &c.old_row,
                &c.new_row,
                cols_changed,
                c.pk_changed,
                col_map_partial,
            );
            if !del {
                continue;
            }
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            match idx.kind {
                crate::types::IndexKind::BTree => {
                    let old_idx_key =
                        encode_index_key_with_schema(idx, &c.old_row, &old_pk, table_schema);
                    wtx.table_delete(&idx_table, &old_idx_key)
                        .map_err(SqlError::Storage)?;
                }
                crate::types::IndexKind::Inverted(inv_kind) => {
                    let col0 = idx.column_positions_iter().next().ok_or_else(|| {
                        SqlError::Unsupported(
                            "inverted index requires at least one column key".into(),
                        )
                    })? as usize;
                    let entries =
                        super::helpers::extract_inverted_entries(&c.old_row[col0], inv_kind)?;
                    let pk_encoded = encode_composite_key(&old_pk);
                    for entry in entries {
                        let full_key = super::helpers::build_inverted_key(&entry, &pk_encoded);
                        wtx.table_delete(&idx_table, &full_key)
                            .map_err(SqlError::Storage)?;
                    }
                }
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            let cols_changed = index_columns_changed(idx, &c.old_row, &c.new_row);
            let (_, ins) = partial_idx_update_actions(
                idx,
                &c.old_row,
                &c.new_row,
                cols_changed,
                c.pk_changed,
                col_map_partial,
            );
            if !ins {
                continue;
            }
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            match idx.kind {
                crate::types::IndexKind::BTree => {
                    let new_idx_key =
                        encode_index_key_with_schema(idx, &c.new_row, &new_pk, table_schema);
                    let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                    let is_new = wtx
                        .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                        .map_err(SqlError::Storage)?;
                    if idx.unique && !is_new {
                        let indexed_values: Vec<Value> = idx
                            .column_positions_iter()
                            .map(|col_idx| c.new_row[col_idx as usize].clone())
                            .collect();
                        let any_null = indexed_values.iter().any(|v| v.is_null());
                        if !any_null {
                            return Err(SqlError::UniqueViolation(idx.name.clone()));
                        }
                    }
                }
                crate::types::IndexKind::Inverted(inv_kind) => {
                    let col0 = idx.column_positions_iter().next().ok_or_else(|| {
                        SqlError::Unsupported(
                            "inverted index requires at least one column key".into(),
                        )
                    })? as usize;
                    let value = &c.new_row[col0];
                    if !value.is_null() {
                        let entries =
                            super::helpers::extract_inverted_entries_with_values(value, inv_kind)?;
                        let pk_encoded = encode_composite_key(&new_pk);
                        for (entry, val_bytes) in entries {
                            let full_key = super::helpers::build_inverted_key(&entry, &pk_encoded);
                            wtx.table_insert(&idx_table, &full_key, &val_bytes)
                                .map_err(SqlError::Storage)?;
                        }
                    }
                }
            }
        }
    }

    let after_update_triggers: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::After
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Update(_)))
        })
        .cloned()
        .collect();
    if !after_update_triggers.is_empty() {
        let changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
        for c in &changes {
            super::triggers::fire_row_triggers(
                &mut wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::After,
                super::triggers::FireEvent::Update {
                    changed_columns: &changed_cols,
                },
                Some(c.old_row.clone()),
                Some(c.new_row.clone()),
                &table_schema.columns,
            )?;
        }
    }

    super::triggers::fire_statement_triggers(
        &mut wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::After,
        super::triggers::FireEvent::Update {
            changed_columns: &stmt_changed_cols,
        },
        &table_schema.columns,
        &stmt_old_rows,
        &stmt_new_rows,
    )?;

    if let Some(returning_cols) = stmt.returning.as_ref() {
        let rows: Vec<super::helpers::ReturningRow> = changes
            .iter()
            .map(|c| (Some(c.old_row.clone()), Some(c.new_row.clone())))
            .collect();
        let qr = super::helpers::project_returning(table_schema, returning_cols, &rows)?;
        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::Query(qr));
    }

    let count = changes.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

pub(super) fn exec_delete(
    db: &Database,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(schema, &user_name, super::triggers::FireEvent::Delete) {
            let aliases = view_def.column_aliases.clone();
            let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
            let r =
                exec_instead_of_view_delete_in_txn(&mut wtx, schema, &user_name, &aliases, stmt)?;
            wtx.commit().map_err(SqlError::Storage)?;
            return Ok(r);
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    if schema.get_matview(&user_name).is_some() {
        return Err(SqlError::CannotModifyView(format!(
            "materialized view '{}' is read-only — use REFRESH MATERIALIZED VIEW",
            stmt.table
        )));
    }
    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    schema.mark_dml(&table_schema.name);
    let lower_name = table_schema.name.clone();

    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: None,
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let select_stmt = SelectStmt {
            columns: vec![SelectColumn::AllColumns],
            from: stmt.table.clone(),
            from_alias: None,
            from_subquery: None,
            from_args: None,
            from_json_table: None,
            joins: vec![],
            distinct: false,
            where_clause: stmt.where_clause.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            group_by: vec![],
            having: None,
        };
        let (mut rows, _) = collect_rows_read(db, table_schema, &None, None)?;
        let remaining =
            handle_correlated_where_read(db, schema, &select_stmt, &corr_ctx, &mut rows)?;

        if let Some(ref w) = remaining {
            let col_map = ColumnMap::new(&table_schema.columns);
            rows.retain(|row| match eval_expr(w, &EvalCtx::new(&col_map, row)) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            });
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_col = &table_schema.columns[pk_indices[0]].name;
        let in_set: rustc_hash::FxHashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_col.clone())),
                values: in_set,
                has_null: false,
                negated: false,
            })
        };

        let rewritten = DeleteStmt {
            table: stmt.table.clone(),
            where_clause: new_where,
            returning: stmt.returning.clone(),
        };
        return exec_delete(db, schema, &rewritten);
    }

    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_read(db, schema, sub, &CteContext::default())
        })?;
        &materialized
    } else {
        stmt
    };

    let col_map = ColumnMap::new(&table_schema.columns);
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    super::ann_persist::purge_segment(&mut wtx, &lower_name)?;

    // Fast TRUNCATE path skips per-row firing; gate on no DELETE triggers (ROW + STATEMENT).
    let has_delete_triggers = schema.triggers_for(&table_schema.name).iter().any(|t| {
        t.enabled
            && (t.timing == crate::parser::TriggerTiming::After
                || t.timing == crate::parser::TriggerTiming::Before)
            && t.events
                .iter()
                .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
    });
    if stmt.where_clause.is_none()
        && schema.child_fks_for(&lower_name).is_empty()
        && stmt.returning.is_none()
        && !has_delete_triggers
    {
        let count = wtx
            .table_truncate(lower_name.as_bytes())
            .map_err(SqlError::Storage)?;
        for idx in &table_schema.indices {
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            wtx.table_truncate(&idx_table).map_err(SqlError::Storage)?;
        }
        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    let all_candidates = collect_keyed_rows_write(&mut wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &EvalCtx::new(&col_map, row)) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        if let Some(returning_cols) = stmt.returning.as_ref() {
            let qr = super::helpers::project_returning(table_schema, returning_cols, &[])?;
            super::helpers::drain_deferred_fk_checks(&mut wtx)?;
            wtx.commit().map_err(SqlError::Storage)?;
            return Ok(ExecutionResult::Query(qr));
        }
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();
    let has_child_fks = !schema.child_fks_for(&lower_name).is_empty();
    let mut deleted_pk_keys: Vec<Vec<u8>> = if has_child_fks {
        Vec::with_capacity(rows_to_delete.len())
    } else {
        Vec::new()
    };

    let before_delete_triggers: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
        })
        .cloned()
        .collect();
    if !before_delete_triggers.is_empty() {
        for (_, row) in &rows_to_delete {
            super::triggers::fire_row_triggers(
                &mut wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Delete,
                Some(row.clone()),
                None,
                &table_schema.columns,
            )?;
        }
    }

    let old_rows_for_stmt: Vec<Vec<Value>> =
        rows_to_delete.iter().map(|(_, r)| r.clone()).collect();
    super::triggers::fire_statement_triggers(
        &mut wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::Before,
        super::triggers::FireEvent::Delete,
        &table_schema.columns,
        &old_rows_for_stmt,
        &[],
    )?;

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(&mut wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
        if has_child_fks {
            deleted_pk_keys.push(encode_composite_key(&pk_values));
        }
    }

    if has_child_fks {
        cascade_after_parent_delete(&mut wtx, schema, &lower_name, &deleted_pk_keys)?;
    }

    let after_delete_triggers: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::After
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
        })
        .cloned()
        .collect();
    if !after_delete_triggers.is_empty() {
        for (_, row) in &rows_to_delete {
            super::triggers::fire_row_triggers(
                &mut wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::After,
                super::triggers::FireEvent::Delete,
                Some(row.clone()),
                None,
                &table_schema.columns,
            )?;
        }
    }

    super::triggers::fire_statement_triggers(
        &mut wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::After,
        super::triggers::FireEvent::Delete,
        &table_schema.columns,
        &old_rows_for_stmt,
        &[],
    )?;

    if let Some(returning_cols) = stmt.returning.as_ref() {
        let rows: Vec<super::helpers::ReturningRow> = rows_to_delete
            .iter()
            .map(|(_, row)| (Some(row.clone()), None))
            .collect();
        let qr = super::helpers::project_returning(table_schema, returning_cols, &rows)?;
        super::helpers::drain_deferred_fk_checks(&mut wtx)?;
        wtx.commit().map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::Query(qr));
    }

    let count = rows_to_delete.len() as u64;
    wtx.commit().map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn has_derived_in_stmt(stmt: &SelectStmt) -> bool {
    stmt.from_subquery.is_some() || stmt.joins.iter().any(|j| j.subquery.is_some())
}

pub(super) fn exec_select_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &SelectStmt,
    ctes: &CteContext,
) -> Result<ExecutionResult> {
    if stmt.from.is_empty() && stmt.from_subquery.is_none() {
        let materialized;
        let stmt = if stmt_has_subquery(stmt) {
            materialized =
                materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
            &materialized
        } else {
            stmt
        };
        return super::exec_select_no_from(stmt);
    }

    if stmt
        .joins
        .iter()
        .any(|j| j.subquery.as_ref().is_some_and(|s| s.lateral))
    {
        return super::select::exec_select_lateral_in_txn(wtx, schema, stmt, ctes);
    }
    if has_derived_in_stmt(stmt) {
        let mut new_ctes = ctes.clone();
        let mut new_stmt = stmt.clone();
        if let Some(d) = stmt.from_subquery.as_ref() {
            let inner_body = match &d.query.body {
                QueryBody::Select(s) => s.as_ref(),
                _ => return Err(SqlError::Unsupported("derived must be SELECT".into())),
            };
            let qr = match super::exec_select_in_txn(wtx, schema, inner_body, ctes)? {
                ExecutionResult::Query(qr) => qr,
                _ => return Err(SqlError::Unsupported("derived returned non-Query".into())),
            };
            new_ctes.insert(d.alias.to_ascii_lowercase(), qr);
            new_stmt.from = d.alias.clone();
            new_stmt.from_alias = None;
            new_stmt.from_subquery = None;
        }
        for j in new_stmt.joins.iter_mut() {
            if let Some(d) = j.subquery.take() {
                let inner_body = match &d.query.body {
                    QueryBody::Select(s) => s.as_ref(),
                    _ => return Err(SqlError::Unsupported("derived must be SELECT".into())),
                };
                let qr = match super::exec_select_in_txn(wtx, schema, inner_body, ctes)? {
                    ExecutionResult::Query(qr) => qr,
                    _ => return Err(SqlError::Unsupported("derived returned non-Query".into())),
                };
                new_ctes.insert(d.alias.to_ascii_lowercase(), qr);
                j.table = crate::parser::TableRef {
                    name: d.alias.clone(),
                    alias: None,
                    args: None,
                };
            }
        }
        return super::exec_select_in_txn(wtx, schema, &new_stmt, &new_ctes);
    }

    let lower_name = stmt.from.to_ascii_lowercase();

    if let Some(cte_result) = ctes.get(&lower_name) {
        if stmt.joins.is_empty() {
            return super::exec_select_from_cte(cte_result, stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            });
        } else {
            return super::exec_select_join_with_ctes(stmt, ctes, &mut |name| {
                super::scan_table_write(wtx, schema, name)
            });
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return super::exec_select_join_with_ctes(stmt, ctes, &mut |name| {
            super::scan_table_write_or_view(wtx, schema, name)
        });
    }

    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return super::exec_select_in_txn(wtx, schema, &fused, ctes);
        }
        let view_qr = exec_view_write(wtx, schema, view_def)?;
        if stmt.joins.is_empty() {
            return super::exec_select_from_cte(&view_qr, stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            });
        } else {
            let mut view_ctes = ctes.clone();
            view_ctes.insert(lower_name.clone(), view_qr);
            return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
                super::scan_table_write_or_view(wtx, schema, name)
            });
        }
    }

    let any_join_view = stmt.joins.iter().any(|j| {
        schema
            .get_view(&j.table.name.to_ascii_lowercase())
            .is_some()
    });
    if any_join_view {
        let mut view_ctes = ctes.clone();
        for j in &stmt.joins {
            let jname = j.table.name.to_ascii_lowercase();
            if let Some(vd) = schema.get_view(&jname) {
                if let std::collections::hash_map::Entry::Vacant(e) = view_ctes.entry(jname) {
                    let vqr = exec_view_write(wtx, schema, vd)?;
                    e.insert(vqr);
                }
            }
        }
        return super::exec_select_join_with_ctes(stmt, &view_ctes, &mut |name| {
            super::scan_table_write(wtx, schema, name)
        });
    }

    if !stmt.joins.is_empty() {
        return super::exec_select_join_in_txn(wtx, schema, stmt);
    }

    let user_name = stmt.from.to_ascii_lowercase();
    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.from.clone()))?;
    let lower_name = table_schema.name.clone();

    let corr_ctx = CorrelationCtx {
        outer_schema: table_schema,
        outer_alias: stmt.from_alias.as_deref(),
    };
    if has_correlated_where(&stmt.where_clause, &corr_ctx, schema) {
        let (mut rows, _) = collect_rows_write(wtx, table_schema, &None, None)?;
        let remaining_where =
            handle_correlated_where_write(wtx, schema, stmt, &corr_ctx, &mut rows)?;
        let clean_stmt = SelectStmt {
            where_clause: remaining_where,
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            from_alias: stmt.from_alias.clone(),
            from_subquery: stmt.from_subquery.clone(),
            from_args: stmt.from_args.clone(),
            from_json_table: stmt.from_json_table.clone(),
            joins: stmt.joins.clone(),
            distinct: stmt.distinct,
            order_by: stmt.order_by.clone(),
            limit: stmt.limit.clone(),
            offset: stmt.offset.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
        };
        let final_stmt;
        let s = if stmt_has_subquery(&clean_stmt) {
            final_stmt = materialize_stmt(&clean_stmt, &mut |sub| {
                exec_subquery_write(wtx, schema, sub, ctes)
            })?;
            &final_stmt
        } else {
            &clean_stmt
        };
        return super::process_select(&table_schema.columns, rows, s, false);
    }

    let materialized;
    let stmt = if stmt_has_subquery(stmt) {
        materialized =
            materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
        &materialized
    } else {
        stmt
    };

    if let Some(result) = try_count_star_shortcut(stmt, || {
        wtx.table_entry_count(lower_name.as_bytes())
            .map_err(SqlError::Storage)
    })? {
        return Ok(result);
    }

    if let Some(plan) = StreamAggPlan::try_new(stmt, table_schema)? {
        let mut states: Vec<AggState> = plan.ops.iter().map(|(op, _)| AggState::new(op)).collect();
        let mut scan_err: Option<SqlError> = None;
        if stmt.where_clause.is_none() {
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row_raw(key, value, &mut states, &mut scan_err))
            })
            .map_err(SqlError::Storage)?;
        } else {
            let col_map = ColumnMap::new(&table_schema.columns);
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row(
                    key,
                    value,
                    table_schema,
                    &col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                ))
            })
            .map_err(SqlError::Storage)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        return Ok(plan.finish(states));
    }

    if let Some(plan) = StreamGroupByPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(|cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    if let Some(plan) = super::ann_topk::VectorTopKPlan::try_new(stmt, table_schema)? {
        return plan.execute(wtx, table_schema, stmt);
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(table_schema, stmt, |cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    let scan_limit = compute_scan_limit(stmt);
    let (rows, predicate_applied) =
        collect_rows_write(wtx, table_schema, &stmt.where_clause, scan_limit)?;
    super::process_select(&table_schema.columns, rows, stmt, predicate_applied)
}

fn exec_update_in_txn_compiled(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
    compiled: &CompiledUpdate,
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    if compiled.is_view {
        return exec_update_in_txn(wtx, schema, stmt);
    }
    if compiled.has_correlated_where || compiled.has_subquery || !compiled.can_fast_path {
        return exec_update_in_txn(wtx, schema, stmt);
    }
    let fast = match &compiled.fast {
        Some(f) => f,
        None => return exec_update_in_txn(wtx, schema, stmt),
    };

    let table_schema = schema
        .get(&compiled.table_name_lower)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    if stmt.returning.is_some() {
        return exec_update_in_txn(wtx, schema, stmt);
    }

    let single_int_pk = fast.single_int_pk;
    let num_pk_cols = fast.num_pk_cols;
    let pk_idx_cache = &fast.pk_idx_cache;
    let col_map = &fast.col_map;
    let targets = &fast.targets;
    let gen_targets = &fast.gen_targets;
    let gen_extra_cols = &fast.gen_extra_cols;

    bufs.partial_row.clear();
    bufs.partial_row.resize(fast.num_columns, Value::Null);

    if let Some(ref pkl) = fast.pk_lookup_fast {
        let pk_value = match &pkl.source {
            PkLookupSource::Literal(v) => v.clone(),
            PkLookupSource::Parameter(n) => crate::eval::resolve_scoped_param(*n)?,
        };
        return exec_pk_lookup_update(
            wtx,
            &compiled.table_name_lower,
            &pk_value,
            pk_idx_cache,
            col_map,
            targets,
            gen_targets,
            gen_extra_cols,
            bufs,
        );
    }

    let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);

    let set_cols_safe = targets
        .iter()
        .all(|t| !t.col.nullable && is_fixed_width_type(t.col.data_type));
    let gen_cols_safe = gen_targets
        .iter()
        .all(|g| !g.col.nullable && is_fixed_width_type(g.col.data_type));
    let patch_safe = set_cols_safe && gen_cols_safe;

    if let (
        true,
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            ..
        },
    ) = (patch_safe, &plan)
    {
        let range_conds = range_conds.clone();
        let partial_row = &mut bufs.partial_row;
        let patch_buf = &mut bufs.patch_buf;

        let count = wtx.table_update_range::<_, SqlError>(
            compiled.table_name_lower.as_bytes(),
            start_key,
            |key, value| {
                if single_int_pk {
                    let pk_int = Value::Integer(decode_pk_integer(key)?);
                    for (op, bound) in &range_conds {
                        match op {
                            BinOp::Lt if &pk_int >= bound => return Ok(None),
                            BinOp::LtEq if &pk_int > bound => return Ok(None),
                            BinOp::Gt if &pk_int <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_int < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    partial_row[pk_idx_cache[0]] = pk_int;
                } else {
                    let pk_vals = decode_composite_key(key, num_pk_cols)?;
                    for (op, bound) in &range_conds {
                        match op {
                            BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                            BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                            BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                    for (i, &pi) in pk_idx_cache.iter().enumerate() {
                        partial_row[pi] = pk_vals[i].clone();
                    }
                }
                for target in targets {
                    partial_row[target.schema_idx] =
                        decode_column_raw(value, target.phys_idx)?.to_value();
                }
                for target in targets {
                    let new_val = compiled_target_eval(target, partial_row, col_map)?;
                    let coerced = coerce_gen_value(new_val, &target.col)?;
                    if !patch_column_in_place(value, target.phys_idx, &coerced)? {
                        patch_row_column(value, target.phys_idx, &coerced, patch_buf)?;
                        value[..patch_buf.len()].copy_from_slice(patch_buf);
                    }
                    if targets.len() == 1 {
                        partial_row[target.schema_idx] = coerced;
                    }
                }
                apply_gen_col_patches_slice(
                    value,
                    partial_row,
                    gen_targets,
                    gen_extra_cols,
                    col_map,
                    patch_buf,
                )?;
                Ok(Some(true))
            },
        )?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    if let crate::planner::ScanPlan::PkLookup { pk_values } = &plan {
        let key = encode_composite_key(pk_values);
        let mut raw_value = match wtx
            .table_get(compiled.table_name_lower.as_bytes(), &key)
            .map_err(SqlError::Storage)?
        {
            Some(v) => v,
            None => return Ok(ExecutionResult::RowsAffected(0)),
        };
        let partial_row = &mut bufs.partial_row;
        let patch_buf = &mut bufs.patch_buf;
        if single_int_pk {
            partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(&key)?);
        } else {
            let pk_vals = decode_composite_key(&key, num_pk_cols)?;
            for (i, &pi) in pk_idx_cache.iter().enumerate() {
                partial_row[pi] = pk_vals[i].clone();
            }
        }
        for target in targets {
            partial_row[target.schema_idx] =
                decode_column_raw(&raw_value, target.phys_idx)?.to_value();
        }
        for target in targets {
            let new_val = compiled_target_eval(target, partial_row, col_map)?;
            let coerced = coerce_gen_value(new_val, &target.col)?;
            if !patch_column_in_place(&mut raw_value, target.phys_idx, &coerced)? {
                patch_row_column(&raw_value, target.phys_idx, &coerced, patch_buf)?;
                std::mem::swap(&mut raw_value, patch_buf);
            }
            if targets.len() == 1 {
                partial_row[target.schema_idx] = coerced;
            }
        }
        apply_gen_col_patches_vec(
            &mut raw_value,
            partial_row,
            gen_targets,
            gen_extra_cols,
            col_map,
            patch_buf,
        )?;
        wtx.table_insert(compiled.table_name_lower.as_bytes(), &key, &raw_value)
            .map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(1));
    }

    bufs.kv_pairs.clear();
    bufs.patched.clear();
    match &plan {
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            ..
        } => {
            let range_conds = range_conds.clone();
            let mut scan_err: Option<SqlError> = None;
            let kv_pairs = &mut bufs.kv_pairs;
            wtx.table_scan_from(
                compiled.table_name_lower.as_bytes(),
                start_key,
                |key, value| {
                    let in_range = range_in_bounds(
                        key,
                        single_int_pk,
                        num_pk_cols,
                        &range_conds,
                        &mut scan_err,
                    );
                    match in_range {
                        RangeStatus::Stop => Ok(false),
                        RangeStatus::Skip => Ok(true),
                        RangeStatus::Hit => {
                            kv_pairs.push((key.to_vec(), value.to_vec()));
                            Ok(true)
                        }
                        RangeStatus::Err => Ok(false),
                    }
                },
            )
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
        }
        crate::planner::ScanPlan::SeqScan => {
            let kv_pairs = &mut bufs.kv_pairs;
            wtx.table_for_each(compiled.table_name_lower.as_bytes(), |key, value| {
                kv_pairs.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .map_err(SqlError::Storage)?;
        }
        _ => return exec_update_in_txn(wtx, schema, stmt),
    }

    let partial_row = &mut bufs.partial_row;
    let patch_buf = &mut bufs.patch_buf;

    for (key, raw_value) in bufs.kv_pairs.iter_mut() {
        if matches!(plan, crate::planner::ScanPlan::SeqScan) {
            if let Some(ref w) = stmt.where_clause {
                let row = decode_full_row(table_schema, key, raw_value)?;
                if !eval_expr(w, &EvalCtx::new(col_map, &row)).is_ok_and(|v| is_truthy(&v)) {
                    continue;
                }
            }
        }
        if single_int_pk {
            partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
        } else {
            let pk_vals = decode_composite_key(key, num_pk_cols)?;
            for (i, &pi) in pk_idx_cache.iter().enumerate() {
                partial_row[pi] = pk_vals[i].clone();
            }
        }
        for target in targets {
            partial_row[target.schema_idx] =
                decode_column_raw(raw_value, target.phys_idx)?.to_value();
        }
        for target in targets {
            let new_val = compiled_target_eval(target, partial_row, col_map)?;
            let coerced = coerce_gen_value(new_val, &target.col)?;
            if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                patch_row_column(raw_value, target.phys_idx, &coerced, patch_buf)?;
                std::mem::swap(raw_value, patch_buf);
            }
            if targets.len() == 1 {
                partial_row[target.schema_idx] = coerced;
            }
        }
        apply_gen_col_patches_vec(
            raw_value,
            partial_row,
            gen_targets,
            gen_extra_cols,
            col_map,
            patch_buf,
        )?;
        bufs.patched
            .push((std::mem::take(key), std::mem::take(raw_value)));
    }

    if !bufs.patched.is_empty() {
        let refs: Vec<(&[u8], &[u8])> = bufs
            .patched
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        wtx.table_update_sorted(compiled.table_name_lower.as_bytes(), &refs)
            .map_err(SqlError::Storage)?;
    }
    Ok(ExecutionResult::RowsAffected(bufs.patched.len() as u64))
}

#[allow(clippy::too_many_arguments)]
fn exec_pk_lookup_update(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_name_lower: &str,
    pk_value: &Value,
    pk_idx_cache: &[usize],
    col_map: &ColumnMap,
    targets: &[CompiledTarget],
    gen_targets: &[GenColPatch],
    gen_extra_cols: &[(usize, usize)],
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    let key = encode_composite_key(std::slice::from_ref(pk_value));
    let mut raw_value = match wtx
        .table_get(table_name_lower.as_bytes(), &key)
        .map_err(SqlError::Storage)?
    {
        Some(v) => v,
        None => return Ok(ExecutionResult::RowsAffected(0)),
    };
    let partial_row = &mut bufs.partial_row;
    let patch_buf = &mut bufs.patch_buf;
    partial_row[pk_idx_cache[0]] = pk_value.clone();
    for target in targets {
        partial_row[target.schema_idx] = decode_column_raw(&raw_value, target.phys_idx)?.to_value();
    }
    for target in targets {
        let new_val = compiled_target_eval(target, partial_row, col_map)?;
        let coerced = coerce_gen_value(new_val, &target.col)?;
        if !patch_column_in_place(&mut raw_value, target.phys_idx, &coerced)? {
            patch_row_column(&raw_value, target.phys_idx, &coerced, patch_buf)?;
            std::mem::swap(&mut raw_value, patch_buf);
        }
        if targets.len() == 1 {
            partial_row[target.schema_idx] = coerced;
        }
    }
    apply_gen_col_patches_vec(
        &mut raw_value,
        partial_row,
        gen_targets,
        gen_extra_cols,
        col_map,
        patch_buf,
    )?;
    wtx.table_insert(table_name_lower.as_bytes(), &key, &raw_value)
        .map_err(SqlError::Storage)?;
    Ok(ExecutionResult::RowsAffected(1))
}

fn compiled_target_eval(
    target: &CompiledTarget,
    partial_row: &[Value],
    col_map: &ColumnMap,
) -> Result<Value> {
    let generic = || eval_expr(&target.expr, &EvalCtx::new(col_map, partial_row));
    match target.fast_eval {
        FastEval::IntAdd(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_add(n))),
            _ => generic(),
        },
        FastEval::IntSub(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_sub(n))),
            _ => generic(),
        },
        FastEval::IntMul(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_mul(n))),
            _ => generic(),
        },
        FastEval::IntSet(n) => Ok(Value::Integer(n)),
        FastEval::IntAddParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => Ok(Value::Integer(v.wrapping_add(n))),
            _ => generic(),
        },
        FastEval::IntSubParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => Ok(Value::Integer(v.wrapping_sub(n))),
            _ => generic(),
        },
        FastEval::IntMulParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => Ok(Value::Integer(v.wrapping_mul(n))),
            _ => generic(),
        },
        FastEval::IntSetParam(p) => match resolve_int_param(p) {
            Some(n) => Ok(Value::Integer(n)),
            None => generic(),
        },
        FastEval::None => generic(),
    }
}

fn try_fast_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
    table_schema: &TableSchema,
    col_map: &ColumnMap,
) -> Result<Option<ExecutionResult>> {
    let lower_name = stmt.table.to_ascii_lowercase();
    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });
    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&lower_name).is_empty();
    if pk_changed_by_set
        || has_fk
        || has_indices
        || has_child_fk
        || table_schema.has_checks()
        || stmt.returning.is_some()
    {
        return Ok(None);
    }

    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let num_pk_cols = table_schema.primary_key_columns.len();

    struct AssignTarget {
        schema_idx: usize,
        phys_idx: usize,
        expr: Expr,
        col: ColumnDef,
    }
    let mut targets: Vec<AssignTarget> = Vec::with_capacity(stmt.assignments.len());
    for (col_name, expr) in &stmt.assignments {
        let schema_idx = table_schema
            .column_index(col_name)
            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
        let col = &table_schema.columns[schema_idx];
        if col.generated_kind.is_some() {
            return Err(SqlError::CannotUpdateGeneratedColumn(col.name.clone()));
        }
        let nonpk_order = non_pk
            .iter()
            .position(|&i| i == schema_idx)
            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
        let phys_idx = enc_pos[nonpk_order] as usize;
        targets.push(AssignTarget {
            schema_idx,
            phys_idx,
            expr: expr.clone(),
            col: col.clone(),
        });
    }

    let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);
    let single_int_pk = num_pk_cols == 1
        && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
            == DataType::Integer;

    let pk_idx_cache = table_schema.pk_indices().to_vec();
    let set_target_indices: Vec<usize> = targets.iter().map(|t| t.schema_idx).collect();
    let (gen_targets, gen_extra_cols) =
        compute_gen_col_targets(table_schema, &set_target_indices, &pk_idx_cache);

    let set_cols: Vec<ColumnDef> = targets.iter().map(|t| t.col.clone()).collect();
    let gen_cols: Vec<ColumnDef> = gen_targets.iter().map(|g| g.col.clone()).collect();
    let patch_safe = pk_range_patch_safe(&set_cols, &gen_cols);

    if let (
        true,
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            ..
        },
    ) = (patch_safe, &plan)
    {
        let range_conds = range_conds.clone();
        let mut partial_row = vec![Value::Null; table_schema.columns.len()];
        let mut patch_buf: Vec<u8> = Vec::with_capacity(256);

        let count = wtx.table_update_range::<_, SqlError>(
            lower_name.as_bytes(),
            start_key,
            |key, value| {
                if single_int_pk {
                    let pk_int = Value::Integer(decode_pk_integer(key)?);
                    for (op, bound) in &range_conds {
                        match op {
                            BinOp::Lt if &pk_int >= bound => return Ok(None),
                            BinOp::LtEq if &pk_int > bound => return Ok(None),
                            BinOp::Gt if &pk_int <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_int < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                } else {
                    let pk_vals = decode_composite_key(key, num_pk_cols)?;
                    for (op, bound) in &range_conds {
                        match op {
                            BinOp::Lt if &pk_vals[0] >= bound => return Ok(None),
                            BinOp::LtEq if &pk_vals[0] > bound => return Ok(None),
                            BinOp::Gt if &pk_vals[0] <= bound => return Ok(Some(false)),
                            BinOp::GtEq if &pk_vals[0] < bound => return Ok(Some(false)),
                            _ => {}
                        }
                    }
                }

                if single_int_pk {
                    partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
                } else {
                    let pk_vals = decode_composite_key(key, num_pk_cols)?;
                    for (i, &pi) in pk_idx_cache.iter().enumerate() {
                        partial_row[pi] = pk_vals[i].clone();
                    }
                }
                for target in &targets {
                    partial_row[target.schema_idx] =
                        decode_column_raw(value, target.phys_idx)?.to_value();
                }
                for target in &targets {
                    let new_val = eval_expr(&target.expr, &EvalCtx::new(col_map, &partial_row))?;
                    let coerced = coerce_gen_value(new_val, &target.col)?;
                    partial_row[target.schema_idx] = coerced.clone();
                    if !patch_column_in_place(value, target.phys_idx, &coerced)? {
                        patch_row_column(value, target.phys_idx, &coerced, &mut patch_buf)?;
                        value[..patch_buf.len()].copy_from_slice(&patch_buf);
                    }
                }
                apply_gen_col_patches_slice(
                    value,
                    &mut partial_row,
                    &gen_targets,
                    &gen_extra_cols,
                    col_map,
                    &mut patch_buf,
                )?;
                Ok(Some(true))
            },
        )?;
        return Ok(Some(ExecutionResult::RowsAffected(count)));
    }

    let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    match &plan {
        crate::planner::ScanPlan::PkLookup { pk_values } => {
            let key = encode_composite_key(pk_values);
            if let Some(value) = wtx
                .table_get(lower_name.as_bytes(), &key)
                .map_err(SqlError::Storage)?
            {
                kv_pairs.push((key, value));
            }
        }
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            ..
        } => {
            let range_conds = range_conds.clone();
            let mut scan_err: Option<SqlError> = None;
            wtx.table_scan_from(lower_name.as_bytes(), start_key, |key, value| {
                let in_range =
                    range_in_bounds(key, single_int_pk, num_pk_cols, &range_conds, &mut scan_err);
                match in_range {
                    RangeStatus::Stop => Ok(false),
                    RangeStatus::Skip => Ok(true),
                    RangeStatus::Hit => {
                        kv_pairs.push((key.to_vec(), value.to_vec()));
                        Ok(true)
                    }
                    RangeStatus::Err => Ok(false),
                }
            })
            .map_err(SqlError::Storage)?;
            if let Some(e) = scan_err {
                return Err(e);
            }
        }
        crate::planner::ScanPlan::SeqScan => {
            wtx.table_for_each(lower_name.as_bytes(), |key, value| {
                kv_pairs.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .map_err(SqlError::Storage)?;
        }
        _ => return Ok(None),
    }

    let mut patch_buf: Vec<u8> = Vec::with_capacity(256);
    let mut partial_row = vec![Value::Null; table_schema.columns.len()];
    let mut patched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(kv_pairs.len());

    for (key, raw_value) in &mut kv_pairs {
        if matches!(plan, crate::planner::ScanPlan::SeqScan) {
            if let Some(ref w) = stmt.where_clause {
                let row = decode_full_row(table_schema, key, raw_value)?;
                if !eval_expr(w, &EvalCtx::new(col_map, &row)).is_ok_and(|v| is_truthy(&v)) {
                    continue;
                }
            }
        }
        if single_int_pk {
            partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
        } else {
            let pk_vals = decode_composite_key(key, num_pk_cols)?;
            for (i, &pi) in pk_idx_cache.iter().enumerate() {
                partial_row[pi] = pk_vals[i].clone();
            }
        }
        for target in &targets {
            partial_row[target.schema_idx] =
                decode_column_raw(raw_value, target.phys_idx)?.to_value();
        }
        for target in &targets {
            let new_val = eval_expr(&target.expr, &EvalCtx::new(col_map, &partial_row))?;
            let coerced = coerce_gen_value(new_val, &target.col)?;
            partial_row[target.schema_idx] = coerced.clone();
            if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                patch_row_column(raw_value, target.phys_idx, &coerced, &mut patch_buf)?;
                std::mem::swap(raw_value, &mut patch_buf);
            }
        }
        apply_gen_col_patches_vec(
            raw_value,
            &mut partial_row,
            &gen_targets,
            &gen_extra_cols,
            col_map,
            &mut patch_buf,
        )?;
        patched.push((std::mem::take(key), std::mem::take(raw_value)));
    }

    if !patched.is_empty() {
        let refs: Vec<(&[u8], &[u8])> = patched
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        wtx.table_update_sorted(lower_name.as_bytes(), &refs)
            .map_err(SqlError::Storage)?;
    }
    let count = patched.len() as u64;
    Ok(Some(ExecutionResult::RowsAffected(count)))
}

pub(super) fn exec_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if update_has_subquery(stmt) {
        materialized = materialize_update(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &CteContext::default())
        })?;
        &materialized
    } else {
        stmt
    };

    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(
            schema,
            &user_name,
            super::triggers::FireEvent::Update {
                changed_columns: &[],
            },
        ) {
            let aliases = view_def.column_aliases.clone();
            return exec_instead_of_view_update_in_txn(wtx, schema, &user_name, &aliases, stmt);
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    schema.mark_dml(&table_schema.name);
    super::ann_persist::purge_segment(wtx, &table_schema.name)?;
    let lower_name = table_schema.name.clone();
    let strict = table_schema.is_strict();

    let col_map = ColumnMap::new(&table_schema.columns);

    if let Some(result) = try_fast_update_in_txn(wtx, schema, stmt, table_schema, &col_map)? {
        return Ok(result);
    }

    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let matching_rows: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &EvalCtx::new(&col_map, row)) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if matching_rows.is_empty() {
        if let Some(returning_cols) = stmt.returning.as_ref() {
            let qr = super::helpers::project_returning(table_schema, returning_cols, &[])?;
            return Ok(ExecutionResult::Query(qr));
        }
        return Ok(ExecutionResult::RowsAffected(0));
    }

    struct UpdateChange {
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_value: Vec<u8>,
        pk_changed: bool,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    }

    let pk_indices = table_schema.pk_indices();
    let mut changes: Vec<UpdateChange> = Vec::new();

    let stored_gen_cols: Vec<&ColumnDef> = table_schema
        .columns
        .iter()
        .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
        .collect();
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let phys_count = table_schema.physical_non_pk_count();
    let mut value_values = vec![Value::Null; phys_count];

    for (old_key, row) in &matching_rows {
        let mut new_row = row.clone();
        let mut pk_changed = false;

        // Evaluate all SET expressions against the original row (SQL standard).
        let mut evaluated: Vec<(usize, Value)> = Vec::with_capacity(stmt.assignments.len());
        for (col_name, expr) in &stmt.assignments {
            let col_idx = table_schema
                .column_index(col_name)
                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
            let col = &table_schema.columns[col_idx];
            if col.generated_kind.is_some() {
                return Err(SqlError::CannotUpdateGeneratedColumn(col.name.clone()));
            }
            let new_val = eval_expr(expr, &EvalCtx::new(&col_map, row))?;

            let coerced = if new_val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
                Value::Null
            } else {
                super::helpers::coerce_for_column(new_val, col, strict)?
            };

            evaluated.push((col_idx, coerced));
        }

        for (col_idx, coerced) in evaluated {
            if table_schema.primary_key_columns.contains(&(col_idx as u16)) {
                pk_changed = true;
            }
            new_row[col_idx] = coerced;
        }

        for col in &stored_gen_cols {
            let val = eval_expr(
                col.generated_expr.as_ref().unwrap(),
                &EvalCtx::new(&col_map, &new_row),
            )?;
            let pos = col.position as usize;
            new_row[pos] = if val.is_null() {
                if !col.nullable {
                    return Err(SqlError::NotNullViolation(col.name.clone()));
                }
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

        if table_schema.has_checks() {
            for col in &table_schema.columns {
                if let Some(ref check) = col.check_expr {
                    let result = eval_expr(check, &EvalCtx::new(&col_map, &new_row))?;
                    if !is_truthy(&result) && !result.is_null() {
                        let name = col.check_name.as_deref().unwrap_or(&col.name);
                        return Err(SqlError::CheckViolation(name.to_string()));
                    }
                }
            }
            for tc in &table_schema.check_constraints {
                let result = eval_expr(&tc.expr, &EvalCtx::new(&col_map, &new_row))?;
                if !is_truthy(&result) && !result.is_null() {
                    let name = tc.name.as_deref().unwrap_or(&tc.sql);
                    return Err(SqlError::CheckViolation(name.to_string()));
                }
            }
        }

        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| new_row[i].clone()).collect();
        let new_key = encode_composite_key(&pk_values);

        for v in value_values.iter_mut() {
            *v = Value::Null;
        }
        for (j, &i) in non_pk.iter().enumerate() {
            let col = &table_schema.columns[i];
            value_values[enc_pos[j] as usize] = if matches!(
                col.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            ) {
                Value::Null
            } else {
                new_row[i].clone()
            };
        }
        let new_value = encode_row(&value_values);

        changes.push(UpdateChange {
            old_key: old_key.clone(),
            new_key,
            new_value,
            pk_changed,
            old_row: row.clone(),
            new_row,
        });
    }

    {
        let mut new_keys: rustc_hash::FxHashSet<Vec<u8>> = rustc_hash::FxHashSet::default();
        for c in &changes {
            if c.pk_changed && c.new_key != c.old_key && !new_keys.insert(c.new_key.clone()) {
                return Err(SqlError::DuplicateKey);
            }
        }
    }

    if !table_schema.foreign_keys.is_empty() {
        for c in &changes {
            for fk in &table_schema.foreign_keys {
                let fk_changed = fk
                    .columns
                    .iter()
                    .any(|&ci| c.old_row[ci as usize] != c.new_row[ci as usize]);
                if !fk_changed {
                    continue;
                }
                let any_null = fk
                    .columns
                    .iter()
                    .any(|&ci| c.new_row[ci as usize].is_null());
                if any_null {
                    continue;
                }
                let fk_vals: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|&ci| c.new_row[ci as usize].clone())
                    .collect();
                let fk_key = encode_composite_key(&fk_vals);
                if fk.deferrable && fk.initially_deferred {
                    let name = fk.name.as_deref().unwrap_or(&fk.foreign_table).to_string();
                    wtx.defer_fk_check(citadel_txn::write_txn::DeferredFkCheck {
                        fk_name: name,
                        foreign_table: fk.foreign_table.as_bytes().to_vec(),
                        parent_key: fk_key,
                    });
                    continue;
                }
                if !wtx.fk_check_cached(fk.foreign_table.as_bytes(), &fk_key) {
                    let found = wtx
                        .table_get(fk.foreign_table.as_bytes(), &fk_key)
                        .map_err(SqlError::Storage)?;
                    if found.is_none() {
                        let name = fk.name.as_deref().unwrap_or(&fk.foreign_table);
                        return Err(SqlError::ForeignKeyViolation(name.to_string()));
                    }
                    wtx.mark_fk_verified(fk.foreign_table.as_bytes(), &fk_key);
                }
            }
        }
    }

    if !schema.child_fks_for(&lower_name).is_empty() {
        let parent_changes: Vec<(Vec<u8>, Vec<Value>, Vec<Value>)> = changes
            .iter()
            .map(|c| {
                let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();
                (
                    encode_composite_key(&old_pk),
                    c.old_row.clone(),
                    c.new_row.clone(),
                )
            })
            .collect();
        cascade_after_parent_update(
            &mut *wtx,
            schema,
            &lower_name,
            table_schema,
            &parent_changes,
        )?;
    }

    let before_update_triggers_in_txn: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Update(_)))
        })
        .cloned()
        .collect();
    let stmt_changed_cols_in_txn: Vec<String> =
        stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
    let stmt_old_rows_in_txn: Vec<Vec<Value>> = changes.iter().map(|c| c.old_row.clone()).collect();
    let stmt_new_rows_in_txn: Vec<Vec<Value>> = changes.iter().map(|c| c.new_row.clone()).collect();
    super::triggers::fire_statement_triggers(
        wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::Before,
        super::triggers::FireEvent::Update {
            changed_columns: &stmt_changed_cols_in_txn,
        },
        &table_schema.columns,
        &stmt_old_rows_in_txn,
        &stmt_new_rows_in_txn,
    )?;

    if !before_update_triggers_in_txn.is_empty() {
        let changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
        for c in &changes {
            super::triggers::fire_row_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Update {
                    changed_columns: &changed_cols,
                },
                Some(c.old_row.clone()),
                Some(c.new_row.clone()),
                &table_schema.columns,
            )?;
        }
    }

    let col_map_partial = any_partial_index(table_schema).then(|| table_schema.column_map());

    for c in &changes {
        let old_pk: Vec<Value> = pk_indices.iter().map(|&i| c.old_row[i].clone()).collect();

        for idx in &table_schema.indices {
            let cols_changed = index_columns_changed(idx, &c.old_row, &c.new_row);
            let (del, _) = partial_idx_update_actions(
                idx,
                &c.old_row,
                &c.new_row,
                cols_changed,
                c.pk_changed,
                col_map_partial,
            );
            if !del {
                continue;
            }
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            match idx.kind {
                crate::types::IndexKind::BTree => {
                    let old_idx_key =
                        encode_index_key_with_schema(idx, &c.old_row, &old_pk, table_schema);
                    wtx.table_delete(&idx_table, &old_idx_key)
                        .map_err(SqlError::Storage)?;
                }
                crate::types::IndexKind::Inverted(inv_kind) => {
                    let col0 = idx.column_positions_iter().next().ok_or_else(|| {
                        SqlError::Unsupported(
                            "inverted index requires at least one column key".into(),
                        )
                    })? as usize;
                    let entries =
                        super::helpers::extract_inverted_entries(&c.old_row[col0], inv_kind)?;
                    let pk_encoded = encode_composite_key(&old_pk);
                    for entry in entries {
                        let full_key = super::helpers::build_inverted_key(&entry, &pk_encoded);
                        wtx.table_delete(&idx_table, &full_key)
                            .map_err(SqlError::Storage)?;
                    }
                }
            }
        }

        if c.pk_changed {
            wtx.table_delete(lower_name.as_bytes(), &c.old_key)
                .map_err(SqlError::Storage)?;
        }
    }

    for c in &changes {
        let new_pk: Vec<Value> = pk_indices.iter().map(|&i| c.new_row[i].clone()).collect();

        if c.pk_changed {
            let is_new = wtx
                .table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
            if !is_new {
                return Err(SqlError::DuplicateKey);
            }
        } else {
            wtx.table_insert(lower_name.as_bytes(), &c.new_key, &c.new_value)
                .map_err(SqlError::Storage)?;
        }

        for idx in &table_schema.indices {
            let cols_changed = index_columns_changed(idx, &c.old_row, &c.new_row);
            let (_, ins) = partial_idx_update_actions(
                idx,
                &c.old_row,
                &c.new_row,
                cols_changed,
                c.pk_changed,
                col_map_partial,
            );
            if !ins {
                continue;
            }
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            match idx.kind {
                crate::types::IndexKind::BTree => {
                    let new_idx_key =
                        encode_index_key_with_schema(idx, &c.new_row, &new_pk, table_schema);
                    let new_idx_val = encode_index_value(idx, &c.new_row, &new_pk);
                    let is_new = wtx
                        .table_insert(&idx_table, &new_idx_key, &new_idx_val)
                        .map_err(SqlError::Storage)?;
                    if idx.unique && !is_new {
                        let indexed_values: Vec<Value> = idx
                            .column_positions_iter()
                            .map(|col_idx| c.new_row[col_idx as usize].clone())
                            .collect();
                        let any_null = indexed_values.iter().any(|v| v.is_null());
                        if !any_null {
                            return Err(SqlError::UniqueViolation(idx.name.clone()));
                        }
                    }
                }
                crate::types::IndexKind::Inverted(inv_kind) => {
                    let col0 = idx.column_positions_iter().next().ok_or_else(|| {
                        SqlError::Unsupported(
                            "inverted index requires at least one column key".into(),
                        )
                    })? as usize;
                    let value = &c.new_row[col0];
                    if !value.is_null() {
                        let entries =
                            super::helpers::extract_inverted_entries_with_values(value, inv_kind)?;
                        let pk_encoded = encode_composite_key(&new_pk);
                        for (entry, val_bytes) in entries {
                            let full_key = super::helpers::build_inverted_key(&entry, &pk_encoded);
                            wtx.table_insert(&idx_table, &full_key, &val_bytes)
                                .map_err(SqlError::Storage)?;
                        }
                    }
                }
            }
        }
    }

    let after_update_triggers_in_txn: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::After
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Update(_)))
        })
        .cloned()
        .collect();
    if !after_update_triggers_in_txn.is_empty() {
        let changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
        for c in &changes {
            super::triggers::fire_row_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::After,
                super::triggers::FireEvent::Update {
                    changed_columns: &changed_cols,
                },
                Some(c.old_row.clone()),
                Some(c.new_row.clone()),
                &table_schema.columns,
            )?;
        }
    }

    super::triggers::fire_statement_triggers(
        wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::After,
        super::triggers::FireEvent::Update {
            changed_columns: &stmt_changed_cols_in_txn,
        },
        &table_schema.columns,
        &stmt_old_rows_in_txn,
        &stmt_new_rows_in_txn,
    )?;

    if let Some(returning_cols) = stmt.returning.as_ref() {
        let rows: Vec<super::helpers::ReturningRow> = changes
            .iter()
            .map(|c| (Some(c.old_row.clone()), Some(c.new_row.clone())))
            .collect();
        let qr = super::helpers::project_returning(table_schema, returning_cols, &rows)?;
        return Ok(ExecutionResult::Query(qr));
    }

    let count = changes.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

pub(super) fn exec_delete_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let materialized;
    let stmt = if delete_has_subquery(stmt) {
        materialized = materialize_delete(stmt, &mut |sub| {
            exec_subquery_write(wtx, schema, sub, &CteContext::default())
        })?;
        &materialized
    } else {
        stmt
    };

    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(schema, &user_name, super::triggers::FireEvent::Delete) {
            let aliases = view_def.column_aliases.clone();
            return exec_instead_of_view_delete_in_txn(wtx, schema, &user_name, &aliases, stmt);
        }
        return Err(SqlError::CannotModifyView(stmt.table.clone()));
    }
    let table_schema = schema
        .get(&user_name)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
    schema.mark_dml(&table_schema.name);
    super::ann_persist::purge_segment(wtx, &table_schema.name)?;
    let lower_name = table_schema.name.clone();

    let has_delete_triggers_in_txn = schema.triggers_for(&table_schema.name).iter().any(|t| {
        t.enabled
            && (t.timing == crate::parser::TriggerTiming::After
                || t.timing == crate::parser::TriggerTiming::Before)
            && t.events
                .iter()
                .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
    });
    if stmt.where_clause.is_none()
        && schema.child_fks_for(&user_name).is_empty()
        && stmt.returning.is_none()
        && !has_delete_triggers_in_txn
    {
        let count = wtx
            .table_truncate(lower_name.as_bytes())
            .map_err(SqlError::Storage)?;
        for idx in &table_schema.indices {
            let idx_table = TableSchema::index_table_name(&lower_name, &idx.name);
            wtx.table_truncate(&idx_table).map_err(SqlError::Storage)?;
        }
        return Ok(ExecutionResult::RowsAffected(count));
    }

    let col_map = ColumnMap::new(&table_schema.columns);
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete: Vec<(Vec<u8>, Vec<Value>)> = all_candidates
        .into_iter()
        .filter(|(_, row)| match &stmt.where_clause {
            Some(where_expr) => match eval_expr(where_expr, &EvalCtx::new(&col_map, row)) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            },
            None => true,
        })
        .collect();

    if rows_to_delete.is_empty() {
        return Ok(ExecutionResult::RowsAffected(0));
    }

    let pk_indices = table_schema.pk_indices();
    let has_child_fks = !schema.child_fks_for(&lower_name).is_empty();
    let mut deleted_pk_keys: Vec<Vec<u8>> = if has_child_fks {
        Vec::with_capacity(rows_to_delete.len())
    } else {
        Vec::new()
    };

    let before_delete_triggers_in_txn: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::Before
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
        })
        .cloned()
        .collect();
    if !before_delete_triggers_in_txn.is_empty() {
        for (_, row) in &rows_to_delete {
            super::triggers::fire_row_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::Before,
                super::triggers::FireEvent::Delete,
                Some(row.clone()),
                None,
                &table_schema.columns,
            )?;
        }
    }

    let stmt_old_rows_in_txn: Vec<Vec<Value>> =
        rows_to_delete.iter().map(|(_, r)| r.clone()).collect();
    super::triggers::fire_statement_triggers(
        wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::Before,
        super::triggers::FireEvent::Delete,
        &table_schema.columns,
        &stmt_old_rows_in_txn,
        &[],
    )?;

    for (key, row) in &rows_to_delete {
        let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
        delete_index_entries(wtx, table_schema, row, &pk_values)?;
        wtx.table_delete(lower_name.as_bytes(), key)
            .map_err(SqlError::Storage)?;
        if has_child_fks {
            deleted_pk_keys.push(encode_composite_key(&pk_values));
        }
    }

    if has_child_fks {
        cascade_after_parent_delete(&mut *wtx, schema, &lower_name, &deleted_pk_keys)?;
    }

    let after_delete_triggers_in_txn: Vec<crate::types::TriggerDef> = schema
        .triggers_for(&table_schema.name)
        .iter()
        .filter(|t| {
            t.enabled
                && t.timing == crate::parser::TriggerTiming::After
                && t.granularity == crate::parser::TriggerGranularity::ForEachRow
                && t.events
                    .iter()
                    .any(|e| matches!(e, crate::parser::TriggerEvent::Delete))
        })
        .cloned()
        .collect();
    if !after_delete_triggers_in_txn.is_empty() {
        for (_, row) in &rows_to_delete {
            super::triggers::fire_row_triggers(
                wtx,
                schema,
                &table_schema.name,
                crate::parser::TriggerTiming::After,
                super::triggers::FireEvent::Delete,
                Some(row.clone()),
                None,
                &table_schema.columns,
            )?;
        }
    }

    super::triggers::fire_statement_triggers(
        wtx,
        schema,
        &table_schema.name,
        crate::parser::TriggerTiming::After,
        super::triggers::FireEvent::Delete,
        &table_schema.columns,
        &stmt_old_rows_in_txn,
        &[],
    )?;

    if let Some(returning_cols) = stmt.returning.as_ref() {
        let rows: Vec<super::helpers::ReturningRow> = rows_to_delete
            .iter()
            .map(|(_, row)| (Some(row.clone()), None))
            .collect();
        let qr = super::helpers::project_returning(table_schema, returning_cols, &rows)?;
        return Ok(ExecutionResult::Query(qr));
    }

    let count = rows_to_delete.len() as u64;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_instead_of_view_update_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    view_name: &str,
    aliases: &[String],
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let select_sq = build_select_for_view(view_name, &stmt.where_clause);
    let qr = super::cte::exec_select_query_in_txn(wtx, schema, &select_sq)?;
    let (resolved_aliases, rows) = match qr {
        ExecutionResult::Query(q) => {
            let cols = if aliases.is_empty() {
                q.columns
            } else {
                aliases.to_vec()
            };
            (cols, q.rows)
        }
        _ => (aliases.to_vec(), Vec::new()),
    };
    let view_cols = super::triggers::view_columns_from_aliases(&resolved_aliases);

    let view_col_map = crate::eval::ColumnMap::new(&view_cols);

    let assignment_targets: Vec<(usize, &Expr)> = stmt
        .assignments
        .iter()
        .map(|(col, expr)| {
            let lower = col.to_ascii_lowercase();
            let idx = resolved_aliases
                .iter()
                .position(|a| a.eq_ignore_ascii_case(&lower))
                .ok_or_else(|| SqlError::ColumnNotFound(col.clone()))?;
            Ok((idx, expr))
        })
        .collect::<Result<_>>()?;

    let mut count: u64 = 0;
    for old_row in rows {
        if old_row.len() != resolved_aliases.len() {
            return Err(SqlError::Unsupported(
                "view source row width does not match column aliases".into(),
            ));
        }
        let mut new_row = old_row.clone();
        for (idx, expr) in &assignment_targets {
            let v = eval_expr(expr, &EvalCtx::new(&view_col_map, &old_row))?;
            new_row[*idx] = v;
        }
        let changed_cols: Vec<String> = stmt.assignments.iter().map(|(c, _)| c.clone()).collect();
        super::triggers::fire_row_triggers(
            wtx,
            schema,
            view_name,
            crate::parser::TriggerTiming::InsteadOf,
            super::triggers::FireEvent::Update {
                changed_columns: &changed_cols,
            },
            Some(old_row),
            Some(new_row),
            &view_cols,
        )?;
        count += 1;
    }
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_instead_of_view_delete_in_txn(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    view_name: &str,
    aliases: &[String],
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let select_sq = build_select_for_view(view_name, &stmt.where_clause);
    let qr = super::cte::exec_select_query_in_txn(wtx, schema, &select_sq)?;
    let (resolved_aliases, rows) = match qr {
        ExecutionResult::Query(q) => {
            let cols = if aliases.is_empty() {
                q.columns
            } else {
                aliases.to_vec()
            };
            (cols, q.rows)
        }
        _ => (aliases.to_vec(), Vec::new()),
    };
    let view_cols = super::triggers::view_columns_from_aliases(&resolved_aliases);

    let mut count: u64 = 0;
    for old_row in rows {
        if old_row.len() != resolved_aliases.len() {
            return Err(SqlError::Unsupported(
                "view source row width does not match column aliases".into(),
            ));
        }
        super::triggers::fire_row_triggers(
            wtx,
            schema,
            view_name,
            crate::parser::TriggerTiming::InsteadOf,
            super::triggers::FireEvent::Delete,
            Some(old_row),
            None,
            &view_cols,
        )?;
        count += 1;
    }
    Ok(ExecutionResult::RowsAffected(count))
}

fn build_select_for_view(
    view_name: &str,
    where_clause: &Option<Expr>,
) -> crate::parser::SelectQuery {
    use crate::parser::{QueryBody, SelectColumn, SelectQuery, SelectStmt};
    let sel = SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: view_name.to_string(),
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: where_clause.clone(),
        order_by: vec![],
        limit: None,
        offset: None,
        group_by: vec![],
        having: None,
    };
    SelectQuery {
        ctes: vec![],
        recursive: false,
        body: QueryBody::Select(Box::new(sel)),
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
