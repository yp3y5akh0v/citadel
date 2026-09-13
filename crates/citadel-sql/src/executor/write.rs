use std::cell::RefCell;

use citadel::Database;

use crate::encoding::{
    decode_column_raw, decode_composite_key, decode_pk_integer, encode_composite_key,
    encode_composite_key_into, patch_column_in_place, patch_row_column, RawColumn, RowLayout,
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
use super::{CteContext, CteRows};

struct UpdateBufs {
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    partial_row: Vec<Value>,
    patch_buf: Vec<u8>,
    row_layout: RowLayout,
    kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    patched: Vec<(Vec<u8>, Vec<u8>)>,
    materializer: UpdateRowMaterializer,
}

impl UpdateBufs {
    fn new() -> Self {
        Self {
            key_buf: Vec::with_capacity(32),
            value_buf: Vec::new(),
            partial_row: Vec::new(),
            patch_buf: Vec::with_capacity(256),
            row_layout: RowLayout::default(),
            kv_pairs: Vec::new(),
            patched: Vec::new(),
            materializer: UpdateRowMaterializer::default(),
        }
    }
}

thread_local! {
    static UPDATE_SCRATCH: RefCell<UpdateBufs> = RefCell::new(UpdateBufs::new());
}

fn with_update_scratch<R>(f: impl FnOnce(&mut UpdateBufs) -> R) -> R {
    struct Guard<'a>(std::cell::RefMut<'a, UpdateBufs>);
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            // An inline row may grow into overflow storage during a callback.
            // Retain small values, including on errors, without keeping a large
            // replacement alive in TLS after an error or panic.
            if self.0.value_buf.capacity() > citadel_core::MAX_INLINE_VALUE_SIZE {
                self.0.value_buf = Vec::new();
            }
        }
    }
    UPDATE_SCRATCH.with(|slot| {
        let mut guard = Guard(slot.borrow_mut());
        f(&mut guard.0)
    })
}

fn filter_keyed_rows(
    rows: Vec<(Vec<u8>, Vec<Value>)>,
    where_clause: &Option<Expr>,
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let Some(where_expr) = where_clause else {
        return Ok(rows);
    };
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let value = eval_expr(
            where_expr,
            &EvalCtx::new(col_map, &row.1).with_cancel(cancel),
        )?;
        if is_truthy(&value) {
            kept.push(row);
        }
    }
    Ok(kept)
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
    strict: bool,
    targets: Vec<CompiledTarget>,
    pk_idx_cache: Vec<usize>,
    col_map: ColumnMap,
    gen_targets: Vec<GenColPatch>,
    gen_extra_cols: Vec<(usize, usize)>,
    rhs_extra_cols: Vec<(usize, usize)>,
    pk_lookup_fast: Option<PkLookupFast>,
    returning_fast: Option<ReturningFast>,
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

/// Plain-column RETURNING for the pk-lookup lane; other shapes interpret.
struct ReturningFast {
    col_names: Vec<String>,
    out_idx: Vec<usize>,
    /// Columns decoded from the post-patch row bytes (new values).
    extra_decode: Vec<(usize, usize)>,
}

#[derive(Clone)]
struct GenColPatch {
    schema_idx: usize,
    phys_idx: usize,
    expr: Expr,
    col: ColumnDef,
    strict: bool,
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

fn compiled_target_patch_safe(target: &CompiledTarget) -> bool {
    if !default_preserves_storage_type(&target.col) {
        return false;
    }
    if !target.col.nullable {
        return is_fixed_width_type(target.col.data_type);
    }
    if target.col.data_type != DataType::Integer {
        return false;
    }
    // Integer self-arithmetic preserves both the payload width and NULL bitmap.
    match target.fast_eval {
        FastEval::IntAdd(_) | FastEval::IntSub(_) | FastEval::IntMul(_) => true,
        FastEval::IntAddParam(p) | FastEval::IntSubParam(p) | FastEval::IntMulParam(p) => {
            resolve_int_param(p).is_some()
        }
        FastEval::None | FastEval::IntSet(_) | FastEval::IntSetParam(_) => false,
    }
}

/// Bare-column refs of a SET expression; false = refs not provably decodable
/// from the stored row (qualified/subquery forms), take the interpreted path.
/// No wildcard arm on purpose: new Expr variants must be classified here.
fn fast_lane_column_refs(expr: &Expr, out: &mut Vec<String>) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) | Expr::TypedNullRecord(_) => true,
        Expr::Column(name) => {
            out.push(name.to_ascii_lowercase());
            true
        }
        Expr::BinaryOp { left, right, .. } => {
            fast_lane_column_refs(left, out) && fast_lane_column_refs(right, out)
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            fast_lane_column_refs(expr, out)
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => fast_lane_column_refs(e, out),
        Expr::Function { args, distinct, .. } => {
            !*distinct && args.iter().all(|a| fast_lane_column_refs(a, out))
        }
        Expr::InList { expr, list, .. } => {
            fast_lane_column_refs(expr, out) && list.iter().all(|e| fast_lane_column_refs(e, out))
        }
        Expr::InSet { expr, .. } => fast_lane_column_refs(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            fast_lane_column_refs(expr, out)
                && fast_lane_column_refs(low, out)
                && fast_lane_column_refs(high, out)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            fast_lane_column_refs(left, out) && fast_lane_column_refs(right, out)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            fast_lane_column_refs(expr, out)
                && fast_lane_column_refs(pattern, out)
                && match escape {
                    Some(e) => fast_lane_column_refs(e, out),
                    None => true,
                }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            (match operand {
                Some(o) => fast_lane_column_refs(o, out),
                None => true,
            }) && conditions
                .iter()
                .all(|(c, r)| fast_lane_column_refs(c, out) && fast_lane_column_refs(r, out))
                && (match else_result {
                    Some(e) => fast_lane_column_refs(e, out),
                    None => true,
                })
        }
        Expr::Coalesce(items) | Expr::ArrayLiteral(items) => {
            items.iter().all(|e| fast_lane_column_refs(e, out))
        }
        Expr::QualifiedColumn { .. }
        | Expr::CountStar
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_)
        | Expr::WindowFunction { .. }
        | Expr::Quantified { .. } => false,
    }
}

/// (schema_idx, phys_idx) decode pairs for `names`, minus pk and
/// `skip_targets`. None = unresolvable or virtual (stored as a NULL
/// placeholder): interpreted path.
fn resolve_extra_decode_cols(
    table_schema: &TableSchema,
    names: &[String],
    skip_targets: &[usize],
    pk_indices: &[usize],
) -> Option<Vec<(usize, usize)>> {
    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let mut extras: Vec<(usize, usize)> = Vec::new();
    for name in names {
        let schema_idx = table_schema.column_index(name)?;
        if pk_indices.contains(&schema_idx)
            || skip_targets.contains(&schema_idx)
            || extras.iter().any(|&(si, _)| si == schema_idx)
        {
            continue;
        }
        if matches!(
            table_schema.columns[schema_idx].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        ) {
            return None;
        }
        let nonpk_order = non_pk.iter().position(|&i| i == schema_idx)?;
        extras.push((schema_idx, enc_pos[nonpk_order] as usize));
    }
    Some(extras)
}

/// SET-referenced columns that are neither pk nor targets; None = interpreted
/// path.
fn compute_rhs_extra_cols(
    table_schema: &TableSchema,
    assignments: &[(String, Expr)],
    set_target_schema_indices: &[usize],
    pk_indices: &[usize],
) -> Option<Vec<(usize, usize)>> {
    let mut names: Vec<String> = Vec::new();
    for (_, expr) in assignments {
        if !fast_lane_column_refs(expr, &mut names) {
            return None;
        }
    }
    resolve_extra_decode_cols(table_schema, &names, set_target_schema_indices, pk_indices)
}

fn decode_cols_into(
    value: &[u8],
    cols: &[(usize, usize)],
    partial_row: &mut [Value],
) -> Result<()> {
    for &(schema_idx, phys_idx) in cols {
        partial_row[schema_idx] = decode_column_raw(value, phys_idx)?.to_value();
    }
    Ok(())
}

/// `live` = columns already holding new values in partial_row post-patch.
fn compile_returning_fast(
    table_schema: &TableSchema,
    returning: &[SelectColumn],
    live: impl Fn(usize) -> bool,
) -> Option<ReturningFast> {
    let virtual_col = |idx: usize| {
        matches!(
            table_schema.columns[idx].generated_kind,
            Some(crate::parser::GeneratedKind::Virtual)
        )
    };
    let mut col_names = Vec::new();
    let mut out_idx = Vec::new();
    for sel in returning {
        match sel {
            SelectColumn::Expr {
                expr: Expr::Column(name),
                alias,
            } => {
                let idx = table_schema.column_index(name)?;
                if virtual_col(idx) {
                    return None;
                }
                // Naming parity with project_returning.
                col_names.push(alias.clone().unwrap_or_else(|| name.clone()));
                out_idx.push(idx);
            }
            SelectColumn::AllColumns => {
                for c in &table_schema.columns {
                    if virtual_col(c.position as usize) {
                        return None;
                    }
                    col_names.push(c.name.clone());
                    out_idx.push(c.position as usize);
                }
            }
            _ => return None,
        }
    }

    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let mut extra_decode: Vec<(usize, usize)> = Vec::new();
    for &idx in &out_idx {
        if live(idx) || extra_decode.iter().any(|&(si, _)| si == idx) {
            continue;
        }
        let nonpk_order = non_pk.iter().position(|&i| i == idx)?;
        extra_decode.push((idx, enc_pos[nonpk_order] as usize));
    }

    Some(ReturningFast {
        col_names,
        out_idx,
        extra_decode,
    })
}

/// Gen-col patches plus the (schema_idx, phys_idx) columns their exprs read.
type GenColPlan = (Vec<GenColPatch>, Vec<(usize, usize)>);

/// None when a generated expr's refs aren't provably decodable: interpreted
/// path.
fn compute_gen_col_targets(
    table_schema: &TableSchema,
    set_target_schema_indices: &[usize],
    pk_indices: &[usize],
) -> Option<GenColPlan> {
    let stored_gen_cols: Vec<&ColumnDef> = table_schema
        .columns
        .iter()
        .filter(|c| matches!(c.generated_kind, Some(crate::parser::GeneratedKind::Stored)))
        .collect();
    if stored_gen_cols.is_empty() {
        return Some((Vec::new(), Vec::new()));
    }

    let non_pk = table_schema.non_pk_indices();
    let enc_pos = table_schema.encoding_positions();
    let mut gen_targets = Vec::with_capacity(stored_gen_cols.len());
    for c in &stored_gen_cols {
        let schema_idx = c.position as usize;
        let nonpk_order = non_pk.iter().position(|&i| i == schema_idx)?;
        let phys_idx = enc_pos[nonpk_order] as usize;
        let expr = c.generated_expr.clone()?;
        let fast_eval = detect_fast_gen_eval(&expr, table_schema);
        gen_targets.push(GenColPatch {
            schema_idx,
            phys_idx,
            expr,
            col: (*c).clone(),
            strict: table_schema.is_strict(),
            fast_eval,
        });
    }

    let mut needed_names: Vec<String> = Vec::new();
    for gp in &gen_targets {
        if !fast_lane_column_refs(&gp.expr, &mut needed_names) {
            return None;
        }
    }

    // Single-column UPDATE: the set-target's new value is live in partial_row,
    // skip re-decode. Multi-column SET re-decodes targets (from the
    // already-patched row bytes = new values).
    let skip_targets: &[usize] = if set_target_schema_indices.len() == 1 {
        set_target_schema_indices
    } else {
        &[]
    };
    let gen_eval_decode_cols =
        resolve_extra_decode_cols(table_schema, &needed_names, skip_targets, pk_indices)?;

    Some((gen_targets, gen_eval_decode_cols))
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

fn default_preserves_storage_type(column: &ColumnDef) -> bool {
    match &column.default_expr {
        None => true,
        Some(Expr::Literal(value)) => {
            value.data_type() == column.data_type || (value.is_null() && column.nullable)
        }
        Some(_) => false,
    }
}

fn pk_range_patch_safe(set_cols: &[ColumnDef], gen_cols: &[ColumnDef]) -> bool {
    set_cols.iter().chain(gen_cols.iter()).all(|c| {
        !c.nullable && is_fixed_width_type(c.data_type) && default_preserves_storage_type(c)
    })
}

fn coerce_update_value(val: Value, col: &ColumnDef, strict: bool) -> Result<Value> {
    if val.is_null() {
        if !col.nullable {
            return Err(SqlError::NotNullViolation(col.name.clone()));
        }
        Ok(Value::Null)
    } else {
        coerce_for_column(val, col, strict)
    }
}

enum UpdateStorage<'a> {
    Fixed(&'a mut [u8]),
    Growable(&'a mut Vec<u8>),
}

impl UpdateStorage<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Fixed(value) => value,
            Self::Growable(value) => value,
        }
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Fixed(value) => value,
            Self::Growable(value) => value,
        }
    }
}

struct UpdateValue<'a> {
    storage: UpdateStorage<'a>,
    layout: &'a mut RowLayout,
}

impl<'a> UpdateValue<'a> {
    fn fixed(bytes: &'a mut [u8], layout: &'a mut RowLayout) -> Self {
        layout.reset();
        Self {
            storage: UpdateStorage::Fixed(bytes),
            layout,
        }
    }

    fn growable(bytes: &'a mut Vec<u8>, layout: &'a mut RowLayout) -> Self {
        layout.reset();
        Self {
            storage: UpdateStorage::Growable(bytes),
            layout,
        }
    }

    fn column(&mut self, column: usize) -> Result<RawColumn<'_>> {
        self.layout.column(self.storage.bytes(), column)
    }

    fn decode_columns(&mut self, columns: &[(usize, usize)], row: &mut [Value]) -> Result<()> {
        for &(schema_idx, physical_idx) in columns {
            row[schema_idx] = self.column(physical_idx)?.to_value();
        }
        Ok(())
    }

    fn patch(&mut self, column: usize, value: &Value, scratch: &mut Vec<u8>) -> Result<()> {
        if self.layout.patch(self.storage.bytes_mut(), column, value)? {
            return Ok(());
        }
        patch_row_column(self.storage.bytes(), column, value, scratch)?;
        match &mut self.storage {
            UpdateStorage::Fixed(bytes) => {
                if bytes.len() != scratch.len() {
                    return Err(SqlError::InvalidValue(
                        "fixed-width UPDATE changed the encoded row length".into(),
                    ));
                }
                bytes.copy_from_slice(scratch);
            }
            UpdateStorage::Growable(bytes) => std::mem::swap(*bytes, scratch),
        }
        self.layout.reset();
        Ok(())
    }
}

fn apply_gen_col_patches(
    value: &mut UpdateValue<'_>,
    partial_row: &mut [Value],
    gen_targets: &[GenColPatch],
    gen_extra_cols: &[(usize, usize)],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
    patch_buf: &mut Vec<u8>,
) -> Result<()> {
    if gen_targets.is_empty() {
        return Ok(());
    }
    value.decode_columns(gen_extra_cols, partial_row)?;
    for gp in gen_targets {
        let raw = eval_fast_gen_with_cancel(&gp.fast_eval, &gp.expr, partial_row, col_map, cancel)?;
        let coerced = coerce_update_value(raw, &gp.col, gp.strict)?;
        partial_row[gp.schema_idx] = coerced.clone();
        value.patch(gp.phys_idx, &coerced, patch_buf)?;
    }
    Ok(())
}

fn apply_updated_rows(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table: &str,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<u64> {
    if !rows.is_empty() {
        let refs: Vec<(&[u8], &[u8])> = rows
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect();
        wtx.table_update_sorted(table.as_bytes(), &refs)
            .map_err(SqlError::Storage)?;
    }
    Ok(rows.len() as u64)
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

pub struct CompiledDelete {
    table_name_lower: String,
    is_view: bool,
    has_correlated_where: bool,
    has_subquery: bool,
    fast: Option<CompiledDeleteFast>,
}

struct CompiledDeleteFast {
    single_int_pk: bool,
    pk_type: DataType,
    num_columns: usize,
    pk_idx: usize,
    shape: DeleteShape,
    returning_fast: Option<ReturningFast>,
}

enum DeleteShape {
    PkLookup(PkLookupFast),
    PkRange(Vec<(BinOp, PkLookupSource)>),
}

/// Every conjunct must be a pk range op with a Literal/Parameter bound; the
/// lane consumes the WHERE in full (never a planner superset prefilter).
fn detect_pk_range_fast(
    where_clause: &Option<Expr>,
    table_schema: &TableSchema,
) -> Option<Vec<(BinOp, PkLookupSource)>> {
    let pk = &table_schema.primary_key_columns;
    if pk.len() != 1 {
        return None;
    }
    let pk_name = table_schema.columns[pk[0] as usize]
        .name
        .to_ascii_lowercase();
    let mut out = Vec::new();
    collect_pk_range_conjuncts(where_clause.as_ref()?, &pk_name, &mut out).then_some(out)
}

fn collect_pk_range_conjuncts(
    expr: &Expr,
    pk_name: &str,
    out: &mut Vec<(BinOp, PkLookupSource)>,
) -> bool {
    let (left, op, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left.as_ref(), *op, right.as_ref()),
        _ => return false,
    };
    if op == BinOp::And {
        return collect_pk_range_conjuncts(left, pk_name, out)
            && collect_pk_range_conjuncts(right, pk_name, out);
    }
    let flipped = match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::LtEq => BinOp::GtEq,
        BinOp::Gt => BinOp::Lt,
        BinOp::GtEq => BinOp::LtEq,
        _ => return false,
    };
    let col_matches = |e: &Expr| match e {
        Expr::Column(name) => name.eq_ignore_ascii_case(pk_name),
        Expr::QualifiedColumn { column, .. } => column.eq_ignore_ascii_case(pk_name),
        _ => false,
    };
    let source = |e: &Expr| match e {
        Expr::Literal(v) => Some(PkLookupSource::Literal(v.clone())),
        Expr::Parameter(n) => Some(PkLookupSource::Parameter(*n)),
        _ => None,
    };
    let pushed = if col_matches(left) {
        source(right).map(|s| (op, s))
    } else if col_matches(right) {
        source(left).map(|s| (flipped, s))
    } else {
        None
    };
    match pushed {
        Some(cond) => {
            out.push(cond);
            true
        }
        None => false,
    }
}

impl CompiledDelete {
    pub fn try_compile(schema: &SchemaManager, stmt: &DeleteStmt) -> Option<Self> {
        let user_name = stmt.table.to_ascii_lowercase();
        // Matview names resolve to their backing table; only the interpreted
        // path raises the modification error.
        if schema.get_view(&user_name).is_some() || schema.get_matview(&user_name).is_some() {
            return Some(Self {
                table_name_lower: user_name,
                is_view: true,
                has_correlated_where: false,
                has_subquery: false,
                fast: None,
            });
        }
        let table_schema = schema.get(&user_name)?;
        // Storage name (post-TEMP-alias resolution); used by wtx.table_* calls.
        let table_name_lower = table_schema.name.clone();

        let corr_ctx = CorrelationCtx {
            outer_schema: table_schema,
            outer_alias: None,
        };
        let has_correlated = has_correlated_where(&stmt.where_clause, &corr_ctx, schema);
        let has_sub = super::dml::delete_has_subquery(stmt);

        // No-WHERE keeps the truncate fast path in the interpreted executors.
        let fast_eligible = !has_correlated
            && !has_sub
            && stmt.where_clause.is_some()
            && table_schema.indices.is_empty()
            && schema.child_fks_for(&table_name_lower).is_empty()
            && !super::triggers::has_delete_triggers(schema, &table_schema.name);

        let fast = if fast_eligible {
            let pk_indices = table_schema.pk_indices();
            let single_int_pk = table_schema.primary_key_columns.len() == 1
                && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                    == DataType::Integer;
            let shape = if let Some(pk) = detect_pk_lookup_fast(&stmt.where_clause, table_schema) {
                Some(DeleteShape::PkLookup(pk))
            } else {
                detect_pk_range_fast(&stmt.where_clause, table_schema).map(DeleteShape::PkRange)
            };
            shape.map(|shape| {
                let returning_fast = match (&shape, stmt.returning.as_ref()) {
                    (DeleteShape::PkLookup(_), Some(r)) => {
                        compile_returning_fast(table_schema, r, |idx| pk_indices.contains(&idx))
                    }
                    _ => None,
                };
                CompiledDeleteFast {
                    single_int_pk,
                    pk_type: table_schema.columns[pk_indices[0]].data_type,
                    num_columns: table_schema.columns.len(),
                    pk_idx: pk_indices[0],
                    shape,
                    returning_fast,
                }
            })
        } else {
            None
        };

        Some(Self {
            table_name_lower,
            is_view: false,
            has_correlated_where: has_correlated,
            has_subquery: has_sub,
            fast,
        })
    }

    fn run_fast(
        &self,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        fast: &CompiledDeleteFast,
        bufs: &mut UpdateBufs,
        empty_query_on_zero_match: bool,
    ) -> Result<Option<ExecutionResult>> {
        let result = match &fast.shape {
            DeleteShape::PkLookup(pk) => {
                let pk_value = match &pk.source {
                    PkLookupSource::Literal(v) => v.clone(),
                    PkLookupSource::Parameter(n) => crate::eval::resolve_scoped_param(*n)?,
                };
                let Some((_, pk_value)) =
                    crate::planner::key_predicate(fast.pk_type, BinOp::Eq, &pk_value)
                else {
                    return Ok(None);
                };
                exec_pk_lookup_delete(
                    wtx,
                    &self.table_name_lower,
                    &pk_value,
                    fast,
                    bufs,
                    empty_query_on_zero_match,
                )
            }
            DeleteShape::PkRange(bounds) => {
                let mut range_conds = Vec::with_capacity(bounds.len());
                for (op, source) in bounds {
                    let value = match source {
                        PkLookupSource::Literal(value) => value.clone(),
                        PkLookupSource::Parameter(n) => crate::eval::resolve_scoped_param(*n)?,
                    };
                    let Some(cond) = crate::planner::key_predicate(fast.pk_type, *op, &value)
                    else {
                        return Ok(None);
                    };
                    range_conds.push(cond);
                }
                exec_pk_range_delete(
                    wtx,
                    &self.table_name_lower,
                    &range_conds,
                    fast.single_int_pk,
                    bufs,
                )
            }
        }?;
        Ok(Some(result))
    }
}

impl CompiledPlan for CompiledDelete {
    fn execute(
        &self,
        db: &Database,
        schema: &SchemaManager,
        stmt: &Statement,
        _params: &[Value],
        txn: super::compile::ActiveTxnRef<'_, '_>,
    ) -> Result<ExecutionResult> {
        let del = match stmt {
            Statement::Delete(d) => d,
            _ => {
                return Err(SqlError::Unsupported(
                    "CompiledDelete received non-DELETE statement".into(),
                ))
            }
        };
        let fast = match &self.fast {
            Some(f)
                if !self.is_view
                    && !self.has_correlated_where
                    && !self.has_subquery
                    // Only the pk-lookup lane produces RETURNING rows.
                    && (del.returning.is_none() || f.returning_fast.is_some()) =>
            {
                Some(f)
            }
            _ => None,
        };
        use super::compile::ActiveTxnRef;
        match txn {
            ActiveTxnRef::None => {
                let Some(fast) = fast else {
                    return exec_delete(db, schema, del);
                };
                let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
                // No segment purge: this lane compiles only for index-free
                // tables.
                schema.mark_dml(&self.table_name_lower);
                let result = with_update_scratch(|bufs| -> Result<ExecutionResult> {
                    match self.run_fast(&mut wtx, fast, bufs, true)? {
                        Some(result) => Ok(result),
                        None => {
                            let result = exec_delete_in_txn(&mut wtx, schema, del)?;
                            match (&result, &fast.returning_fast) {
                                (ExecutionResult::RowsAffected(0), Some(returning)) => {
                                    Ok(ExecutionResult::Query(QueryResult {
                                        columns: returning.col_names.clone(),
                                        rows: Vec::new(),
                                    }))
                                }
                                _ => Ok(result),
                            }
                        }
                    }
                })?;
                super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
                super::commit_with_ann_publication(wtx, schema)?;
                Ok(result)
            }
            ActiveTxnRef::Read(_) => Err(SqlError::Unsupported(
                "cannot execute mutating statement inside a read-only transaction".into(),
            )),
            ActiveTxnRef::Write(outer) => {
                let Some(fast) = fast else {
                    return exec_delete_in_txn(outer, schema, del);
                };
                // No mark_dml: index-free at compile generation, like the
                // compiled UPDATE in-txn lane; fallbacks mark on their own.
                with_update_scratch(|bufs| match self.run_fast(outer, fast, bufs, false)? {
                    Some(result) => Ok(result),
                    None => exec_delete_in_txn(outer, schema, del),
                })
            }
        }
    }
}

fn exec_pk_lookup_delete(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_name_lower: &str,
    pk_value: &Value,
    fast: &CompiledDeleteFast,
    bufs: &mut UpdateBufs,
    empty_query_on_zero_match: bool,
) -> Result<ExecutionResult> {
    let key = crate::encoding::encode_composite_key(std::slice::from_ref(pk_value));
    let Some(rf) = fast.returning_fast.as_ref() else {
        let deleted = wtx
            .table_delete(table_name_lower.as_bytes(), &key)
            .map_err(SqlError::Storage)?;
        return Ok(ExecutionResult::RowsAffected(u64::from(deleted)));
    };
    let Some(bytes) = wtx
        .table_get(table_name_lower.as_bytes(), &key)
        .map_err(SqlError::Storage)?
    else {
        // Zero-match RETURNING shape differs per path; pinned by tests.
        return Ok(if empty_query_on_zero_match {
            ExecutionResult::Query(QueryResult {
                columns: rf.col_names.clone(),
                rows: Vec::new(),
            })
        } else {
            ExecutionResult::RowsAffected(0)
        });
    };
    bufs.partial_row.clear();
    bufs.partial_row.resize(fast.num_columns, Value::Null);
    bufs.partial_row[fast.pk_idx] = pk_value.clone();
    decode_cols_into(&bytes, &rf.extra_decode, &mut bufs.partial_row)?;
    let row: Vec<Value> = rf
        .out_idx
        .iter()
        .map(|&i| bufs.partial_row[i].clone())
        .collect();
    wtx.table_delete(table_name_lower.as_bytes(), &key)
        .map_err(SqlError::Storage)?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: rf.col_names.clone(),
        rows: vec![row],
    }))
}

fn exec_pk_range_delete(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_name_lower: &str,
    range_conds: &[(BinOp, Value)],
    single_int_pk: bool,
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    let start_key = range_conds
        .iter()
        .filter(|(op, _)| matches!(op, BinOp::GtEq | BinOp::Gt))
        .map(|(_, v)| crate::encoding::encode_composite_key(std::slice::from_ref(v)))
        .max()
        .unwrap_or_default();

    bufs.kv_pairs.clear();
    let mut scan_err: Option<SqlError> = None;
    wtx.table_scan_from(
        table_name_lower.as_bytes(),
        &start_key,
        |key, _value| match range_in_bounds(key, single_int_pk, 1, range_conds, &mut scan_err) {
            RangeStatus::Stop | RangeStatus::Err => Ok(false),
            RangeStatus::Skip => Ok(true),
            RangeStatus::Hit => {
                bufs.kv_pairs.push((key.to_vec(), Vec::new()));
                Ok(true)
            }
        },
    )
    .map_err(SqlError::Storage)?;
    if let Some(e) = scan_err {
        return Err(e);
    }

    let mut count = 0u64;
    for (key, _) in &bufs.kv_pairs {
        if wtx
            .table_delete(table_name_lower.as_bytes(), key)
            .map_err(SqlError::Storage)?
        {
            count += 1;
        }
    }
    Ok(ExecutionResult::RowsAffected(count))
}

fn compile_update_impl(schema: &SchemaManager, stmt: &UpdateStmt) -> Result<CompiledUpdate> {
    let user_name = stmt.table.to_ascii_lowercase();
    // Matview names resolve to their backing table; only the interpreted
    // path raises the modification error.
    let is_view = schema.get_view(&user_name).is_some() || schema.get_matview(&user_name).is_some();
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
    // Storage name (post-TEMP-alias resolution); used by wtx.table_* calls
    // below.
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
    let fast_eligible = !pk_changed_by_set
        && !has_fk
        && !has_indices
        && !has_child_fk
        && !table_schema.has_checks()
        && !super::triggers::has_update_triggers(schema, &table_schema.name);

    let fast = if fast_eligible {
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

        let single_int_pk = num_pk_cols == 1
            && table_schema.columns[table_schema.primary_key_columns[0] as usize].data_type
                == DataType::Integer;

        let set_target_indices: Vec<usize> = targets.iter().map(|t| t.schema_idx).collect();
        let gen = compute_gen_col_targets(table_schema, &set_target_indices, pk_indices);
        let rhs = compute_rhs_extra_cols(
            table_schema,
            &stmt.assignments,
            &set_target_indices,
            pk_indices,
        );
        let pk_lookup_fast = detect_pk_lookup_fast(&stmt.where_clause, table_schema);

        match (gen, rhs) {
            (Some((gen_targets, gen_extra_cols)), Some(rhs_extra_cols)) => {
                let returning_fast = stmt.returning.as_ref().and_then(|r| {
                    // Post-patch: pk, single-target value, gen targets, extras.
                    let live = |idx: usize| {
                        pk_indices.contains(&idx)
                            || (targets.len() == 1 && targets[0].schema_idx == idx)
                            || gen_targets.iter().any(|g| g.schema_idx == idx)
                            || rhs_extra_cols.iter().any(|&(si, _)| si == idx)
                            || gen_extra_cols.iter().any(|&(si, _)| si == idx)
                    };
                    compile_returning_fast(table_schema, r, live)
                });
                Some(CompiledFastPath {
                    num_pk_cols,
                    num_columns: table_schema.columns.len(),
                    single_int_pk,
                    strict: table_schema.is_strict(),
                    targets,
                    pk_idx_cache: pk_indices.to_vec(),
                    col_map: ColumnMap::new(&table_schema.columns),
                    gen_targets,
                    gen_extra_cols,
                    rhs_extra_cols,
                    pk_lookup_fast,
                    returning_fast,
                })
            }
            _ => None,
        }
    } else {
        None
    };

    Ok(CompiledUpdate {
        table_name_lower,
        is_view: false,
        has_correlated_where: false,
        has_subquery: false,
        can_fast_path: fast.is_some(),
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
    if compiled.is_view
        || compiled.has_correlated_where
        || compiled.has_subquery
        || !compiled.can_fast_path
        || stmt.returning.is_some()
    {
        return exec_update(db, schema, stmt);
    }

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    schema.mark_dml(&compiled.table_name_lower);
    let result = exec_update_in_txn_compiled(&mut wtx, schema, stmt, compiled, bufs)?;
    super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
    super::commit_with_ann_publication(wtx, schema)?;
    Ok(result)
}

fn exec_compiled_range_update(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &TableSchema,
    fast: &CompiledFastPath,
    start_key: &[u8],
    range_conds: &[(BinOp, Value)],
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    let range_bounds_i64: Option<Vec<(BinOp, i64)>> = if fast.single_int_pk {
        range_conds
            .iter()
            .map(|(op, value)| match value {
                Value::Integer(value) => Some((*op, *value)),
                _ => None,
            })
            .collect()
    } else {
        None
    };
    bufs.patched.clear();

    let count =
        wtx.table_update_range::<_, SqlError>(schema.name.as_bytes(), start_key, |key, value| {
            if let Some(ref bounds) = range_bounds_i64 {
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
            let mut expanded = bufs.materializer.expand(schema, key, value, cancel)?;
            let mut value = match expanded.as_mut() {
                Some(bytes) => UpdateValue::growable(bytes, &mut bufs.row_layout),
                None => UpdateValue::fixed(value, &mut bufs.row_layout),
            };
            patch_compiled_update_value(
                &mut value,
                fast,
                &mut bufs.partial_row,
                cancel,
                &mut bufs.patch_buf,
            )?;
            if let Some(expanded) = expanded {
                bufs.patched.push((key.to_vec(), expanded));
                Ok(Some(false))
            } else {
                Ok(Some(true))
            }
        })?;

    let expanded_count = apply_updated_rows(wtx, &schema.name, &bufs.patched)?;
    Ok(ExecutionResult::RowsAffected(count + expanded_count))
}

pub(super) fn exec_update(
    db: &Database,
    schema: &SchemaManager,
    stmt: &UpdateStmt,
) -> Result<ExecutionResult> {
    let cancel = db.cancel_token();
    let cancel = cancel.as_ref();
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
            super::commit_with_ann_publication(wtx, schema)?;
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
    // Use storage name (post-TEMP-alias resolution) for all wtx.* storage calls
    // below.
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
            let col_map = table_schema.column_map();
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                let value = eval_expr(w, &EvalCtx::new(col_map, &row).with_cancel(cancel))?;
                if is_truthy(&value) {
                    kept.push(row);
                }
            }
            rows = kept;
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_column = &table_schema.columns[pk_indices[0]];
        let in_set: rustc_hash::FxHashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_column.name.clone())),
                values: in_set,
                has_null: false,
                negated: false,
                // The values came out of this very column, so it supplies the collation.
                collation: pk_column.collation,
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

    let col_map = table_schema.column_map();
    let pk_changed_by_set = stmt.assignments.iter().any(|(col_name, _)| {
        table_schema
            .column_index(col_name)
            .is_some_and(|idx| table_schema.primary_key_columns.contains(&(idx as u16)))
    });

    let has_fk = !table_schema.foreign_keys.is_empty();
    let has_indices = !table_schema.indices.is_empty();
    let has_child_fk = !schema.child_fks_for(&lower_name).is_empty();
    // Fast paths skip the trigger-firing site at the slow path's tail. Gate on
    // "no UPDATE triggers" so AFTER UPDATE row triggers always run.
    let has_update_triggers = super::triggers::has_update_triggers(schema, &table_schema.name);
    'fast: {
        if pk_changed_by_set
            || has_fk
            || has_indices
            || has_child_fk
            || has_update_triggers
            || table_schema.has_checks()
            || stmt.returning.is_some()
        {
            break 'fast;
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

        let pk_indices_vec = table_schema.pk_indices().to_vec();
        let set_target_indices: Vec<usize> = targets.iter().map(|t| t.schema_idx).collect();
        let (gen_targets, gen_extra_cols, rhs_extra_cols) = match (
            compute_gen_col_targets(table_schema, &set_target_indices, &pk_indices_vec),
            compute_rhs_extra_cols(
                table_schema,
                &stmt.assignments,
                &set_target_indices,
                &pk_indices_vec,
            ),
        ) {
            (Some((g, ge)), Some(r)) => (g, ge, r),
            _ => break 'fast,
        };

        let set_cols: Vec<ColumnDef> = targets.iter().map(|t| t.col.clone()).collect();
        let gen_cols: Vec<ColumnDef> = gen_targets.iter().map(|g| g.col.clone()).collect();
        let patch_safe = pk_range_patch_safe(&set_cols, &gen_cols);

        // No segment purge: this lane requires an index-free table, so an ANN
        // segment cannot exist.
        let mut wtx = db.begin_write().map_err(SqlError::Storage)?;

        // `value: &mut [u8]` can't grow; nullable/variable-width fall through.
        if let (
            true,
            crate::planner::ScanPlan::PkRangeScan {
                start_key,
                range_conds,
                full_cover: true,
                ..
            },
        ) = (patch_safe, &plan)
        {
            let range_conds = range_conds.clone();
            let mut partial_row = vec![Value::Null; table_schema.columns.len()];
            let pk_idx_cache = table_schema.pk_indices().to_vec();
            let mut patch_buf: Vec<u8> = Vec::with_capacity(256);
            let mut row_layout = RowLayout::default();
            let mut materializer = UpdateRowMaterializer::default();
            let mut expanded_rows = Vec::new();

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
                    let mut expanded = materializer.expand(table_schema, key, value, cancel)?;
                    let mut value = match expanded.as_mut() {
                        Some(bytes) => UpdateValue::growable(bytes, &mut row_layout),
                        None => UpdateValue::fixed(value, &mut row_layout),
                    };
                    for target in &targets {
                        partial_row[target.schema_idx] = value.column(target.phys_idx)?.to_value();
                    }
                    value.decode_columns(&rhs_extra_cols, &mut partial_row)?;
                    for target in &targets {
                        let new_val = eval_expr(
                            &target.expr,
                            &EvalCtx::new(col_map, &partial_row).with_cancel(cancel),
                        )?;
                        let coerced = if new_val.is_null() {
                            if !target.col.nullable {
                                return Err(SqlError::NotNullViolation(target.col.name.clone()));
                            }
                            Value::Null
                        } else {
                            coerce_for_column(new_val, &target.col, strict)?
                        };
                        value.patch(target.phys_idx, &coerced, &mut patch_buf)?;
                        if targets.len() == 1 {
                            partial_row[target.schema_idx] = coerced;
                        }
                    }
                    apply_gen_col_patches(
                        &mut value,
                        &mut partial_row,
                        &gen_targets,
                        &gen_extra_cols,
                        col_map,
                        cancel,
                        &mut patch_buf,
                    )?;
                    if let Some(expanded) = expanded {
                        expanded_rows.push((key.to_vec(), expanded));
                        Ok(Some(false))
                    } else {
                        Ok(Some(true))
                    }
                })?;

            let expanded_count = apply_updated_rows(&mut wtx, &lower_name, &expanded_rows)?;
            super::commit_with_ann_publication(wtx, schema)?;
            return Ok(ExecutionResult::RowsAffected(count + expanded_count));
        }

        let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            match &plan {
                crate::planner::ScanPlan::PkLookup { pk_values, .. } => {
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
        let mut row_layout = RowLayout::default();
        let mut partial_row = vec![Value::Null; table_schema.columns.len()];
        let pk_idx_cache = table_schema.pk_indices().to_vec();
        let mut patched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(kv_pairs.len());
        let mut materializer = UpdateRowMaterializer::default();

        for (key, raw_value) in &mut kv_pairs {
            if let Some(expanded) = materializer.expand(table_schema, key, raw_value, cancel)? {
                *raw_value = expanded;
            }
            if !plan.covers_where() {
                if let Some(ref w) = stmt.where_clause {
                    let row = decode_full_row_with_cancel(table_schema, key, raw_value, cancel)?;
                    let value = eval_expr(w, &EvalCtx::new(col_map, &row).with_cancel(cancel))?;
                    if !is_truthy(&value) {
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
            decode_cols_into(raw_value, &rhs_extra_cols, &mut partial_row)?;
            for target in &targets {
                let new_val = eval_expr(
                    &target.expr,
                    &EvalCtx::new(col_map, &partial_row).with_cancel(cancel),
                )?;
                let coerced = if new_val.is_null() {
                    if !target.col.nullable {
                        return Err(SqlError::NotNullViolation(target.col.name.clone()));
                    }
                    Value::Null
                } else {
                    coerce_for_column(new_val, &target.col, strict)?
                };
                if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                    patch_row_column(raw_value, target.phys_idx, &coerced, &mut patch_buf)?;
                    std::mem::swap(raw_value, &mut patch_buf);
                }
                if targets.len() == 1 {
                    partial_row[target.schema_idx] = coerced;
                }
            }
            apply_gen_col_patches(
                &mut UpdateValue::growable(raw_value, &mut row_layout),
                &mut partial_row,
                &gen_targets,
                &gen_extra_cols,
                col_map,
                cancel,
                &mut patch_buf,
            )?;
            patched.push((std::mem::take(key), std::mem::take(raw_value)));
        }

        let count = apply_updated_rows(&mut wtx, &lower_name, &patched)?;
        super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
        super::commit_with_ann_publication(wtx, schema)?;
        return Ok(ExecutionResult::RowsAffected(count));
    }

    let all_candidates = collect_keyed_rows_read(db, table_schema, &stmt.where_clause)?;
    let matching_rows = filter_keyed_rows(all_candidates, &stmt.where_clause, col_map, cancel)?;

    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let result =
        super::row_mutation::update_rows(&mut wtx, schema, table_schema, stmt, matching_rows)?;
    drain_deferred_fk_checks(&mut wtx, schema)?;
    super::commit_with_ann_publication(wtx, schema)?;
    Ok(result)
}

pub(super) fn exec_delete(
    db: &Database,
    schema: &SchemaManager,
    stmt: &DeleteStmt,
) -> Result<ExecutionResult> {
    let cancel = db.cancel_token();
    let cancel = cancel.as_ref();
    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(schema, &user_name, super::triggers::FireEvent::Delete) {
            let aliases = view_def.column_aliases.clone();
            let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
            let r =
                exec_instead_of_view_delete_in_txn(&mut wtx, schema, &user_name, &aliases, stmt)?;
            super::commit_with_ann_publication(wtx, schema)?;
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
            let col_map = table_schema.column_map();
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                let value = eval_expr(w, &EvalCtx::new(col_map, &row).with_cancel(cancel))?;
                if is_truthy(&value) {
                    kept.push(row);
                }
            }
            rows = kept;
        }

        let pk_indices = table_schema.pk_indices();
        let pk_values: Vec<Value> = rows.iter().map(|row| row[pk_indices[0]].clone()).collect();
        let pk_column = &table_schema.columns[pk_indices[0]];
        let in_set: rustc_hash::FxHashSet<Value> = pk_values.into_iter().collect();
        let new_where = if in_set.is_empty() {
            Some(Expr::Literal(Value::Boolean(false)))
        } else {
            Some(Expr::InSet {
                expr: Box::new(Expr::Column(pk_column.name.clone())),
                values: in_set,
                has_null: false,
                negated: false,
                // The values came out of this very column, so it supplies the collation.
                collation: pk_column.collation,
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

    let col_map = table_schema.column_map();
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    if table_schema.has_ann_index() {
        super::ann_persist::purge_segment(&mut wtx, &lower_name)?;
    }

    if let Some(result) = try_truncate_delete(&mut wtx, schema, table_schema, &user_name, stmt)? {
        super::helpers::drain_deferred_fk_checks(&mut wtx, schema)?;
        super::commit_with_ann_publication(wtx, schema)?;
        return Ok(result);
    }

    let all_candidates = collect_keyed_rows_write(&mut wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete = filter_keyed_rows(all_candidates, &stmt.where_clause, col_map, cancel)?;

    let result = super::row_mutation::delete_rows(
        &mut wtx,
        schema,
        table_schema,
        stmt.returning.clone(),
        rows_to_delete,
    )?;
    drain_deferred_fk_checks(&mut wtx, schema)?;
    super::commit_with_ann_publication(wtx, schema)?;
    Ok(result)
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
    if stmt.from.is_empty() && stmt.from_subquery.is_none() {
        let materialized;
        let stmt = if stmt_has_subquery(stmt) {
            materialized =
                materialize_stmt(stmt, &mut |sub| exec_subquery_write(wtx, schema, sub, ctes))?;
            &materialized
        } else {
            stmt
        };
        return super::exec_select_no_from(stmt, cancel);
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
            let collations =
                super::dml::query_output_collations(schema, ctes, &d.query, qr.columns.len());
            new_ctes.insert(
                d.alias.to_ascii_lowercase(),
                CteRows::new(qr, collations).shared(),
            );
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
                let collations =
                    super::dml::query_output_collations(schema, ctes, &d.query, qr.columns.len());
                new_ctes.insert(
                    d.alias.to_ascii_lowercase(),
                    CteRows::new(qr, collations).shared(),
                );
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
            return super::exec_select_from_cte(
                cte_result,
                stmt,
                &mut |sub| exec_subquery_write(wtx, schema, sub, ctes),
                cancel,
            );
        } else {
            return super::exec_select_join_with_ctes(
                stmt,
                ctes,
                &mut |name| super::scan_table_write(wtx, schema, name),
                cancel,
            );
        }
    }

    if !ctes.is_empty()
        && stmt
            .joins
            .iter()
            .any(|j| ctes.contains_key(&j.table.name.to_ascii_lowercase()))
    {
        return super::exec_select_join_with_ctes(
            stmt,
            ctes,
            &mut |name| super::scan_table_write_or_view(wtx, schema, name),
            cancel,
        );
    }

    if let Some(view_def) = schema.get_view(&lower_name) {
        if let Some(fused) = try_fuse_view(stmt, schema, view_def)? {
            return super::exec_select_in_txn(wtx, schema, &fused, ctes);
        }
        let view_qr = exec_view_write(wtx, schema, view_def)?;
        if stmt.joins.is_empty() {
            return super::exec_select_from_cte(
                &view_qr,
                stmt,
                &mut |sub| exec_subquery_write(wtx, schema, sub, ctes),
                cancel,
            );
        } else {
            let mut view_ctes = ctes.clone();
            view_ctes.insert(lower_name.clone(), view_qr.shared());
            return super::exec_select_join_with_ctes(
                stmt,
                &view_ctes,
                &mut |name| super::scan_table_write_or_view(wtx, schema, name),
                cancel,
            );
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
                    e.insert(vqr.shared());
                }
            }
        }
        return super::exec_select_join_with_ctes(
            stmt,
            &view_ctes,
            &mut |name| super::scan_table_write(wtx, schema, name),
            cancel,
        );
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
        return super::process_select(
            rows,
            super::SelectCtx::new(&table_schema.columns, s, cancel),
        );
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
            let col_map = table_schema.column_map();
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| {
                Ok(plan.feed_row(
                    key,
                    value,
                    table_schema,
                    col_map,
                    &stmt.where_clause,
                    &mut states,
                    &mut scan_err,
                    cancel,
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
        return plan.execute_scan(cancel, |cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    if let Some(plan) = super::ann_topk::VectorTopKPlan::try_new(stmt, table_schema)? {
        return plan.execute(wtx, table_schema, stmt);
    }

    if let Some(plan) = TopKScanPlan::try_new(stmt, table_schema)? {
        let lower = lower_name.clone();
        return plan.execute_scan(table_schema, stmt, cancel, |cb| {
            wtx.table_scan_from(lower.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        });
    }

    if let Some(result) =
        super::select::try_plain_projection_scan(stmt, table_schema, cancel, |cb| {
            wtx.table_scan_from(lower_name.as_bytes(), b"", |key, value| Ok(cb(key, value)))
        })
    {
        return result;
    }

    let scan_limit = compute_scan_limit(stmt, table_schema);
    let (rows, predicate_applied) = collect_select_rows_write(wtx, table_schema, stmt, scan_limit)?;
    super::process_select(
        rows,
        super::SelectCtx::new(&table_schema.columns, stmt, cancel)
            .predicate_applied(predicate_applied),
    )
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();

    let table_schema = schema
        .get(&compiled.table_name_lower)
        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

    let ret_fast = if stmt.returning.is_some() {
        match fast.returning_fast.as_ref() {
            Some(rf) => Some(rf),
            None => return exec_update_in_txn(wtx, schema, stmt),
        }
    } else {
        None
    };

    let single_int_pk = fast.single_int_pk;
    let num_pk_cols = fast.num_pk_cols;
    let pk_idx_cache = &fast.pk_idx_cache;
    let col_map = &fast.col_map;
    let targets = &fast.targets;

    bufs.partial_row.clear();
    bufs.partial_row.resize(fast.num_columns, Value::Null);

    if let Some(ref pkl) = fast.pk_lookup_fast {
        let pk_value = match &pkl.source {
            PkLookupSource::Literal(v) => v.clone(),
            PkLookupSource::Parameter(n) => crate::eval::resolve_scoped_param(*n)?,
        };
        if let Some((_, pk_value)) = crate::planner::key_predicate(
            table_schema.columns[pk_idx_cache[0]].data_type,
            BinOp::Eq,
            &pk_value,
        ) {
            return exec_pk_lookup_update(
                wtx,
                table_schema,
                &pk_value,
                fast,
                ret_fast,
                cancel,
                bufs,
            );
        }
    }

    // Only the pk-lookup lane produces RETURNING rows; other plans fall back.
    if ret_fast.is_some() {
        return exec_update_in_txn(wtx, schema, stmt);
    }

    let plan = crate::planner::plan_select(table_schema, &stmt.where_clause);

    let set_cols_safe = targets.iter().all(compiled_target_patch_safe);
    let gen_cols_safe = fast
        .gen_targets
        .iter()
        .all(|g| !g.col.nullable && is_fixed_width_type(g.col.data_type));
    let patch_safe = set_cols_safe && gen_cols_safe;

    if let (
        true,
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            full_cover: true,
            ..
        },
    ) = (patch_safe, &plan)
    {
        return exec_compiled_range_update(wtx, table_schema, fast, start_key, range_conds, bufs);
    }

    if let crate::planner::ScanPlan::PkLookup {
        pk_values,
        full_cover: true,
    } = &plan
    {
        let UpdateBufs {
            key_buf,
            value_buf,
            partial_row,
            patch_buf,
            row_layout,
            materializer,
            ..
        } = bufs;
        encode_composite_key_into(pk_values, key_buf);
        let key = key_buf.as_slice();
        let updated = wtx.table_update_with_buffer(
            compiled.table_name_lower.as_bytes(),
            key,
            value_buf,
            |raw_value| -> Result<()> {
                if let Some(expanded) = materializer.expand(table_schema, key, raw_value, cancel)? {
                    *raw_value = expanded;
                }
                if single_int_pk {
                    partial_row[pk_idx_cache[0]] = Value::Integer(decode_pk_integer(key)?);
                } else {
                    let pk_vals = decode_composite_key(key, num_pk_cols)?;
                    for (i, &pi) in pk_idx_cache.iter().enumerate() {
                        partial_row[pi] = pk_vals[i].clone();
                    }
                }
                patch_compiled_update_value(
                    &mut UpdateValue::growable(raw_value, row_layout),
                    fast,
                    partial_row,
                    cancel,
                    patch_buf,
                )
            },
        )?;
        return Ok(ExecutionResult::RowsAffected(u64::from(updated.is_some())));
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
    let row_layout = &mut bufs.row_layout;

    for (key, raw_value) in bufs.kv_pairs.iter_mut() {
        if let Some(expanded) = bufs
            .materializer
            .expand(table_schema, key, raw_value, cancel)?
        {
            *raw_value = expanded;
        }
        if !plan.covers_where() {
            if let Some(ref w) = stmt.where_clause {
                let row = decode_full_row_with_cancel(table_schema, key, raw_value, cancel)?;
                let value = eval_expr(w, &EvalCtx::new(col_map, &row).with_cancel(cancel))?;
                if !is_truthy(&value) {
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
        patch_compiled_update_value(
            &mut UpdateValue::growable(raw_value, row_layout),
            fast,
            partial_row,
            cancel,
            patch_buf,
        )?;
        bufs.patched
            .push((std::mem::take(key), std::mem::take(raw_value)));
    }

    let count = apply_updated_rows(wtx, &compiled.table_name_lower, &bufs.patched)?;
    Ok(ExecutionResult::RowsAffected(count))
}

fn exec_pk_lookup_update(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &TableSchema,
    pk_value: &Value,
    fast: &CompiledFastPath,
    ret_fast: Option<&ReturningFast>,
    cancel: Option<&citadel::CancelToken>,
    bufs: &mut UpdateBufs,
) -> Result<ExecutionResult> {
    let UpdateBufs {
        key_buf,
        value_buf,
        partial_row,
        patch_buf,
        row_layout,
        materializer,
        ..
    } = bufs;
    encode_composite_key_into(std::slice::from_ref(pk_value), key_buf);
    let key = key_buf.as_slice();
    let updated = wtx.table_update_with_buffer(
        schema.name.as_bytes(),
        key,
        value_buf,
        |raw_value| -> Result<ExecutionResult> {
            if let Some(expanded) = materializer.expand(schema, key, raw_value, cancel)? {
                *raw_value = expanded;
            }
            partial_row[fast.pk_idx_cache[0]] = pk_value.clone();
            patch_compiled_update_value(
                &mut UpdateValue::growable(raw_value, row_layout),
                fast,
                partial_row,
                cancel,
                patch_buf,
            )?;
            if let Some(rf) = ret_fast {
                // Build RETURNING from the replacement before it is stored.
                decode_cols_into(raw_value, &rf.extra_decode, partial_row)?;
                let row = rf.out_idx.iter().map(|&i| partial_row[i].clone()).collect();
                return Ok(ExecutionResult::Query(QueryResult {
                    columns: rf.col_names.clone(),
                    rows: vec![row],
                }));
            }
            Ok(ExecutionResult::RowsAffected(1))
        },
    )?;
    Ok(updated.unwrap_or_else(|| match ret_fast {
        Some(rf) => ExecutionResult::Query(QueryResult {
            columns: rf.col_names.clone(),
            rows: Vec::new(),
        }),
        None => ExecutionResult::RowsAffected(0),
    }))
}

fn patch_compiled_update_value(
    value: &mut UpdateValue<'_>,
    fast: &CompiledFastPath,
    partial_row: &mut [Value],
    cancel: Option<&citadel::CancelToken>,
    patch_buf: &mut Vec<u8>,
) -> Result<()> {
    let targets = &fast.targets;
    let col_map = &fast.col_map;
    // Capture every SET input before patching: multiple assignments share the
    // old row, while generated expressions below observe the completed SET.
    for target in targets {
        partial_row[target.schema_idx] = value.column(target.phys_idx)?.to_value();
    }
    value.decode_columns(&fast.rhs_extra_cols, partial_row)?;
    for target in targets {
        let new_val = compiled_target_eval(target, partial_row, col_map, cancel)?;
        let coerced = coerce_update_value(new_val, &target.col, fast.strict)?;
        if !(coerced.is_null() && matches!(value.column(target.phys_idx)?, RawColumn::Null)) {
            value.patch(target.phys_idx, &coerced, patch_buf)?;
        }
        if targets.len() == 1 {
            partial_row[target.schema_idx] = coerced;
        }
    }
    apply_gen_col_patches(
        value,
        partial_row,
        &fast.gen_targets,
        &fast.gen_extra_cols,
        col_map,
        cancel,
        patch_buf,
    )
}

fn compiled_target_eval(
    target: &CompiledTarget,
    partial_row: &[Value],
    col_map: &ColumnMap,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Value> {
    let generic = || {
        eval_expr(
            &target.expr,
            &EvalCtx::new(col_map, partial_row).with_cancel(cancel),
        )
    };
    match target.fast_eval {
        FastEval::IntAdd(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => v
                .checked_add(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => generic(),
        },
        FastEval::IntSub(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => v
                .checked_sub(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => generic(),
        },
        FastEval::IntMul(n) => match partial_row[target.schema_idx] {
            Value::Integer(v) => v
                .checked_mul(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => generic(),
        },
        FastEval::IntSet(n) => Ok(Value::Integer(n)),
        FastEval::IntAddParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => v
                .checked_add(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => generic(),
        },
        FastEval::IntSubParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => v
                .checked_sub(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
            _ => generic(),
        },
        FastEval::IntMulParam(p) => match (resolve_int_param(p), &partial_row[target.schema_idx]) {
            (Some(n), Value::Integer(v)) => v
                .checked_mul(n)
                .map(Value::Integer)
                .ok_or(SqlError::IntegerOverflow),
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
    let strict = table_schema.is_strict();
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();
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
        || super::triggers::has_update_triggers(schema, &table_schema.name)
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
    let (gen_targets, gen_extra_cols, rhs_extra_cols) = match (
        compute_gen_col_targets(table_schema, &set_target_indices, &pk_idx_cache),
        compute_rhs_extra_cols(
            table_schema,
            &stmt.assignments,
            &set_target_indices,
            &pk_idx_cache,
        ),
    ) {
        (Some((g, ge)), Some(r)) => (g, ge, r),
        _ => return Ok(None),
    };

    let set_cols: Vec<ColumnDef> = targets.iter().map(|t| t.col.clone()).collect();
    let gen_cols: Vec<ColumnDef> = gen_targets.iter().map(|g| g.col.clone()).collect();
    let patch_safe = pk_range_patch_safe(&set_cols, &gen_cols);

    if let (
        true,
        crate::planner::ScanPlan::PkRangeScan {
            start_key,
            range_conds,
            full_cover: true,
            ..
        },
    ) = (patch_safe, &plan)
    {
        let range_conds = range_conds.clone();
        let mut partial_row = vec![Value::Null; table_schema.columns.len()];
        let mut patch_buf: Vec<u8> = Vec::with_capacity(256);
        let mut row_layout = RowLayout::default();
        let mut materializer = UpdateRowMaterializer::default();
        let mut expanded_rows = Vec::new();

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
                let mut expanded = materializer.expand(table_schema, key, value, cancel)?;
                let mut value = match expanded.as_mut() {
                    Some(bytes) => UpdateValue::growable(bytes, &mut row_layout),
                    None => UpdateValue::fixed(value, &mut row_layout),
                };
                for target in &targets {
                    partial_row[target.schema_idx] = value.column(target.phys_idx)?.to_value();
                }
                value.decode_columns(&rhs_extra_cols, &mut partial_row)?;
                for target in &targets {
                    let new_val = eval_expr(
                        &target.expr,
                        &EvalCtx::new(col_map, &partial_row).with_cancel(cancel),
                    )?;
                    let coerced = coerce_update_value(new_val, &target.col, strict)?;
                    value.patch(target.phys_idx, &coerced, &mut patch_buf)?;
                    if targets.len() == 1 {
                        partial_row[target.schema_idx] = coerced;
                    }
                }
                apply_gen_col_patches(
                    &mut value,
                    &mut partial_row,
                    &gen_targets,
                    &gen_extra_cols,
                    col_map,
                    cancel,
                    &mut patch_buf,
                )?;
                if let Some(expanded) = expanded {
                    expanded_rows.push((key.to_vec(), expanded));
                    Ok(Some(false))
                } else {
                    Ok(Some(true))
                }
            },
        )?;
        let expanded_count = apply_updated_rows(wtx, &lower_name, &expanded_rows)?;
        return Ok(Some(ExecutionResult::RowsAffected(count + expanded_count)));
    }

    let mut kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    match &plan {
        crate::planner::ScanPlan::PkLookup { pk_values, .. } => {
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
    let mut row_layout = RowLayout::default();
    let mut partial_row = vec![Value::Null; table_schema.columns.len()];
    let mut patched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(kv_pairs.len());
    let mut materializer = UpdateRowMaterializer::default();

    for (key, raw_value) in &mut kv_pairs {
        if let Some(expanded) = materializer.expand(table_schema, key, raw_value, cancel)? {
            *raw_value = expanded;
        }
        if !plan.covers_where() {
            if let Some(ref w) = stmt.where_clause {
                let row = decode_full_row_with_cancel(table_schema, key, raw_value, cancel)?;
                let value = eval_expr(w, &EvalCtx::new(col_map, &row).with_cancel(cancel))?;
                if !is_truthy(&value) {
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
        decode_cols_into(raw_value, &rhs_extra_cols, &mut partial_row)?;
        for target in &targets {
            let new_val = eval_expr(
                &target.expr,
                &EvalCtx::new(col_map, &partial_row).with_cancel(cancel),
            )?;
            let coerced = coerce_update_value(new_val, &target.col, strict)?;
            if !patch_column_in_place(raw_value, target.phys_idx, &coerced)? {
                patch_row_column(raw_value, target.phys_idx, &coerced, &mut patch_buf)?;
                std::mem::swap(raw_value, &mut patch_buf);
            }
            if targets.len() == 1 {
                partial_row[target.schema_idx] = coerced;
            }
        }
        apply_gen_col_patches(
            &mut UpdateValue::growable(raw_value, &mut row_layout),
            &mut partial_row,
            &gen_targets,
            &gen_extra_cols,
            col_map,
            cancel,
            &mut patch_buf,
        )?;
        patched.push((std::mem::take(key), std::mem::take(raw_value)));
    }

    let count = apply_updated_rows(wtx, &lower_name, &patched)?;
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();

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
    if table_schema.has_ann_index() {
        super::ann_persist::purge_segment(wtx, &table_schema.name)?;
    }
    let col_map = table_schema.column_map();

    if let Some(result) = try_fast_update_in_txn(wtx, schema, stmt, table_schema, col_map)? {
        return Ok(result);
    }

    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let matching_rows = filter_keyed_rows(all_candidates, &stmt.where_clause, col_map, cancel)?;

    super::row_mutation::update_rows(wtx, schema, table_schema, stmt, matching_rows)
}

/// Without a predicate, referencing children or DELETE triggers, every live
/// row is removed independently. RETURNING is projected after all mutations in
/// the general row executor too, so keep its old rows and use the same projector.
fn try_truncate_delete(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    schema: &SchemaManager,
    table: &TableSchema,
    requested_name: &str,
    stmt: &DeleteStmt,
) -> Result<Option<ExecutionResult>> {
    if stmt.where_clause.is_some()
        || !schema.child_fks_for(&table.name).is_empty()
        || (requested_name != table.name && !schema.child_fks_for(requested_name).is_empty())
        || super::triggers::has_delete_triggers(schema, &table.name)
        // The general operation refreshes outgoing-FK rows, which also spends
        // their materialization budget. Keep that contract for RETURNING.
        || (stmt.returning.is_some() && !table.foreign_keys.is_empty())
    {
        return Ok(None);
    }
    let returning_rows = stmt
        .returning
        .as_ref()
        .map(|_| {
            collect_keyed_rows_write(wtx, table, &None).map(|rows| {
                rows.into_iter()
                    .map(|(_, old)| (Some(old), None))
                    .collect::<Vec<_>>()
            })
        })
        .transpose()?;
    let count = if returning_rows.as_ref().is_some_and(Vec::is_empty) {
        // An empty RETURNING operation keeps its metadata without creating a
        // physical mutation that the ordinary row executor would not perform.
        0
    } else {
        let count = wtx
            .table_truncate(table.name.as_bytes())
            .map_err(SqlError::Storage)?;
        for index in &table.indices {
            let index_table = TableSchema::index_table_name(&table.name, &index.name);
            wtx.table_truncate(&index_table)
                .map_err(SqlError::Storage)?;
        }
        count
    };
    let result = match (&stmt.returning, returning_rows) {
        (Some(columns), Some(rows)) => ExecutionResult::Query(project_returning(
            table,
            columns,
            &rows,
            wtx.cancel_token(),
        )?),
        _ => ExecutionResult::RowsAffected(count),
    };
    Ok(Some(result))
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();

    let user_name = stmt.table.to_ascii_lowercase();
    if let Some(view_def) = schema.get_view(&user_name) {
        if super::triggers::has_instead_of(schema, &user_name, super::triggers::FireEvent::Delete) {
            let aliases = view_def.column_aliases.clone();
            return exec_instead_of_view_delete_in_txn(wtx, schema, &user_name, &aliases, stmt);
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
    if table_schema.has_ann_index() {
        super::ann_persist::purge_segment(wtx, &table_schema.name)?;
    }

    if let Some(result) = try_truncate_delete(wtx, schema, table_schema, &user_name, stmt)? {
        return Ok(result);
    }

    let col_map = table_schema.column_map();
    let all_candidates = collect_keyed_rows_write(wtx, table_schema, &stmt.where_clause)?;
    let rows_to_delete = filter_keyed_rows(all_candidates, &stmt.where_clause, col_map, cancel)?;

    super::row_mutation::delete_rows(
        wtx,
        schema,
        table_schema,
        stmt.returning.clone(),
        rows_to_delete,
    )
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
    let cancel = wtx.cancel_token().cloned();
    let cancel = cancel.as_ref();

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
            let v = eval_expr(
                expr,
                &EvalCtx::new(&view_col_map, &old_row).with_cancel(cancel),
            )?;
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
