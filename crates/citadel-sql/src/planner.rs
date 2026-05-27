//! Query planner: chooses between seq scan, PK lookup, or index scan.

use crate::encoding::encode_composite_key;
use crate::parser::{BinOp, Expr};
use crate::types::{IndexDef, IndexKey, IndexKind, InvertedKind, TableSchema, Value};

/// Canonical form of an expression for symbolic-equivalence matching against expression indexes.
/// Strips table qualifiers, lowercases identifiers and function names, sorts commutative operands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CanonicalExpr {
    Literal(String),
    Column(String),
    Function {
        name: String,
        args: Vec<CanonicalExpr>,
    },
    BinaryOp {
        op: BinOp,
        operands: Vec<CanonicalExpr>,
    },
    UnaryOp {
        op: crate::parser::UnaryOp,
        operand: Box<CanonicalExpr>,
    },
    Cast {
        expr: Box<CanonicalExpr>,
        data_type: crate::types::DataType,
    },
    Other(String),
}

fn canonicalize(expr: &Expr) -> CanonicalExpr {
    match expr {
        Expr::Literal(v) => CanonicalExpr::Literal(format!("{v:?}")),
        Expr::Column(name) => CanonicalExpr::Column(name.to_ascii_lowercase()),
        Expr::QualifiedColumn { column, .. } => CanonicalExpr::Column(column.to_ascii_lowercase()),
        Expr::Function { name, args, .. } => {
            let canon_args: Vec<CanonicalExpr> = args.iter().map(canonicalize).collect();
            CanonicalExpr::Function {
                name: name.to_ascii_lowercase(),
                args: canon_args,
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let mut operands = vec![canonicalize(left), canonicalize(right)];
            if is_commutative(*op) {
                operands.sort_by_key(|e| format!("{e:?}"));
            }
            CanonicalExpr::BinaryOp { op: *op, operands }
        }
        Expr::UnaryOp { op, expr: inner } => CanonicalExpr::UnaryOp {
            op: *op,
            operand: Box::new(canonicalize(inner)),
        },
        Expr::Cast {
            expr: inner,
            data_type,
        } => CanonicalExpr::Cast {
            expr: Box::new(canonicalize(inner)),
            data_type: *data_type,
        },
        Expr::Collate { expr: inner, .. } => canonicalize(inner),
        other => CanonicalExpr::Other(format!("{other:?}")),
    }
}

fn is_commutative(op: BinOp) -> bool {
    matches!(op, BinOp::Add | BinOp::Mul | BinOp::And | BinOp::Or)
}

#[derive(Debug, Clone)]
pub enum ScanPlan {
    SeqScan,
    PkLookup {
        pk_values: Vec<Value>,
    },
    PkRangeScan {
        start_key: Vec<u8>,
        range_conds: Vec<(BinOp, Value)>,
        num_pk_cols: usize,
    },
    IndexScan {
        index_name: String,
        idx_table: Vec<u8>,
        prefix: Vec<u8>,
        num_prefix_cols: usize,
        range_conds: Vec<(BinOp, Value)>,
        is_unique: bool,
        index_columns: Vec<u16>,
    },
    InvertedScan {
        kind: InvertedKind,
        idx_table: Vec<u8>,
        column_idx: u16,
        probe_entries: Vec<Vec<u8>>,
        recheck_expr: Expr,
        recheck_needed: bool,
    },
}

struct SimplePredicate {
    col_idx: usize,
    op: BinOp,
    value: Value,
}

fn flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = flatten_and(left);
            v.extend(flatten_and(right));
            v
        }
        _ => vec![expr],
    }
}

fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
    )
}

fn is_range_op(op: BinOp) -> bool {
    matches!(op, BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq)
}

fn flip_op(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::LtEq => BinOp::GtEq,
        BinOp::Gt => BinOp::Lt,
        BinOp::GtEq => BinOp::LtEq,
        other => other,
    }
}

fn resolve_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(name) => Some(name.as_str()),
        Expr::QualifiedColumn { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

fn resolve_literal(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Parameter(n) => crate::eval::resolve_scoped_param(*n).ok(),
        Expr::Function { .. } | Expr::Cast { .. } => {
            let col_map = crate::eval::ColumnMap::new(&[]);
            let ctx = crate::eval::EvalCtx::new(&col_map, &[]);
            crate::eval::eval_expr(expr, &ctx).ok()
        }
        _ => None,
    }
}

fn extract_simple_predicate(expr: &Expr, schema: &TableSchema) -> Option<SimplePredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } if is_comparison(*op) => {
            if let (Some(name), Some(val)) = (resolve_column_name(left), resolve_literal(right)) {
                let col_idx = schema.column_index(name)?;
                return Some(SimplePredicate {
                    col_idx,
                    op: *op,
                    value: val,
                });
            }
            if let (Some(val), Some(name)) = (resolve_literal(left), resolve_column_name(right)) {
                let col_idx = schema.column_index(name)?;
                return Some(SimplePredicate {
                    col_idx,
                    op: flip_op(*op),
                    value: val,
                });
            }
            None
        }
        _ => None,
    }
}

/// Decompose BETWEEN into two range predicates for planner use.
fn flatten_between(expr: &Expr, schema: &TableSchema, out: &mut Vec<SimplePredicate>) {
    match expr {
        Expr::Between {
            expr: col_expr,
            low,
            high,
            negated: false,
        } => {
            if let (Some(name), Some(lo), Some(hi)) = (
                resolve_column_name(col_expr),
                resolve_literal(low),
                resolve_literal(high),
            ) {
                if let Some(col_idx) = schema.column_index(name) {
                    out.push(SimplePredicate {
                        col_idx,
                        op: BinOp::GtEq,
                        value: lo,
                    });
                    out.push(SimplePredicate {
                        col_idx,
                        op: BinOp::LtEq,
                        value: hi,
                    });
                }
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            flatten_between(left, schema, out);
            flatten_between(right, schema, out);
        }
        _ => {}
    }
}

pub fn plan_select(schema: &TableSchema, where_clause: &Option<Expr>) -> ScanPlan {
    plan_select_inner(schema, where_clause, false)
}

pub fn plan_select_inverted(schema: &TableSchema, where_clause: &Option<Expr>) -> ScanPlan {
    plan_select_inner(schema, where_clause, true)
}

fn plan_select_inner(
    schema: &TableSchema,
    where_clause: &Option<Expr>,
    allow_inverted: bool,
) -> ScanPlan {
    let where_expr = match where_clause {
        Some(e) => e,
        None => return ScanPlan::SeqScan,
    };

    let predicates = flatten_and(where_expr);
    let simple: Vec<Option<SimplePredicate>> = predicates
        .iter()
        .map(|p| extract_simple_predicate(p, schema))
        .collect();

    if let Some(plan) = try_pk_lookup(schema, &simple) {
        return plan;
    }

    let mut range_preds: Vec<SimplePredicate> = simple
        .iter()
        .filter_map(|p| {
            let p = p.as_ref()?;
            if is_range_op(p.op) {
                Some(SimplePredicate {
                    col_idx: p.col_idx,
                    op: p.op,
                    value: p.value.clone(),
                })
            } else {
                None
            }
        })
        .collect();
    flatten_between(where_expr, schema, &mut range_preds);

    if let Some(plan) = try_pk_range_scan(schema, &range_preds) {
        return plan;
    }

    if allow_inverted {
        if let Some(plan) = try_inverted_scan(schema, where_expr) {
            return plan;
        }
    }

    if let Some(plan) = try_best_index(schema, where_expr, &simple) {
        return plan;
    }

    ScanPlan::SeqScan
}

fn try_inverted_scan(schema: &TableSchema, where_expr: &Expr) -> Option<ScanPlan> {
    use crate::parser::BinOp as B;
    let (col_idx, rhs_val, op) = match where_expr {
        Expr::BinaryOp {
            left,
            op: B::JsonContains,
            right,
        } => {
            let name = resolve_column_name(left)?;
            let col_idx = schema.column_index(name)? as u16;
            let rhs = resolve_literal(right)?;
            (col_idx, rhs, B::JsonContains)
        }
        Expr::BinaryOp {
            left,
            op: B::JsonPathMatch,
            right,
        } => {
            let name = resolve_column_name(left)?;
            let col_idx = schema.column_index(name)? as u16;
            let rhs = resolve_literal(right)?;
            (col_idx, rhs, B::JsonPathMatch)
        }
        _ => return None,
    };
    let idx = schema.indices.iter().find(|i| {
        matches!(i.kind, IndexKind::Inverted(_))
            && i.column_positions_iter()
                .next()
                .is_some_and(|c| c == col_idx)
            && i.predicate_expr.is_none()
    })?;
    let kind = match idx.kind {
        IndexKind::Inverted(k) => k,
        _ => return None,
    };
    match (kind, op) {
        (InvertedKind::Gin(_), B::JsonContains) => {}
        (InvertedKind::Fts { .. }, B::JsonPathMatch) => {}
        _ => return None,
    }
    let probe_entries = extract_inverted_probe(&rhs_val, kind)?;
    if probe_entries.is_empty() {
        return None;
    }
    let recheck_needed = inverted_recheck_needed(kind, &rhs_val);
    let idx_table = TableSchema::index_table_name(&schema.name, &idx.name);
    Some(ScanPlan::InvertedScan {
        kind,
        idx_table,
        column_idx: col_idx,
        probe_entries,
        recheck_expr: where_expr.clone(),
        recheck_needed,
    })
}

fn inverted_recheck_needed(kind: InvertedKind, rhs: &Value) -> bool {
    match kind {
        InvertedKind::Gin(_) => true,
        InvertedKind::Fts { .. } => match rhs {
            Value::TsQuery(bytes) => match crate::fts::TsQueryAst::decode(bytes) {
                Ok(ast) => !fts_ast_exact_for_index(&ast),
                Err(_) => true,
            },
            _ => true,
        },
    }
}

fn fts_ast_exact_for_index(ast: &crate::fts::TsQueryAst) -> bool {
    use crate::fts::TsQueryAst;
    match ast {
        TsQueryAst::Lexeme {
            prefix: false,
            weight_mask: 0,
            ..
        } => true,
        TsQueryAst::Lexeme { .. } => false,
        TsQueryAst::And(l, r) => fts_ast_exact_for_index(l) && fts_ast_exact_for_index(r),
        _ => false,
    }
}

pub(crate) fn fts_ast_is_pure_phrase(ast: &crate::fts::TsQueryAst) -> bool {
    use crate::fts::TsQueryAst;
    match ast {
        TsQueryAst::Lexeme {
            prefix: false,
            weight_mask: 0,
            ..
        } => true,
        TsQueryAst::Phrase { left, right, .. } => {
            fts_ast_is_pure_phrase(left) && fts_ast_is_pure_phrase(right)
        }
        _ => false,
    }
}

fn extract_inverted_probe(rhs: &Value, kind: InvertedKind) -> Option<Vec<Vec<u8>>> {
    use crate::types::GinOpsClass;
    match kind {
        InvertedKind::Gin(ops) => {
            let entries = crate::json::extract_gin_entries(rhs, ops).ok()?;
            let filtered: Vec<Vec<u8>> = match ops {
                GinOpsClass::JsonbOps => entries
                    .into_iter()
                    .filter(|e| !matches!(e.first(), Some(&0x01)))
                    .collect(),
                GinOpsClass::JsonbPathOps => entries,
            };
            Some(filtered)
        }
        InvertedKind::Fts { .. } => match rhs {
            Value::TsQuery(bytes) => {
                let ast = crate::fts::TsQueryAst::decode(bytes).ok()?;
                let required = fts_required_lexemes(&ast)?;
                if required.is_empty() {
                    None
                } else {
                    Some(required)
                }
            }
            _ => None,
        },
    }
}

fn fts_required_lexemes(ast: &crate::fts::TsQueryAst) -> Option<Vec<Vec<u8>>> {
    let mut out: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let ok = collect_required(ast, &mut out);
    if !ok || out.is_empty() {
        None
    } else {
        Some(out.into_iter().collect())
    }
}

fn collect_required(
    ast: &crate::fts::TsQueryAst,
    out: &mut std::collections::BTreeSet<Vec<u8>>,
) -> bool {
    use crate::fts::TsQueryAst;
    match ast {
        TsQueryAst::Lexeme { prefix, .. } if *prefix => false,
        TsQueryAst::Lexeme { lexeme, .. } => {
            out.insert(lexeme.clone());
            true
        }
        TsQueryAst::And(l, r) => {
            let lo = collect_required(l, out);
            let ro = collect_required(r, out);
            lo || ro
        }
        TsQueryAst::Or(..) => false,
        TsQueryAst::Not(_) => false,
        TsQueryAst::Phrase { left, right, .. } => {
            let lo = collect_required(left, out);
            let ro = collect_required(right, out);
            lo && ro
        }
    }
}

fn try_pk_range_scan(schema: &TableSchema, range_preds: &[SimplePredicate]) -> Option<ScanPlan> {
    if schema.primary_key_columns.len() != 1 {
        return None; // Only single-column PK for now
    }
    let pk_col = schema.primary_key_columns[0] as usize;
    let conds: Vec<(BinOp, Value)> = range_preds
        .iter()
        .filter(|p| p.col_idx == pk_col)
        .map(|p| (p.op, p.value.clone()))
        .collect();
    if conds.is_empty() {
        return None;
    }
    let start_key = conds
        .iter()
        .filter(|(op, _)| matches!(op, BinOp::GtEq | BinOp::Gt))
        .map(|(_, v)| encode_composite_key(std::slice::from_ref(v)))
        .min_by(|a, b| a.cmp(b))
        .unwrap_or_default();
    Some(ScanPlan::PkRangeScan {
        start_key,
        range_conds: conds,
        num_pk_cols: 1,
    })
}

fn try_pk_lookup(schema: &TableSchema, predicates: &[Option<SimplePredicate>]) -> Option<ScanPlan> {
    let pk_cols = &schema.primary_key_columns;
    // No PK → fall through to SeqScan. An empty-key PkLookup would silently match 0 rows.
    if pk_cols.is_empty() {
        return None;
    }
    let mut pk_values: Vec<Option<Value>> = vec![None; pk_cols.len()];

    for pred in predicates.iter().flatten() {
        if pred.op == BinOp::Eq {
            if let Some(pk_pos) = pk_cols.iter().position(|&c| c == pred.col_idx as u16) {
                pk_values[pk_pos] = Some(pred.value.clone());
            }
        }
    }

    if pk_values.iter().all(|v| v.is_some()) {
        let values: Vec<Value> = pk_values.into_iter().map(|v| v.unwrap()).collect();
        Some(ScanPlan::PkLookup { pk_values: values })
    } else {
        None
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct IndexScore {
    num_equality: usize,
    has_range: bool,
    is_unique: bool,
}

fn try_best_index(
    schema: &TableSchema,
    where_expr: &Expr,
    predicates: &[Option<SimplePredicate>],
) -> Option<ScanPlan> {
    let mut best_score: Option<IndexScore> = None;
    let mut best_plan: Option<ScanPlan> = None;

    let conjuncts = flatten_and(where_expr);
    for idx in &schema.indices {
        if !partial_predicate_implied(idx, where_expr, &conjuncts) {
            continue;
        }
        if let Some((score, plan)) = try_index_scan(schema, idx, predicates) {
            if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                best_score = Some(score);
                best_plan = Some(plan);
            }
        }
        if !idx.is_pure_column_index() {
            if let Some((score, plan)) = try_expr_index_scan(schema, idx, &conjuncts) {
                if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                    best_score = Some(score);
                    best_plan = Some(plan);
                }
            }
        }
    }

    best_plan
}

fn try_expr_index_scan(
    schema: &TableSchema,
    idx: &IndexDef,
    conjuncts: &[&Expr],
) -> Option<(IndexScore, ScanPlan)> {
    // Only equality on the first expression key supported (`WHERE LOWER(email) = ?`).
    let first_key = idx.keys.first()?;
    let key_expr = match first_key {
        IndexKey::Expr { expr, .. } => expr,
        IndexKey::Column { .. } => return None,
    };
    let canonical_key = canonicalize(key_expr);

    let mut matched: Option<Value> = None;
    for conj in conjuncts {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = conj
        {
            let (expr_side, value_side) = match (left.as_ref(), right.as_ref()) {
                (Expr::Literal(v), other) | (other, Expr::Literal(v)) => (other, v.clone()),
                _ => continue,
            };
            if canonicalize(expr_side) == canonical_key {
                matched = Some(value_side);
                break;
            }
        }
    }

    let value = matched?;
    let score = IndexScore {
        num_equality: 1,
        has_range: false,
        is_unique: idx.unique,
    };
    let prefix = encode_composite_key(&[value]);
    let idx_table = TableSchema::index_table_name(&schema.name, &idx.name);
    Some((
        score,
        ScanPlan::IndexScan {
            index_name: idx.name.clone(),
            idx_table,
            prefix,
            num_prefix_cols: 1,
            range_conds: vec![],
            is_unique: idx.unique,
            index_columns: vec![],
        },
    ))
}

fn partial_predicate_implied(idx: &IndexDef, where_expr: &Expr, conjuncts: &[&Expr]) -> bool {
    let Some(pred) = idx.predicate_expr.as_ref() else {
        return true;
    };
    if expr_structurally_eq(pred, where_expr) {
        return true;
    }
    if conjuncts.iter().any(|c| expr_structurally_eq(pred, c)) {
        return true;
    }
    if let Expr::IsNotNull(target) = pred {
        if let Expr::Column(col) = target.as_ref() {
            return conjuncts.iter().any(|c| conjunct_proves_not_null(c, col));
        }
    }
    false
}

fn expr_structurally_eq(a: &Expr, b: &Expr) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

fn conjunct_proves_not_null(expr: &Expr, col: &str) -> bool {
    let mentions = |e: &Expr| matches!(e, Expr::Column(n) if n.eq_ignore_ascii_case(col));
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq,
            right,
        } => mentions(left) || mentions(right),
        Expr::IsNotNull(inner) => mentions(inner),
        _ => false,
    }
}

fn try_index_scan(
    schema: &TableSchema,
    idx: &IndexDef,
    predicates: &[Option<SimplePredicate>],
) -> Option<(IndexScore, ScanPlan)> {
    let mut used = Vec::new();
    let mut equality_values: Vec<Value> = Vec::new();
    let mut range_conds: Vec<(BinOp, Value)> = Vec::new();

    // Expression-key indexes go through `try_expr_index_scan`, not the column path.
    if !idx.is_pure_column_index() {
        return None;
    }
    let idx_columns = idx.columns_vec();
    for &col_idx in &idx_columns {
        let mut found_eq = false;
        for (i, pred) in predicates.iter().enumerate() {
            if used.contains(&i) {
                continue;
            }
            if let Some(sp) = pred {
                if sp.col_idx == col_idx as usize && sp.op == BinOp::Eq {
                    equality_values.push(sp.value.clone());
                    used.push(i);
                    found_eq = true;
                    break;
                }
            }
        }
        if !found_eq {
            for (i, pred) in predicates.iter().enumerate() {
                if used.contains(&i) {
                    continue;
                }
                if let Some(sp) = pred {
                    if sp.col_idx == col_idx as usize && is_range_op(sp.op) {
                        range_conds.push((sp.op, sp.value.clone()));
                        used.push(i);
                    }
                }
            }
            break;
        }
    }

    if equality_values.is_empty() && range_conds.is_empty() {
        return None;
    }

    let score = IndexScore {
        num_equality: equality_values.len(),
        has_range: !range_conds.is_empty(),
        is_unique: idx.unique,
    };

    let prefix = encode_composite_key(&equality_values);
    let idx_table = TableSchema::index_table_name(&schema.name, &idx.name);

    Some((
        score,
        ScanPlan::IndexScan {
            index_name: idx.name.clone(),
            idx_table,
            prefix,
            num_prefix_cols: equality_values.len(),
            range_conds,
            is_unique: idx.unique,
            index_columns: idx_columns.clone(),
        },
    ))
}

pub fn describe_plan(plan: &ScanPlan, table_schema: &TableSchema) -> String {
    match plan {
        ScanPlan::SeqScan => String::new(),

        ScanPlan::PkLookup { pk_values } => {
            let pk_cols: Vec<&str> = table_schema
                .primary_key_columns
                .iter()
                .map(|&idx| table_schema.columns[idx as usize].name.as_str())
                .collect();
            let conditions: Vec<String> = pk_cols
                .iter()
                .zip(pk_values.iter())
                .map(|(col, val)| format!("{col} = {}", format_value(val)))
                .collect();
            format!("USING PRIMARY KEY ({})", conditions.join(", "))
        }

        ScanPlan::PkRangeScan { range_conds, .. } => {
            let pk_col = &table_schema.columns[table_schema.primary_key_columns[0] as usize].name;
            let conditions: Vec<String> = range_conds
                .iter()
                .map(|(op, val)| format!("{pk_col} {} {}", op_symbol(*op), format_value(val)))
                .collect();
            format!("USING PRIMARY KEY RANGE ({})", conditions.join(", "))
        }

        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            range_conds,
            index_columns,
            ..
        } => {
            let mut conditions = Vec::new();
            for &col in index_columns.iter().take(*num_prefix_cols) {
                let col_idx = col as usize;
                let col_name = &table_schema.columns[col_idx].name;
                conditions.push(format!("{col_name} = ?"));
            }
            if !range_conds.is_empty() && *num_prefix_cols < index_columns.len() {
                let col_idx = index_columns[*num_prefix_cols] as usize;
                let col_name = &table_schema.columns[col_idx].name;
                for (op, _) in range_conds {
                    conditions.push(format!("{col_name} {} ?", op_symbol(*op)));
                }
            }
            if conditions.is_empty() {
                format!("USING INDEX {index_name}")
            } else {
                format!("USING INDEX {index_name} ({})", conditions.join(", "))
            }
        }

        ScanPlan::InvertedScan { .. } => "USING INVERTED INDEX".to_string(),
    }
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{f}"),
        Value::Text(s) => format!("'{s}'"),
        Value::Blob(_) => "BLOB".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Date(d) => format!("DATE '{}'", crate::datetime::format_date(*d)),
        Value::Time(t) => format!("TIME '{}'", crate::datetime::format_time(*t)),
        Value::Timestamp(t) => format!("TIMESTAMP '{}'", crate::datetime::format_timestamp(*t)),
        Value::Interval {
            months,
            days,
            micros,
        } => format!(
            "INTERVAL '{}'",
            crate::datetime::format_interval(*months, *days, *micros)
        ),
        Value::Json(s) => format!("JSON '{s}'"),
        Value::Jsonb(_) => "JSONB '<binary>'".into(),
        Value::TsVector(_) => "TSVECTOR '<binary>'".into(),
        Value::TsQuery(_) => "TSQUERY '<binary>'".into(),
        Value::Array(_) => val.to_string(),
    }
}

fn op_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::Lt => "<",
        BinOp::LtEq => "<=",
        BinOp::Gt => ">",
        BinOp::GtEq => ">=",
        BinOp::Eq => "=",
        BinOp::NotEq => "!=",
        _ => "?",
    }
}

#[cfg(test)]
#[path = "planner_tests.rs"]
mod tests;
