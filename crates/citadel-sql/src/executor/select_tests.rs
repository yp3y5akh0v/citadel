use super::*;
use crate::eval::ColumnMap;
use crate::parser::{Expr, SelectColumn, SelectStmt};
use crate::types::{Collation, ColumnDef, DataType, ExecutionResult, Value};

fn col(name: &str, dt: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable: true,
        position: 0,
        default_expr: None,
        default_sql: None,
        check_expr: None,
        check_sql: None,
        check_name: None,
        is_with_timezone: false,
        generated_expr: None,
        generated_sql: None,
        generated_kind: None,
        collation: Collation::Binary,
    }
}

fn cols(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(i, (n, t))| {
            let mut c = col(n, *t);
            c.position = i as u16;
            c
        })
        .collect()
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn empty_select(from: &str) -> SelectStmt {
    SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: from.into(),
        from_alias: None,
        from_subquery: None,
        from_args: None,
        from_json_table: None,
        joins: vec![],
        distinct: false,
        where_clause: None,
        order_by: vec![],
        limit: None,
        offset: None,
        group_by: vec![],
        having: None,
    }
}

#[test]
fn compute_scan_limit_none_when_no_limit() {
    let s = empty_select("t");
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_simple_limit() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    assert_eq!(compute_scan_limit(&s), Some(10));
}

#[test]
fn compute_scan_limit_with_offset_adds() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(5)));
    s.offset = Some(Expr::Literal(i(3)));
    assert_eq!(compute_scan_limit(&s), Some(8));
}

#[test]
fn compute_scan_limit_none_with_order_by() {
    use crate::parser::OrderByItem;
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.order_by = vec![OrderByItem {
        expr: Expr::Column("x".into()),
        descending: false,
        nulls_first: None,
    }];
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_none_with_group_by() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.group_by = vec![Expr::Column("x".into())];
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn compute_scan_limit_none_with_distinct() {
    let mut s = empty_select("t");
    s.limit = Some(Expr::Literal(i(10)));
    s.distinct = true;
    assert_eq!(compute_scan_limit(&s), None);
}

#[test]
fn try_count_star_shortcut_matches_select_count_star() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    let r = try_count_star_shortcut(&s, || Ok(42)).unwrap();
    assert!(matches!(
        r,
        Some(ExecutionResult::Query(q)) if q.rows[0][0] == i(42)
    ));
}

#[test]
fn try_count_star_shortcut_rejects_where_clause() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: None,
    }];
    s.where_clause = Some(Expr::Literal(Value::Boolean(true)));
    let r = try_count_star_shortcut(&s, || Ok(1)).unwrap();
    assert!(r.is_none());
}

#[test]
fn try_count_star_shortcut_rejects_extra_columns() {
    let mut s = empty_select("t");
    s.columns = vec![
        SelectColumn::Expr {
            expr: Expr::CountStar,
            alias: None,
        },
        SelectColumn::Expr {
            expr: Expr::Column("x".into()),
            alias: None,
        },
    ];
    let r = try_count_star_shortcut(&s, || Ok(1)).unwrap();
    assert!(r.is_none());
}

#[test]
fn try_count_star_shortcut_uses_alias() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: Expr::CountStar,
        alias: Some("n".into()),
    }];
    let r = try_count_star_shortcut(&s, || Ok(7)).unwrap();
    if let Some(ExecutionResult::Query(q)) = r {
        assert_eq!(q.columns[0], "n");
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn resolve_simple_col_unqualified_resolves() {
    let cs = cols(&[("a", DataType::Integer), ("b", DataType::Text)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(resolve_simple_col(&Expr::Column("a".into()), &cm), Some(0));
    assert_eq!(resolve_simple_col(&Expr::Column("b".into()), &cm), Some(1));
}

#[test]
fn resolve_simple_col_unknown_returns_none() {
    let cs = cols(&[("a", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(
        resolve_simple_col(&Expr::Column("missing".into()), &cm),
        None
    );
}

#[test]
fn resolve_simple_col_non_column_returns_none() {
    let cs = cols(&[("a", DataType::Integer)]);
    let cm = ColumnMap::new(&cs);
    assert_eq!(resolve_simple_col(&Expr::Literal(i(1)), &cm), None);
}
