use super::*;
use crate::parser::{BinOp, Expr};
use crate::types::{Collation, ColumnDef, DataType, TableSchema, Value};

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

fn schema(name: &str, cs: Vec<ColumnDef>) -> TableSchema {
    TableSchema::new(name.into(), cs, vec![], vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn table_alias_or_name_uses_alias_when_present() {
    assert_eq!(table_alias_or_name("customers", &Some("c".into())), "c");
}

#[test]
fn table_alias_or_name_falls_back_to_table_name_lowercased() {
    assert_eq!(table_alias_or_name("Customers", &None), "customers");
}

#[test]
fn build_joined_columns_two_tables_prefixes_with_alias() {
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    let b = schema("b", cols(&[("y", DataType::Text)]));
    let result = build_joined_columns(&[("a".into(), &a), ("b".into(), &b)]);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].name, "a.x");
    assert_eq!(result[1].name, "b.y");
}

#[test]
fn build_joined_columns_alias_lowercased() {
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    let result = build_joined_columns(&[("UpperAlias".into(), &a)]);
    assert_eq!(result[0].name, "upperalias.x");
}

#[test]
fn build_joined_columns_position_is_sequential() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Text)]),
    );
    let b = schema("b", cols(&[("z", DataType::Integer)]));
    let result = build_joined_columns(&[("a".into(), &a), ("b".into(), &b)]);
    for (i, c) in result.iter().enumerate() {
        assert_eq!(c.position as usize, i);
    }
}

#[test]
fn build_joined_columns_data_types_preserved() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Text)]),
    );
    let result = build_joined_columns(&[("a".into(), &a)]);
    assert_eq!(result[0].data_type, DataType::Integer);
    assert_eq!(result[1].data_type, DataType::Text);
}

#[test]
fn extend_joined_columns_appends_alias_prefixed() {
    let mut out: Vec<ColumnDef> = vec![];
    let a = schema("a", cols(&[("x", DataType::Integer)]));
    extend_joined_columns(&mut out, &("a".into(), &a));
    assert_eq!(out[0].name, "a.x");
    assert_eq!(out[0].position, 0);
}

#[test]
fn extend_joined_columns_continues_position_count() {
    let a = schema(
        "a",
        cols(&[("x", DataType::Integer), ("y", DataType::Integer)]),
    );
    let mut out: Vec<ColumnDef> = build_joined_columns(&[("a".into(), &a)]);
    let b = schema("b", cols(&[("z", DataType::Integer)]));
    extend_joined_columns(&mut out, &("b".into(), &b));
    assert_eq!(out.len(), 3);
    assert_eq!(out[2].name, "b.z");
    assert_eq!(out[2].position, 2);
}

#[test]
fn resolve_col_idx_unqualified_unique() {
    let cs = cols(&[("a.x", DataType::Integer), ("a.y", DataType::Integer)]);
    let r = resolve_col_idx(&Expr::Column("x".into()), &cs);
    assert_eq!(r, Some(0));
}

#[test]
fn resolve_col_idx_ambiguous_returns_none() {
    let cs = cols(&[("a.x", DataType::Integer), ("b.x", DataType::Integer)]);
    let r = resolve_col_idx(&Expr::Column("x".into()), &cs);
    assert_eq!(r, None);
}

#[test]
fn resolve_col_idx_qualified_finds_exact_match() {
    let cs = cols(&[("a.x", DataType::Integer), ("b.x", DataType::Integer)]);
    let r = resolve_col_idx(
        &Expr::QualifiedColumn {
            table: "b".into(),
            column: "x".into(),
        },
        &cs,
    );
    assert_eq!(r, Some(1));
}

#[test]
fn resolve_col_idx_unknown_returns_none() {
    let cs = cols(&[("a.x", DataType::Integer)]);
    assert_eq!(resolve_col_idx(&Expr::Column("missing".into()), &cs), None);
}

#[test]
fn hash_key_extracts_indices_in_order() {
    let row = vec![i(1), i(2), i(3), i(4)];
    let key = hash_key(&row, &[2, 0]);
    assert_eq!(key, vec![i(3), i(1)]);
}

#[test]
fn hash_key_empty_indices_yields_empty_key() {
    let row = vec![i(1), i(2)];
    let key = hash_key(&row, &[]);
    assert!(key.is_empty());
}

#[test]
fn count_conjuncts_single() {
    assert_eq!(count_conjuncts(&Expr::Literal(i(1))), 1);
}

#[test]
fn count_conjuncts_nested_and() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("a".into())),
            op: BinOp::And,
            right: Box::new(Expr::Column("b".into())),
        }),
        op: BinOp::And,
        right: Box::new(Expr::Column("c".into())),
    };
    assert_eq!(count_conjuncts(&e), 3);
}

#[test]
fn count_conjuncts_or_does_not_split() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Column("b".into())),
    };
    assert_eq!(count_conjuncts(&e), 1);
}

#[test]
fn combine_row_concatenates() {
    let combined = combine_row(&[i(1), i(2)], &[i(3), i(4)], 4);
    assert_eq!(combined, vec![i(1), i(2), i(3), i(4)]);
}

#[test]
fn extract_equi_join_keys_simple_equi() {
    let combined = cols(&[("a.id", DataType::Integer), ("b.a_id", DataType::Integer)]);
    let on = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedColumn {
            table: "a".into(),
            column: "id".into(),
        }),
        op: BinOp::Eq,
        right: Box::new(Expr::QualifiedColumn {
            table: "b".into(),
            column: "a_id".into(),
        }),
    };
    let pairs = extract_equi_join_keys(&on, &combined, 1);
    assert_eq!(pairs, vec![(0, 0)]);
}

#[test]
fn extract_equi_join_keys_non_equi_returns_empty() {
    let combined = cols(&[("a.id", DataType::Integer), ("b.a_id", DataType::Integer)]);
    let on = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedColumn {
            table: "a".into(),
            column: "id".into(),
        }),
        op: BinOp::Lt,
        right: Box::new(Expr::QualifiedColumn {
            table: "b".into(),
            column: "a_id".into(),
        }),
    };
    let pairs = extract_equi_join_keys(&on, &combined, 1);
    assert!(pairs.is_empty());
}
