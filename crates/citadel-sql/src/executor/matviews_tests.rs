use super::*;
use crate::parser::{
    CompoundSelect, Expr, QueryBody, SelectColumn, SelectQuery, SelectStmt, SetOp,
};
use crate::types::{DataType, Value};

fn empty_select() -> SelectStmt {
    SelectStmt {
        columns: vec![SelectColumn::AllColumns],
        from: "t".into(),
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
fn encode_pk_key_integer_round_trip() {
    let mut buf = Vec::new();
    encode_pk_key(&Value::Integer(42), &mut buf);
    let decoded = decode_pk_value(&buf).unwrap();
    assert_eq!(decoded, Value::Integer(42));
}

#[test]
fn encode_pk_key_text_round_trip() {
    let mut buf = Vec::new();
    encode_pk_key(&Value::Text("hello".into()), &mut buf);
    let decoded = decode_pk_value(&buf).unwrap();
    assert_eq!(decoded, Value::Text("hello".into()));
}

#[test]
fn encode_pk_key_clears_buffer_first() {
    let mut buf = vec![1, 2, 3];
    encode_pk_key(&Value::Integer(1), &mut buf);
    let decoded = decode_pk_value(&buf).unwrap();
    assert_eq!(decoded, Value::Integer(1));
}

#[test]
fn derive_columns_infers_type_from_rows() {
    let names = vec!["id".into(), "label".into()];
    let rows = vec![vec![Value::Integer(1), Value::Text("a".into())]];
    let cols = derive_columns(&names, &rows);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].data_type, DataType::Integer);
    assert_eq!(cols[1].data_type, DataType::Text);
}

#[test]
fn derive_columns_first_column_not_nullable() {
    let names = vec!["pk".into(), "val".into()];
    let rows = vec![vec![Value::Integer(1), Value::Integer(2)]];
    let cols = derive_columns(&names, &rows);
    assert!(!cols[0].nullable);
    assert!(cols[1].nullable);
}

#[test]
fn derive_columns_lowercases_names() {
    let names = vec!["MixedCase".into()];
    let rows = vec![vec![Value::Integer(1)]];
    let cols = derive_columns(&names, &rows);
    assert_eq!(cols[0].name, "mixedcase");
}

#[test]
fn derive_columns_falls_back_to_text_when_all_null() {
    let names = vec!["x".into()];
    let rows = vec![vec![Value::Null], vec![Value::Null]];
    let cols = derive_columns(&names, &rows);
    assert_eq!(cols[0].data_type, DataType::Text);
}

#[test]
fn derive_columns_skips_null_when_inferring_type() {
    let names = vec!["x".into()];
    let rows = vec![vec![Value::Null], vec![Value::Integer(7)]];
    let cols = derive_columns(&names, &rows);
    assert_eq!(cols[0].data_type, DataType::Integer);
}

#[test]
fn reject_non_deterministic_now_rejected() {
    let mut sel = empty_select();
    sel.columns = vec![SelectColumn::Expr {
        expr: Expr::Function {
            name: "NOW".into(),
            args: vec![],
            distinct: false,
        },
        alias: None,
    }];
    let sq = SelectQuery {
        ctes: vec![],
        body: QueryBody::Select(Box::new(sel)),
        recursive: false,
    };
    assert!(reject_non_deterministic(&sq).is_err());
}

#[test]
fn reject_non_deterministic_random_rejected() {
    let mut sel = empty_select();
    sel.where_clause = Some(Expr::Function {
        name: "random".into(),
        args: vec![],
        distinct: false,
    });
    let sq = SelectQuery {
        ctes: vec![],
        body: QueryBody::Select(Box::new(sel)),
        recursive: false,
    };
    assert!(reject_non_deterministic(&sq).is_err());
}

#[test]
fn reject_non_deterministic_deterministic_ok() {
    let mut sel = empty_select();
    sel.columns = vec![SelectColumn::Expr {
        expr: Expr::Function {
            name: "UPPER".into(),
            args: vec![Expr::Column("name".into())],
            distinct: false,
        },
        alias: None,
    }];
    let sq = SelectQuery {
        ctes: vec![],
        body: QueryBody::Select(Box::new(sel)),
        recursive: false,
    };
    assert!(reject_non_deterministic(&sq).is_ok());
}

#[test]
fn reject_non_deterministic_dml_body_rejected() {
    let sq = SelectQuery {
        ctes: vec![],
        body: QueryBody::Insert(Box::new(crate::parser::InsertStmt {
            table: "t".into(),
            columns: vec![],
            source: crate::parser::InsertSource::Values(vec![]),
            on_conflict: None,
            returning: None,
        })),
        recursive: false,
    };
    assert!(reject_non_deterministic(&sq).is_err());
}

#[test]
fn reject_non_deterministic_compound_walks_both_sides() {
    let mut bad = empty_select();
    bad.where_clause = Some(Expr::Function {
        name: "NOW".into(),
        args: vec![],
        distinct: false,
    });
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select()))),
        right: Box::new(QueryBody::Select(Box::new(bad))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let sq = SelectQuery {
        ctes: vec![],
        body: QueryBody::Compound(Box::new(comp)),
        recursive: false,
    };
    assert!(reject_non_deterministic(&sq).is_err());
}

#[test]
fn references_matview_in_from_clause() {
    assert!(references_matview("SELECT * FROM mv WHERE x = 1", "mv"));
}

#[test]
fn references_matview_in_join_clause() {
    assert!(references_matview("SELECT * FROM t JOIN mv ON x = y", "mv"));
}

#[test]
fn references_matview_case_insensitive() {
    assert!(references_matview("SELECT * FROM MV", "mv"));
}

#[test]
fn references_matview_not_referenced_returns_false() {
    assert!(!references_matview("SELECT * FROM other", "mv"));
}
