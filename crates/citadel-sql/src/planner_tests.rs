use super::*;
use crate::types::{ColumnDef, DataType};

fn col(name: &str, dt: DataType, nullable: bool, pos: u16) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: dt,
        nullable,
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
    }
}

fn test_schema() -> TableSchema {
    TableSchema::new(
        "users".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("name", DataType::Text, true, 1),
            col("age", DataType::Integer, true, 2),
            col("email", DataType::Text, true, 3),
        ],
        vec![0],
        vec![
            IndexDef {
                name: "idx_name".into(),
                columns: vec![1],
                unique: false,
            },
            IndexDef {
                name: "idx_email".into(),
                columns: vec![3],
                unique: true,
            },
            IndexDef {
                name: "idx_name_age".into(),
                columns: vec![1, 2],
                unique: false,
            },
        ],
        vec![],
        vec![],
    )
}

#[test]
fn no_where_is_seq_scan() {
    let schema = test_schema();
    let plan = plan_select(&schema, &None);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn pk_equality_is_pk_lookup() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Integer(42))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::PkLookup { pk_values } => {
            assert_eq!(pk_values, vec![Value::Integer(42)]);
        }
        other => panic!("expected PkLookup, got {other:?}"),
    }
}

#[test]
fn unique_index_equality() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("email".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("alice@test.com".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            is_unique,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_email");
            assert!(is_unique);
            assert_eq!(num_prefix_cols, 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn non_unique_index_equality() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert!(index_name == "idx_name" || index_name == "idx_name_age");
            assert_eq!(num_prefix_cols, 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn composite_index_full_prefix() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(30))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 2);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn range_scan_on_indexed_column() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("name".into())),
        op: BinOp::Gt,
        right: Box::new(Expr::Literal(Value::Text("M".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            range_conds,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(num_prefix_cols, 0);
            assert_eq!(range_conds.len(), 1);
            assert_eq!(range_conds[0].0, BinOp::Gt);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn composite_equality_plus_range() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Gt,
            right: Box::new(Expr::Literal(Value::Integer(25))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            range_conds,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 1);
            assert_eq!(range_conds.len(), 1);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn or_condition_falls_back_to_seq_scan() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Bob".into()))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn non_indexed_column_is_seq_scan() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("age".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Integer(30))),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::SeqScan));
}

#[test]
fn reversed_literal_column() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Literal(Value::Integer(42))),
        op: BinOp::Eq,
        right: Box::new(Expr::Column("id".into())),
    });
    let plan = plan_select(&schema, &where_clause);
    assert!(matches!(plan, ScanPlan::PkLookup { .. }));
}

#[test]
fn reversed_comparison_flips_op() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Literal(Value::Integer(5))),
        op: BinOp::Lt,
        right: Box::new(Expr::Column("name".into())),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan { range_conds, .. } => {
            assert_eq!(range_conds[0].0, BinOp::Gt);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn prefers_unique_index() {
    let schema = TableSchema::new(
        "t".into(),
        vec![
            col("id", DataType::Integer, false, 0),
            col("code", DataType::Text, false, 1),
        ],
        vec![0],
        vec![
            IndexDef {
                name: "idx_code".into(),
                columns: vec![1],
                unique: false,
            },
            IndexDef {
                name: "idx_code_uniq".into(),
                columns: vec![1],
                unique: true,
            },
        ],
        vec![],
        vec![],
    );
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("code".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(Value::Text("X".into()))),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            is_unique,
            ..
        } => {
            assert_eq!(index_name, "idx_code_uniq");
            assert!(is_unique);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}

#[test]
fn prefers_more_equality_columns() {
    let schema = test_schema();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("name".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Text("Alice".into()))),
        }),
        op: BinOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".into())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(Value::Integer(30))),
        }),
    });
    let plan = plan_select(&schema, &where_clause);
    match plan {
        ScanPlan::IndexScan {
            index_name,
            num_prefix_cols,
            ..
        } => {
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(num_prefix_cols, 2);
        }
        other => panic!("expected IndexScan, got {other:?}"),
    }
}
