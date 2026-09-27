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

fn schema(name: &str, cs: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(name.into(), cs, pk, vec![], vec![], vec![])
}

fn i(n: i64) -> Value {
    Value::Integer(n)
}

#[test]
fn resolves_in_lowercases_input() {
    let ts = schema(
        "t",
        cols(&[("name", DataType::Text), ("id", DataType::Integer)]),
        vec![1],
    );
    assert!(resolves_in("name", &ts));
    assert!(resolves_in("NAME", &ts));
    assert!(resolves_in("id", &ts));
}

#[test]
fn resolves_in_unknown_column_false() {
    let ts = schema("t", cols(&[("id", DataType::Integer)]), vec![0]);
    assert!(!resolves_in("missing", &ts));
}

fn unqualified(columns: &[&str]) -> Vec<ColumnName> {
    columns
        .iter()
        .map(|column| ColumnName {
            table: None,
            column: (*column).into(),
        })
        .collect()
}

#[test]
fn collect_column_names_single_column() {
    let mut out = Vec::new();
    collect_column_names(&Expr::Column("X".into()), &mut out);
    assert_eq!(out, unqualified(&["x"]));
}

#[test]
fn collect_column_names_qualified_lowercase() {
    let mut out = Vec::new();
    collect_column_names(
        &Expr::QualifiedColumn {
            table: "T".into(),
            column: "Col".into(),
        },
        &mut out,
    );
    assert_eq!(
        out,
        vec![ColumnName {
            table: Some("t".into()),
            column: "col".into(),
        }]
    );
}

#[test]
fn collect_column_names_keeps_a_dotted_qualifier_whole() {
    let mut out = Vec::new();
    collect_column_names(
        &Expr::QualifiedColumn {
            table: "information_schema.tables".into(),
            column: "table_name".into(),
        },
        &mut out,
    );
    assert_eq!(
        out,
        vec![ColumnName {
            table: Some("information_schema.tables".into()),
            column: "table_name".into(),
        }]
    );
}

#[test]
fn collect_column_names_binary_op_collects_both_sides() {
    let mut out = Vec::new();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Column("b".into())),
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, unqualified(&["a", "b"]));
}

#[test]
fn collect_column_names_literal_yields_empty() {
    let mut out = Vec::new();
    collect_column_names(&Expr::Literal(i(1)), &mut out);
    assert!(out.is_empty());
}

#[test]
fn collect_column_names_function_args() {
    let mut out = Vec::new();
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![Expr::Column("x".into())],
        distinct: false,
        filter: None,
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, unqualified(&["x"]));
}

#[test]
fn collect_column_names_coalesce_collects_all() {
    let mut out = Vec::new();
    let e = Expr::Coalesce(vec![
        Expr::Column("a".into()),
        Expr::Column("b".into()),
        Expr::Column("c".into()),
    ]);
    collect_column_names(&e, &mut out);
    assert_eq!(out, unqualified(&["a", "b", "c"]));
}

#[test]
fn collect_column_names_case_branches() {
    let mut out = Vec::new();
    let e = Expr::Case {
        operand: Some(Box::new(Expr::Column("op".into()))),
        conditions: vec![(Expr::Column("c".into()), Expr::Column("r".into()))],
        else_result: Some(Box::new(Expr::Column("el".into()))),
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, unqualified(&["op", "c", "r", "el"]));
}

#[test]
fn collect_column_names_between() {
    let mut out = Vec::new();
    let e = Expr::Between {
        expr: Box::new(Expr::Column("x".into())),
        low: Box::new(Expr::Column("lo".into())),
        high: Box::new(Expr::Column("hi".into())),
        negated: false,
    };
    collect_column_names(&e, &mut out);
    assert_eq!(out, unqualified(&["x", "lo", "hi"]));
}

#[test]
fn collect_column_names_unary_and_isnull() {
    let mut out = Vec::new();
    let inner = Expr::Column("x".into());
    collect_column_names(&Expr::IsNull(Box::new(inner.clone())), &mut out);
    collect_column_names(&Expr::IsNotNull(Box::new(inner)), &mut out);
    assert_eq!(out, unqualified(&["x", "x"]));
}

#[test]
fn flatten_and_exprs_no_and_returns_single() {
    let e = Expr::Literal(i(1));
    let v = flatten_and_exprs(&e);
    assert_eq!(v.len(), 1);
}

#[test]
fn flatten_and_exprs_chained_and_flattens() {
    let inner = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::And,
        right: Box::new(Expr::Column("b".into())),
    };
    let outer = Expr::BinaryOp {
        left: Box::new(inner),
        op: BinOp::And,
        right: Box::new(Expr::Column("c".into())),
    };
    let v = flatten_and_exprs(&outer);
    assert_eq!(v.len(), 3);
}

#[test]
fn flatten_and_exprs_or_does_not_flatten() {
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Or,
        right: Box::new(Expr::Column("b".into())),
    };
    let v = flatten_and_exprs(&e);
    assert_eq!(v.len(), 1);
}

#[test]
fn has_correlated_where_no_where_clause() {
    let outer = schema("o", cols(&[("x", DataType::Integer)]), vec![]);
    let ctx = CorrelationCtx {
        outer_schema: &outer,
        outer_alias: None,
    };
    let mgr = crate::schema::SchemaManager::empty();
    assert!(!has_correlated_where(&None, &ctx, &mgr));
}

#[test]
fn correlated_materialization_stops_after_a_mid_loop_cancel() {
    let token = citadel::CancelToken::new();
    let mut values: Vec<usize> = (0..1_000).collect();

    let err = retain_cancellable(&mut values, Some(&token), |value| {
        if *value == 1 {
            token.cancel();
        }
        Ok(true)
    })
    .unwrap_err();

    assert!(matches!(
        err,
        crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    assert_eq!(
        values.len(),
        crate::executor::helpers::CANCEL_CHECK_INTERVAL
    );
}

#[test]
fn correlated_in_probe_passes_cancellation_into_scalar_evaluation() {
    use citadel::{Argon2Profile, DatabaseBuilder};

    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("correlated-value-cancel.citadel"))
        .passphrase(b"correlated-value-cancel-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE outer_docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE inner_docs (id INTEGER PRIMARY KEY, outer_id INTEGER, body TEXT)")
        .unwrap();
    conn.execute("INSERT INTO outer_docs VALUES (1, 'several words to tokenize')")
        .unwrap();
    conn.execute("INSERT INTO inner_docs VALUES (1, 1, 'irrelevant')")
        .unwrap();

    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    let _cancel = crate::fts::cancel_tokenize_after(token, 1);
    let err = conn
        .query(
            "SELECT id FROM outer_docs AS o \
             WHERE TO_TSVECTOR(o.body) IN \
                   (SELECT i.body FROM inner_docs AS i WHERE i.outer_id = o.id)",
        )
        .expect_err("the correlated IN probe discarded its cancellation token");

    assert!(matches!(
        err,
        crate::error::SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn correlated_in_reused_key_survives_probe_branches_and_cancellation() {
    let rows = InRows::from_distinct_tuples(
        vec![vec![i(1), i(7)], vec![i(2), i(9)], vec![i(2), Value::Null]],
        &[Collation::Binary],
        Collation::Binary,
        None,
    )
    .unwrap();
    let mut key = Vec::with_capacity(2);
    for (group, selected, negated, expected) in [
        (1, i(7), false, true),
        (1, i(9), false, false),
        (1, i(9), true, true),
        (2, i(7), true, false),
        (2, i(9), true, false),
        (1, Value::Null, false, false),
    ] {
        key.clear();
        key.push(i(group));
        assert_eq!(
            rows.passes(&mut key, negated, None, || Ok(selected))
                .unwrap(),
            expected,
        );
        assert_eq!(key, [i(group)], "probe value leaked into correlation key");
    }
    key[0] = i(3);
    assert!(rows
        .passes(&mut key, true, None, || panic!(
            "empty group evaluated operand"
        ))
        .unwrap());
    assert_eq!(key, [i(3)]);

    key[0] = i(1);
    let token = citadel::CancelToken::new();
    let error = rows
        .passes(&mut key, false, Some(&token), || {
            token.cancel();
            Ok(i(7))
        })
        .unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    assert_eq!(key, [i(1)], "cancelled probe left an appended value");
    assert!(rows.passes(&mut key, false, None, || Ok(i(7))).unwrap());
    assert_eq!(key, [i(1)]);
}

#[test]
fn shared_in_tuple_indexes_match_rowwise_coercion_collation_and_null_semantics() {
    fn verify(
        source: Vec<Vec<Value>>,
        collations: &[Collation],
        value_collation: Collation,
        outer_keys: &[Vec<Value>],
        operands: &[Value],
    ) {
        let distinct: FxHashSet<_> = source.iter().cloned().map(InTupleKey).collect();
        let rows = InRows::from_distinct_tuples(
            distinct.into_iter().map(|tuple| tuple.0).collect(),
            collations,
            value_collation,
            None,
        )
        .unwrap();
        for key in outer_keys {
            let matched: Vec<_> = source
                .iter()
                .filter(|row| {
                    key.iter()
                        .zip(row.iter())
                        .zip(collations)
                        .all(|((a, b), coll)| crate::eval::collated_eq(a, b, Some(*coll)).unwrap())
                })
                .map(|row| &row[key.len()])
                .collect();
            for operand in operands {
                for negated in [false, true] {
                    let expected = if matched.is_empty() {
                        negated
                    } else if operand.is_null() {
                        false
                    } else if matched.iter().any(|value| {
                        crate::eval::collated_eq(operand, value, Some(value_collation)).unwrap()
                    }) {
                        !negated
                    } else {
                        negated && !matched.iter().any(|value| value.is_null())
                    };
                    let mut probe = key.clone();
                    let mut calls = 0;
                    assert_eq!(
                        rows.passes(&mut probe, negated, None, || {
                            calls += 1;
                            Ok(operand.clone())
                        })
                        .unwrap(),
                        expected,
                        "key={key:?}, operand={operand:?}, negated={negated}"
                    );
                    assert_eq!(probe, *key);
                    assert_eq!(calls, usize::from(!matched.is_empty()));
                }
            }
        }
    }
    let text = |s: &str| Value::Text(s.into());
    verify(
        vec![
            vec![text("A"), i(1), text("one")],
            vec![text("a"), i(1), text("TWO")],
            vec![text("a"), i(1), Value::Null],
            vec![text("a"), i(2), text("three")],
            vec![Value::Null, i(1), text("one")],
        ],
        &[Collation::NoCase, Collation::Binary],
        Collation::NoCase,
        &[
            vec![text("A"), i(1)],
            vec![text("a"), i(2)],
            vec![text("missing"), i(1)],
            vec![Value::Null, i(1)],
        ],
        &[
            text("ONE"),
            text("two"),
            text("three"),
            text("absent"),
            Value::Null,
        ],
    );
    let values = [
        Value::Date(0),
        Value::Date(1),
        Value::Timestamp(0),
        text("1970-01-01"),
        text("1970-01-01 00:00:00"),
        i(0),
        Value::Real(0.0),
        Value::Null,
    ];
    // Equal INTEGER/REAL values must both survive deduplication: their
    // comparisons to temporal values follow different coercion rules.
    let mut source = vec![vec![Value::Real(0.0), i(42)], vec![i(0), i(42)]];
    for (index, value) in values.iter().enumerate() {
        source.push(vec![
            value.clone(),
            values[(index + 2) % values.len()].clone(),
        ]);
        source.push(vec![
            value.clone(),
            values[(index + 5) % values.len()].clone(),
        ]);
    }
    verify(
        source,
        &[Collation::Binary],
        Collation::Binary,
        &values.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>(),
        &values.iter().cloned().chain([i(42)]).collect::<Vec<_>>(),
    );
}
