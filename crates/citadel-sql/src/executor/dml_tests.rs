use super::*;
use crate::parser::{
    CompoundSelect, DeleteStmt, Expr, InsertSource, InsertStmt, QueryBody, SelectColumn,
    SelectStmt, SetOp, UpdateStmt,
};
use crate::types::{ExecutionResult, QueryResult, Value};

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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn qr(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult {
        columns: columns.into_iter().map(String::from).collect(),
        rows,
    }
}

fn scalar_subq(from: &str) -> Expr {
    Expr::ScalarSubquery(Box::new(empty_select(from)))
}

fn integer_template_columns(names: &[&str]) -> Vec<ColumnDef> {
    names
        .iter()
        .enumerate()
        .map(|(position, name)| ColumnDef {
            name: (*name).into(),
            data_type: DataType::Integer,
            nullable: false,
            position: position as u16,
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
        })
        .collect()
}

#[test]
fn selected_row_binding_moves_heap_values_to_target_positions() {
    let mut columns = integer_template_columns(&["id", "name", "payload"]);
    columns[1].data_type = DataType::Text;
    columns[2].data_type = DataType::Blob;
    let schema = TableSchema::new("t".into(), columns, vec![0], vec![], vec![], vec![]);
    let text = "selected-row-text".repeat(32);
    let source_text = Value::Text(text.clone().into());
    let source_blob = Value::Blob(vec![0xa5; 1_024]);
    let text_ptr = match &source_text {
        Value::Text(value) => value.as_ptr(),
        _ => unreachable!(),
    };
    let blob_ptr = match &source_blob {
        Value::Blob(value) => value.as_ptr(),
        _ => unreachable!(),
    };
    let mut source = vec![source_blob, source_text];
    let mut row = vec![i(7), Value::Null, Value::Null];

    bind_selected_row(&mut source, &mut row, &[2, 1], &schema).unwrap();

    assert!(source.is_empty());
    assert_eq!(row[0], i(7));
    let Value::Text(value) = &row[1] else {
        panic!("expected text");
    };
    assert_eq!(value.as_str(), text);
    assert_eq!(value.as_ptr(), text_ptr);
    let Value::Blob(value) = &row[2] else {
        panic!("expected blob");
    };
    assert_eq!(value.as_slice(), &[0xa5; 1_024]);
    assert_eq!(value.as_ptr(), blob_ptr);
}

#[test]
fn selected_row_binding_checks_width_before_moving_values() {
    let schema = TableSchema::new(
        "t".into(),
        integer_template_columns(&["id", "value"]),
        vec![0],
        vec![],
        vec![],
        vec![],
    );
    let mut source = vec![i(1)];
    let mut row = vec![i(7), i(9)];

    let error = bind_selected_row(&mut source, &mut row, &[1, 0], &schema).unwrap_err();

    assert!(matches!(error, SqlError::InvalidValue(message)
        if message == "INSERT ... SELECT column count mismatch: expected 2, got 1"));
    assert_eq!(source, vec![i(1)]);
    assert_eq!(row, vec![i(7), i(9)]);
}

#[test]
fn selected_rows_validate_metadata_independently_of_row_count() {
    for rows in [vec![], vec![vec![i(1)]]] {
        let error = insert_select_rows(qr(vec!["id"], rows), 2).unwrap_err();
        assert!(matches!(error, SqlError::InvalidValue(message)
            if message == "INSERT ... SELECT column count mismatch: expected 2, got 1"));
    }
    assert!(insert_select_rows(qr(vec!["id", "value"], vec![]), 2)
        .unwrap()
        .is_empty());
    let result = qr(vec!["id", "value"], vec![vec![i(1), i(2)]]);
    let rows_ptr = result.rows.as_ptr();
    let rows = insert_select_rows(result, 2).unwrap();
    assert_eq!(rows, vec![vec![i(1), i(2)]]);
    assert_eq!(rows.as_ptr(), rows_ptr);
}

fn compile_generated_insert_template(generated: &str, values: &str) -> CompiledInsert {
    let mut columns = integer_template_columns(&["id", "a", "b", "g"]);
    columns[3].generated_expr = Some(crate::parser::parse_sql_expr(generated).unwrap());
    columns[3].generated_sql = Some(generated.into());
    columns[3].generated_kind = Some(GeneratedKind::Stored);
    let mut schema = SchemaManager::empty();
    schema.register(TableSchema::new(
        "t".into(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    ));
    let Statement::Insert(stmt) =
        crate::parser::parse_sql(&format!("INSERT INTO t (id, a, b) VALUES ({values})")).unwrap()
    else {
        panic!("expected INSERT statement");
    };
    CompiledInsert::try_compile(&schema, &stmt)
        .expect("generated arithmetic must not prevent preparing the INSERT")
}

#[test]
fn trivial_generated_insert_templates_preserve_parameter_shapes() {
    for (generated, values) in [
        ("a + b", "$1, $2, $3"),
        ("a + b", "$1, $2, 3"),
        ("a + b", "$1, 3, $2"),
        ("a * 2 + 1", "$1, $2, 0"),
    ] {
        let compiled = compile_generated_insert_template(generated, values);
        let cache = compiled.cached.as_ref().unwrap();
        assert!(
            cache.trivial_fast_program.is_some(),
            "generated: {generated}; values: {values}"
        );
        assert!(cache.is_trivial_fast);
    }
}

#[test]
fn trivial_generated_insert_templates_preserve_valid_literal_folds() {
    for (generated, values, expected) in [("a + b", "$1, 3, 4", 7), ("a * 2 + 1", "$1, 5, 0", 11)] {
        let compiled = compile_generated_insert_template(generated, values);
        let cache = compiled.cached.as_ref().unwrap();
        let program = cache.trivial_fast_program.as_ref().unwrap();
        assert!(cache.is_trivial_fast);
        assert!(program
            .ops
            .iter()
            .any(|op| { matches!(op, WriteOp::LiteralI64 { value, .. } if *value == expected) }));
    }
}

#[test]
fn trivial_generated_insert_templates_defer_literal_addition_overflow() {
    let compiled = compile_generated_insert_template("a + b", &format!("$1, {}, 1", i64::MAX));
    let cache = compiled.cached.as_ref().unwrap();
    assert!(cache.trivial_fast_program.is_none());
    assert!(!cache.is_trivial_fast);
}

#[test]
fn trivial_generated_insert_templates_defer_literal_mul_add_overflow() {
    for (generated, value) in [("a * 2 + 1", i64::MAX), ("a * 2 + 2", i64::MAX / 2)] {
        let compiled = compile_generated_insert_template(generated, &format!("$1, {value}, 0"));
        let cache = compiled.cached.as_ref().unwrap();
        assert!(cache.trivial_fast_program.is_none(), "{generated}");
        assert!(!cache.is_trivial_fast);
    }
}

fn compile_upsert_counter_template(
    generated_kind: Option<GeneratedKind>,
    assignments: &str,
) -> CompiledInsert {
    let mut columns = integer_template_columns(if generated_kind.is_some() {
        &["id", "counter", "g"]
    } else {
        &["id", "counter"]
    });
    if let Some(kind) = generated_kind {
        columns[2].generated_expr = Some(crate::parser::parse_sql_expr("counter * 2 + 1").unwrap());
        columns[2].generated_sql = Some("counter * 2 + 1".into());
        columns[2].generated_kind = Some(kind);
    }
    let mut schema = SchemaManager::empty();
    schema.register(TableSchema::new(
        "t".into(),
        columns,
        vec![0],
        vec![],
        vec![],
        vec![],
    ));
    let Statement::Insert(stmt) = crate::parser::parse_sql(&format!(
        "INSERT INTO t (id, counter) VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET {assignments}"
    ))
    .unwrap() else {
        panic!("expected INSERT statement");
    };
    CompiledInsert::try_compile(&schema, &stmt).expect("UPSERT must remain preparable")
}

fn assert_trivial_upsert_patch(assignments: &str) {
    let compiled = compile_upsert_counter_template(None, assignments);
    let cache = compiled.cached.as_ref().unwrap();
    let program = cache
        .trivial_fast_program
        .as_ref()
        .expect("simple PK counters must retain the direct template");
    assert!(cache.is_trivial_fast);
    assert!(matches!(&program.on_dup, DupPolicy::Patch(paths) if paths.len() == 1));
}

#[test]
fn trivial_upsert_counter_addition_retains_patch() {
    assert_trivial_upsert_patch("counter = counter + 1");
}

#[test]
fn trivial_upsert_counter_subtract_min_retains_patch() {
    assert_trivial_upsert_patch("counter = counter - -9223372036854775808");
}

fn assert_generated_upsert_excludes_patch(kind: GeneratedKind) {
    let compiled = compile_upsert_counter_template(Some(kind), "counter = counter + 1");
    let cache = compiled.cached.as_ref().unwrap();
    assert!(!matches!(
        cache.trivial_fast_program.as_ref().map(|p| &p.on_dup),
        Some(DupPolicy::Patch(_))
    ));
}

#[test]
fn trivial_upsert_stored_generated_excludes_patch() {
    assert_generated_upsert_excludes_patch(GeneratedKind::Stored);
}

#[test]
fn trivial_upsert_virtual_generated_excludes_patch() {
    assert_generated_upsert_excludes_patch(GeneratedKind::Virtual);
}

#[test]
fn trivial_upsert_duplicate_targets_exclude_fast_paths() {
    let compiled =
        compile_upsert_counter_template(None, "counter = counter + 1, counter = counter + 2");
    let cache = compiled.cached.as_ref().unwrap();
    let Some(CompiledOnConflict::DoUpdate {
        assignments,
        fast_paths,
        ..
    }) = cache.on_conflict.as_deref()
    else {
        panic!("expected DO UPDATE conflict action");
    };
    assert_eq!(assignments.len(), 2);
    assert!(fast_paths.is_none());
    assert!(!matches!(
        cache.trivial_fast_program.as_ref().map(|p| &p.on_dup),
        Some(DupPolicy::Patch(_))
    ));
}

#[test]
fn has_subquery_literal_false() {
    assert!(!has_subquery(&Expr::Literal(i(1))));
}

#[test]
fn has_subquery_scalar_subquery() {
    assert!(has_subquery(&scalar_subq("t")));
}

#[test]
fn stmt_has_subquery_in_where() {
    let mut s = empty_select("t");
    s.where_clause = Some(scalar_subq("inner"));
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_in_columns() {
    let mut s = empty_select("t");
    s.columns = vec![SelectColumn::Expr {
        expr: scalar_subq("inner"),
        alias: None,
    }];
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_in_having() {
    let mut s = empty_select("t");
    s.having = Some(scalar_subq("inner"));
    assert!(stmt_has_subquery(&s));
}

#[test]
fn stmt_has_subquery_none_returns_false() {
    let s = empty_select("t");
    assert!(!stmt_has_subquery(&s));
}

#[test]
fn update_has_subquery_in_where() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), Expr::Literal(i(1)))],
        where_clause: Some(scalar_subq("inner")),
        returning: None,
    };
    assert!(update_has_subquery(&s));
}

#[test]
fn update_has_subquery_in_assignment() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), scalar_subq("inner"))],
        where_clause: None,
        returning: None,
    };
    assert!(update_has_subquery(&s));
}

#[test]
fn update_has_subquery_none() {
    let s = UpdateStmt {
        table: "t".into(),
        assignments: vec![("v".into(), Expr::Literal(i(1)))],
        where_clause: Some(Expr::Literal(i(1))),
        returning: None,
    };
    assert!(!update_has_subquery(&s));
}

#[test]
fn delete_has_subquery_in_where() {
    let s = DeleteStmt {
        table: "t".into(),
        where_clause: Some(scalar_subq("inner")),
        returning: None,
    };
    assert!(delete_has_subquery(&s));
}

#[test]
fn delete_has_subquery_none() {
    let s = DeleteStmt {
        table: "t".into(),
        where_clause: None,
        returning: None,
    };
    assert!(!delete_has_subquery(&s));
}

#[test]
fn insert_has_subquery_in_values() {
    let s = InsertStmt {
        table: "t".into(),
        columns: vec!["id".into()],
        source: InsertSource::Values(vec![vec![scalar_subq("inner")]]),
        on_conflict: None,
        returning: None,
    };
    assert!(insert_has_subquery(&s));
}

#[test]
fn insert_has_subquery_select_source_returns_false() {
    let sq = crate::parser::SelectQuery {
        ctes: vec![],
        body: QueryBody::Select(Box::new(empty_select("src"))),
        recursive: false,
    };
    let s = InsertStmt {
        table: "t".into(),
        columns: vec!["id".into()],
        source: InsertSource::Select(Box::new(sq)),
        on_conflict: None,
        returning: None,
    };
    assert!(!insert_has_subquery(&s));
}

#[test]
fn apply_set_operation_union_all_concatenates() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: true,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(
        &SchemaManager::empty(),
        &CteContext::default(),
        &comp,
        left,
        right,
        None,
    )
    .unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 4);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_union_dedupes() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(
        &SchemaManager::empty(),
        &CteContext::default(),
        &comp,
        left,
        right,
        None,
    )
    .unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 3);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_intersect_keeps_common() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)], vec![i(3)]]);
    let right = qr(vec!["x"], vec![vec![i(2)], vec![i(3)], vec![i(4)]]);
    let comp = CompoundSelect {
        op: SetOp::Intersect,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(
        &SchemaManager::empty(),
        &CteContext::default(),
        &comp,
        left,
        right,
        None,
    )
    .unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 2);
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_except_removes_right() {
    let left = qr(vec!["x"], vec![vec![i(1)], vec![i(2)], vec![i(3)]]);
    let right = qr(vec!["x"], vec![vec![i(2)]]);
    let comp = CompoundSelect {
        op: SetOp::Except,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let result = apply_set_operation(
        &SchemaManager::empty(),
        &CteContext::default(),
        &comp,
        left,
        right,
        None,
    )
    .unwrap();
    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.rows.len(), 2);
        assert!(q.rows.contains(&vec![i(1)]));
        assert!(q.rows.contains(&vec![i(3)]));
    } else {
        panic!("expected Query result");
    }
}

#[test]
fn apply_set_operation_column_count_mismatch_errors() {
    let left = qr(vec!["a", "b"], vec![vec![i(1), i(2)]]);
    let right = qr(vec!["c"], vec![vec![i(3)]]);
    let comp = CompoundSelect {
        op: SetOp::Union,
        all: false,
        left: Box::new(QueryBody::Select(Box::new(empty_select("a")))),
        right: Box::new(QueryBody::Select(Box::new(empty_select("b")))),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    assert!(apply_set_operation(
        &SchemaManager::empty(),
        &CteContext::default(),
        &comp,
        left,
        right,
        None,
    )
    .is_err());
}

#[test]
fn materialize_expr_in_subquery_converts_to_in_set() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let e = Expr::InSubquery {
        expr: Box::new(Expr::Column("v".into())),
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::InSet { .. }));
}

#[test]
fn materialize_expr_scalar_subquery_becomes_literal() {
    let inner_qr = qr(vec!["x"], vec![vec![i(42)]]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Integer(42))));
}

#[test]
fn materialize_expr_scalar_subquery_empty_becomes_null() {
    let inner_qr = qr(vec!["x"], vec![]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Null)));
}

#[test]
fn materialize_expr_exists_true() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)]]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(true))));
}

#[test]
fn materialize_expr_not_exists_false_when_rows_present() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)]]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: true,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(false))));
}

#[test]
fn materialize_expr_exists_false_for_empty_subquery() {
    let inner_qr = qr(vec!["x"], vec![]);
    let e = Expr::Exists {
        subquery: Box::new(empty_select("inner")),
        negated: false,
    };
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Boolean(false))));
}

#[test]
fn materialize_expr_scalar_multiple_rows_errors() {
    let inner_qr = qr(vec!["x"], vec![vec![i(1)], vec![i(2)]]);
    let e = scalar_subq("inner");
    let mut exec_sub = |_: &SelectStmt| Ok(CteRows::binary(inner_qr.clone()));
    assert!(materialize_expr(&e, &mut exec_sub).is_err());
}

#[test]
fn materialize_expr_pass_through_literal() {
    let mut exec_sub = |_: &SelectStmt| {
        Ok(CteRows::binary(QueryResult {
            columns: vec![],
            rows: vec![],
        }))
    };
    let e = Expr::Literal(i(5));
    let result = materialize_expr(&e, &mut exec_sub).unwrap();
    assert!(matches!(result, Expr::Literal(Value::Integer(5))));
}

#[test]
fn materialize_query_body_pass_through_dml() {
    let body = QueryBody::Insert(Box::new(InsertStmt {
        table: "t".into(),
        columns: vec![],
        source: InsertSource::Values(vec![]),
        on_conflict: None,
        returning: None,
    }));
    let mut exec_sub = |_: &SelectStmt| {
        Ok(CteRows::binary(QueryResult {
            columns: vec![],
            rows: vec![],
        }))
    };
    let result = materialize_query_body(&body, &mut exec_sub).unwrap();
    assert!(matches!(result, QueryBody::Insert(_)));
}
