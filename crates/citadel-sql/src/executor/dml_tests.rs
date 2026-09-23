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
fn compiled_insert_context_proof_checks_unreferenced_legacy_virtual_expressions() {
    for (generated, independent) in [
        ("a * 2 + 1", true),
        ("CURRENT_DATE", false),
        ("COALESCE(a, CURRENT_DATE)", false),
        ("DATE($1)", false),
    ] {
        let mut columns = integer_template_columns(&["id", "a", "g"]);
        columns[2].nullable = true;
        columns[2].generated_expr = Some(crate::parser::parse_sql_expr(generated).unwrap());
        columns[2].generated_sql = Some(generated.into());
        columns[2].generated_kind = Some(GeneratedKind::Virtual);
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
            crate::parser::parse_sql("INSERT INTO t(id,a) VALUES ($1,$2)").unwrap()
        else {
            panic!("expected INSERT statement");
        };
        let compiled = CompiledInsert::try_compile(&schema, &stmt).unwrap();
        // These legacy virtuals are not needed by the current template, but
        // their expressions must still participate in the context proof.
        assert!(compiled.cached.as_ref().unwrap().is_trivial_fast);
        assert_eq!(
            compiled.can_skip_session_context(),
            independent,
            "{generated}"
        );
    }
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

#[test]
fn insert_scratch_bounds_upsert_retention_on_return_error_panic_and_reentry() {
    for outcome in 0..3 {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_insert_scratch(|bufs| -> Result<()> {
                bufs.upsert_value_buf
                    .resize(citadel_core::MAX_INLINE_VALUE_SIZE * 3, 0x5a);
                with_insert_scratch(|nested| {
                    assert!(nested.upsert_value_buf.is_empty());
                    nested.upsert_value_buf.resize(8, 0xa5);
                });
                assert_eq!(
                    bufs.upsert_value_buf.len(),
                    citadel_core::MAX_INLINE_VALUE_SIZE * 3
                );
                match outcome {
                    0 => Ok(()),
                    1 => Err(SqlError::IntegerOverflow),
                    _ => panic!("scratch callback panicked"),
                }
            })
        }));
        match outcome {
            0 => assert!(matches!(result, Ok(Ok(())))),
            1 => assert!(matches!(result, Ok(Err(SqlError::IntegerOverflow)))),
            _ => assert!(result.is_err()),
        }
        INSERT_SCRATCH.with(|slot| assert_eq!(slot.borrow().upsert_value_buf.capacity(), 0));
    }
}

#[test]
fn prepared_mixed_upsert_reuses_inline_patch_buffer_and_preserves_fallbacks() {
    use citadel::{Argon2Profile, DatabaseBuilder};
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("mixed-buffer.db"))
        .passphrase(b"mixed-buffer")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c INTEGER, d INTEGER)")
        .unwrap();
    let insert = conn.prepare("INSERT INTO t VALUES($1,$2,$3)").unwrap();
    for id in 0..50 {
        insert
            .execute(&[i(id), if id == 3 { Value::Null } else { i(0) }, i(10)])
            .unwrap();
    }
    let mixed = conn
        .prepare("INSERT INTO t VALUES($1,1,12) ON CONFLICT(id) DO UPDATE SET c=c+1,d=d+2")
        .unwrap();
    INSERT_SCRATCH.with(|slot| {
        slot.borrow_mut().upsert_value_buf = Vec::with_capacity(64);
    });
    let allocation = INSERT_SCRATCH.with(|slot| slot.borrow().upsert_value_buf.as_ptr());
    conn.execute("BEGIN").unwrap();
    for id in 0..100 {
        mixed.execute(&[i(id)]).unwrap();
        INSERT_SCRATCH.with(|slot| {
            assert_eq!(
                slot.borrow().upsert_value_buf.as_ptr(),
                allocation,
                "inline allocation changed while processing row {id}"
            );
        });
    }
    INSERT_SCRATCH.with(|slot| {
        let scratch = slot.borrow();
        assert_eq!(scratch.upsert_value_buf.as_ptr(), allocation);
        assert!(scratch.upsert_value_buf.capacity() <= citadel_core::MAX_INLINE_VALUE_SIZE);
    });
    conn.execute("COMMIT").unwrap();
    let rows = conn.query("SELECT id,c,d FROM t ORDER BY id").unwrap().rows;
    assert_eq!(rows.len(), 100);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(
            *row,
            vec![
                i(id as i64),
                if id == 3 { Value::Null } else { i(1) },
                i(12)
            ]
        );
    }
    // Cross-target expressions use the ordinary simultaneous-assignment lane.
    let swap = conn
        .prepare("INSERT INTO t VALUES($1,0,0) ON CONFLICT(id) DO UPDATE SET c=d,d=c RETURNING c,d")
        .unwrap();
    assert_eq!(
        swap.query_collect(&[i(0)]).unwrap().rows,
        [vec![i(12), i(1)]]
    );
    // The binding fallback still rejects a parameter with the wrong PK type.
    conn.execute("BEGIN").unwrap();
    assert!(matches!(
        mixed.execute(&[Value::Text("50".into())]),
        Err(SqlError::TypeMismatch { .. })
    ));
    mixed.execute(&[i(50)]).unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT c,d FROM t WHERE id=50").unwrap().rows,
        [vec![i(2), i(14)]]
    );
    assert!(db.manager().integrity_check().unwrap().is_ok());
}

#[test]
fn prepared_text_key_upsert_retains_fused_scratch_across_null_and_savepoint() {
    use citadel::{Argon2Profile, DatabaseBuilder};
    let db = DatabaseBuilder::new("")
        .passphrase(b"text-upsert-buffer")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE ct(k TEXT NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO ct VALUES ('hot',5),('nullable',NULL)")
        .unwrap();
    let stmt = conn
        .prepare("INSERT INTO ct VALUES($1,1) ON CONFLICT(k) DO UPDATE SET c=c+1 RETURNING old.k,new.k,old.c,new.c")
        .unwrap();
    let hot = Value::Text("hot".into());
    let nullable = Value::Text("nullable".into());
    let fresh = Value::Text("fresh".into());
    INSERT_SCRATCH.with(|slot| {
        slot.borrow_mut().upsert_value_buf = Vec::with_capacity(64);
    });
    let allocation = INSERT_SCRATCH.with(|slot| slot.borrow().upsert_value_buf.as_ptr());
    let assert_buffer = |expected: &Value| {
        INSERT_SCRATCH.with(|slot| {
            let bufs = slot.borrow();
            assert_eq!(bufs.upsert_value_buf.as_ptr(), allocation);
            // Contents prove that the general TEXT-key route actually used the
            // retained buffer; pointer equality alone would also pass if unused.
            assert_eq!(
                crate::encoding::decode_columns(&bufs.upsert_value_buf, &[0])
                    .unwrap()
                    .as_slice(),
                std::slice::from_ref(expected)
            );
        });
    };
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT reusable").unwrap();
    for _ in 0..2 {
        for step in 0..3 {
            assert_eq!(
                stmt.query_collect(std::slice::from_ref(&hot)).unwrap().rows,
                [vec![hot.clone(), hot.clone(), i(5 + step), i(6 + step)]]
            );
            assert_buffer(&i(6 + step));
        }
        assert_eq!(
            stmt.query_collect(std::slice::from_ref(&nullable))
                .unwrap()
                .rows,
            [vec![
                nullable.clone(),
                nullable.clone(),
                Value::Null,
                Value::Null
            ]]
        );
        assert_buffer(&Value::Null);
        assert_eq!(
            stmt.query_collect(std::slice::from_ref(&fresh))
                .unwrap()
                .rows,
            [vec![Value::Null, fresh.clone(), Value::Null, i(1)]]
        );
        assert_buffer(&Value::Null);
        conn.execute("ROLLBACK TO reusable").unwrap();
        assert_eq!(
            conn.query("SELECT k,c FROM ct ORDER BY k").unwrap().rows,
            [vec![hot.clone(), i(5)], vec![nullable.clone(), Value::Null]]
        );
    }
    conn.execute("RELEASE reusable").unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(db.manager().integrity_check().unwrap().is_ok());
}

#[test]
fn text_key_upsert_scratch_handles_schema_expansion_and_interpreted_replacement() {
    use citadel::{Argon2Profile, DatabaseBuilder};
    let db = DatabaseBuilder::new("")
        .passphrase(b"text-upsert-expansion")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = crate::Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE ct(k TEXT NOT NULL PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO ct VALUES ('hot',5)").unwrap();
    let arithmetic = conn
        .prepare("INSERT INTO ct(k,c) VALUES($1,1) ON CONFLICT(k) DO UPDATE SET c=c+1 RETURNING old.c,new.c")
        .unwrap();
    conn.execute("ALTER TABLE ct ADD COLUMN d INTEGER DEFAULT 7")
        .unwrap();
    let hot = Value::Text("hot".into());
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        arithmetic
            .query_collect(std::slice::from_ref(&hot))
            .unwrap()
            .rows,
        [vec![i(5), i(6)]]
    );
    let expanded_allocation = INSERT_SCRATCH.with(|slot| {
        let bufs = slot.borrow();
        assert_eq!(
            crate::encoding::decode_columns(&bufs.upsert_value_buf, &[0, 1]).unwrap(),
            [i(6), i(7)]
        );
        bufs.upsert_value_buf.as_ptr()
    });
    assert_eq!(
        arithmetic
            .query_collect(std::slice::from_ref(&hot))
            .unwrap()
            .rows,
        [vec![i(6), i(7)]]
    );
    INSERT_SCRATCH.with(|slot| {
        assert_eq!(slot.borrow().upsert_value_buf.as_ptr(), expanded_allocation);
    });
    let replacement = conn
        .prepare("INSERT INTO ct(k,c) VALUES($1,$2) ON CONFLICT(k) DO UPDATE SET c=excluded.c RETURNING old.c,new.c,d")
        .unwrap();
    assert_eq!(
        replacement
            .query_collect(&[hot.clone(), Value::Null])
            .unwrap()
            .rows,
        [vec![i(7), Value::Null, i(7)]]
    );
    assert_eq!(
        replacement
            .query_collect(&[hot.clone(), i(9)])
            .unwrap()
            .rows,
        [vec![Value::Null, i(9), i(7)]]
    );
    assert!(conn
        .prepare("INSERT INTO ct(k,c) VALUES($1,0) ON CONFLICT(k) DO UPDATE SET c=excluded.c WHERE FALSE RETURNING c")
        .unwrap()
        .query_collect(std::slice::from_ref(&hot))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        arithmetic
            .query_collect(std::slice::from_ref(&hot))
            .unwrap()
            .rows,
        [vec![i(9), i(10)]]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT c,d FROM ct").unwrap().rows,
        [vec![i(10), i(7)]]
    );
    assert!(db.manager().integrity_check().unwrap().is_ok());
}

#[test]
fn text_key_upsert_scratch_isolates_nested_triggers_and_returning() {
    use citadel::{Argon2Profile, DatabaseBuilder};
    for explicit in [false, true] {
        let db = DatabaseBuilder::new("")
            .passphrase(b"text-upsert-trigger")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE ct(k TEXT NOT NULL PRIMARY KEY, c INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE nested(k TEXT NOT NULL PRIMARY KEY, c INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE TABLE audit(id INTEGER NOT NULL PRIMARY KEY, was INTEGER, now_ INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO ct VALUES ('hot',10)").unwrap();
        conn.execute("INSERT INTO nested VALUES ('nested',0)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER capture_ct AFTER UPDATE ON ct FOR EACH ROW BEGIN \
             INSERT INTO nested VALUES ('nested',1) ON CONFLICT(k) DO UPDATE SET c=c+1; \
             INSERT INTO audit VALUES (NEW.c,OLD.c,NEW.c); END",
        )
        .unwrap();
        let stmt = conn
            .prepare("INSERT INTO ct VALUES($1,1),($1,1) ON CONFLICT(k) DO UPDATE SET c=c+1 RETURNING old.c,new.c")
            .unwrap();
        let hot = Value::Text("hot".into());
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(
            stmt.query_collect(std::slice::from_ref(&hot)).unwrap().rows,
            [vec![i(10), i(11)], vec![i(11), i(12)]]
        );
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(conn.query("SELECT c FROM ct").unwrap().rows, [vec![i(12)]]);
        assert_eq!(
            conn.query("SELECT c FROM nested").unwrap().rows,
            [vec![i(2)]]
        );
        let audit = vec![vec![i(11), i(10), i(11)], vec![i(12), i(11), i(12)]];
        assert_eq!(
            conn.query("SELECT * FROM audit ORDER BY id").unwrap().rows,
            audit
        );

        // The nested conflict patches its own row before this trigger fails.
        // Neither it nor the outer UPDATE may become a committed prefix.
        conn.execute(
            "CREATE TRIGGER reject_nested AFTER UPDATE ON nested FOR EACH ROW \
             BEGIN INSERT INTO audit VALUES (11,0,0); END",
        )
        .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert!(matches!(
            stmt.query_collect(std::slice::from_ref(&hot)),
            Err(SqlError::DuplicateKey)
        ));
        if explicit {
            assert!(matches!(
                conn.execute("COMMIT"),
                Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
            ));
        }
        assert_eq!(conn.query("SELECT c FROM ct").unwrap().rows, [vec![i(12)]]);
        assert_eq!(
            conn.query("SELECT c FROM nested").unwrap().rows,
            [vec![i(2)]]
        );
        assert_eq!(
            conn.query("SELECT * FROM audit ORDER BY id").unwrap().rows,
            audit
        );
        assert!(db.manager().integrity_check().unwrap().is_ok());
    }
}

#[test]
fn selective_materialization_evaluates_closed_siblings_once_and_keeps_nested_scopes() {
    let predicate = crate::parser::parse_sql_expr("(SELECT v FROM fixed) > 0 AND EXISTS (SELECT 1 FROM dependent WHERE dependent.v=outer_t.v AND EXISTS (SELECT 1 FROM nested WHERE nested.v=dependent.v))").unwrap();
    let mut visits = Vec::new();
    let closed = materialize_expr_selective(&predicate, &mut |query| {
        visits.push(query.from.clone());
        match query.from.as_str() {
            "fixed" => Ok(Some(CteRows::binary(qr(vec!["v"], vec![vec![i(1)]])))),
            "dependent" => Ok(None),
            other => panic!("entered a deferred query's nested scope: {other}"),
        }
    })
    .unwrap();
    assert_eq!(visits, ["fixed", "dependent"]);
    let mut dependent_calls = 0;
    for _ in 0..32 {
        let result = materialize_expr(&closed, &mut |query| {
            assert_eq!(query.from, "dependent");
            dependent_calls += 1;
            Ok(CteRows::binary(qr(vec!["v"], vec![vec![i(1)]])))
        })
        .unwrap();
        assert!(!has_subquery(&result));
    }
    assert_eq!(dependent_calls, 32);
    assert_eq!(visits.iter().filter(|name| *name == "fixed").count(), 1);
}

#[test]
fn selective_materialization_preserves_collation_nulls_and_refusal() {
    use crate::types::Collation;
    let predicate = crate::parser::parse_sql_expr("'UPPER' IN (SELECT v FROM fixed)").unwrap();
    let result = materialize_expr_selective(&predicate, &mut |_| {
        Ok(Some(CteRows::new(
            qr(
                vec!["v"],
                vec![vec![Value::Text("upper".into())], vec![Value::Null]],
            ),
            vec![Collation::NoCase],
        )))
    })
    .unwrap();
    assert!(matches!(
        result,
        Expr::InSet {
            has_null: true,
            collation: Collation::NoCase,
            ..
        }
    ));
    let predicate = crate::parser::parse_sql_expr("(SELECT v FROM fixed) COLLATE NOCASE").unwrap();
    let result = materialize_expr_selective(&predicate, &mut |_| {
        Ok(Some(CteRows::binary(qr(
            vec!["v"],
            vec![vec![Value::Text("upper".into())]],
        ))))
    })
    .unwrap();
    assert!(
        matches!(result, Expr::Collate { expr, collation: Collation::NoCase } if matches!(*expr, Expr::Literal(Value::Text(_))))
    );
    let error = materialize_expr_selective(&predicate, &mut |_| {
        Err(SqlError::Storage(citadel_core::Error::Interrupted))
    })
    .unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}
