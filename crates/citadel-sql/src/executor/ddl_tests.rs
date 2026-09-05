use super::*;
use crate::parser::{BinOp, Expr, GeneratedKind};
use crate::types::{Collation, ColumnDef, DataType, Value};

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

fn i(n: i64) -> Value {
    Value::Integer(n)
}

fn repeated_primary_key_statement(columns: &str) -> CreateTableStmt {
    let sql = format!(
        "CREATE TABLE items (a INTEGER, z INTEGER, email TEXT UNIQUE, PRIMARY KEY ({columns}))"
    );
    let Statement::CreateTable(stmt) = crate::parser::parse_sql(&sql).unwrap() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(stmt.primary_key, columns.split(", ").collect::<Vec<_>>());
    stmt
}

fn ddl_database() -> Database {
    citadel::DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn repeated_primary_key_declarations_preserve_names_for_validation() {
    let cases = [
        (
            "CREATE TABLE items (a INTEGER PRIMARY KEY, z INTEGER, PRIMARY KEY (a, z))",
            vec!["a", "a", "z"],
        ),
        (
            "CREATE TABLE items (a INTEGER PRIMARY KEY, z INTEGER, PRIMARY KEY (A, z))",
            vec!["a", "A", "z"],
        ),
        (
            "CREATE TABLE items (a INTEGER, z INTEGER, PRIMARY KEY (a), PRIMARY KEY (a, z))",
            vec!["a", "a", "z"],
        ),
    ];
    let columns = [col("a", DataType::Integer), col("z", DataType::Integer)];
    for (sql, expected) in cases {
        let Statement::CreateTable(stmt) = crate::parser::parse_sql(sql).unwrap() else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(stmt.primary_key, expected);
        assert!(matches!(
            resolve_primary_key_columns(&columns, &stmt.primary_key),
            Err(SqlError::DuplicateColumn(name)) if name.eq_ignore_ascii_case("a")
        ));
    }
}

#[test]
fn create_table_rejects_repeated_primary_key_columns_without_catalog_changes() {
    for columns in ["a, a, z", "a, A, z", "a, z, A"] {
        let db = ddl_database();
        let mut schema = SchemaManager::empty();
        let before = db.manager().commit_generation();
        let stmt = repeated_primary_key_statement(columns);

        let error = exec_create_table(&db, &mut schema, &stmt).unwrap_err();
        assert!(
            matches!(error, SqlError::DuplicateColumn(ref name) if name.eq_ignore_ascii_case("a"))
        );
        assert!(schema.table_names().is_empty());
        assert_eq!(db.manager().commit_generation(), before);
        assert!(db.begin_read().list_tables().unwrap().is_empty());
        assert!(SchemaManager::load(&db).unwrap().table_names().is_empty());

        let mut valid = stmt;
        valid.primary_key = vec!["a".into(), "z".into()];
        exec_create_table(&db, &mut schema, &valid).unwrap();
        assert_eq!(schema.get("items").unwrap().primary_key_columns, [0, 1]);
    }
}

#[test]
fn create_table_in_txn_rejects_repeated_primary_key_columns_without_catalog_changes() {
    for columns in ["a, a, z", "a, A, z", "a, z, A"] {
        let db = ddl_database();
        let mut schema = SchemaManager::empty();
        let stmt = repeated_primary_key_statement(columns);
        let mut wtx = db.begin_write().unwrap();

        let error = exec_create_table_in_txn(&mut wtx, &mut schema, &stmt).unwrap_err();
        assert!(
            matches!(error, SqlError::DuplicateColumn(ref name) if name.eq_ignore_ascii_case("a"))
        );
        assert!(schema.table_names().is_empty());
        assert!(wtx.table_root_stamp(b"items").unwrap().is_none());
        assert!(wtx.table_root_stamp(b"_schema").unwrap().is_none());
        wtx.commit().unwrap();
        assert!(db.begin_read().list_tables().unwrap().is_empty());
        assert!(SchemaManager::load(&db).unwrap().table_names().is_empty());

        let mut valid = stmt;
        valid.primary_key = vec!["a".into(), "z".into()];
        let mut wtx = db.begin_write().unwrap();
        exec_create_table_in_txn(&mut wtx, &mut schema, &valid).unwrap();
        wtx.commit().unwrap();
        let loaded = SchemaManager::load(&db).unwrap();
        assert_eq!(loaded.get("items").unwrap().primary_key_columns, [0, 1]);
    }
}

#[test]
fn collect_column_refs_simple_column() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Column("X".into()), &mut out);
    assert_eq!(out, vec!["x"]);
}

#[test]
fn collect_column_refs_qualified_uses_column_only() {
    let mut out = Vec::new();
    collect_column_refs(
        &Expr::QualifiedColumn {
            table: "T".into(),
            column: "Y".into(),
        },
        &mut out,
    );
    assert_eq!(out, vec!["y"]);
}

#[test]
fn collect_column_refs_binary_op() {
    let mut out = Vec::new();
    let e = Expr::BinaryOp {
        left: Box::new(Expr::Column("a".into())),
        op: BinOp::Mul,
        right: Box::new(Expr::Column("b".into())),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["a", "b"]);
}

#[test]
fn collect_column_refs_function_args() {
    let mut out = Vec::new();
    let e = Expr::Function {
        name: "ABS".into(),
        args: vec![Expr::Column("v".into())],
        distinct: false,
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["v"]);
}

#[test]
fn collect_column_refs_literal_yields_empty() {
    let mut out = Vec::new();
    collect_column_refs(&Expr::Literal(i(1)), &mut out);
    assert!(out.is_empty());
}

#[test]
fn collect_column_refs_case_branches() {
    let mut out = Vec::new();
    let e = Expr::Case {
        operand: None,
        conditions: vec![(Expr::Column("c".into()), Expr::Column("r".into()))],
        else_result: Some(Box::new(Expr::Column("el".into()))),
    };
    collect_column_refs(&e, &mut out);
    assert_eq!(out, vec!["c", "r", "el"]);
}

#[test]
fn validate_no_chained_generated_no_generated_columns_ok() {
    let cs = vec![col("a", DataType::Integer), col("b", DataType::Integer)];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_self_reference_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("g".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_references_non_generated_ok() {
    let mut gen_col = col("g", DataType::Integer);
    gen_col.generated_kind = Some(GeneratedKind::Stored);
    gen_col.generated_expr = Some(Expr::Column("a".into()));
    let cs = vec![col("a", DataType::Integer), gen_col];
    assert!(validate_no_chained_generated(&cs).is_ok());
}

#[test]
fn validate_no_chained_generated_chain_rejected() {
    let mut g1 = col("g1", DataType::Integer);
    g1.generated_kind = Some(GeneratedKind::Stored);
    g1.generated_expr = Some(Expr::Column("a".into()));
    let mut g2 = col("g2", DataType::Integer);
    g2.generated_kind = Some(GeneratedKind::Stored);
    g2.generated_expr = Some(Expr::Column("g1".into()));
    let cs = vec![col("a", DataType::Integer), g1, g2];
    assert!(validate_no_chained_generated(&cs).is_err());
}
