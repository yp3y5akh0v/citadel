use super::*;
use crate::parser::Statement;
use crate::schema::SchemaManager;

#[test]
fn compile_returns_none_for_non_dml_statements() {
    let schema = SchemaManager::empty();
    let stmt = crate::parser::parse_sql("BEGIN").unwrap();
    assert!(compile(&schema, &stmt).is_none());
}

#[test]
fn compile_returns_none_for_create_table() {
    let schema = SchemaManager::empty();
    let stmt = crate::parser::parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert!(compile(&schema, &stmt).is_none());
}

#[test]
fn compile_returns_none_for_explain() {
    let schema = SchemaManager::empty();
    let stmt = crate::parser::parse_sql("EXPLAIN SELECT * FROM t").unwrap();
    assert!(compile(&schema, &stmt).is_none());
}

#[test]
fn compile_insert_into_unknown_table_returns_none() {
    let schema = SchemaManager::empty();
    let stmt = crate::parser::parse_sql("INSERT INTO missing (id) VALUES (1)").unwrap();
    assert!(matches!(stmt, Statement::Insert(_)));
    assert!(compile(&schema, &stmt).is_none());
}

#[test]
fn compile_delete_into_unknown_table_returns_none() {
    let schema = SchemaManager::empty();
    let stmt = crate::parser::parse_sql("DELETE FROM missing WHERE id = 1").unwrap();
    assert!(matches!(stmt, Statement::Delete(_)));
    assert!(compile(&schema, &stmt).is_none());
}
