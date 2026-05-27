use super::*;
use crate::types::{QueryResult, Value};

fn qr(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult {
        columns: columns.into_iter().map(String::from).collect(),
        rows,
    }
}

#[test]
fn apply_view_aliases_renames_in_order() {
    let mut q = qr(vec!["a", "b", "c"], vec![]);
    apply_view_aliases(&mut q, &["x".into(), "y".into(), "z".into()]);
    assert_eq!(q.columns, vec!["x", "y", "z"]);
}

#[test]
fn apply_view_aliases_partial_keeps_remaining() {
    let mut q = qr(vec!["a", "b", "c"], vec![]);
    apply_view_aliases(&mut q, &["x".into()]);
    assert_eq!(q.columns, vec!["x", "b", "c"]);
}

#[test]
fn apply_view_aliases_more_aliases_than_columns_ignored() {
    let mut q = qr(vec!["a"], vec![]);
    apply_view_aliases(&mut q, &["x".into(), "y".into(), "z".into()]);
    assert_eq!(q.columns, vec!["x"]);
}

#[test]
fn apply_view_aliases_empty_aliases_no_change() {
    let mut q = qr(vec!["a", "b"], vec![]);
    apply_view_aliases(&mut q, &[]);
    assert_eq!(q.columns, vec!["a", "b"]);
}

#[test]
fn build_view_schema_columns_derived_from_query_result() {
    let q = qr(vec!["id", "name"], vec![]);
    let ts = build_view_schema("v", &q);
    assert_eq!(ts.name, "v");
    assert_eq!(ts.columns.len(), 2);
    assert_eq!(ts.columns[0].name, "id");
    assert_eq!(ts.columns[1].name, "name");
}

#[test]
fn build_view_schema_empty_columns() {
    let q = qr(vec![], vec![]);
    let ts = build_view_schema("v", &q);
    assert!(ts.columns.is_empty());
}

#[test]
fn build_view_schema_preserves_column_count() {
    let q = qr(vec!["a", "b", "c", "d", "e"], vec![]);
    let ts = build_view_schema("v", &q);
    assert_eq!(ts.columns.len(), 5);
    for (i, want) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        assert_eq!(&ts.columns[i].name, want);
    }
}

#[test]
fn build_view_schema_uses_position_index() {
    let q = qr(vec!["a", "b", "c"], vec![]);
    let ts = build_view_schema("v", &q);
    for (i, col) in ts.columns.iter().enumerate() {
        assert_eq!(col.position as usize, i);
    }
}
