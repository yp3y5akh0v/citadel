use super::*;
use crate::encoding::encode_row;
use crate::parser::{parse_sql_expr, GeneratedKind};
use crate::types::{Collation, ColumnDef};

fn columns(specs: &[(&str, DataType)]) -> Vec<ColumnDef> {
    specs
        .iter()
        .enumerate()
        .map(|(position, (name, data_type))| ColumnDef {
            name: (*name).into(),
            data_type: *data_type,
            nullable: true,
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

fn index(keys: Vec<IndexKey>) -> IndexDef {
    IndexDef {
        name: "by_value".into(),
        keys,
        unique: false,
        predicate_sql: None,
        predicate_expr: None,
        kind: IndexKind::BTree,
        ann_filter_cols: vec![],
    }
}

#[test]
fn predicate_and_key_share_one_missing_default_evaluation() {
    let default = parse_sql_expr("TO_TSVECTOR('x')").unwrap();
    let expected = super::super::helpers::eval_const_expr(&default).unwrap();
    let mut columns = columns(&[("id", DataType::Integer), ("search", DataType::TsVector)]);
    columns[1].default_expr = Some(default);
    let table = TableSchema::new("docs".into(), columns, vec![0], vec![], vec![], vec![]);
    let mut index = index(vec![IndexKey::Column {
        idx: 1,
        collate: Collation::Binary,
    }]);
    index.predicate_expr = Some(parse_sql_expr("search IS NOT NULL").unwrap());
    let key = encode_composite_key(&[Value::Integer(1)]);
    let value = encode_row(&[]);
    let token = CancelToken::new();
    let plan = IndexBuildPlan::new(&table, &index, Some(&token)).unwrap();
    let _cancel = crate::fts::cancel_tokenize_after(token.clone(), 3);

    let entries = plan
        .collect(|visit| {
            assert!(visit(&key, &value)?);
            Ok(())
        })
        .expect("predicate and key must share one default evaluation");

    let IndexEntries::Btree(entries) = entries else {
        panic!("expected B-tree entries")
    };
    assert_eq!(entries.len(), 1);
    let row = vec![Value::Integer(1), expected];
    assert_eq!(
        entries[0].key,
        encode_index_key_with_schema_and_cancel(&index, &row, &row[..1], &table, None).unwrap()
    );
    assert_eq!(
        entries[0].value,
        encode_index_value(&index, &row, &row[..1])
    );
    assert!(!token.is_cancelled());
}

#[test]
fn collect_stops_at_the_first_sql_error() {
    let mut columns = columns(&[
        ("id", DataType::Integer),
        ("a", DataType::Integer),
        ("g", DataType::Integer),
    ]);
    columns[2].generated_kind = Some(GeneratedKind::Virtual);
    columns[2].generated_expr = Some(parse_sql_expr("a * 2").unwrap());
    let table = TableSchema::new("t".into(), columns, vec![0], vec![], vec![], vec![]);
    let index = index(vec![IndexKey::Expr {
        expr: parse_sql_expr("g + 1").unwrap(),
        original_sql: "g + 1".into(),
    }]);
    let plan = IndexBuildPlan::new(&table, &index, None).unwrap();
    let first_key = encode_composite_key(&[Value::Integer(1)]);
    let second_key = encode_composite_key(&[Value::Integer(2)]);
    let first_value = encode_row(&[Value::Integer(i64::MAX)]);
    let second_value = encode_row(&[Value::Integer(1)]);
    let mut visited = 0;

    let result = plan.collect(|visit| {
        for (key, value) in [(&first_key, &first_value), (&second_key, &second_value)] {
            visited += 1;
            if !visit(key, value)? {
                break;
            }
        }
        Ok(())
    });

    assert!(matches!(result, Err(SqlError::IntegerOverflow)));
    assert_eq!(visited, 1);
}
