use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"numeric-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn values(data_type: &str) -> Vec<Value> {
    if data_type == "INTEGER" {
        [
            Some(1),
            Some(9_007_199_254_740_992),
            Some(9_007_199_254_740_993),
            Some(i64::MAX),
            Some(i64::MIN),
            Some(0),
            Some(2),
            None,
        ]
        .into_iter()
        .map(|value| value.map_or(Value::Null, Value::Integer))
        .collect()
    } else {
        [
            Some(1.0),
            Some(9_007_199_254_740_992.0),
            Some(i64::MAX as f64),
            Some(i64::MIN as f64),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(3.5),
            None,
        ]
        .into_iter()
        .map(|value| value.map_or(Value::Null, Value::Real))
        .collect()
    }
}

fn seed(conn: &Connection<'_>, table: &str, data_type: &str, numbers: &[Value]) {
    conn.execute(&format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, n {data_type}, guard INTEGER)"
    ))
    .unwrap();
    for (id, value) in numbers.iter().enumerate() {
        conn.execute_params(
            &format!("INSERT INTO {table} VALUES ($1, $2, 0)"),
            &[Value::Integer(id as i64), value.clone()],
        )
        .unwrap();
    }
}

fn reference(left: &[Value], right: &[Value], kind: &str, residual: bool) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut right_matched = vec![false; right.len()];
    for (a, left) in left.iter().enumerate() {
        let mut matched = false;
        for (b, right) in right.iter().enumerate() {
            if !left.is_null() && !right.is_null() && left == right && (!residual || a != 0) {
                rows.push(vec![Value::Integer(a as i64), Value::Integer(b as i64)]);
                matched = true;
                right_matched[b] = true;
            }
        }
        if !matched && matches!(kind, "LEFT JOIN" | "FULL OUTER JOIN") {
            rows.push(vec![Value::Integer(a as i64), Value::Null]);
        }
    }
    if matches!(kind, "RIGHT JOIN" | "FULL OUTER JOIN") {
        for (b, matched) in right_matched.into_iter().enumerate() {
            if !matched {
                rows.push(vec![Value::Null, Value::Integer(b as i64)]);
            }
        }
    }
    rows.sort();
    rows
}

fn check_joins(composite: bool, residual: bool) {
    for (left_type, right_type) in [("REAL", "INTEGER"), ("INTEGER", "REAL")] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        let left = values(left_type);
        let right = values(right_type);
        seed(&conn, "a", left_type, &left);
        seed(&conn, "b", right_type, &right);
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] {
                let on = format!(
                    "a.n = b.n{}{}",
                    if composite {
                        " AND a.guard = b.guard"
                    } else {
                        ""
                    },
                    if residual { " AND a.id <> 0" } else { "" }
                );
                let sql = format!("SELECT a.id, b.id FROM a {kind} b ON {on} ORDER BY a.id, b.id");
                let expected = reference(&left, &right, kind, residual);
                assert_eq!(
                    conn.query(&sql).unwrap().rows,
                    expected,
                    "{left_type}/{right_type}, {begin:?}: {sql}"
                );
                let prepared = conn.prepare(&sql).unwrap();
                for attempt in 0..2 {
                    assert_eq!(
                        prepared.query_collect(&[]).unwrap().rows,
                        expected,
                        "prepared {attempt}, {left_type}/{right_type}, {begin:?}: {sql}"
                    );
                }
            }
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn mixed_numeric_single_key_joins_match_scalar_equality() {
    check_joins(false, false);
}

#[test]
fn mixed_numeric_composite_joins_match_scalar_equality() {
    check_joins(true, false);
}

#[test]
fn mixed_numeric_residual_joins_preserve_outer_rows() {
    check_joins(true, true);
}

#[test]
fn mixed_numeric_scalar_equality_contract() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for (left, right, expected) in [
        (Value::Real(1.0), Value::Integer(1), Value::Boolean(true)),
        (
            Value::Real(9_007_199_254_740_992.0),
            Value::Integer(9_007_199_254_740_993),
            Value::Boolean(true),
        ),
        (
            Value::Integer(9_007_199_254_740_992),
            Value::Integer(9_007_199_254_740_993),
            Value::Boolean(false),
        ),
        (
            Value::Real(f64::NAN),
            Value::Real(f64::NAN),
            Value::Boolean(false),
        ),
        (Value::Null, Value::Null, Value::Null),
    ] {
        assert_eq!(
            conn.query_params("SELECT $1 = $2", &[left, right])
                .unwrap()
                .rows,
            vec![vec![expected]]
        );
    }
}
