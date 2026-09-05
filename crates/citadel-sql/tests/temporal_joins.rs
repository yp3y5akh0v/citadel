use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, DataType, Value};

fn values(data_type: DataType) -> Vec<Value> {
    let mut values: Vec<Value> = match data_type {
        DataType::Date => [0, 1, -1, i32::MAX, i32::MIN]
            .into_iter()
            .map(Value::Date)
            .collect(),
        DataType::Timestamp => [
            0,
            1_000_000,
            -1_000_000,
            86_400_000_000,
            1,
            i64::MAX,
            i64::MIN,
        ]
        .into_iter()
        .map(Value::Timestamp)
        .collect(),
        DataType::Time => [0, 1, 86_400_000_000]
            .into_iter()
            .map(Value::Time)
            .collect(),
        DataType::Interval => [(0, 0, 0), (0, 1, 0), (1, 0, 0), (0, 0, 1)]
            .into_iter()
            .map(|(months, days, micros)| Value::Interval {
                months,
                days,
                micros,
            })
            .collect(),
        DataType::Integer => [
            0,
            1,
            -1,
            30,
            86_400_000_000,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ]
        .into_iter()
        .map(Value::Integer)
        .collect(),
        DataType::Text => [
            "1970-01-01",
            "1970-01-02",
            "1969-12-31",
            "1970-01-01 00:00:00",
            "1970-01-01 00:00:01",
            "00:00:00",
            "00:00:00.000001",
            "1 day",
            "24 hours",
            "1 month",
            "30 days",
            "invalid",
            "infinity",
            "-infinity",
        ]
        .into_iter()
        .map(|s| Value::Text(s.into()))
        .collect(),
        _ => unreachable!(),
    };
    values.push(Value::Null);
    values
}

fn seed(conn: &Connection<'_>, name: &str, data_type: DataType, values: &[Value]) {
    conn.execute(&format!(
        "CREATE TABLE {name} (id INTEGER PRIMARY KEY, k {data_type}, category INTEGER)"
    ))
    .unwrap();
    for (index, value) in values.iter().enumerate() {
        conn.execute_params(
            &format!("INSERT INTO {name} VALUES ($1, $2, $3)"),
            &[
                Value::Integer(index as i64),
                value.clone(),
                Value::Integer((index % 2) as i64),
            ],
        )
        .unwrap();
    }
}

#[test]
fn temporal_composite_join_keeps_correlated_keys_together() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"temporal-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    seed(
        &conn,
        "a",
        DataType::Timestamp,
        &vec![Value::Timestamp(0); 64],
    );
    let values = (0..64)
        .map(|id| {
            Value::Text(
                if id % 2 == 0 {
                    "1970-01-01"
                } else {
                    "1970-01-02"
                }
                .into(),
            )
        })
        .collect::<Vec<_>>();
    seed(&conn, "b", DataType::Text, &values);
    conn.execute("UPDATE a SET category = 1").unwrap();
    let prepared = conn
        .prepare("SELECT a.id, b.id FROM a JOIN b ON a.k = b.k AND a.category = b.category WHERE $1 >= 0 ORDER BY a.id, b.id")
        .unwrap();
    for parameter in [0, 1] {
        assert!(prepared
            .query_collect(&[Value::Integer(parameter)])
            .unwrap()
            .rows
            .is_empty());
    }
    conn.execute("UPDATE b SET category = 1 WHERE id = 0")
        .unwrap();
    let expected: Vec<_> = (0..64)
        .map(|id| vec![Value::Integer(id), Value::Integer(0)])
        .collect();
    for parameter in [2, 3] {
        assert_eq!(
            prepared
                .query_collect(&[Value::Integer(parameter)])
                .unwrap()
                .rows,
            expected
        );
    }
}

#[test]
fn prepared_temporal_join_invalidates_coercion_index_after_inner_writes() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"temporal-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    seed(
        &conn,
        "a",
        DataType::Date,
        &[Value::Date(0), Value::Date(1)],
    );
    seed(
        &conn,
        "b",
        DataType::Text,
        &[Value::Text("1970-01-01".into())],
    );
    let prepared = conn
        .prepare("SELECT a.id, b.id FROM a JOIN b ON a.k = b.k WHERE $1 >= 0 ORDER BY a.id, b.id")
        .unwrap();
    let mut parameter = 0;
    let mut check = |pairs: &[(i64, i64)]| {
        let expected: Vec<_> = pairs
            .iter()
            .map(|&(a, b)| vec![Value::Integer(a), Value::Integer(b)])
            .collect();
        for _ in 0..2 {
            parameter += 1;
            assert_eq!(
                prepared
                    .query_collect(&[Value::Integer(parameter)])
                    .unwrap()
                    .rows,
                expected
            );
        }
    };
    check(&[(0, 0)]);
    conn.execute("UPDATE b SET k = '1970-01-02' WHERE id = 0")
        .unwrap();
    check(&[(1, 0)]);
    conn.execute("INSERT INTO b VALUES (1, '1970-01-01', 0)")
        .unwrap();
    check(&[(0, 1), (1, 0)]);
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM b WHERE id = 0").unwrap();
    check(&[(0, 1)]);
    conn.execute("ROLLBACK").unwrap();
    check(&[(0, 1), (1, 0)]);
    conn.execute("DELETE FROM b WHERE id = 1").unwrap();
    check(&[(1, 0)]);
}

fn check_types(left_type: DataType, right_type: DataType) {
    let db = DatabaseBuilder::new("")
        .passphrase(b"temporal-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let left = values(left_type);
    let right = values(right_type);
    seed(&conn, "a", left_type, &left);
    seed(&conn, "b", right_type, &right);
    let mut equal = vec![vec![false; right.len()]; left.len()];
    for (a, left) in left.iter().enumerate() {
        for (b, right) in right.iter().enumerate() {
            let rows = conn
                .query_params("SELECT $1 = $2", &[left.clone(), right.clone()])
                .unwrap()
                .rows;
            equal[a][b] = rows == vec![vec![Value::Boolean(true)]];
        }
    }
    assert!(equal.iter().flatten().any(|&matched| matched));

    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for (composite, residual) in [(false, false), (true, false), (true, true)] {
            for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] {
                let mut expected = Vec::new();
                let mut right_matched = vec![false; right.len()];
                for (a, matches) in equal.iter().enumerate() {
                    let mut matched = false;
                    for (b, &same) in matches.iter().enumerate() {
                        if same && (!composite || a % 2 == b % 2) && (!residual || a != 0) {
                            expected.push(vec![Value::Integer(a as i64), Value::Integer(b as i64)]);
                            matched = true;
                            right_matched[b] = true;
                        }
                    }
                    if !matched && matches!(kind, "LEFT JOIN" | "FULL OUTER JOIN") {
                        expected.push(vec![Value::Integer(a as i64), Value::Null]);
                    }
                }
                if matches!(kind, "RIGHT JOIN" | "FULL OUTER JOIN") {
                    for (b, matched) in right_matched.into_iter().enumerate() {
                        if !matched {
                            expected.push(vec![Value::Null, Value::Integer(b as i64)]);
                        }
                    }
                }
                expected.sort();
                for equality in ["a.k = b.k", "b.k = a.k"] {
                    let query = format!(
                        "SELECT a.id, b.id FROM a {kind} b ON {equality}{}{}",
                        if composite {
                            " AND a.category = b.category"
                        } else {
                            ""
                        },
                        if residual { " AND a.id <> 0" } else { "" },
                    );
                    let sql = format!("{query} ORDER BY a.id, b.id");
                    assert_eq!(
                        conn.query(&sql).unwrap().rows,
                        expected,
                        "{left_type}/{right_type}, {begin:?}: {sql}"
                    );
                    let prepared = conn
                        .prepare(&format!("{query} WHERE $1 >= 0 ORDER BY a.id, b.id"))
                        .unwrap();
                    for parameter in [0, 1, 0] {
                        assert_eq!(
                            prepared
                                .query_collect(&[Value::Integer(parameter)])
                                .unwrap()
                                .rows,
                            expected,
                            "prepared {parameter}, {left_type}/{right_type}, {begin:?}: {sql}"
                        );
                    }
                }
            }
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn date_timestamp_joins_match_scalar_coercion() {
    check_types(DataType::Date, DataType::Timestamp);
    check_types(DataType::Timestamp, DataType::Date);
}

#[test]
fn temporal_text_joins_match_scalar_coercion() {
    for temporal in [
        DataType::Date,
        DataType::Time,
        DataType::Timestamp,
        DataType::Interval,
    ] {
        check_types(temporal, DataType::Text);
        check_types(DataType::Text, temporal);
    }
}

#[test]
fn temporal_integer_joins_match_scalar_coercion() {
    for temporal in [
        DataType::Date,
        DataType::Time,
        DataType::Timestamp,
        DataType::Interval,
    ] {
        check_types(temporal, DataType::Integer);
        check_types(DataType::Integer, temporal);
    }
}

#[test]
fn heterogeneous_cte_join_keys_keep_runtime_coercion_and_numeric_equality() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"heterogeneous-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE ids (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO ids VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11)")
        .unwrap();
    let source = "SELECT id, id % 2 AS category, CASE id \
        WHEN 0 THEN DATE '1970-01-01' \
        WHEN 1 THEN TIMESTAMP '1970-01-01 00:00:00' \
        WHEN 2 THEN TIME '00:00:00' \
        WHEN 3 THEN INTERVAL '0 days' \
        WHEN 4 THEN '1970-01-01' \
        WHEN 5 THEN 0 \
        WHEN 6 THEN NULL \
        WHEN 7 THEN 0.0 \
        WHEN 8 THEN 'plain' \
        WHEN 9 THEN '0 days' \
        WHEN 10 THEN INTERVAL '1 month' \
        ELSE '30 days' END AS k FROM ids";
    let input = conn.query(&format!("{source} ORDER BY id")).unwrap().rows;
    assert_eq!(
        input
            .iter()
            .map(|row| row[2].data_type())
            .collect::<Vec<_>>(),
        vec![
            DataType::Date,
            DataType::Timestamp,
            DataType::Time,
            DataType::Interval,
            DataType::Text,
            DataType::Integer,
            DataType::Null,
            DataType::Real,
            DataType::Text,
            DataType::Text,
            DataType::Interval,
            DataType::Text,
        ]
    );
    let mut pairs = Vec::new();
    for a in &input {
        for b in &input {
            if conn
                .query_params("SELECT $1 = $2", &[a[2].clone(), b[2].clone()])
                .unwrap()
                .rows
                == vec![vec![Value::Boolean(true)]]
            {
                pairs.push(vec![a[0].clone(), b[0].clone()]);
            }
        }
    }
    assert!(pairs.contains(&vec![Value::Integer(0), Value::Integer(1)]));
    assert!(pairs.contains(&vec![Value::Integer(3), Value::Integer(9)]));
    assert!(!pairs.contains(&vec![Value::Integer(0), Value::Integer(2)]));
    assert!(!pairs.contains(&vec![Value::Integer(10), Value::Integer(11)]));
    for width in [1, 12] {
        let on = std::iter::repeat_n("a.k = b.k", width)
            .collect::<Vec<_>>()
            .join(" AND ");
        for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] {
            let mut expected = pairs.clone();
            if matches!(kind, "LEFT JOIN" | "FULL OUTER JOIN") {
                expected.push(vec![Value::Integer(6), Value::Null]);
            }
            if matches!(kind, "RIGHT JOIN" | "FULL OUTER JOIN") {
                expected.push(vec![Value::Null, Value::Integer(6)]);
            }
            expected.sort();
            let sql = format!(
                "WITH a AS ({source}), b AS ({source}) \
                SELECT a.id, b.id FROM a {kind} b ON {on} ORDER BY a.id, b.id"
            );
            assert_eq!(conn.query(&sql).unwrap().rows, expected, "{sql}");
        }
    }
}
