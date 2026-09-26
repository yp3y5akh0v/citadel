use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

const DAY: i64 = 86_400_000_000;

fn interval(months: i32, days: i32, micros: i64) -> Value {
    Value::Interval {
        months,
        days,
        micros,
    }
}

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"interval-join-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn seed(conn: &Connection<'_>, name: &str, values: &[Value]) {
    conn.execute(&format!(
        "CREATE TABLE {name} (id INTEGER PRIMARY KEY, duration INTERVAL, category INTEGER)"
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

fn check_joins(composite: bool, residual: bool) {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    let left = [
        interval(1, 0, 0),
        interval(0, 1, 0),
        interval(0, 0, 0),
        interval(-1, 1, 0),
        interval(1, 0, 1),
        interval(i32::MAX - 1, 0, i64::MAX),
        interval(i32::MIN + 1, 0, i64::MIN),
        interval(0, 10, 0),
        Value::Null,
    ];
    let right = [
        interval(0, 30, 0),
        interval(0, 0, DAY),
        interval(1, -30, 0),
        interval(0, -29, 0),
        interval(0, 30, 1),
        interval(i32::MAX, -30, i64::MAX),
        interval(i32::MIN, 30, i64::MIN),
        interval(0, 0, 30 * DAY),
        interval(0, 20, 0),
        Value::Null,
    ];
    seed(&conn, "a", &left);
    seed(&conn, "b", &right);

    let mut equal = vec![vec![false; right.len()]; left.len()];
    for (a, left) in left.iter().enumerate() {
        for (b, right) in right.iter().enumerate() {
            let actual = conn
                .query_params("SELECT $1 = $2", &[left.clone(), right.clone()])
                .unwrap()
                .rows;
            let expected = if left.is_null() || right.is_null() {
                Value::Null
            } else {
                Value::Boolean((a == b && a < 7) || (a == 0 && b == 7))
            };
            assert_eq!(actual, vec![vec![expected.clone()]], "scalar {a}/{b}");
            equal[a][b] = expected == Value::Boolean(true)
                && (!composite || a % 2 == b % 2)
                && (!residual || a != 0);
        }
    }

    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] {
            let mut expected = Vec::new();
            let mut right_matched = vec![false; right.len()];
            for (a, matches) in equal.iter().enumerate() {
                for (b, &matched) in matches.iter().enumerate() {
                    if matched {
                        expected.push(vec![Value::Integer(a as i64), Value::Integer(b as i64)]);
                        right_matched[b] = true;
                    }
                }
                if !matches.iter().any(|&matched| matched)
                    && matches!(kind, "LEFT JOIN" | "FULL OUTER JOIN")
                {
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
            let query = format!(
                "SELECT a.id, b.id FROM a {kind} b ON a.duration = b.duration{}{}",
                if composite {
                    " AND a.category = b.category"
                } else {
                    ""
                },
                if residual { " AND a.id <> 0" } else { "" },
            );
            let sql = format!("{query} ORDER BY a.id, b.id");
            assert_eq!(conn.query(&sql).unwrap().rows, expected, "{begin:?}: {sql}");
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
                    "prepared parameter={parameter}, {begin:?}: {sql}"
                );
            }
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn interval_single_key_joins_match_normalized_equality() {
    check_joins(false, false);
}

#[test]
fn interval_composite_joins_match_normalized_equality() {
    check_joins(true, false);
}

#[test]
fn interval_residual_joins_preserve_unmatched_rows() {
    check_joins(true, true);
}
