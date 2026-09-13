use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"numeric-key-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn seed(conn: &Connection<'_>, key_type: &str) {
    conn.execute(&format!(
        "CREATE TABLE items (id {key_type} PRIMARY KEY, marker INTEGER)"
    ))
    .unwrap();
    for id in -3..=3 {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, $2)",
            &[Value::Integer(id), Value::Integer(id)],
        )
        .unwrap();
    }
}

fn in_transactions(conn: &Connection<'_>, check: impl Fn()) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        check();
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

fn assert_markers(conn: &Connection<'_>, predicate: &str, params: &[Value], markers: &[i64]) {
    let sql = format!("SELECT marker FROM items WHERE {predicate} ORDER BY marker");
    let expected: Vec<_> = markers
        .iter()
        .map(|&marker| vec![Value::Integer(marker)])
        .collect();
    assert_eq!(
        conn.query_params(&sql, params).unwrap().rows,
        expected,
        "{predicate}: {params:?}"
    );
    let statement = conn.prepare(&sql).unwrap();
    assert_eq!(
        statement.query_collect(params).unwrap().rows,
        expected,
        "prepared {predicate}: {params:?}"
    );
}

#[test]
fn integer_primary_key_equality_matches_real_literals_and_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    in_transactions(&conn, || {
        assert_markers(&conn, "id = 2.0", &[], &[2]);
        assert_markers(&conn, "2.0 = id", &[], &[2]);
        assert_markers(&conn, "id = -2.0", &[], &[-2]);
        assert_markers(&conn, "id = 2.5", &[], &[]);
        assert_markers(&conn, "id = $1", &[Value::Real(-2.0)], &[-2]);
        assert_markers(&conn, "id = $1", &[Value::Real(-2.5)], &[]);
    });
}

#[test]
fn real_primary_key_equality_matches_integer_literals_and_parameters() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "REAL");
    in_transactions(&conn, || {
        assert_markers(&conn, "id = 2", &[], &[2]);
        assert_markers(&conn, "2 = id", &[], &[2]);
        assert_markers(&conn, "id = $1", &[Value::Integer(-2)], &[-2]);
        assert_markers(&conn, "id = $1", &[Value::Real(2.5)], &[]);
    });
}

#[test]
fn composite_primary_key_equality_matches_mixed_numeric_components() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE items (tenant INTEGER, id INTEGER, marker INTEGER, \
         PRIMARY KEY (tenant, id))",
    )
    .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 2, 12), (1, 3, 13), (2, 2, 22)")
        .unwrap();
    in_transactions(&conn, || {
        assert_markers(&conn, "tenant = 1 AND id = 2.0", &[], &[12]);
        assert_markers(&conn, "tenant = 1.0 AND id = 2", &[], &[12]);
        assert_markers(&conn, "tenant = 1.0 AND id = 2.5", &[], &[]);
        assert_markers(
            &conn,
            "tenant = $1 AND id = $2",
            &[Value::Real(1.0), Value::Real(2.0)],
            &[12],
        );
        assert_markers(&conn, "tenant = 1.0", &[], &[12, 13]);
    });
}

#[test]
fn integer_primary_key_ranges_match_fractional_and_negative_bounds() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    in_transactions(&conn, || {
        for (predicate, expected) in [
            ("id > 1.5", vec![2, 3]),
            ("1.5 < id", vec![2, 3]),
            ("id >= 2.0", vec![2, 3]),
            ("id > -1.5", vec![-1, 0, 1, 2, 3]),
            ("id BETWEEN -1.5 AND 1.5", vec![-1, 0, 1]),
            ("id < 2.5", vec![-3, -2, -1, 0, 1, 2]),
        ] {
            assert_markers(&conn, predicate, &[], &expected);
        }
        assert_markers(&conn, "id >= $1", &[Value::Real(-1.5)], &[-1, 0, 1, 2, 3]);
    });
}

#[test]
fn integer_key_comparisons_preserve_f64_rounding_equivalence_classes() {
    let base = 1i64 << 53;
    for (ids, bound, equal, greater) in [
        (
            vec![base, base + 1, base + 2],
            base as f64,
            vec![1, 2],
            vec![3],
        ),
        (
            vec![-base - 2, -base - 1, -base],
            -(base as f64),
            vec![2, 3],
            vec![],
        ),
        (
            vec![i64::MAX - 1, i64::MAX],
            i64::MAX as f64,
            vec![1, 2],
            vec![],
        ),
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, marker INTEGER)")
            .unwrap();
        for (index, id) in ids.iter().enumerate() {
            conn.execute_params(
                "INSERT INTO items VALUES ($1, $2)",
                &[Value::Integer(*id), Value::Integer(index as i64 + 1)],
            )
            .unwrap();
        }
        in_transactions(&conn, || {
            assert_markers(&conn, "id = $1", &[Value::Real(bound)], &equal);
            assert_markers(&conn, "id > $1", &[Value::Real(bound)], &greater);
        });
    }
}

#[test]
fn prepared_primary_key_lookup_rechecks_parameter_numeric_type() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    let statement = conn
        .prepare("SELECT marker FROM items WHERE id = $1")
        .unwrap();
    in_transactions(&conn, || {
        for (bound, expected) in [
            (Value::Integer(1), vec![vec![Value::Integer(1)]]),
            (Value::Real(2.0), vec![vec![Value::Integer(2)]]),
            (Value::Real(2.5), vec![]),
            (Value::Integer(3), vec![vec![Value::Integer(3)]]),
        ] {
            assert_eq!(statement.query_collect(&[bound]).unwrap().rows, expected);
        }
    });
}

#[test]
fn numeric_primary_key_predicates_update_and_delete_matching_rows() {
    for explicit in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        seed(&conn, "INTEGER");
        let update = conn
            .prepare("UPDATE items SET marker = 20 WHERE id = $1")
            .unwrap();
        let delete = conn.prepare("DELETE FROM items WHERE id > $1").unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        assert_eq!(update.execute(&[Value::Real(2.0)]).unwrap(), 1);
        assert_eq!(delete.execute(&[Value::Real(2.5)]).unwrap(), 1);
        assert_markers(&conn, "id = 2", &[], &[20]);
        assert_markers(&conn, "id = 3", &[], &[]);
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_markers(&conn, "id = 2", &[], &[2]);
            assert_markers(&conn, "id = 3", &[], &[3]);
        }
    }
}

#[test]
fn secondary_indexes_preserve_mixed_numeric_comparisons() {
    for key_type in ["INTEGER", "REAL"] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, marker {key_type})"
        ))
        .unwrap();
        conn.execute("CREATE INDEX items_marker ON items (marker)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, -2), (2, 0), (3, 2), (4, 3)")
            .unwrap();
        in_transactions(&conn, || {
            for (predicate, expected) in [
                ("marker = 2.0", vec![2]),
                ("marker = 2", vec![2]),
                ("marker > 1.5", vec![2, 3]),
                ("marker <= 2.5", vec![-2, 0, 2]),
                ("marker >= 0.0", vec![0, 2, 3]),
            ] {
                assert_markers(&conn, predicate, &[], &expected);
            }
        });
    }
}

#[test]
fn real_key_comparisons_include_both_signed_zero_encodings() {
    for stored in [-0.0, 0.0] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id REAL PRIMARY KEY, marker INTEGER)")
            .unwrap();
        conn.execute_params("INSERT INTO items VALUES ($1, 7)", &[Value::Real(stored)])
            .unwrap();
        let point = conn
            .prepare("SELECT marker FROM items WHERE id = $1")
            .unwrap();
        in_transactions(&conn, || {
            for bound in [Value::Real(-0.0), Value::Real(0.0), Value::Integer(0)] {
                assert_markers(&conn, "id = $1", std::slice::from_ref(&bound), &[7]);
                assert_eq!(
                    point
                        .query_collect(std::slice::from_ref(&bound))
                        .unwrap()
                        .rows,
                    vec![vec![Value::Integer(7)]]
                );
                assert_markers(&conn, "id >= $1", std::slice::from_ref(&bound), &[7]);
                assert_markers(&conn, "id <= $1", std::slice::from_ref(&bound), &[7]);
                assert_markers(&conn, "id > $1", std::slice::from_ref(&bound), &[]);
                assert_markers(&conn, "id < $1", std::slice::from_ref(&bound), &[]);
            }
        });
        assert_eq!(
            conn.prepare("UPDATE items SET marker = 8 WHERE id = $1",)
                .unwrap()
                .execute(&[Value::Real(-stored)])
                .unwrap(),
            1
        );
        assert_eq!(
            conn.prepare("DELETE FROM items WHERE id = $1")
                .unwrap()
                .execute(&[Value::Real(-stored)])
                .unwrap(),
            1
        );
    }
}

#[test]
fn real_secondary_index_ranges_include_or_exclude_both_zero_keys() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, marker REAL)")
        .unwrap();
    conn.execute("CREATE INDEX items_marker ON items (marker)")
        .unwrap();
    for (id, value) in [(1, -1.0), (2, -0.0), (3, 0.0), (4, 1.0)] {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, $2)",
            &[Value::Integer(id), Value::Real(value)],
        )
        .unwrap();
    }
    in_transactions(&conn, || {
        for zero in [Value::Real(-0.0), Value::Real(0.0), Value::Integer(0)] {
            assert_markers(&conn, "marker = $1", std::slice::from_ref(&zero), &[0, 0]);
            assert_markers(
                &conn,
                "marker >= $1",
                std::slice::from_ref(&zero),
                &[0, 0, 1],
            );
            assert_markers(
                &conn,
                "marker <= $1",
                std::slice::from_ref(&zero),
                &[-1, 0, 0],
            );
            assert_markers(&conn, "marker > $1", std::slice::from_ref(&zero), &[1]);
            assert_markers(&conn, "marker < $1", std::slice::from_ref(&zero), &[-1]);
        }
    });
}

#[test]
fn joined_outer_point_lookup_preserves_numeric_comparison() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    conn.execute("CREATE TABLE labels (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO labels VALUES (2, 'two')")
        .unwrap();
    let statement = conn
        .prepare("SELECT items.marker FROM items JOIN labels ON items.id = labels.id WHERE items.id = $1")
        .unwrap();
    in_transactions(&conn, || {
        assert_eq!(
            statement.query_collect(&[Value::Real(2.0)]).unwrap().rows,
            vec![vec![Value::Integer(2)]]
        );
    });
}

#[test]
fn numeric_in_and_or_match_full_predicate_evaluation() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    in_transactions(&conn, || {
        assert_markers(&conn, "id IN (1.0, 2.0, 2.5)", &[], &[1, 2]);
        assert_markers(&conn, "id = 1.0 OR id = 2.0 OR id = 2.5", &[], &[1, 2]);
    });
}

#[test]
fn numeric_expression_indexes_do_not_narrow_cross_type_equality() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    seed(&conn, "INTEGER");
    conn.execute("CREATE INDEX items_expr ON items ((marker + 0))")
        .unwrap();
    in_transactions(&conn, || {
        assert_markers(&conn, "marker + 0 = 2.0", &[], &[2]);
        assert_markers(&conn, "marker + 0 = 2", &[], &[2]);
    });
}

#[test]
fn signed_zero_delete_returning_preserves_rows_columns_and_empty_result_shape() {
    for explicit in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id REAL PRIMARY KEY, marker INTEGER)")
            .unwrap();
        for (id, marker) in [(-0.0, 1), (0.0, 2), (1.0, 3)] {
            conn.execute_params(
                "INSERT INTO items VALUES ($1, $2)",
                &[Value::Real(id), Value::Integer(marker)],
            )
            .unwrap();
        }
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let sql = "DELETE FROM items WHERE id = $1 RETURNING id, marker";
        let result = conn.execute_params(sql, &[Value::Real(0.0)]).unwrap();
        let ExecutionResult::Query(result) = result else {
            panic!("expected two returned rows");
        };
        assert_eq!(result.columns, vec!["id", "marker"]);
        assert_eq!(result.rows.len(), 2);
        for (row, zero, marker) in [(&result.rows[0], -0.0f64, 1), (&result.rows[1], 0.0f64, 2)] {
            let Value::Real(value) = row[0] else {
                panic!("expected stored REAL key");
            };
            assert_eq!(value.to_bits(), zero.to_bits());
            assert_eq!(row[1], Value::Integer(marker));
        }
        let ExecutionResult::Query(result) =
            conn.execute_params(sql, &[Value::Real(-0.0)]).unwrap()
        else {
            panic!("expected an empty RETURNING result with column metadata");
        };
        assert_eq!(result.columns, vec!["id", "marker"]);
        assert!(result.rows.is_empty());
        assert_markers(&conn, "id = 1.0", &[], &[3]);
        if explicit {
            conn.execute("ROLLBACK").unwrap();
            assert_markers(&conn, "id = 0.0", &[], &[1, 2]);
        }
    }
}
