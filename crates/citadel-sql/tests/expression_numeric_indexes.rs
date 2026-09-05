use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"expression-index-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn seed(db: &citadel::Database, target: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, external_id TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1, $2)").unwrap();
    for id in 0..1_024 {
        insert
            .execute(&[Value::Integer(id), Value::Text(id.to_string().into())])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute(&format!(
        "CREATE INDEX by_external_id ON items (CAST(external_id AS {target}))"
    ))
    .unwrap();
}

fn expected(ids: &[i64]) -> Vec<Vec<Value>> {
    ids.iter().map(|&id| vec![Value::Integer(id)]).collect()
}

fn assert_indexed(db: &citadel::Database, predicate: &str, params: &[Value], ids: &[i64]) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        for prepared in [false, true] {
            let conn = Connection::open(db).unwrap();
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            let sql = format!("SELECT id FROM items WHERE {predicate} ORDER BY id");
            let statement = prepared.then(|| conn.prepare(&sql).unwrap());
            let measured = db.measure_scans();
            let rows = match statement {
                Some(statement) => statement.query_collect(params).unwrap().rows,
                None => conn.query_params(&sql, params).unwrap().rows,
            };
            assert_eq!(rows, expected(ids), "{predicate}: {params:?}");
            assert!(
                measured.rows_scanned() <= ids.len() as u64 + 1,
                "{predicate}, {begin:?}, prepared={prepared}: {} rows scanned",
                measured.rows_scanned()
            );
            drop(measured);
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn numeric_cast_expression_indexes_seek_literal_and_parameter_equalities() {
    for target in ["INTEGER", "REAL"] {
        let db = database();
        seed(&db, target);
        for suffix in ["= 900", "= 900.0"] {
            assert_indexed(
                &db,
                &format!("CAST(external_id AS {target}) {suffix}"),
                &[],
                &[900],
            );
        }
        assert_indexed(
            &db,
            &format!("900.0 = CAST(external_id AS {target})"),
            &[],
            &[900],
        );
        for value in [Value::Integer(901), Value::Real(901.0)] {
            assert_indexed(
                &db,
                &format!("CAST(external_id AS {target}) = $1"),
                std::slice::from_ref(&value),
                &[901],
            );
            assert_indexed(
                &db,
                &format!("$1 = CAST(external_id AS {target})"),
                &[value],
                &[901],
            );
        }
    }
}

#[test]
fn prepared_cast_expression_probe_rebinds_numeric_types_and_null() {
    let db = database();
    seed(&db, "INTEGER");
    let conn = Connection::open(&db).unwrap();
    let statement = conn
        .prepare("SELECT id FROM items WHERE CAST(external_id AS INTEGER) = $1 ORDER BY id")
        .unwrap();
    for (value, ids) in [
        (Value::Integer(2), vec![2]),
        (Value::Real(3.0), vec![3]),
        (Value::Real(3.5), vec![]),
        (Value::Null, vec![]),
        (Value::Text("2".into()), vec![]),
        (Value::Integer(4), vec![4]),
    ] {
        assert_eq!(
            statement.query_collect(&[value]).unwrap().rows,
            expected(&ids)
        );
    }
}

#[test]
fn cast_index_uses_cast_values_not_source_values() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, external_id TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, '2'), (2, '2.9'), (3, NULL), (4, '-2.9')")
        .unwrap();
    conn.execute("CREATE INDEX by_external_id ON items (CAST(external_id AS INTEGER))")
        .unwrap();
    assert_indexed(&db, "CAST(external_id AS INTEGER) = 2.0", &[], &[1, 2]);
    assert_indexed(
        &db,
        "CAST(external_id AS INTEGER) = $1",
        &[Value::Real(-2.0)],
        &[4],
    );
}

#[test]
fn cast_index_preserves_large_integer_equivalence_classes_and_signed_zero() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, external_id INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 9007199254740992), (2, 9007199254740993), (3, 9223372036854775806), (4, 9223372036854775807)").unwrap();
    conn.execute("CREATE INDEX by_external_id ON items (CAST(external_id AS INTEGER))")
        .unwrap();
    let statement = conn
        .prepare("SELECT id FROM items WHERE CAST(external_id AS INTEGER) = $1 ORDER BY id")
        .unwrap();
    for (value, ids) in [
        (Value::Real(9_007_199_254_740_992.0), vec![1, 2]),
        (Value::Integer(9_007_199_254_740_992), vec![1]),
        (Value::Real(i64::MAX as f64), vec![3, 4]),
        (Value::Integer(i64::MAX), vec![4]),
    ] {
        assert_eq!(
            statement.query_collect(&[value]).unwrap().rows,
            expected(&ids)
        );
    }
    conn.execute("CREATE INDEX by_real ON items (CAST(external_id AS REAL))")
        .unwrap();
    assert_indexed(
        &db,
        "CAST(external_id AS REAL) = $1",
        &[Value::Integer(9_007_199_254_740_993)],
        &[1, 2],
    );
    assert_indexed(
        &db,
        "CAST(external_id AS REAL) = $1",
        &[Value::Integer(i64::MAX)],
        &[3, 4],
    );
    conn.execute("CREATE TABLE zeros (id INTEGER PRIMARY KEY, n REAL)")
        .unwrap();
    for (id, value) in [(1, -0.0), (2, 0.0), (3, 1.0)] {
        conn.execute_params(
            "INSERT INTO zeros VALUES ($1, $2)",
            &[Value::Integer(id), Value::Real(value)],
        )
        .unwrap();
    }
    conn.execute("CREATE INDEX by_zero ON zeros (CAST(n AS REAL))")
        .unwrap();
    let statement = conn
        .prepare("SELECT id, n FROM zeros WHERE CAST(n AS REAL) = $1 ORDER BY id")
        .unwrap();
    for value in [Value::Integer(0), Value::Real(-0.0), Value::Real(0.0)] {
        let result = statement.query_collect(&[value]).unwrap();
        assert_eq!(result.rows.len(), 2);
        for (row, bits) in result
            .rows
            .iter()
            .zip([(-0.0_f64).to_bits(), 0.0_f64.to_bits()])
        {
            let Value::Real(n) = row[1] else {
                panic!("expected REAL")
            };
            assert_eq!(n.to_bits(), bits);
        }
    }
}

#[test]
fn expression_index_parameters_support_text_without_matching_other_expressions() {
    let db = database();
    seed(&db, "INTEGER");
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE INDEX by_lower ON items (LOWER(external_id))")
        .unwrap();
    assert_indexed(
        &db,
        "LOWER(external_id) = $1",
        &[Value::Text("902".into())],
        &[902],
    );
    assert_eq!(
        conn.query_params(
            "SELECT id FROM items WHERE CAST(external_id AS REAL) = $1 ORDER BY id",
            &[Value::Real(900.0)]
        )
        .unwrap()
        .rows,
        expected(&[900])
    );
    assert!(conn
        .query("SELECT id FROM items WHERE CAST(external_id AS REAL) = $1")
        .is_err());
    assert!(conn
        .query_params(
            "SELECT id FROM items WHERE CAST(external_id AS INTEGER) = CAST($1 AS INTEGER)",
            &[Value::Text("bad".into())]
        )
        .is_err());
}

#[test]
fn cast_index_mutations_maintain_entries_and_rollback_invalid_values() {
    let db = database();
    seed(&db, "INTEGER");
    let conn = Connection::open(&db).unwrap();
    conn.execute("BEGIN").unwrap();
    let measured = db.measure_scans();
    assert!(matches!(
        conn.execute_params(
            "UPDATE items SET external_id = '2000' WHERE CAST(external_id AS INTEGER) = $1",
            &[Value::Real(900.0)]
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    assert!(
        measured.rows_scanned() < 10,
        "{} rows scanned",
        measured.rows_scanned()
    );
    drop(measured);
    conn.execute("SAVEPOINT changed").unwrap();
    assert!(conn
        .execute("INSERT INTO items VALUES (2001, 'bad')")
        .is_err());
    conn.execute("ROLLBACK TO changed").unwrap();
    assert_eq!(
        conn.query("SELECT id FROM items WHERE CAST(external_id AS INTEGER) = 2000")
            .unwrap()
            .rows,
        expected(&[900])
    );
    assert!(matches!(
        conn.execute_params(
            "DELETE FROM items WHERE CAST(external_id AS INTEGER) = $1",
            &[Value::Integer(2000)]
        )
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    ));
    conn.execute("ROLLBACK").unwrap();
    assert_indexed(&db, "CAST(external_id AS INTEGER) = 900.0", &[], &[900]);
}

#[test]
fn cast_expression_index_is_usable_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cast-index.cdl");
    {
        let db = DatabaseBuilder::new(&path)
            .passphrase(b"expression-index-test")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        seed(&db, "INTEGER");
    }
    let db = DatabaseBuilder::new(&path)
        .passphrase(b"expression-index-test")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    assert_indexed(
        &db,
        "CAST(external_id AS INTEGER) = $1",
        &[Value::Real(902.0)],
        &[902],
    );
}

#[test]
fn cast_index_with_embedded_parameters_is_not_used_as_a_stable_key() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE INDEX parameter_key ON items (CAST($1 AS INTEGER))")
        .unwrap();
    for id in [1, 2] {
        conn.execute_params("INSERT INTO items VALUES ($1)", &[Value::Integer(id)])
            .unwrap();
    }
    assert_eq!(
        conn.query_params(
            "SELECT id FROM items WHERE CAST($1 AS INTEGER) = $2 ORDER BY id",
            &[Value::Integer(9), Value::Integer(9)]
        )
        .unwrap()
        .rows,
        expected(&[1, 2])
    );
}

#[test]
fn cast_index_matching_preserves_inner_collations() {
    for (target, literal, value) in [
        ("INTEGER", "0", Value::Integer(0)),
        ("TEXT", "'FALSE'", Value::Text("FALSE".into())),
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 'X')").unwrap();
        conn.execute(&format!(
            "CREATE INDEX by_match ON items (CAST((name COLLATE NOCASE) = 'x' AS {target}))"
        ))
        .unwrap();
        let query = format!("SELECT id FROM items WHERE CAST(name = 'x' AS {target}) = ");
        assert_eq!(
            conn.query(&format!("{query}{literal}")).unwrap().rows,
            expected(&[1])
        );
        assert_eq!(
            conn.query_params(&format!("{query}$1"), &[value])
                .unwrap()
                .rows,
            expected(&[1])
        );
    }
}

#[test]
fn text_expression_probe_respects_comparison_and_index_collations() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'X'), (2, 'other'), (3, 'third')")
        .unwrap();
    conn.execute("CREATE INDEX by_name ON items (CAST(name AS TEXT))")
        .unwrap();
    for sql in [
        "SELECT id FROM items WHERE CAST(name AS TEXT) = 'x'",
        "SELECT id FROM items WHERE CAST(name AS TEXT) = $1",
    ] {
        let params = if sql.contains("$1") {
            vec![Value::Text("x".into())]
        } else {
            vec![]
        };
        assert_eq!(
            conn.query_params(sql, &params).unwrap().rows,
            expected(&[1])
        );
    }
    for collation in ["NOCASE", "RTRIM", "unknown_collation"] {
        for parentheses in 0..=2 {
            let expression = format!(
                "{}CAST(name AS TEXT) COLLATE {collation}{}",
                "(".repeat(parentheses),
                ")".repeat(parentheses)
            );
            assert!(
                matches!(
                    conn.execute(&format!("CREATE INDEX folded_name ON items ({expression})")),
                    Err(citadel_sql::SqlError::Unsupported(_))
                ),
                "{expression}"
            );
        }
    }
    conn.execute("CREATE INDEX binary_name ON items (CAST(name AS TEXT) COLLATE BINARY)")
        .unwrap();
}

#[test]
fn expression_index_does_not_commute_collation_sensitive_operands() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'X')").unwrap();
    let first = "CAST(CAST(((name COLLATE NOCASE) = 'x') AND ((name COLLATE BINARY) = 'X') AS TEXT) = 'true' AS TEXT)";
    let swapped = "CAST(CAST(((name COLLATE BINARY) = 'X') AND ((name COLLATE NOCASE) = 'x') AS TEXT) = 'true' AS TEXT)";
    let query = format!("SELECT id FROM items WHERE {swapped} = $1");
    let params = [Value::Text("FALSE".into())];
    assert_eq!(
        conn.query_params(&query, &params).unwrap().rows,
        expected(&[1])
    );
    conn.execute(&format!("CREATE INDEX by_match ON items ({first})"))
        .unwrap();
    assert_eq!(
        conn.query_params(&query, &params).unwrap().rows,
        expected(&[1])
    );
}

#[test]
fn temporal_expression_indexes_preserve_coerced_equality() {
    for expr in [
        "CAST(s AS DATE)",
        "COALESCE(CAST(s AS DATE), DATE '2000-01-01')",
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, '2020-01-01')")
            .unwrap();
        let query = format!("SELECT id FROM items WHERE {expr} = $1");
        let params = [Value::Text("2020-01-01".into())];
        assert_eq!(
            conn.query_params(&query, &params).unwrap().rows,
            expected(&[1])
        );
        conn.execute(&format!("CREATE INDEX by_date ON items ({expr})"))
            .unwrap();
        assert_eq!(
            conn.query_params(&query, &params).unwrap().rows,
            expected(&[1])
        );
        assert_eq!(
            conn.query(&format!("SELECT id FROM items WHERE {expr} = '2020-01-01'"))
                .unwrap()
                .rows,
            expected(&[1])
        );
    }
}

#[test]
fn interval_expression_index_preserves_normalized_equality() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, s TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, '1 month')")
        .unwrap();
    let query = "SELECT id FROM items WHERE CAST(s AS INTERVAL) = INTERVAL '30 days'";
    assert_eq!(conn.query(query).unwrap().rows, expected(&[1]));
    conn.execute("CREATE INDEX by_duration ON items (CAST(s AS INTERVAL))")
        .unwrap();
    assert_eq!(conn.query(query).unwrap().rows, expected(&[1]));
}

#[test]
fn known_text_expression_probes_match_unindexed_results() {
    for function in [
        "LOWER(v)",
        "UPPER(v)",
        "SUBSTR(v, 1, 2)",
        "SUBSTRING(v, 1, 2)",
        "TRIM(v)",
        "LTRIM(v)",
        "RTRIM(v)",
        "REPLACE(v, 'X', 'Y')",
        "CONCAT(v, '!')",
        "TYPEOF(v)",
        "HEX(v)",
        "CAST(v AS TEXT)",
    ] {
        for (data_type, values) in [
            ("TEXT", "' X ', 'other', NULL"),
            ("INTEGER", "12, 34, NULL"),
        ] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, v {data_type})"
            ))
            .unwrap();
            for (id, value) in values.split(", ").enumerate() {
                conn.execute(&format!("INSERT INTO items VALUES ({}, {value})", id + 1))
                    .unwrap();
            }
            let probes = conn
                .query(&format!("SELECT {function} FROM items ORDER BY id"))
                .unwrap()
                .rows;
            let query = format!("SELECT id FROM items WHERE {function} = $1 ORDER BY id");
            let reference: Vec<_> = probes
                .iter()
                .map(|row| conn.query_params(&query, row).unwrap().rows)
                .collect();
            conn.execute(&format!("CREATE INDEX by_value ON items ({function})"))
                .unwrap();
            for (probe, expected) in probes.iter().zip(reference) {
                let measured = db.measure_scans();
                assert_eq!(
                    conn.query_params(&query, probe).unwrap().rows,
                    expected,
                    "{function} on {data_type}: {probe:?}"
                );
                if let Value::Text(text) = &probe[0] {
                    assert!(measured.rows_scanned() <= expected.len() as u64 + 1);
                    drop(measured);
                    assert_eq!(
                        conn.query(&format!(
                            "SELECT id FROM items WHERE {function} = '{}' ORDER BY id",
                            text.replace('\'', "''")
                        ))
                        .unwrap()
                        .rows,
                        expected
                    );
                }
            }
        }
    }
}

#[test]
fn text_expression_index_preserves_temporal_probe_conversion() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, s TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, '2020-01-01')")
        .unwrap();
    let query = "SELECT id FROM items WHERE CAST(s AS TEXT) = DATE '2020-01-01'";
    assert_eq!(conn.query(query).unwrap().rows, expected(&[1]));
    conn.execute("CREATE INDEX by_text ON items (CAST(s AS TEXT))")
        .unwrap();
    assert_eq!(conn.query(query).unwrap().rows, expected(&[1]));
}
