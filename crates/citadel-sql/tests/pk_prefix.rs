use citadel::DatabaseBuilder;
use citadel_sql::planner::{plan_select, ScanPlan};
use citadel_sql::{Connection, ReadBudget, SqlError, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"test-passphrase")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute(
        "CREATE TABLE edges (src INTEGER, dst INTEGER, kind TEXT, payload TEXT, \
         PRIMARY KEY (src, dst, kind))",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for src in 0..64 {
        for dst in 0..16 {
            conn.execute_params(
                "INSERT INTO edges VALUES ($1, $2, 'edge', 'small')",
                &[Value::Integer(src), Value::Integer(dst)],
            )
            .unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
}

#[test]
fn composite_prefix_seeks_and_stops_at_limit_in_read_and_write_transactions() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        let measurement = db.measure_scans();
        let rows = conn
            .query_params(
                "SELECT dst FROM edges WHERE src = $1 LIMIT 3",
                &[Value::Integer(42)],
            )
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(0)],
                vec![Value::Integer(1)],
                vec![Value::Integer(2)]
            ]
        );
        assert_eq!(measurement.rows_scanned(), 3);
        drop(measurement);
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn prefix_scan_rechecks_residuals_and_preserves_order_offset_and_empty_limit() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for tail in [
        "AND dst > 11 LIMIT 2",
        "AND dst > 4 ORDER BY dst DESC LIMIT 3 OFFSET 2",
        "AND dst > 4 ORDER BY dst LIMIT 3 OFFSET 2",
        "AND src = 41",
        "LIMIT 0",
    ] {
        let prefix = conn
            .query(&format!("SELECT dst FROM edges WHERE src = 42 {tail}"))
            .unwrap();
        let reference = conn
            .query(&format!("SELECT dst FROM edges WHERE src + 0 = 42 {tail}"))
            .unwrap();
        assert_eq!(prefix.rows, reference.rows, "{tail}");
    }
}

#[test]
fn prefix_scans_keep_write_and_savepoint_visibility() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE edges SET payload = 'changed' WHERE src = 42 AND dst = 3")
        .unwrap();
    conn.execute("SAVEPOINT before_more").unwrap();
    conn.execute("INSERT INTO edges VALUES (42, 99, 'edge', 'new')")
        .unwrap();
    conn.execute("DELETE FROM edges WHERE src = 42 AND dst = 2")
        .unwrap();
    let rows = conn
        .query("SELECT dst FROM edges WHERE src = 42 AND dst IN (2, 99) ORDER BY dst")
        .unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Integer(99)]]);
    conn.execute("ROLLBACK TO before_more").unwrap();
    let rows = conn
        .query("SELECT dst FROM edges WHERE src = 42 AND dst IN (2, 99) ORDER BY dst")
        .unwrap();
    assert_eq!(rows.rows, vec![vec![Value::Integer(2)]]);
    assert_eq!(
        conn.query("SELECT payload FROM edges WHERE src = 42 AND dst = 3")
            .unwrap()
            .rows,
        vec![vec![Value::Text("changed".into())]]
    );
    assert_eq!(
        conn.query("SELECT payload FROM edges WHERE src = 41 AND dst = 3")
            .unwrap()
            .rows,
        vec![vec![Value::Text("small".into())]]
    );
    conn.execute("COMMIT").unwrap();
}

#[test]
fn prefix_scan_does_not_materialize_a_following_overflow_row() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY (a,b))")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1,1,'small')")
        .unwrap();
    conn.execute_params(
        "INSERT INTO items VALUES (2,1,$1)",
        &[Value::Text("x".repeat(32_768).into())],
    )
    .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        let rows = conn
            .query_params_bounded(
                "SELECT body FROM items WHERE a = 1",
                &[],
                &ReadBudget::new(128, 128),
            )
            .unwrap();
        assert_eq!(rows.rows, vec![vec![Value::Text("small".into())]]);
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn prefix_scan_rejects_oversized_matching_rows_and_cancellation() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY (a,b))")
        .unwrap();
    conn.execute_params(
        "INSERT INTO items VALUES (1,1,$1)",
        &[Value::Text("x".repeat(32_768).into())],
    )
    .unwrap();
    let error = conn
        .query_params_bounded(
            "SELECT body FROM items WHERE a = 1",
            &[],
            &ReadBudget::new(128, 128),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
    ));
    let token = citadel::CancelToken::new();
    db.set_cancel(Some(token.clone()));
    token.cancel();
    let error = conn
        .query("SELECT body FROM items WHERE a = 1")
        .unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn binary_text_prefixes_keep_component_boundaries() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (a TEXT, b INTEGER, PRIMARY KEY (a,b))")
        .unwrap();
    for (index, value) in ["a", "a\0", "a\0x", "aa", ""].iter().enumerate() {
        conn.execute_params(
            "INSERT INTO items VALUES ($1,$2)",
            &[Value::Text((*value).into()), Value::Integer(index as i64)],
        )
        .unwrap();
    }
    for (index, value) in ["a", "a\0", "a\0x", "aa", ""].iter().enumerate() {
        let rows = conn
            .query_params(
                "SELECT b FROM items WHERE a = $1",
                &[Value::Text((*value).into())],
            )
            .unwrap();
        assert_eq!(rows.rows, vec![vec![Value::Integer(index as i64)]]);
    }
}

#[test]
fn prefix_planning_refuses_coercions_collations_and_volatile_bounds() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (a INTEGER, b INTEGER, PRIMARY KEY (a,b))")
        .unwrap();
    let schema = conn.table_schema("items").unwrap();
    for expression in [
        "a = RANDOM()",
        "a = ABS(RANDOM())",
        "a = CAST(RANDOM() AS INTEGER)",
        "a = 1.0",
        "a = NULL",
        "b = 1",
    ] {
        let predicate = Some(citadel_sql::parser::parse_sql_expr(expression).unwrap());
        assert!(
            matches!(plan_select(&schema, &predicate), ScanPlan::SeqScan),
            "{expression}"
        );
    }
    let predicate = Some(citadel_sql::parser::parse_sql_expr("a = 1").unwrap());
    let plan = plan_select(&schema, &predicate);
    assert!(matches!(plan, ScanPlan::PkPrefixScan { .. }));
    assert!(!plan.covers_where());
    assert!(
        citadel_sql::planner::describe_plan(&plan, &schema).contains("PRIMARY KEY PREFIX (a = 1)")
    );

    conn.execute("CREATE TABLE names (a TEXT COLLATE NOCASE, b INTEGER, PRIMARY KEY (a,b))")
        .unwrap();
    let schema = conn.table_schema("names").unwrap();
    let predicate = Some(citadel_sql::parser::parse_sql_expr("a = 'x'").unwrap());
    assert!(matches!(
        plan_select(&schema, &predicate),
        ScanPlan::SeqScan
    ));
}

#[test]
fn two_column_prefix_rechecks_a_missing_middle_component() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for predicate in [
        "src = 42 AND dst = 5",
        "src = 42 AND kind = 'other'",
        "src = 42 AND kind = 'edge'",
    ] {
        let rows = conn
            .query(&format!(
                "SELECT dst FROM edges WHERE {predicate} ORDER BY dst"
            ))
            .unwrap();
        let reference = conn
            .query(&format!(
                "SELECT dst FROM edges WHERE {} ORDER BY dst",
                predicate.replace("src =", "src + 0 =")
            ))
            .unwrap();
        assert_eq!(rows.rows, reference.rows, "{predicate}");
    }
}

#[test]
fn selective_unique_index_precedes_a_primary_key_prefix() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE accounts (tenant INTEGER, id INTEGER, email TEXT, \
         PRIMARY KEY (tenant, id))",
    )
    .unwrap();
    conn.execute("CREATE UNIQUE INDEX accounts_email ON accounts (email)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..256 {
        conn.execute_params(
            "INSERT INTO accounts VALUES (1, $1, $2)",
            &[
                Value::Integer(id),
                Value::Text(format!("user{id:03}@example.test").into()),
            ],
        )
        .unwrap();
    }
    conn.execute("INSERT INTO accounts VALUES (2, 0, 'neighbor@example.test')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        let measurement = db.measure_scans();
        let rows = conn
            .query(
                "SELECT id FROM accounts \
                 WHERE tenant = 1 AND email = 'user255@example.test'",
            )
            .unwrap()
            .rows;
        assert_eq!(rows, vec![vec![Value::Integer(255)]]);
        assert!(
            measurement.rows_scanned() <= 2,
            "selective lookup scanned {} rows",
            measurement.rows_scanned()
        );
        drop(measurement);
        assert!(conn
            .query(
                "SELECT id FROM accounts \
                 WHERE tenant = 1 AND email = 'neighbor@example.test'",
            )
            .unwrap()
            .rows
            .is_empty());
        let schema = conn.table_schema("accounts").unwrap();
        let predicate = Some(
            citadel_sql::parser::parse_sql_expr("tenant = 1 AND email = 'user255@example.test'")
                .unwrap(),
        );
        assert!(matches!(
            plan_select(&schema, &predicate),
            ScanPlan::IndexScan { index_name, is_unique: true, .. }
                if index_name == "accounts_email"
        ));
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn fully_covered_primary_key_prefix_keeps_limits_bounded_with_redundant_indexes() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("CREATE INDEX edges_src ON edges (src)")
        .unwrap();
    conn.execute("CREATE INDEX edges_src_dst ON edges (src, dst)")
        .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for (condition, expected) in [
            ("src = 42", vec![0, 1, 2]),
            ("src = 42 AND dst = 5", vec![5]),
            ("5 = dst AND 42 = src", vec![5]),
        ] {
            let measurement = db.measure_scans();
            let rows = conn
                .query(&format!("SELECT dst FROM edges WHERE {condition} LIMIT 3"))
                .unwrap()
                .rows;
            assert_eq!(
                rows,
                expected
                    .iter()
                    .map(|id| vec![Value::Integer(*id)])
                    .collect::<Vec<_>>()
            );
            assert_eq!(measurement.rows_scanned(), expected.len() as u64);
            drop(measurement);
            let schema = conn.table_schema("edges").unwrap();
            let predicate = Some(citadel_sql::parser::parse_sql_expr(condition).unwrap());
            let plan = plan_select(&schema, &predicate);
            assert!(matches!(plan, ScanPlan::PkPrefixScan { .. }));
            assert!(!plan.covers_where());
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn residual_or_duplicate_conjuncts_do_not_promote_primary_key_prefixes() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE items (tenant INTEGER, id INTEGER, seq INTEGER, body TEXT, \
         PRIMARY KEY (tenant, id, seq))",
    )
    .unwrap();
    conn.execute("CREATE INDEX items_tenant ON items (tenant)")
        .unwrap();
    conn.execute("CREATE INDEX items_tenant_id ON items (tenant, id)")
        .unwrap();
    let schema = conn.table_schema("items").unwrap();
    for condition in [
        "tenant = 1 AND tenant = 1",
        "tenant = 1 AND tenant = 2",
        "tenant = 1 AND id = 7 AND id = 7",
        "tenant = 1 AND body = 'selected'",
        "tenant = 1 AND id = 7 AND seq > 2",
        "tenant = 1 AND TRUE",
        "tenant = 1 AND id = ABS(7)",
        "tenant = CAST(1 AS INTEGER)",
    ] {
        let predicate = Some(citadel_sql::parser::parse_sql_expr(condition).unwrap());
        assert!(
            matches!(plan_select(&schema, &predicate), ScanPlan::IndexScan { .. }),
            "{condition}"
        );
    }
}

#[test]
fn repeated_primary_key_columns_do_not_hide_residual_predicates() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (a INTEGER, z INTEGER, email TEXT, PRIMARY KEY (a, A, z))")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX items_email ON items (email)")
        .unwrap();
    let schema = conn.table_schema("items").unwrap();
    assert_eq!(schema.primary_key_columns, vec![0, 0, 1]);
    let predicate = Some(
        citadel_sql::parser::parse_sql_expr("a = 1 AND email = 'selected@example.test'").unwrap(),
    );
    assert!(matches!(
        plan_select(&schema, &predicate),
        ScanPlan::IndexScan { index_name, .. } if index_name == "items_email"
    ));
}

fn setup_prefix_mutation(conn: &Connection<'_>) {
    conn.execute(
        "CREATE TABLE items (tenant INTEGER, id INTEGER, enabled BOOLEAN, body TEXT, \
         PRIMARY KEY (tenant, id))",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for tenant in 0..3 {
        for id in 0..4 {
            conn.execute_params(
                "INSERT INTO items VALUES ($1, $2, $3, 'original')",
                &[
                    Value::Integer(tenant),
                    Value::Integer(id),
                    Value::Boolean(id % 2 == 1),
                ],
            )
            .unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
}

fn mutation_rows(conn: &Connection<'_>) -> Vec<Vec<Value>> {
    conn.query("SELECT tenant, id, enabled, body FROM items ORDER BY tenant, id")
        .unwrap()
        .rows
}

fn assert_prefix_mutation_plan(conn: &Connection<'_>, statement: &str) {
    let schema = conn.table_schema("items").unwrap();
    let predicate =
        Some(citadel_sql::parser::parse_sql_expr("tenant = 1 AND enabled = TRUE").unwrap());
    let plan = plan_select(&schema, &predicate);
    assert!(matches!(plan, ScanPlan::PkPrefixScan { .. }));
    assert!(!plan.covers_where());
    let rows = conn.query(&format!("EXPLAIN {statement}")).unwrap().rows;
    assert!(matches!(
        &rows[0][0],
        Value::Text(line) if line.contains("USING PRIMARY KEY PREFIX (tenant = 1)")
    ));
}

#[test]
fn partial_primary_key_updates_recheck_residuals_and_preserve_neighboring_keys() {
    for explicit_transaction in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        setup_prefix_mutation(&conn);
        let original = mutation_rows(&conn);
        if explicit_transaction {
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO items VALUES (1, 5, TRUE, 'original')")
                .unwrap();
            conn.execute("SAVEPOINT before_update").unwrap();
        }
        let before_update = mutation_rows(&conn);
        let statement = "UPDATE items SET id = id + 10, body = 'updated' \
                         WHERE tenant = 1 AND enabled = TRUE \
                         RETURNING tenant, id, enabled, body";
        assert_prefix_mutation_plan(&conn, statement);
        let mut expected = before_update.clone();
        let mut returned = Vec::new();
        for row in &mut expected {
            if row[0] == Value::Integer(1) && row[2] == Value::Boolean(true) {
                let Value::Integer(id) = &mut row[1] else {
                    panic!("expected integer primary key");
                };
                *id += 10;
                row[3] = Value::Text("updated".into());
                returned.push(row.clone());
            }
        }
        expected.sort_by_key(|row| match (&row[0], &row[1]) {
            (Value::Integer(tenant), Value::Integer(id)) => (*tenant, *id),
            _ => panic!("expected integer primary key"),
        });
        let measurement = db.measure_scans();
        assert_eq!(conn.query(statement).unwrap().rows, returned);
        assert_eq!(
            measurement.rows_scanned(),
            before_update
                .iter()
                .filter(|row| row[0] == Value::Integer(1))
                .count() as u64
        );
        drop(measurement);
        assert_eq!(mutation_rows(&conn), expected);
        if explicit_transaction {
            conn.execute("ROLLBACK TO before_update").unwrap();
            assert_eq!(mutation_rows(&conn), before_update);
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(mutation_rows(&conn), original);
        } else {
            assert_eq!(mutation_rows(&Connection::open(&db).unwrap()), expected);
        }
    }
}

#[test]
fn partial_primary_key_deletes_recheck_residuals_and_preserve_neighboring_keys() {
    for explicit_transaction in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        setup_prefix_mutation(&conn);
        let original = mutation_rows(&conn);
        if explicit_transaction {
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO items VALUES (1, 5, TRUE, 'original')")
                .unwrap();
            conn.execute("SAVEPOINT before_delete").unwrap();
        }
        let before_delete = mutation_rows(&conn);
        let statement = "DELETE FROM items WHERE tenant = 1 AND enabled = TRUE \
                         RETURNING tenant, id, enabled, body";
        assert_prefix_mutation_plan(&conn, statement);
        let (returned, expected): (Vec<_>, Vec<_>) = before_delete
            .iter()
            .cloned()
            .partition(|row| row[0] == Value::Integer(1) && row[2] == Value::Boolean(true));
        let measurement = db.measure_scans();
        assert_eq!(conn.query(statement).unwrap().rows, returned);
        assert_eq!(
            measurement.rows_scanned(),
            before_delete
                .iter()
                .filter(|row| row[0] == Value::Integer(1))
                .count() as u64
        );
        drop(measurement);
        assert_eq!(mutation_rows(&conn), expected);
        if explicit_transaction {
            conn.execute("ROLLBACK TO before_delete").unwrap();
            assert_eq!(mutation_rows(&conn), before_delete);
            conn.execute("ROLLBACK").unwrap();
            assert_eq!(mutation_rows(&conn), original);
        } else {
            assert_eq!(mutation_rows(&Connection::open(&db).unwrap()), expected);
        }
    }
}
