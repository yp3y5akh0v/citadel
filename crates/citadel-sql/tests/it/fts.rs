use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, PreparedStatement, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn create_table_with_tsvector_and_tsquery_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR, q TSQUERY)")
        .unwrap();
    // Re-open to confirm schema round-trips through the catalog.
    drop(conn);
    drop(db);
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("SELECT id FROM docs WHERE id = 0").unwrap();
}

#[test]
fn text_at_at_text_auto_tokenizes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT 'hello world' @@ 'hello'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));

    let rows = conn
        .prepare("SELECT 'hello world' @@ 'mouse'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(false));
}

#[test]
fn json_at_at_still_works_after_overload() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT CAST('{\"a\":1}' AS JSONB) @@ '$.a == 1'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

fn assert_rank_ids(rows: &[Vec<Value>], expected_ids: &[i64]) {
    let mut ids: Vec<_> = rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), 2);
            assert!(matches!(row[1], Value::Real(rank) if rank > 0.0));
            match row[0] {
                Value::Integer(id) => id,
                ref other => panic!("expected document ID, got {other:?}"),
            }
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, expected_ids);
}

fn assert_rank_executes_then_hits_cache(
    db: &citadel::Database,
    statement: &PreparedStatement<'_, '_>,
    params: &[Value],
    expected_ids: &[i64],
) {
    let cold_scan = db.measure_scans();
    let cold = statement.query_collect(params).unwrap();
    assert!(cold_scan.rows_scanned() > 0, "cold ranking did not scan");
    drop(cold_scan);
    assert_rank_ids(&cold.rows, expected_ids);

    let warm_scan = db.measure_scans();
    let warm = statement.query_collect(params).unwrap();
    assert_eq!(warm.columns, cold.columns);
    assert_eq!(warm.rows, cold.rows);
    assert_eq!(warm_scan.rows_scanned(), 0, "warm ranking rescanned");
}

#[test]
fn indexed_ranking_cache_hits_track_parameters_and_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES \
         (1, TO_TSVECTOR('rust database')), (2, TO_TSVECTOR('rust')), \
         (3, TO_TSVECTOR('database')), (4, TO_TSVECTOR('ocean')), \
         (5, TO_TSVECTOR('rust database database'))",
    )
    .unwrap();
    conn.execute("CREATE INDEX by_body ON docs USING fts (body)")
        .unwrap();

    let literal = conn
        .prepare(
            "SELECT id, TS_RANK(body, TO_TSQUERY('rust & database')) AS r \
             FROM docs WHERE body @@ TO_TSQUERY('rust & database') \
             ORDER BY r DESC LIMIT 10",
        )
        .unwrap();
    assert_rank_executes_then_hits_cache(&db, &literal, &[], &[1, 5]);
    let before_transaction = literal.query_collect(&[]).unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (8, TO_TSVECTOR('rust database'))")
        .unwrap();
    assert_rank_ids(&literal.query_collect(&[]).unwrap().rows, &[1, 5, 8]);
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        literal.query_collect(&[]).unwrap().rows,
        before_transaction.rows
    );

    let parameterized = conn
        .prepare(
            "SELECT id, TS_RANK(body, TO_TSQUERY($1)) AS r \
             FROM docs WHERE body @@ TO_TSQUERY($1) ORDER BY r DESC LIMIT 10",
        )
        .unwrap();
    let rust = [Value::Text("rust & database".into())];
    let ocean = [Value::Text("ocean".into())];
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[1, 5]);
    assert_rank_executes_then_hits_cache(&db, &parameterized, &ocean, &[4]);
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[1, 5]);

    conn.execute("INSERT INTO docs VALUES (6, TO_TSVECTOR('rust database'))")
        .unwrap();
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[1, 5, 6]);
    conn.execute("UPDATE docs SET body = TO_TSVECTOR('ocean') WHERE id = 1")
        .unwrap();
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[5, 6]);
    conn.execute("DELETE FROM docs WHERE id = 5").unwrap();
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[6]);

    let other = Connection::open(&db).unwrap();
    other
        .execute("INSERT INTO docs VALUES (7, TO_TSVECTOR('rust database'))")
        .unwrap();
    assert_rank_executes_then_hits_cache(&db, &parameterized, &rust, &[6, 7]);
}

#[test]
fn immutable_fts_schema_expressions_survive_reopen_and_timezone_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let constructors = [
        "TO_TSQUERY",
        "PLAINTO_TSQUERY",
        "PHRASETO_TSQUERY",
        "WEBSEARCH_TO_TSQUERY",
    ];
    for (i, constructor) in constructors.iter().enumerate() {
        let predicate = format!("TO_TSVECTOR(body) @@ {constructor}('rust')");
        conn.execute(&format!(
            "CREATE TABLE docs_{i} (id INTEGER PRIMARY KEY, body TEXT, \
             matched BOOLEAN GENERATED ALWAYS AS ({predicate}) STORED, \
             reversed BOOLEAN GENERATED ALWAYS AS \
             ({constructor}('rust') @@ TO_TSVECTOR(body)) VIRTUAL)"
        ))
        .unwrap();
        conn.execute(&format!(
            "INSERT INTO docs_{i} (id, body) VALUES (1, 'rust database'), (2, 'ocean')"
        ))
        .unwrap();
        conn.execute(&format!(
            "CREATE INDEX matching_{i} ON docs_{i} (id) WHERE {predicate}"
        ))
        .unwrap();
        conn.execute(&format!(
            "CREATE INDEX match_value_{i} ON docs_{i} (CAST({predicate} AS INTEGER))"
        ))
        .unwrap();
        assert_eq!(
            conn.query(&format!(
                "SELECT id FROM docs_{i} WHERE CAST({predicate} AS INTEGER) = 1 ORDER BY id"
            ))
            .unwrap()
            .rows,
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            conn.query(&format!(
                "SELECT id, matched, reversed FROM docs_{i} ORDER BY id"
            ))
            .unwrap()
            .rows,
            vec![
                vec![
                    Value::Integer(1),
                    Value::Boolean(true),
                    Value::Boolean(true)
                ],
                vec![
                    Value::Integer(2),
                    Value::Boolean(false),
                    Value::Boolean(false)
                ],
            ],
            "{constructor}"
        );
        let matching = conn
            .prepare(&format!(
                "SELECT id FROM docs_{i} WHERE {predicate} ORDER BY id"
            ))
            .unwrap();
        assert_eq!(
            matching.query_collect(&[]).unwrap().rows,
            vec![vec![Value::Integer(1)]]
        );
        conn.execute(&format!("UPDATE docs_{i} SET body = 'rust' WHERE id = 2"))
            .unwrap();
        assert_eq!(
            matching.query_collect(&[]).unwrap().rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
    }
    drop(conn);
    drop(db);
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("SET TIME ZONE '+10:00'").unwrap();
    assert_eq!(conn.session_timezone(), "+10:00");
    for (i, constructor) in constructors.iter().enumerate() {
        conn.execute(&format!(
            "INSERT INTO docs_{i} (id, body) VALUES (3, 'ocean')"
        ))
        .unwrap();
        assert_eq!(
            conn.query(&format!(
                "SELECT id, matched, reversed FROM docs_{i} ORDER BY id"
            ))
            .unwrap()
            .rows,
            vec![
                vec![
                    Value::Integer(1),
                    Value::Boolean(true),
                    Value::Boolean(true)
                ],
                vec![
                    Value::Integer(2),
                    Value::Boolean(true),
                    Value::Boolean(true)
                ],
                vec![
                    Value::Integer(3),
                    Value::Boolean(false),
                    Value::Boolean(false)
                ],
            ]
        );
        assert_eq!(
            conn.query(&format!(
                "SELECT id FROM docs_{i} WHERE \
                 CAST(TO_TSVECTOR(body) @@ {constructor}('rust') AS INTEGER) = 1 ORDER BY id"
            ))
            .unwrap()
            .rows,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
    }
}

#[test]
fn fts_type_proofs_do_not_admit_session_dependent_schema_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR, doc JSONB, path TEXT)")
        .unwrap();
    for (i, expression) in [
        "doc @@ path",
        "(doc COLLATE BINARY) @@ path",
        "doc @@ CAST(TO_TSQUERY('rust') AS TEXT)",
        "doc @? CAST(TO_TSQUERY('rust') AS TEXT)",
        r#"doc @@ '$.time_tz().string() == "17:04:56+10:00"'"#,
        "body @@ TO_TSQUERY(CAST(JSONB_PATH_QUERY_FIRST_TZ(doc, '$.time().string()') AS TEXT))",
        "body @@ CAST(JSONB_PATH_QUERY_FIRST_TZ(doc, '$.time().string()') AS TSQUERY)",
    ]
    .iter()
    .enumerate()
    {
        for sql in [
            format!(
                "CREATE TABLE bad_{i} (id INTEGER PRIMARY KEY, body TSVECTOR, doc JSONB, \
                 path TEXT, matched BOOLEAN GENERATED ALWAYS AS ({expression}) STORED)"
            ),
            format!("CREATE INDEX bad_{i} ON docs (id) WHERE {expression}"),
        ] {
            let error = conn.execute(&sql).unwrap_err();
            assert!(
                matches!(error, SqlError::Unsupported(ref message)
                    if message.contains("session-time-zone-dependent JSON path")),
                "{sql}: {error}"
            );
        }
    }
    for (i, expression) in [
        "body @@ TO_TSQUERY(CAST(RANDOM() AS TEXT))",
        "TO_TSVECTOR(CAST(NOW() AS TEXT)) @@ TO_TSQUERY('rust')",
    ]
    .iter()
    .enumerate()
    {
        for sql in [
            format!(
                "CREATE TABLE volatile_{i} (id INTEGER PRIMARY KEY, body TSVECTOR, \
                 matched BOOLEAN GENERATED ALWAYS AS ({expression}) STORED)"
            ),
            format!("CREATE INDEX volatile_{i} ON docs (id) WHERE {expression}"),
        ] {
            let error = conn.execute(&sql).unwrap_err();
            assert!(
                matches!(error, SqlError::Unsupported(ref message)
                    if message.contains("volatile") || message.contains("non-deterministic")),
                "{sql}: {error}"
            );
        }
    }
}

#[test]
fn stripped_vectors_preserve_matching_with_and_without_an_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE stripped (id INTEGER PRIMARY KEY, body TSVECTOR)")
        .unwrap();
    conn.execute("INSERT INTO stripped VALUES (1, STRIP(TO_TSVECTOR('cat dog')))")
        .unwrap();
    let cancel = citadel::CancelToken::new();
    for indexed in [false, true] {
        if indexed {
            conn.execute("CREATE INDEX stripped_body ON stripped USING fts (body)")
                .unwrap();
        }
        for token in [None, Some(cancel.clone())] {
            db.set_cancel(token);
            let conn = Connection::open(&db).unwrap();
            for (query, matched) in [
                ("cat", true),
                ("ca:*", true),
                ("cat:D", true),
                ("cat:A", true),
                ("ca:*A", true),
                ("cat & dog", true),
                ("cat | absent", true),
                ("!cat", false),
                ("absent", false),
                ("cat <-> dog", false),
            ] {
                let scalar = conn
                    .query(&format!(
                        "SELECT STRIP(TO_TSVECTOR('cat dog')) @@ TO_TSQUERY('{query}')"
                    ))
                    .unwrap();
                assert_eq!(scalar.rows, vec![vec![Value::Boolean(matched)]], "{query}");
                let rows = conn
                    .query(&format!(
                        "SELECT id FROM stripped WHERE body @@ TO_TSQUERY('{query}')"
                    ))
                    .unwrap()
                    .rows;
                assert_eq!(
                    rows.len(),
                    usize::from(matched),
                    "{query}, indexed={indexed}"
                );
            }
            let rows = conn
                .query(
                    "SELECT id, TS_RANK(body, TO_TSQUERY('cat & dog')) AS r FROM stripped \
                 WHERE body @@ TO_TSQUERY('cat & dog') ORDER BY r DESC LIMIT 10",
                )
                .unwrap()
                .rows;
            assert_eq!(rows, vec![vec![Value::Integer(1), Value::Real(0.0)]]);
        }
    }
    db.set_cancel(None);
}
