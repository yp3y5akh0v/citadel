use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn create_fts_index_on_text_column_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
}

#[test]
fn create_fts_index_with_simple_config() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body) WITH (config = 'simple')")
        .unwrap();
}

#[test]
fn create_fts_index_on_jsonb_column_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body JSONB)")
        .unwrap();
    let res = conn.execute("CREATE INDEX idx_body ON docs USING fts (body)");
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("FTS index requires a TEXT or TSVECTOR column"),
        "expected type check failure, got: {err}"
    );
}

#[test]
fn create_fts_index_with_unknown_config_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let res =
        conn.execute("CREATE INDEX idx_body ON docs USING fts (body) WITH (config = 'klingon')");
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("unknown text search configuration"),
        "expected unknown-config error, got: {err}"
    );
}

#[test]
fn fts_index_query_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'the quick brown fox jumps')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'lazy dog sleeping in shade')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'cats are nocturnal hunters')")
        .unwrap();

    // 'fox' should match row 1 only.
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('fox') ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));

    // 'jumping' (stems to 'jump') should match row 1.
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('jumping') ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));

    // 'cat' should match row 3 (cats stems to cat).
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(3));
}

#[test]
fn fts_index_maintained_on_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'cats run fast')")
        .unwrap();

    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);

    conn.execute("UPDATE docs SET body = 'dogs bark loud' WHERE id = 1")
        .unwrap();

    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 0, "old lexeme should be gone after UPDATE");

    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(
        rows.rows.len(),
        1,
        "new lexeme should be present after UPDATE"
    );

    conn.execute("DELETE FROM docs WHERE id = 1").unwrap();
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 0, "row should be gone after DELETE");
}

#[test]
fn fts_index_and_query_finds_all_required_lexemes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'cats and dogs together')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'only cats here')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'only dogs here')")
        .unwrap();

    // AND: both required → only row 1
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat & dog') ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}

#[test]
fn fts_index_or_query_returns_union_of_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'cats here')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'dogs here')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'fish here')")
        .unwrap();

    // OR: pure-OR falls back to seqscan but result must still be correct.
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat | dog') ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn fts_index_works_on_tsvector_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR)")
        .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    let stmt = conn
        .prepare("INSERT INTO docs VALUES (1, to_tsvector($1))")
        .unwrap();
    stmt.query_collect(&[Value::Text("cats jumping over".into())])
        .unwrap();
    let stmt = conn
        .prepare("INSERT INTO docs VALUES (2, to_tsvector($1))")
        .unwrap();
    stmt.query_collect(&[Value::Text("dogs running fast".into())])
        .unwrap();

    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('jump')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}

fn check_index_only_pagination(key_type: &str) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(&format!(
        "CREATE TABLE docs (id {key_type} PRIMARY KEY, body TEXT)"
    ))
    .unwrap();
    conn.execute("CREATE INDEX idx_body ON docs USING fts (body)")
        .unwrap();
    let key = |id: i64| match key_type {
        "INTEGER" => Value::Integer(id),
        "TEXT" => Value::Text(format!("doc-{id}").into()),
        _ => unreachable!(),
    };
    for id in 0..=5 {
        let body = if id == 0 { "fish swims" } else { "fox jumps" };
        conn.execute_params(
            "INSERT INTO docs VALUES ($1, $2)",
            &[key(id), Value::Text(body.into())],
        )
        .unwrap();
    }
    drop(conn);

    let cases: [(&str, &[i64]); 8] = [
        ("ORDER BY id LIMIT 2 OFFSET 1", &[2, 3]),
        ("ORDER BY id LIMIT 1 OFFSET 2", &[3]),
        ("ORDER BY id OFFSET 2", &[3, 4, 5]),
        ("ORDER BY id LIMIT 2 OFFSET 0", &[1, 2]),
        ("ORDER BY id LIMIT 0 OFFSET 1", &[]),
        ("ORDER BY id LIMIT 2 OFFSET 5", &[]),
        ("ORDER BY id OFFSET 6", &[]),
        ("ORDER BY id DESC LIMIT 2 OFFSET 1", &[4, 3]),
    ];
    for query in ["fox", "fox & jump", "fox <-> jump"] {
        for (page, ids) in cases {
            let sql = format!("SELECT id FROM docs WHERE body @@ to_tsquery('{query}') {page}");
            let expected: Vec<_> = ids.iter().map(|&id| vec![key(id)]).collect();
            for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
                for prepared in [false, true] {
                    let conn = Connection::open(&db).unwrap();
                    if let Some(begin) = begin {
                        conn.execute(begin).unwrap();
                    }
                    let rows = if prepared {
                        conn.prepare(&sql).unwrap().query_collect(&[]).unwrap().rows
                    } else {
                        conn.query(&sql).unwrap().rows
                    };
                    assert_eq!(rows, expected, "{sql}; {begin:?}; prepared={prepared}");
                    if begin.is_some() {
                        conn.execute("ROLLBACK").unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn fts_integer_index_only_results_apply_offset_before_limit() {
    check_index_only_pagination("INTEGER");
}

#[test]
fn fts_text_index_only_results_apply_offset_before_limit() {
    check_index_only_pagination("TEXT");
}
