use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn conn() -> (tempfile::TempDir, citadel::Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    (dir, db)
}

#[test]
fn english_tokenizer_stems_running_to_run() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT ts_lexize('english', 'running')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Text("run".into()));
}

#[test]
fn english_stop_word_returns_null() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT ts_lexize('english', 'the')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Null);
}

#[test]
fn simple_tokenizer_lowercases_no_stem() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT ts_lexize('simple', 'Running')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Text("running".into()));
}

#[test]
fn to_tsvector_strips_stop_words() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let v_rows = conn
        .prepare("SELECT to_tsvector('the quick brown fox')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let formatted = format!("{}", v_rows.rows[0][0]);
    // "the" is a stop-word; "quick brown fox" stem to quick/brown/fox
    assert!(
        !formatted.contains("'the'"),
        "stop word leaked: {formatted}"
    );
    assert!(formatted.contains("'quick'"), "missing quick: {formatted}");
}

#[test]
fn phraseto_tsquery_preserves_stop_word_gap() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    // "cat the dog" — middle is stop-word; phrase should be 'cat' <2> 'dog'
    let q_rows = conn
        .prepare("SELECT phraseto_tsquery('english', 'cat the dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let formatted = format!("{}", q_rows.rows[0][0]);
    assert!(
        formatted.contains("<2>"),
        "expected gap of 2 across stop-word, got: {formatted}"
    );
}

#[test]
fn websearch_quoted_phrase_and_or_and_negation() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT websearch_to_tsquery('\"hello world\" OR cat -dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let formatted = format!("{}", rows.rows[0][0]);
    // Should contain OR (|) and NOT (!) somewhere.
    assert!(
        formatted.contains('|'),
        "expected OR in websearch output: {formatted}"
    );
    assert!(
        formatted.contains('!') || formatted.contains("!'dog'"),
        "expected negation in websearch output: {formatted}"
    );
}

#[test]
fn to_tsvector_handles_unicode_nfkc() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    // U+FB01 LATIN SMALL LIGATURE FI → "fi" under NFKC
    let rows = conn
        .prepare("SELECT to_tsvector('\u{FB01}le')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let formatted = format!("{}", rows.rows[0][0]);
    assert!(
        formatted.contains("file") || formatted.contains("fi"),
        "expected fi-ligature to fold via NFKC, got: {formatted}"
    );
}

#[test]
fn empty_input_to_tsvector_returns_empty() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT length(to_tsvector(''))")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Integer(0));
}

#[test]
fn stop_word_only_input_to_plainto_errors() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let res = conn
        .prepare("SELECT plainto_tsquery('the a an')")
        .unwrap()
        .query_collect(&[]);
    assert!(res.is_err(), "expected empty-query error");
}

#[test]
fn at_at_text_to_tsquery_works() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT to_tsvector('a quick brown fox jumps') @@ to_tsquery('jump')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    // 'jumps' stems to 'jump' which matches the query's 'jump'.
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn unknown_config_name_errors() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let res = conn
        .prepare("SELECT to_tsvector('klingon', 'taH')")
        .unwrap()
        .query_collect(&[]);
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("unknown text search configuration"),
        "expected unknown-config error, got: {err}"
    );
}

fn create_docs_table(c: &Connection<'_>) {
    c.execute(
        "CREATE TABLE docs (\
            id INTEGER NOT NULL PRIMARY KEY, \
            body TSVECTOR)",
    )
    .unwrap();
    c.execute("CREATE INDEX docs_body ON docs USING fts (body)")
        .unwrap();
}

fn count_match(c: &Connection<'_>, query: &str) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM docs WHERE body @@ to_tsquery({})",
        query
    );
    let r = c.prepare(&sql).unwrap().query_collect(&[]).unwrap();
    match r.rows[0][0] {
        Value::Integer(n) => n,
        ref v => panic!("expected integer count, got {v:?}"),
    }
}

#[test]
fn bulk_insert_all_findable_via_inverted_index() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    for i in 0..500i64 {
        let text = if i == 42 {
            "alpha beta gamma needle delta".to_string()
        } else {
            "alpha beta gamma delta".to_string()
        };
        conn.execute_params(
            "INSERT INTO docs (id, body) VALUES ($1, to_tsvector($2))",
            &[Value::Integer(i), Value::Text(text.into())],
        )
        .unwrap();
    }
    assert_eq!(count_match(&conn, "'alpha'"), 500);
    assert_eq!(count_match(&conn, "'needle'"), 1);
    assert_eq!(count_match(&conn, "'nonexistent'"), 0);
}

#[test]
fn update_rewrites_inverted_index_entries() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('old quick brown'))")
        .unwrap();
    assert_eq!(count_match(&conn, "'quick'"), 1);
    conn.execute("UPDATE docs SET body = to_tsvector('fresh shiny new') WHERE id = 1")
        .unwrap();
    assert_eq!(count_match(&conn, "'quick'"), 0);
    assert_eq!(count_match(&conn, "'shiny'"), 1);
}

#[test]
fn delete_clears_inverted_index_entries() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute(
        "INSERT INTO docs (id, body) VALUES \
         (1, to_tsvector('apple banana')), \
         (2, to_tsvector('apple cherry'))",
    )
    .unwrap();
    assert_eq!(count_match(&conn, "'apple'"), 2);
    conn.execute("DELETE FROM docs WHERE id = 1").unwrap();
    assert_eq!(count_match(&conn, "'apple'"), 1);
    assert_eq!(count_match(&conn, "'banana'"), 0);
    assert_eq!(count_match(&conn, "'cherry'"), 1);
}

#[test]
fn mixed_dml_in_single_txn_leaves_index_consistent() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('alpha bravo'))")
        .unwrap();
    conn.execute("INSERT INTO docs (id, body) VALUES (2, to_tsvector('charlie delta'))")
        .unwrap();
    conn.execute("UPDATE docs SET body = to_tsvector('echo foxtrot') WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 2").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count_match(&conn, "'alpha'"), 0);
    assert_eq!(count_match(&conn, "'echo'"), 1);
    assert_eq!(count_match(&conn, "'charlie'"), 0);
}

#[test]
fn rollback_leaves_no_stray_index_entries() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('phantom marker'))")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(count_match(&conn, "'phantom'"), 0);
    assert_eq!(count_match(&conn, "'marker'"), 0);
}

#[test]
fn persistence_across_connection_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        create_docs_table(&conn);
        conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('persist me'))")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    assert_eq!(count_match(&conn, "'persist'"), 1);
}

#[test]
fn replace_same_key_in_one_txn() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('first iteration'))")
        .unwrap();
    conn.execute("UPDATE docs SET body = to_tsvector('second iteration') WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE docs SET body = to_tsvector('third iteration') WHERE id = 1")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(count_match(&conn, "'first'"), 0);
    assert_eq!(count_match(&conn, "'second'"), 0);
    assert_eq!(count_match(&conn, "'third'"), 1);
}

#[test]
fn drop_table_purges_index_entries() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('throwaway'))")
        .unwrap();
    conn.execute("DROP TABLE docs").unwrap();
    create_docs_table(&conn);
    assert_eq!(count_match(&conn, "'throwaway'"), 0);
}

#[test]
fn phrase_query_on_overflowed_tsvector_errors() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let many = "word ".repeat(20_000);
    let res = conn
        .prepare(&format!(
            "SELECT to_tsvector('{}') @@ to_tsquery('word <-> word')",
            many.trim()
        ))
        .unwrap()
        .query_collect(&[]);
    assert!(res.is_err(), "expected phrase-overflow error");
}

#[test]
fn nonphrase_query_on_overflowed_tsvector_still_works() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let many = "word ".repeat(20_000);
    let rows = conn
        .prepare(&format!(
            "SELECT to_tsvector('{}') @@ to_tsquery('word')",
            many.trim()
        ))
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn phrase_distance_larger_than_doc_yields_no_match() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT to_tsvector('alpha quick brown fox jumped') \
                @@ to_tsquery('alpha <50> jumped')",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(false));
}

#[test]
fn three_term_phrase_matches_contiguous() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT to_tsvector('alpha beta gamma delta') \
                @@ to_tsquery('alpha <-> beta <-> gamma')",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn phrase_across_stop_word_gap_matches() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT to_tsvector('english', 'cat the dog') \
                @@ phraseto_tsquery('english', 'cat the dog')",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn english_and_simple_configs_produce_different_vectors() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let eng = conn
        .prepare("SELECT length(to_tsvector('english', 'the running cats jumped'))")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let simple = conn
        .prepare("SELECT length(to_tsvector('simple', 'the running cats jumped'))")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_ne!(eng.rows[0][0], simple.rows[0][0]);
}

#[test]
fn mixed_config_rows_in_same_column() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute(
        "INSERT INTO docs (id, body) VALUES \
         (1, to_tsvector('english', 'the running cats')), \
         (2, to_tsvector('simple', 'the running cats'))",
    )
    .unwrap();
    let eng = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('english', 'run')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(eng.rows.len(), 1);
    assert_eq!(eng.rows[0][0], Value::Integer(1));
    let simple = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('simple', 'the')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(simple.rows.len(), 1);
    assert_eq!(simple.rows[0][0], Value::Integer(2));
}

#[test]
fn ts_rank_norm_bit_zero_returns_raw_sum() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT ts_rank(to_tsvector('english', 'cat cat cat dog'), \
                to_tsquery('cat'), 0)",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let v = match &rows.rows[0][0] {
        Value::Real(r) => *r,
        _ => panic!("expected real"),
    };
    assert!(v > 0.0);
}

#[test]
fn ts_rank_norm_bit_32_squashes_to_unit_interval() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT ts_rank(to_tsvector('english', 'cat cat cat dog'), \
                to_tsquery('cat'), 32)",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let v = match &rows.rows[0][0] {
        Value::Real(r) => *r,
        _ => panic!("expected real"),
    };
    assert!((0.0..1.0).contains(&v), "expected [0,1), got {v}");
}

#[test]
fn ts_rank_no_match_is_zero() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT ts_rank(to_tsvector('english', 'cat dog'), \
                to_tsquery('elephant'))",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let v = match &rows.rows[0][0] {
        Value::Real(r) => *r,
        _ => panic!("expected real"),
    };
    assert_eq!(v, 0.0);
}

#[test]
fn ts_rank_cd_returns_higher_for_tighter_window() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let tight = conn
        .prepare(
            "SELECT ts_rank_cd(to_tsvector('english', 'foo bar baz'), \
                to_tsquery('foo & baz'))",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let wide = conn
        .prepare(
            "SELECT ts_rank_cd(to_tsvector('english', \
                'foo a b c d e f g h i j baz'), to_tsquery('foo & baz'))",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let to_f64 = |v: &Value| match v {
        Value::Real(r) => *r,
        _ => panic!("expected real"),
    };
    let t = to_f64(&tight.rows[0][0]);
    let w = to_f64(&wide.rows[0][0]);
    assert!(t > w, "tight={t} should outrank wide={w}");
}

#[test]
fn second_connection_reads_committed_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let w = Connection::open(&db).unwrap();
    create_docs_table(&w);
    w.execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('committedmarker'))")
        .unwrap();
    let r = Connection::open(&db).unwrap();
    let rows = r
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('committedmarker')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
}

#[test]
fn second_connection_sees_writes_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let setup = Connection::open(&db).unwrap();
    create_docs_table(&setup);
    setup
        .execute("INSERT INTO docs (id, body) VALUES (1, to_tsvector('firstmarker'))")
        .unwrap();
    let reader = Connection::open(&db).unwrap();
    let before = count_match(&reader, "'secondmarker'");
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("INSERT INTO docs (id, body) VALUES (2, to_tsvector('secondmarker'))")
        .unwrap();
    let after = count_match(&reader, "'secondmarker'");
    assert_eq!(before, 0);
    assert_eq!(after, 1);
}

#[test]
fn corpus_10k_smoke_finds_planted_term() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute("BEGIN").unwrap();
    for i in 0..10_000i64 {
        let text = if i == 7777 {
            "filler filler uniqueplanted filler".to_string()
        } else {
            "filler filler docfiller filler".to_string()
        };
        conn.execute_params(
            "INSERT INTO docs (id, body) VALUES ($1, to_tsvector($2))",
            &[Value::Integer(i), Value::Text(text.into())],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('uniqueplanted')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(7777));
}

#[test]
fn weight_filter_in_query_respects_setweight() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let with_a = conn
        .prepare("SELECT setweight(to_tsvector('english', 'apple'), 'A') @@ to_tsquery('apple:A')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let with_b_query = conn
        .prepare("SELECT setweight(to_tsvector('english', 'apple'), 'A') @@ to_tsquery('apple:B')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(with_a.rows[0][0], Value::Boolean(true));
    assert_eq!(with_b_query.rows[0][0], Value::Boolean(false));
}

#[test]
fn prefix_query_matches_stemmed_prefix() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT to_tsvector('running runner runs') @@ to_tsquery('run:*')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn or_branch_matches_when_one_side_present() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare("SELECT to_tsvector('only cat here') @@ to_tsquery('elephant | cat')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Boolean(true));
}

#[test]
fn not_branch_excludes_documents_containing_term() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    create_docs_table(&conn);
    conn.execute(
        "INSERT INTO docs (id, body) VALUES \
         (1, to_tsvector('cat only')), \
         (2, to_tsvector('cat and dog'))",
    )
    .unwrap();
    let rows = conn
        .prepare("SELECT id FROM docs WHERE body @@ to_tsquery('cat & !dog')")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Integer(1));
}

#[test]
fn empty_websearch_input_returns_empty_query() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let res = conn
        .prepare("SELECT websearch_to_tsquery('english', '')")
        .unwrap()
        .query_collect(&[]);
    if let Ok(r) = res {
        let formatted = format!("{}", r.rows[0][0]);
        assert!(formatted.is_empty() || formatted == "''");
    }
}

#[test]
fn ts_headline_wraps_matched_terms() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let rows = conn
        .prepare(
            "SELECT ts_headline('english', 'the quick brown fox', \
                to_tsquery('english', 'quick'))",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let v = match &rows.rows[0][0] {
        Value::Text(s) => s.to_string(),
        _ => panic!("expected text"),
    };
    assert!(v.contains("<b>") && v.contains("quick"), "got: {v}");
}

fn tsvector_display(c: &Connection<'_>, sql: &str) -> String {
    let r = c.prepare(sql).unwrap().query_collect(&[]).unwrap();
    format!("{}", r.rows[0][0])
}

#[test]
fn setweight_promotes_default_d_to_a() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let weighted = tsvector_display(
        &conn,
        "SELECT setweight(to_tsvector('english', 'apple'), 'A')",
    );
    assert!(
        weighted.contains(":1A"),
        "expected :1A annotation, got: {weighted}"
    );
}

#[test]
fn setweight_lowercase_char_accepted() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let weighted = tsvector_display(
        &conn,
        "SELECT setweight(to_tsvector('english', 'apple'), 'b')",
    );
    assert!(weighted.contains('B'), "expected B in: {weighted}");
}

#[test]
fn setweight_invalid_char_errors() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .prepare("SELECT setweight(to_tsvector('english', 'apple'), 'Z')")
        .unwrap()
        .query_collect(&[])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unrecognized weight"),
        "expected unrecognized-weight error, got: {err}"
    );
}

#[test]
fn setweight_three_arg_form_deferred() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let res = conn.prepare(
        "SELECT setweight(to_tsvector('english', 'apple banana'), 'A', \
            ARRAY['apple'])",
    );
    let err = match res {
        Err(e) => e.to_string(),
        Ok(p) => p.query_collect(&[]).unwrap_err().to_string(),
    };
    assert!(
        err.contains("Unsupported") || err.contains("ARRAY") || err.contains("3-arg"),
        "expected deferred-feature error, got: {err}"
    );
}

#[test]
fn weight_label_d_omitted_from_tsvector_display() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let plain = tsvector_display(&conn, "SELECT to_tsvector('english', 'apple')");
    assert!(
        !plain.contains(":1D") && !plain.contains(":2D"),
        "D should be invisible, got: {plain}"
    );
    let weighted = tsvector_display(
        &conn,
        "SELECT setweight(to_tsvector('english', 'apple'), 'A')",
    );
    assert!(
        weighted.contains('A'),
        "A should be visible, got: {weighted}"
    );
}

#[test]
fn setweight_then_ts_rank_changes_score() {
    let (_d, db) = conn();
    let conn = Connection::open(&db).unwrap();
    let to_f64 = |v: &Value| match v {
        Value::Real(r) => *r,
        _ => panic!("expected real"),
    };
    let with_a = conn
        .prepare(
            "SELECT ts_rank(setweight(to_tsvector('english', 'apple'), 'A'), \
                to_tsquery('apple'))",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let with_d = conn
        .prepare("SELECT ts_rank(to_tsvector('english', 'apple'), to_tsquery('apple'))")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    let a_score = to_f64(&with_a.rows[0][0]);
    let d_score = to_f64(&with_d.rows[0][0]);
    assert!(
        a_score > d_score,
        "weight A score ({a_score}) should outrank weight D ({d_score})"
    );
}
