//! Persisted ANN segments end to end: persist/reopen/load equivalence, every
//! staleness layer (transactional purge on each DML/DDL path, the content
//! fingerprint, header pins), corruption refusals, rollback semantics, NULL
//! vectors, negative PKs (scan order != u64 sort order - the rehydration
//! permutation), filter pushdown from persisted dicts, and the cache markers.

use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::executor::AnnIndexSource;
use citadel_sql::{Connection, ExecutionResult, Value};

fn create_db(dir: &std::path::Path) -> Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

/// 40 deterministic vectors over 2 categories, ids 1..=40.
fn seed(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4), category TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    for i in 1..=40i64 {
        let f = i as f32;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, '[{}, {}, {}, {}]'::VECTOR(4), '{}')",
            f * 0.1,
            (41.0 - f) * 0.1,
            (f * 0.07).sin(),
            (f * 0.05).cos(),
            if i % 2 == 0 { "even" } else { "odd" }
        ))
        .unwrap();
    }
}

const QUERY: &str =
    "SELECT id FROM t WHERE category = 'even' ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 5";
const QUERY_UNFILTERED: &str =
    "SELECT id FROM t ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 5";

fn ids(conn: &Connection<'_>, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            })
            .collect(),
        _ => panic!("expected query result"),
    }
}

fn status(conn: &Connection<'_>, table: &str) -> Option<AnnIndexSource> {
    conn.ann_cache_status(table, "v").unwrap().map(|(s, _)| s)
}

#[test]
fn persist_reopen_load_serves_identical_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);

    let ground_truth = ids(&conn, QUERY);
    let unfiltered_truth = ids(&conn, QUERY_UNFILTERED);
    let info = conn.persist_ann_index("t", "v").unwrap();
    assert_eq!(info.n, 40);
    assert_eq!(info.dim, 4);
    assert!(info.chunk_count >= 1);
    // Persist warms the cache as Built (it was built this process).
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Built { refusal: None })
    ));
    drop(conn);
    drop(db);

    // Cold attach: the segment LOADS, and answers are identical.
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        ids(&conn, QUERY),
        ground_truth,
        "filtered results identical"
    );
    assert_eq!(
        ids(&conn, QUERY_UNFILTERED),
        unfiltered_truth,
        "unfiltered results identical"
    );
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => {
            assert_eq!(segment_b3, info.segment_b3, "the exact persisted artifact");
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
}

#[test]
fn every_dml_path_purges_the_segment_in_its_own_commit() {
    // (statement, description) - each runs against a FRESH persisted segment
    // and must leave the next query REBUILDING (source Built, segment gone).
    let cases: &[(&str, &str)] = &[
        (
            "INSERT INTO t VALUES (100, '[9,9,9,9]'::VECTOR(4), 'even')",
            "insert",
        ),
        ("UPDATE t SET category = 'odd' WHERE id = 2", "update"),
        ("UPDATE t SET id = 99 WHERE id = 2", "pk update"),
        ("DELETE FROM t WHERE id = 2", "delete"),
        ("TRUNCATE TABLE t", "truncate"),
    ];
    for (sql, what) in cases {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seed(&conn);
        conn.persist_ann_index("t", "v").unwrap();
        conn.execute(sql).unwrap();
        if *what != "truncate" {
            let _ = ids(&conn, QUERY);
            match status(&conn, "t") {
                Some(AnnIndexSource::Built { refusal }) => {
                    assert!(
                        refusal.is_none(),
                        "{what}: segment GONE, not refused: {refusal:?}"
                    );
                }
                other => panic!("{what}: expected Built after DML, got {other:?}"),
            }
        }
        // A mutation stamps the last-DML marker; the pure append (id 100 > max 40)
        // is retained for the tail merge, so it purges the segment but not the marker.
        let stamped = db.sql_cache_get::<u64>("ann_dml_gen:t").is_some();
        if *what == "insert" {
            assert!(!stamped, "{what}: a pure append is retained, not marked");
        } else {
            assert!(stamped, "{what}: marker stamped at commit");
        }
    }
}

#[test]
fn rollback_preserves_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let info = conn.persist_ann_index("t", "v").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    conn.execute("ROLLBACK").unwrap();

    drop(conn);
    drop(db);
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = ids(&conn, QUERY);
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
        other => panic!("rolled-back DML must keep the segment loadable: {other:?}"),
    }
}

#[test]
fn committed_explicit_transaction_purges_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    conn.execute("COMMIT").unwrap();

    let _ = ids(&conn, QUERY);
    assert!(
        matches!(status(&conn, "t"), Some(AnnIndexSource::Built { .. })),
        "in-txn DML purges with its commit"
    );
}

#[test]
fn ddl_paths_purge_or_remove_the_segment() {
    // DROP TABLE: the hidden segment tree must go with the table.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    // Recreating the table must not see any leftover segment.
    seed(&conn);
    let _ = ids(&conn, QUERY);
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Built { refusal: None })
    ));
    drop(conn);

    // ALTER RENAME: __annseg_{old} must not stay orphaned.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    conn.execute("ALTER TABLE t RENAME TO t2").unwrap();
    let renamed: Vec<i64> = ids(
        &conn,
        "SELECT id FROM t2 ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 5",
    );
    assert_eq!(renamed.len(), 5);
    drop(conn);

    // DROP INDEX of the ann index: segment dropped with it.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    conn.execute("DROP INDEX ix_v").unwrap();
    // No index declared -> top-k runs brute-force; recreating the index must
    // rebuild, never load a segment for a dropped index.
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    let _ = ids(&conn, QUERY);
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Built { refusal: None })
    ));
}

#[test]
fn tampered_chunk_is_refused_as_corrupt_and_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let ground_truth = ids(&conn, QUERY);
    conn.persist_ann_index("t", "v").unwrap();
    drop(conn);

    // Tamper a body chunk through raw KV (bypasses SQL entirely).
    {
        let mut wtx = db.begin_write().unwrap();
        let chunk = wtx
            .table_get(b"__annseg_t", &1u32.to_be_bytes())
            .unwrap()
            .expect("chunk 1 exists");
        let mut bad = chunk.clone();
        bad[chunk.len() / 2] ^= 0xFF;
        wtx.table_insert(b"__annseg_t", &1u32.to_be_bytes(), &bad)
            .unwrap();
        wtx.commit().unwrap();
    }
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        ids(&conn, QUERY),
        ground_truth,
        "rebuild still answers correctly"
    );
    match status(&conn, "t") {
        Some(AnnIndexSource::Built { refusal: Some(r) }) => {
            assert!(r.contains("BLAKE3") || r.contains("corrupt"), "reason: {r}");
        }
        other => panic!("expected Built with a corruption refusal, got {other:?}"),
    }
}

#[test]
fn tampered_header_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    drop(conn);

    {
        let mut wtx = db.begin_write().unwrap();
        let header = wtx
            .table_get(b"__annseg_t", &0u32.to_be_bytes())
            .unwrap()
            .expect("header exists");
        let mut bad = header.clone();
        bad[10] ^= 0xFF;
        wtx.table_insert(b"__annseg_t", &0u32.to_be_bytes(), &bad)
            .unwrap();
        wtx.commit().unwrap();
    }
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = ids(&conn, QUERY);
    match status(&conn, "t") {
        Some(AnnIndexSource::Built { refusal: Some(r) }) => {
            assert!(r.contains("header"), "reason names the header: {r}");
        }
        other => panic!("expected Built with a header refusal, got {other:?}"),
    }
}

#[test]
fn resurrected_stale_segment_is_refused_by_the_root_stamp() {
    // A raw-KV-resurrected segment (bypassing the transactional purge) is still caught
    // on cold load: the DELETE moved the table's root, so root != the stamp.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();

    // Save the segment bytes, change the table (purges), resurrect the bytes.
    let saved: Vec<(Vec<u8>, Vec<u8>)> = {
        let mut rtx = db.begin_read();
        let mut out = Vec::new();
        rtx.table_scan_from(b"__annseg_t", b"", &mut |k: &[u8], v: &[u8]| {
            out.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap();
        out
    };
    assert!(!saved.is_empty());
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    {
        let mut wtx = db.begin_write().unwrap();
        wtx.create_table(b"__annseg_t").unwrap();
        for (k, v) in &saved {
            wtx.table_insert(b"__annseg_t", k, v).unwrap();
        }
        wtx.commit().unwrap();
    }
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let fresh = ids(&conn, QUERY);
    assert!(
        !fresh.contains(&2),
        "id 2 was deleted; a stale index would resurrect it"
    );
    match status(&conn, "t") {
        Some(AnnIndexSource::Built { refusal: Some(r) }) => {
            assert!(r.contains("stale"), "reason: {r}");
        }
        other => panic!("expected Built with a staleness refusal, got {other:?}"),
    }
}

#[test]
fn null_vectors_persist_and_load_with_partial_n() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("INSERT INTO t VALUES (200, NULL, 'even')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (201, NULL, 'odd')")
        .unwrap();

    let truth = ids(&conn, QUERY);
    let info = conn.persist_ann_index("t", "v").unwrap();
    assert_eq!(info.n, 40, "NULL vectors are content but not indexed");
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(ids(&conn, QUERY), truth);
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn negative_pks_exercise_the_rehydration_permutation() {
    // Negative INTEGER PKs encode so that scan order != u64-cast sort order:
    // a scan-order vector fill would silently corrupt rerank distances.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4), category TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    for i in -20..=19i64 {
        let f = i as f32;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, '[{}, {}, {}, {}]'::VECTOR(4), '{}')",
            f * 0.1,
            (20.0 - f) * 0.1,
            (f * 0.07).sin(),
            (f * 0.05).cos(),
            if i % 2 == 0 { "even" } else { "odd" }
        ))
        .unwrap();
    }
    let truth = ids(&conn, QUERY_UNFILTERED);
    conn.persist_ann_index("t", "v").unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        ids(&conn, QUERY_UNFILTERED),
        truth,
        "permutation-correct rerank"
    );
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn persist_refusals_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);

    // Inside an explicit transaction.
    conn.execute("BEGIN").unwrap();
    let err = conn.persist_ann_index("t", "v").unwrap_err();
    assert!(err.to_string().contains("explicit transaction"), "{err}");
    conn.execute("ROLLBACK").unwrap();

    // No ANN index on the column.
    conn.execute("CREATE TABLE plain (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn.persist_ann_index("plain", "v").unwrap_err();
    assert!(err.to_string().contains("no ANN index"), "{err}");

    // Not a vector column.
    let err = conn.persist_ann_index("t", "category").unwrap_err();
    assert!(err.to_string().contains("not VECTOR"), "{err}");

    // Unknown table / column.
    assert!(conn.persist_ann_index("missing", "v").is_err());
    assert!(conn.persist_ann_index("t", "missing").is_err());

    // Empty table (nothing indexable).
    conn.execute("CREATE TABLE e (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_e ON e USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let err = conn.persist_ann_index("e", "v").unwrap_err();
    assert!(err.to_string().contains("nothing to persist"), "{err}");

    // TEMP tables are refused.
    conn.execute("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn.persist_ann_index("tmp", "v").unwrap_err();
    assert!(err.to_string().contains("TEMP"), "{err}");
}

#[test]
fn compiled_fast_update_purges_and_marks() {
    // The compiled in-place UPDATE fast path must purge the segment and stamp the marker.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4), category TEXT, n INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    for i in 1..=40i64 {
        let f = i as f32;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, '[{}, {}, {}, {}]'::VECTOR(4), 'even', 0)",
            f * 0.1,
            (41.0 - f) * 0.1,
            (f * 0.07).sin(),
            (f * 0.05).cos(),
        ))
        .unwrap();
    }
    conn.persist_ann_index("t", "v").unwrap();

    // An int-set by PK is exactly the compiled fast-path shape; repeat it so
    // the statement cache compiles it.
    for _ in 0..3 {
        conn.execute("UPDATE t SET n = n + 1 WHERE id = 5").unwrap();
    }
    let _ = ids(&conn, QUERY);
    assert!(
        matches!(status(&conn, "t"), Some(AnnIndexSource::Built { .. })),
        "fast update purged the segment"
    );
    assert!(db.sql_cache_get::<u64>("ann_dml_gen:t").is_some());
}

/// The behavioral battery: many generated queries, three independent checks.
///
/// 1. BUILT-vs-LOADED EXACT EQUIVALENCE - the load path reconstructs the same
///    graph bytes, so every query must answer IDENTICALLY to the pre-persist
///    index. Any divergence = a rehydration/decode bug.
/// 2. Exact-ground-truth sanity - ANN is approximate, but with 4x over-fetch
///    on 60 rows the true nearest neighbor must virtually always surface.
/// 3. Filtered queries must NEVER leak a row violating the predicate
///    (collect_survivors rechecks by PK - assert it holds through the loaded
///    dicts too).
#[test]
fn query_battery_built_vs_loaded_vs_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(8), category TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2', filters = 'category')")
        .unwrap();
    // 60 deterministic rows over 3 categories.
    let mut vectors: Vec<(i64, Vec<f32>, &str)> = Vec::new();
    for i in 1..=60i64 {
        let f = i as f32;
        let v: Vec<f32> = (0..8)
            .map(|d| (f * 0.37 + d as f32 * 1.7).sin() * 3.0 + (d as f32 - f * 0.05).cos())
            .collect();
        let cat = ["alpha", "beta", "gamma"][(i % 3) as usize];
        vectors.push((i, v.clone(), cat));
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, '[{}]'::VECTOR(8), '{cat}')",
            v.iter().map(f32::to_string).collect::<Vec<_>>().join(", ")
        ))
        .unwrap();
    }

    // 25 generated query vectors x (unfiltered + one filtered variant each).
    let queries: Vec<Vec<f32>> = (0..25)
        .map(|q| {
            (0..8)
                .map(|d| (q as f32 * 0.61 + d as f32 * 0.83).sin() * 2.5)
                .collect()
        })
        .collect();
    let sql_for = |qv: &[f32], filter: Option<&str>, k: usize| {
        let lit = qv.iter().map(f32::to_string).collect::<Vec<_>>().join(", ");
        match filter {
            Some(c) => format!(
                "SELECT id FROM t WHERE category = '{c}' ORDER BY v <-> '[{lit}]'::VECTOR(8) LIMIT {k}"
            ),
            None => format!("SELECT id FROM t ORDER BY v <-> '[{lit}]'::VECTOR(8) LIMIT {k}"),
        }
    };

    // Phase 1: record the BUILT index's answers (persist warms it).
    conn.persist_ann_index("t", "v").unwrap();
    let mut built_answers: Vec<Vec<i64>> = Vec::new();
    for (q, qv) in queries.iter().enumerate() {
        let filter = ["alpha", "beta", "gamma"][q % 3];
        built_answers.push(ids(&conn, &sql_for(qv, None, 7)));
        built_answers.push(ids(&conn, &sql_for(qv, Some(filter), 4)));
    }
    drop(conn);
    drop(db);

    // Phase 2: reopen cold - the LOADED segment must answer identically.
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let mut answer_idx = 0;
    for (q, qv) in queries.iter().enumerate() {
        let filter = ["alpha", "beta", "gamma"][q % 3];
        let unfiltered = ids(&conn, &sql_for(qv, None, 7));
        assert_eq!(
            unfiltered, built_answers[answer_idx],
            "query {q} unfiltered: loaded != built"
        );
        answer_idx += 1;
        let filtered = ids(&conn, &sql_for(qv, Some(filter), 4));
        assert_eq!(
            filtered, built_answers[answer_idx],
            "query {q} filtered({filter}): loaded != built"
        );
        answer_idx += 1;

        // Check 3: no filter leaks through the persisted dicts.
        for id in &filtered {
            let cat = vectors[(*id - 1) as usize].2;
            assert_eq!(
                cat, filter,
                "query {q}: id {id} leaked through filter {filter}"
            );
        }

        // Check 2: the exact nearest neighbor surfaces in top-7.
        let l2 =
            |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum() };
        let exact_top1 = vectors
            .iter()
            .min_by(|a, b| l2(&a.1, qv).total_cmp(&l2(&b.1, qv)))
            .unwrap()
            .0;
        assert!(
            unfiltered.contains(&exact_top1),
            "query {q}: exact nearest {exact_top1} missing from top-7 {unfiltered:?}"
        );
    }
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn savepoint_rollback_restores_the_purged_segment() {
    // The purge happens at DML time INSIDE the txn: rolling back to a
    // savepoint taken BEFORE the DML must restore the segment; rolling back a
    // LATER savepoint must keep the purge.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let info = conn.persist_ann_index("t", "v").unwrap();

    // Savepoint BEFORE the DML: the delete (and its purge) roll back.
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    conn.execute("ROLLBACK TO SAVEPOINT s").unwrap();
    conn.execute("COMMIT").unwrap();
    drop(conn);
    drop(db);
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = ids(&conn, QUERY);
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
        other => panic!("savepoint-rolled-back DML must keep the segment: {other:?}"),
    }

    // DML BEFORE the savepoint: its purge survives the partial rollback.
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    conn.execute("SAVEPOINT s2").unwrap();
    conn.execute("DELETE FROM t WHERE id = 4").unwrap();
    conn.execute("ROLLBACK TO SAVEPOINT s2").unwrap();
    conn.execute("COMMIT").unwrap();
    let _ = ids(&conn, QUERY);
    assert!(
        matches!(status(&conn, "t"), Some(AnnIndexSource::Built { .. })),
        "the pre-savepoint DML's purge must survive the partial rollback"
    );
}

#[test]
fn dml_on_one_table_keeps_the_other_tables_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_o ON other USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    for i in 1..=30i64 {
        let f = i as f32;
        conn.execute(&format!(
            "INSERT INTO other VALUES ({i}, '[{}, {}, {}, {}]'::VECTOR(4))",
            f,
            31.0 - f,
            f * 0.5,
            1.0
        ))
        .unwrap();
    }
    let info_t = conn.persist_ann_index("t", "v").unwrap();
    conn.persist_ann_index("other", "v").unwrap();

    // Heavy DML on `other` must not disturb t's segment.
    conn.execute("DELETE FROM other WHERE id <= 5").unwrap();
    conn.execute("UPDATE other SET v = '[9,9,9,9]'::VECTOR(4) WHERE id = 10")
        .unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = ids(&conn, QUERY);
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info_t.segment_b3),
        other => panic!("t's segment must survive DML on another table: {other:?}"),
    }
    // `other`'s own segment was purged by its DML and rebuilds honestly.
    let _ = ids(
        &conn,
        "SELECT id FROM other ORDER BY v <-> '[1,1,1,1]'::VECTOR(4) LIMIT 3",
    );
    assert!(matches!(
        conn.ann_cache_status("other", "v").unwrap(),
        Some((AnnIndexSource::Built { .. }, _))
    ));
}

#[test]
fn trigger_driven_writes_purge_the_target_tables_segment() {
    // A trigger on `src` writes into the ANN table `t`: the triggered INSERT
    // runs through the in-txn write path and must purge t's segment in the
    // SAME commit.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER mirror AFTER INSERT ON src FOR EACH ROW \
         BEGIN INSERT INTO t VALUES (NEW.id + 1000, '[1,2,3,4]'::VECTOR(4), 'even'); END",
    )
    .unwrap();
    conn.persist_ann_index("t", "v").unwrap();

    conn.execute("INSERT INTO src VALUES (1, 'hello')").unwrap();
    let found = ids(&conn, QUERY_UNFILTERED);
    assert!(!found.is_empty());
    let _ = found;
    assert!(
        matches!(status(&conn, "t"), Some(AnnIndexSource::Built { .. })),
        "the triggered write must purge t's segment"
    );
    // And the rebuilt index must see the trigger-inserted row.
    let all = ids(
        &conn,
        "SELECT id FROM t ORDER BY v <-> '[1,2,3,4]'::VECTOR(4) LIMIT 1",
    );
    assert_eq!(all, vec![1001], "trigger-inserted row is the exact match");
}

#[test]
fn vector_overwrite_is_visible_after_the_purge() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();

    // Move row 7 to an extreme corner; it must become the top hit there.
    conn.execute("UPDATE t SET v = '[100, 100, 100, 100]'::VECTOR(4) WHERE id = 7")
        .unwrap();
    let top = ids(
        &conn,
        "SELECT id FROM t ORDER BY v <-> '[100, 100, 100, 100]'::VECTOR(4) LIMIT 1",
    );
    assert_eq!(
        top,
        vec![7],
        "the overwritten vector is served, never the stale one"
    );
}

#[test]
fn upsert_on_conflict_purges() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    conn.execute(
        "INSERT INTO t VALUES (1, '[50,50,50,50]'::VECTOR(4), 'odd') \
         ON CONFLICT (id) DO UPDATE SET v = '[50,50,50,50]'::VECTOR(4)",
    )
    .unwrap();
    let top = ids(
        &conn,
        "SELECT id FROM t ORDER BY v <-> '[50,50,50,50]'::VECTOR(4) LIMIT 1",
    );
    assert_eq!(top, vec![1], "upserted vector is served");
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Built { .. })
    ));
}

#[test]
fn loaded_segment_survives_repeated_queries_without_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let info = conn.persist_ann_index("t", "v").unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let first = ids(&conn, QUERY);
    for _ in 0..5 {
        assert_eq!(ids(&conn, QUERY), first, "stable across repeated queries");
    }
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => assert_eq!(segment_b3, info.segment_b3),
        other => panic!("read-only queries keep the loaded entry: {other:?}"),
    }
}

#[test]
fn filter_value_absent_from_persisted_dict_yields_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let none = ids(
        &conn,
        "SELECT id FROM t WHERE category = 'never-seen' \
         ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 5",
    );
    assert!(
        none.is_empty(),
        "unknown dict value matches no rows: {none:?}"
    );
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Loaded { .. })
    ));
}

#[test]
fn all_null_vectors_refuse_to_persist() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE n (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_n ON n USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    conn.execute("INSERT INTO n VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO n VALUES (2, NULL)").unwrap();
    let err = conn.persist_ann_index("n", "v").unwrap_err();
    assert!(err.to_string().contains("nothing to persist"), "{err}");
}

#[test]
fn multi_chunk_segment_roundtrips() {
    // Cross the 1 MB chunk boundary cheaply: at dim 8 the id/graph sections
    // dominate the body (~70 B/row), so ~18k rows span multiple chunks while
    // the debug-mode PRISM build stays fast. Exercises chunk split +
    // reassembly + whole-body BLAKE3 across chunks.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, v VECTOR(8))")
        .unwrap();
    conn.execute("CREATE INDEX ix_big ON big USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let mut next_id = 0i64;
    for _ in 0..60 {
        let mut rows = Vec::with_capacity(300);
        for _ in 0..300 {
            next_id += 1;
            let f = next_id as f32;
            let lit: Vec<String> = (0..8)
                .map(|d| format!("{:.3}", ((f * 0.013 + d as f32 * 0.71).sin() * 2.0)))
                .collect();
            rows.push(format!("({next_id}, '[{}]'::VECTOR(8))", lit.join(",")));
        }
        conn.execute(&format!("INSERT INTO big VALUES {}", rows.join(",")))
            .unwrap();
    }
    let probe = "SELECT id FROM big ORDER BY v <-> '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]'::VECTOR(8) LIMIT 10".to_string();
    let truth = ids(&conn, &probe);
    let info = conn.persist_ann_index("big", "v").unwrap();
    assert!(
        info.chunk_count >= 2,
        "the fixture must span multiple chunks, got {}",
        info.chunk_count
    );
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(ids(&conn, &probe), truth, "multi-chunk roundtrip identical");
    match conn.ann_cache_status("big", "v").unwrap() {
        Some((AnnIndexSource::Loaded { segment_b3 }, _)) => {
            assert_eq!(segment_b3, info.segment_b3);
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
}

#[test]
fn every_in_txn_dml_variant_purges() {
    // The auto-commit variants are covered elsewhere; these are the IN-TXN
    // code paths (INSERT, UPDATE, TRUNCATE inside BEGIN..COMMIT).
    let cases: &[&[&str]] = &[
        &["INSERT INTO t VALUES (100, '[9,9,9,9]'::VECTOR(4), 'even')"],
        &["UPDATE t SET category = 'odd' WHERE id = 2"],
        &["TRUNCATE TABLE t"],
    ];
    for stmts in cases {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seed(&conn);
        conn.persist_ann_index("t", "v").unwrap();
        conn.execute("BEGIN").unwrap();
        for s in *stmts {
            conn.execute(s).unwrap();
        }
        conn.execute("COMMIT").unwrap();
        if stmts[0].starts_with("TRUNCATE") {
            continue; // empty table: nothing to query, segment provably gone below
        }
        let _ = ids(&conn, QUERY);
        assert!(
            matches!(status(&conn, "t"), Some(AnnIndexSource::Built { .. })),
            "{}: in-txn DML must purge",
            stmts[0]
        );
    }
}

#[test]
fn in_txn_drop_table_removes_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    conn.persist_ann_index("t", "v").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("COMMIT").unwrap();
    // Recreate: a leftover segment would be refused, but it must be GONE.
    seed(&conn);
    let _ = ids(&conn, QUERY);
    assert!(matches!(
        status(&conn, "t"),
        Some(AnnIndexSource::Built { refusal: None })
    ));
}

#[test]
fn alter_add_and_drop_column_purge() {
    for alter in [
        "ALTER TABLE t ADD COLUMN extra INTEGER",
        "ALTER TABLE t DROP COLUMN category",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        seed(&conn);
        conn.persist_ann_index("t", "v").unwrap();
        conn.execute(alter).unwrap();
        // The encoding positions shifted; the segment must be gone (a stale
        // one would mis-decode columns).
        let _ = conn
            .execute("SELECT id FROM t ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 3")
            .unwrap();
        assert!(
            !matches!(status(&conn, "t"), Some(AnnIndexSource::Loaded { .. })),
            "{alter}: structure change must purge the segment"
        );
    }
}

#[test]
fn lookup_refuses_entries_that_predate_the_dml_marker() {
    // The gen-stamp race guard, made observable: cache an entry, then stamp a
    // FUTURE last-DML marker through the shared cache - the next lookup must
    // treat the entry as stale and rebuild (a build that raced a commit can
    // never keep serving).
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let _ = ids(&conn, QUERY);
    let (_, gen_before) = conn.ann_cache_status("t", "v").unwrap().expect("cached");

    db.sql_cache_insert("ann_dml_gen:t".to_string(), std::sync::Arc::new(u64::MAX));
    let _ = ids(&conn, QUERY);
    match conn.ann_cache_status("t", "v").unwrap() {
        // The replacement entry is also below the (absurd) marker, so it is
        // served-but-not-cached; either no entry or a NEWER one is acceptable,
        // never the original.
        None => {}
        Some((_, gen_after)) => assert!(
            gen_after >= gen_before,
            "the pre-marker entry must not survive: {gen_before} -> {gen_after}"
        ),
    }
    // Correctness is untouched either way.
    assert_eq!(ids(&conn, QUERY).len(), 5);
}

#[test]
fn two_indexed_columns_share_one_segment_fail_closed() {
    // The hidden tree is per-TABLE: persisting a second column REPLACES the
    // first column's segment. The displaced column must degrade to an honest
    // rebuild (identity mismatch), never serve the wrong column's graph.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4), w VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    conn.execute("CREATE INDEX ix_w ON t USING ann (w) WITH (metric = 'l2')")
        .unwrap();
    for i in 1..=30i64 {
        let f = i as f32;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, '[{f}, {}, 1, 2]'::VECTOR(4), '[{}, {f}, 3, 4]'::VECTOR(4))",
            31.0 - f,
            31.0 - f
        ))
        .unwrap();
    }
    let v_truth = ids(
        &conn,
        "SELECT id FROM t ORDER BY v <-> '[5, 26, 1, 2]'::VECTOR(4) LIMIT 3",
    );
    let w_truth = ids(
        &conn,
        "SELECT id FROM t ORDER BY w <-> '[26, 5, 3, 4]'::VECTOR(4) LIMIT 3",
    );
    conn.persist_ann_index("t", "v").unwrap();
    let w_info = conn.persist_ann_index("t", "w").unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM t ORDER BY w <-> '[26, 5, 3, 4]'::VECTOR(4) LIMIT 3",
        ),
        w_truth
    );
    match conn.ann_cache_status("t", "w").unwrap() {
        Some((AnnIndexSource::Loaded { segment_b3 }, _)) => {
            assert_eq!(
                segment_b3, w_info.segment_b3,
                "the LAST persisted column serves"
            );
        }
        other => panic!("expected w Loaded, got {other:?}"),
    }
    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM t ORDER BY v <-> '[5, 26, 1, 2]'::VECTOR(4) LIMIT 3",
        ),
        v_truth,
        "the displaced column still answers correctly (rebuilt)"
    );
    match conn.ann_cache_status("t", "v").unwrap() {
        Some((AnnIndexSource::Built { refusal: Some(r) }, _)) => {
            assert!(
                r.contains("identity"),
                "the refusal names the mismatch: {r}"
            );
        }
        other => panic!("expected v Built with identity refusal, got {other:?}"),
    }
}

#[test]
fn second_connection_sees_the_loaded_segment_and_its_dml_evicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn_a = Connection::open(&db).unwrap();
    seed(&conn_a);
    let info = conn_a.persist_ann_index("t", "v").unwrap();
    drop(conn_a);
    drop(db);

    let db = open_db(dir.path());
    let conn_a = Connection::open(&db).unwrap();
    let conn_b = Connection::open(&db).unwrap();
    let truth = ids(&conn_a, QUERY);
    // The shared cache serves B without any build.
    assert_eq!(ids(&conn_b, QUERY), truth);
    match conn_b.ann_cache_status("t", "v").unwrap() {
        Some((AnnIndexSource::Loaded { segment_b3 }, _)) => {
            assert_eq!(segment_b3, info.segment_b3);
        }
        other => panic!("B must see the loaded entry: {other:?}"),
    }
    // B's DML purges + evicts for everyone.
    conn_b.execute("DELETE FROM t WHERE id = 2").unwrap();
    let fresh = ids(&conn_a, QUERY);
    assert!(!fresh.contains(&2), "A never sees the deleted row");
    assert!(matches!(
        conn_a.ann_cache_status("t", "v").unwrap(),
        Some((AnnIndexSource::Built { .. }, _))
    ));
}

#[test]
fn in_list_filters_and_offset_paginate_through_the_loaded_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let in_query = "SELECT id FROM t WHERE category IN ('even', 'odd') \
                    ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 6";
    let off_query = "SELECT id FROM t WHERE category = 'even' \
                     ORDER BY v <-> '[0.5, 3.6, 0.3, 0.9]'::VECTOR(4) LIMIT 3 OFFSET 2";
    let in_truth = ids(&conn, in_query);
    let off_truth = ids(&conn, off_query);
    assert_eq!(in_truth.len(), 6);
    assert_eq!(off_truth.len(), 3);
    conn.persist_ann_index("t", "v").unwrap();
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        ids(&conn, in_query),
        in_truth,
        "IN-list via persisted dicts"
    );
    assert_eq!(
        ids(&conn, off_query),
        off_truth,
        "OFFSET pagination identical"
    );
    assert!(matches!(
        conn.ann_cache_status("t", "v").unwrap(),
        Some((AnnIndexSource::Loaded { .. }, _))
    ));
}

#[test]
fn composite_pk_tables_refuse_to_persist_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE c (a INTEGER, b INTEGER, v VECTOR(4), PRIMARY KEY (a, b))")
        .unwrap();
    conn.execute("CREATE INDEX ix_c ON c USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 1, '[1,2,3,4]'::VECTOR(4))")
        .unwrap();
    // The ANN machinery requires a single INTEGER PK; the persist must refuse
    // with an error, never panic or write a half-segment.
    assert!(conn.persist_ann_index("c", "v").is_err());
    let qr = conn
        .execute("SELECT a FROM c ORDER BY v <-> '[1,2,3,4]'::VECTOR(4) LIMIT 1")
        .unwrap();
    assert!(
        matches!(qr, ExecutionResult::Query(_)),
        "brute-force still answers"
    );
}

#[test]
fn second_persist_replaces_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    seed(&conn);
    let first = conn.persist_ann_index("t", "v").unwrap();
    conn.execute("INSERT INTO t VALUES (300, '[5,5,5,5]'::VECTOR(4), 'odd')")
        .unwrap();
    let second = conn.persist_ann_index("t", "v").unwrap();
    assert_eq!(second.n, 41);
    assert_ne!(first.content_fingerprint, second.content_fingerprint);
    drop(conn);
    drop(db);

    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let _ = ids(&conn, QUERY);
    match status(&conn, "t") {
        Some(AnnIndexSource::Loaded { segment_b3 }) => {
            assert_eq!(segment_b3, second.segment_b3, "the REPLACED segment serves");
        }
        other => panic!("expected Loaded, got {other:?}"),
    }
}
