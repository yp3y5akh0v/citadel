#![cfg(target_arch = "wasm32")]

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_wasm::{CellValue, CitadelDb, ScriptOutcome};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn in_memory_format_upgrade_remains_available() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"pass")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();

    assert!(db.upgrade_format().unwrap().slots_flagged);
}

#[wasm_bindgen_test]
fn create_execute_query_round_trip() {
    let db = CitadelDb::create("pass").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    assert_eq!(
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .unwrap(),
        2
    );
    let qr = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
    assert_eq!(qr.columns, vec!["id", "name"]);
    assert_eq!(qr.rows.len(), 2);
    assert!(matches!(qr.rows[0][0], CellValue::Integer(1)));
    assert!(matches!(&qr.rows[1][1], CellValue::Text(s) if s == "b"));
}

#[wasm_bindgen_test]
fn transactions_persist_across_calls() {
    let db = CitadelDb::create("pass").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("ROLLBACK").unwrap();
    assert_eq!(db.query("SELECT * FROM t").unwrap().rows.len(), 0);
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("COMMIT").unwrap();
    assert_eq!(db.query("SELECT * FROM t").unwrap().rows.len(), 1);
}

#[wasm_bindgen_test]
fn kv_and_table_round_trip() {
    let db = CitadelDb::create("pass").unwrap();
    db.put(b"k1", b"v1").unwrap();
    assert_eq!(db.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
    assert!(db.delete(b"k1").unwrap());
    assert_eq!(db.get(b"k1").unwrap(), None);
    db.create_table("tbl").unwrap();
    assert!(db.create_table("tbl").is_err());
    db.table_put("tbl", b"a", b"1").unwrap();
    assert_eq!(
        db.table_get("tbl", b"a").unwrap().as_deref(),
        Some(&b"1"[..])
    );
    assert!(db.table_delete("tbl", b"a").unwrap());
}

#[wasm_bindgen_test]
fn errors_surface_as_strings() {
    let db = CitadelDb::create("pass").unwrap();
    assert!(db.execute("SELEC nonsense").is_err());
    assert!(db.query("SELECT * FROM missing").is_err());
}

#[wasm_bindgen_test]
fn execute_script_reports_outcomes() {
    let db = CitadelDb::create("pass").unwrap();
    let out = db.execute_script(
        "CREATE TABLE s (id INTEGER PRIMARY KEY); INSERT INTO s VALUES (1); SELECT * FROM s;",
    );
    assert_eq!(out.len(), 3);
    assert!(matches!(out.last(), Some(ScriptOutcome::Query(q)) if q.rows.len() == 1));
}

fn assert_query_ids(db: &CitadelDb, sql: &str, expected: &[i64]) {
    let result = db
        .query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let ids: Vec<_> = result
        .rows
        .iter()
        .map(|row| match row[0] {
            CellValue::Integer(id) => id,
            ref value => panic!("{sql}: expected integer, got {value:?}"),
        })
        .collect();
    assert_eq!(ids, expected, "{sql}");
}

#[wasm_bindgen_test]
fn limit_and_offset_above_u32_preserve_query_results() {
    let db = CitadelDb::create("pass").unwrap();
    db.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER);
         INSERT INTO t VALUES (1, 1, 20), (2, 1, 10), (3, 2, 30);
         CREATE TABLE docs (id INTEGER PRIMARY KEY, body TSVECTOR);
         INSERT INTO docs VALUES (1, TO_TSVECTOR('cat')), (2, TO_TSVECTOR('cat cat')),
                                (3, TO_TSVECTOR('cat cat cat'));
         CREATE INDEX docs_fts ON docs USING fts (body)",
    )
    .unwrap();
    let cases: &[(&str, &[i64])] = &[
        ("SELECT id FROM t ORDER BY id", &[1, 2, 3]),
        ("SELECT id FROM t ORDER BY amount DESC", &[3, 1, 2]),
        ("SELECT id + 0 FROM t ORDER BY amount DESC", &[3, 1, 2]),
        (
            "SELECT category FROM t GROUP BY category ORDER BY category",
            &[1, 2],
        ),
        ("SELECT DISTINCT category FROM t ORDER BY category", &[1, 2]),
        ("SELECT COUNT(*) FROM t", &[3]),
        (
            "SELECT id FROM t UNION ALL SELECT id FROM t ORDER BY id",
            &[1, 1, 2, 2, 3, 3],
        ),
        (
            "SELECT id FROM docs WHERE body @@ TO_TSQUERY('cat') ORDER BY id",
            &[1, 2, 3],
        ),
        (
            "SELECT id, TS_RANK(body, TO_TSQUERY('cat')) AS r FROM docs \
             WHERE body @@ TO_TSQUERY('cat') ORDER BY r DESC",
            &[3, 2, 1],
        ),
    ];
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            db.execute(begin).unwrap();
        }
        for count in [4_294_967_296_i64, 4_294_967_297] {
            for &(query, expected) in cases {
                assert_query_ids(&db, &format!("{query} LIMIT {count}"), expected);
                assert_query_ids(&db, &format!("{query} LIMIT {count} OFFSET {count}"), &[]);
                assert_query_ids(&db, &format!("{query} LIMIT 2 OFFSET {count}"), &[]);
            }
            assert_query_ids(
                &db,
                &format!(
                    "SELECT d.id FROM t AS parent CROSS JOIN LATERAL \
                     (SELECT id FROM t AS child WHERE child.category = parent.category \
                      ORDER BY id LIMIT {count}) AS d ORDER BY d.id"
                ),
                &[1, 1, 2, 2, 3],
            );
        }
        if begin.is_some() {
            db.execute("ROLLBACK").unwrap();
        }
    }
}

#[wasm_bindgen_test]
fn ann_limit_and_offset_above_u32_preserve_query_results() {
    let db = CitadelDb::create("pass").unwrap();
    db.execute_batch(
        "CREATE TABLE vectors (id INTEGER PRIMARY KEY, v VECTOR(3));
         INSERT INTO vectors VALUES (1, '[3,0,0]'::VECTOR(3)),
                                    (2, '[1,0,0]'::VECTOR(3)),
                                    (3, '[2,0,0]'::VECTOR(3));
         CREATE INDEX vectors_ann ON vectors USING ann (v) WITH (metric = 'l2')",
    )
    .unwrap();
    let query = "SELECT id FROM vectors ORDER BY v <-> '[0,0,0]'::VECTOR(3)";
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            db.execute(begin).unwrap();
        }
        for count in [4_294_967_296_i64, 4_294_967_297] {
            assert_query_ids(&db, &format!("{query} LIMIT {count}"), &[2, 3, 1]);
            assert_query_ids(&db, &format!("{query} LIMIT {count} OFFSET {count}"), &[]);
            assert_query_ids(&db, &format!("{query} LIMIT 2 OFFSET {count}"), &[]);
        }
        if begin.is_some() {
            db.execute("ROLLBACK").unwrap();
        }
    }
}

#[wasm_bindgen_test]
fn window_offsets_and_bucket_counts_do_not_narrow_to_u32() {
    let db = CitadelDb::create("pass").unwrap();
    db.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY);
         INSERT INTO t VALUES (1), (2), (3)",
    )
    .unwrap();
    for count in [4_294_967_296_i64, 4_294_967_297, i64::MAX] {
        assert_query_ids(
            &db,
            &format!("SELECT NTILE({count}) OVER (ORDER BY id) FROM t ORDER BY id"),
            &[1, 2, 3],
        );
        for function in ["LAG", "LEAD"] {
            for offset in [count, -count, i64::MIN] {
                assert_query_ids(
                    &db,
                    &format!("SELECT {function}(id, {offset}, id + 10) OVER (ORDER BY id) FROM t ORDER BY id"),
                    &[11, 12, 13],
                );
            }
        }
        for bound in ["PRECEDING", "FOLLOWING"] {
            assert_query_ids(
                &db,
                &format!("SELECT COUNT(*) OVER (ORDER BY id ROWS BETWEEN {count} {bound} AND {count} {bound}) FROM t ORDER BY id"),
                &[0, 0, 0],
            );
        }
    }
}
