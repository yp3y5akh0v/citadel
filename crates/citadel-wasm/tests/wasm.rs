#![cfg(target_arch = "wasm32")]

use citadel_wasm::{CellValue, CitadelDb, ScriptOutcome};
use wasm_bindgen_test::wasm_bindgen_test;

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
