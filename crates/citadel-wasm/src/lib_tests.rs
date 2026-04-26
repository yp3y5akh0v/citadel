use super::*;

#[test]
fn create_in_memory() {
    let db = CitadelDb::create("test-passphrase").unwrap();
    let stats = db.stats();
    assert_eq!(stats.entry_count, 0);
}

#[test]
fn kv_roundtrip() {
    let db = CitadelDb::create("secret").unwrap();

    db.put(b"hello", b"world").unwrap();
    let val = db.get(b"hello").unwrap();
    assert_eq!(val, Some(b"world".to_vec()));

    let missing = db.get(b"nonexistent").unwrap();
    assert_eq!(missing, None);
}

#[test]
fn kv_delete() {
    let db = CitadelDb::create("secret").unwrap();

    db.put(b"key1", b"val1").unwrap();
    assert!(db.delete(b"key1").unwrap());
    assert!(!db.delete(b"key1").unwrap());
    assert_eq!(db.get(b"key1").unwrap(), None);
}

#[test]
fn named_table_roundtrip() {
    let db = CitadelDb::create("secret").unwrap();

    let mut wtx = db.db().begin_write().unwrap();
    wtx.create_table(b"mytable").unwrap();
    wtx.commit().unwrap();

    db.table_put("mytable", b"k1", b"v1").unwrap();
    let val = db.table_get("mytable", b"k1").unwrap();
    assert_eq!(val, Some(b"v1".to_vec()));

    assert!(db.table_delete("mytable", b"k1").unwrap());
    assert_eq!(db.table_get("mytable", b"k1").unwrap(), None);
}

#[test]
fn sql_create_and_query() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();

    let result = db.query("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 2);

    match &result.rows[0][1] {
        CellValue::Text(s) => assert_eq!(s, "Alice"),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn sql_update_and_delete() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO items (id, qty) VALUES (1, 10)")
        .unwrap();

    let affected = db
        .execute("UPDATE items SET qty = 20 WHERE id = 1")
        .unwrap();
    assert_eq!(affected, 1);

    let result = db.query("SELECT qty FROM items WHERE id = 1").unwrap();
    match &result.rows[0][0] {
        CellValue::Integer(q) => assert_eq!(*q, 20),
        other => panic!("expected Integer, got {other:?}"),
    }

    let deleted = db.execute("DELETE FROM items WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);

    let result = db.query("SELECT * FROM items").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn sql_aggregation() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO scores (id, score) VALUES (1, 90)")
        .unwrap();
    db.execute("INSERT INTO scores (id, score) VALUES (2, 80)")
        .unwrap();
    db.execute("INSERT INTO scores (id, score) VALUES (3, 70)")
        .unwrap();

    let result = db
        .query("SELECT COUNT(*), SUM(score), AVG(score) FROM scores")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    match &result.rows[0][0] {
        CellValue::Integer(n) => assert_eq!(*n, 3),
        other => panic!("expected Integer count, got {other:?}"),
    }
}

#[test]
fn execute_batch() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_batch(
        "
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);
        INSERT INTO t (id, v) VALUES (1, 'a');
        INSERT INTO t (id, v) VALUES (2, 'b');
    ",
    )
    .unwrap();

    let result = db.query("SELECT COUNT(*) FROM t").unwrap();
    match &result.rows[0][0] {
        CellValue::Integer(n) => assert_eq!(*n, 2),
        other => panic!("expected Integer, got {other:?}"),
    }
}

#[test]
fn sql_join() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_batch(
        "
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, item TEXT NOT NULL);
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
        INSERT INTO orders (id, user_id, item) VALUES (1, 1, 'Widget');
        INSERT INTO orders (id, user_id, item) VALUES (2, 1, 'Gadget');
        INSERT INTO orders (id, user_id, item) VALUES (3, 2, 'Doohickey');
    ",
    )
    .unwrap();

    let result = db
        .query("SELECT u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id")
        .unwrap();
    assert_eq!(result.rows.len(), 3);

    match &result.rows[0][0] {
        CellValue::Text(s) => assert_eq!(s, "Alice"),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn sql_subquery() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_batch(
        "
        CREATE TABLE products (id INTEGER PRIMARY KEY, price INTEGER NOT NULL);
        INSERT INTO products (id, price) VALUES (1, 10);
        INSERT INTO products (id, price) VALUES (2, 20);
        INSERT INTO products (id, price) VALUES (3, 30);
    ",
    )
    .unwrap();

    let result = db
        .query("SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    match &result.rows[0][0] {
        CellValue::Integer(id) => assert_eq!(*id, 3),
        other => panic!("expected Integer, got {other:?}"),
    }
}

#[test]
fn many_entries() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();

    for i in 0..200 {
        db.execute(&format!(
            "INSERT INTO big (id, val) VALUES ({i}, 'value_{i}')"
        ))
        .unwrap();
    }

    let result = db.query("SELECT COUNT(*) FROM big").unwrap();
    match &result.rows[0][0] {
        CellValue::Integer(n) => assert_eq!(*n, 200),
        other => panic!("expected 200, got {other:?}"),
    }
}

#[test]
fn sql_error_handling() {
    let db = CitadelDb::create("secret").unwrap();

    let err = db.execute("SELECT * FROM nonexistent");
    assert!(err.is_err());

    let err = db.execute("THIS IS NOT SQL");
    assert!(err.is_err());
}

#[test]
fn stats_after_operations() {
    let db = CitadelDb::create("secret").unwrap();
    let s1 = db.stats();
    assert_eq!(s1.entry_count, 0);

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    let s2 = db.stats();
    assert_eq!(s2.entry_count, 2);
}

#[test]
fn kv_overwrite() {
    let db = CitadelDb::create("secret").unwrap();

    db.put(b"key", b"first").unwrap();
    db.put(b"key", b"second").unwrap();
    let val = db.get(b"key").unwrap();
    assert_eq!(val, Some(b"second".to_vec()));
}

#[test]
fn sql_types() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE types (id INTEGER PRIMARY KEY, r REAL, b BOOLEAN, t TEXT)")
        .unwrap();
    db.execute("INSERT INTO types (id, r, b, t) VALUES (1, 3.125, TRUE, 'hello')")
        .unwrap();

    let result = db.query("SELECT * FROM types").unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];

    match &row[0] {
        CellValue::Integer(i) => assert_eq!(*i, 1),
        other => panic!("expected Integer, got {other:?}"),
    }
    match &row[1] {
        CellValue::Real(r) => assert!((r - 3.125).abs() < f64::EPSILON),
        other => panic!("expected Real, got {other:?}"),
    }
    match &row[2] {
        CellValue::Boolean(b) => assert!(*b),
        other => panic!("expected Boolean, got {other:?}"),
    }
    match &row[3] {
        CellValue::Text(s) => assert_eq!(s, "hello"),
        other => panic!("expected Text, got {other:?}"),
    }
}

#[test]
fn sql_null_handling() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute("CREATE TABLE nullable (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO nullable (id) VALUES (1)").unwrap();

    let result = db.query("SELECT val FROM nullable WHERE id = 1").unwrap();
    match &result.rows[0][0] {
        CellValue::Null => {}
        other => panic!("expected Null, got {other:?}"),
    }

    let result = db
        .query("SELECT * FROM nullable WHERE val IS NULL")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn sql_distinct_and_order() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_batch(
        "
        CREATE TABLE dups (id INTEGER PRIMARY KEY, cat TEXT NOT NULL);
        INSERT INTO dups (id, cat) VALUES (1, 'b');
        INSERT INTO dups (id, cat) VALUES (2, 'a');
        INSERT INTO dups (id, cat) VALUES (3, 'b');
        INSERT INTO dups (id, cat) VALUES (4, 'a');
    ",
    )
    .unwrap();

    let result = db
        .query("SELECT DISTINCT cat FROM dups ORDER BY cat")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    match &result.rows[0][0] {
        CellValue::Text(s) => assert_eq!(s, "a"),
        other => panic!("expected 'a', got {other:?}"),
    }
}

#[test]
fn concurrent_read_after_write() {
    let db = CitadelDb::create("secret").unwrap();

    db.put(b"k1", b"v1").unwrap();

    let mut rtx = db.db().begin_read();

    db.put(b"k2", b"v2").unwrap();

    assert!(rtx.get(b"k1").unwrap().is_some());
    assert!(rtx.get(b"k2").unwrap().is_none());

    drop(rtx);

    assert!(db.get(b"k2").unwrap().is_some());
}

#[test]
fn script_mixed_outcomes() {
    let db = CitadelDb::create("secret").unwrap();

    let outcomes = db.execute_script(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
         INSERT INTO t VALUES (1, 10);
         SELECT * FROM t",
    );
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], ScriptOutcome::Ok));
    assert!(matches!(outcomes[1], ScriptOutcome::Rows(1)));
    match &outcomes[2] {
        ScriptOutcome::Query(qr) => assert_eq!(qr.rows.len(), 1),
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn script_runtime_error_mid_script() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let outcomes =
        db.execute_script("INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); SELECT 1");

    assert_eq!(outcomes.len(), 2);
    assert!(matches!(outcomes[0], ScriptOutcome::Rows(1)));
    assert!(matches!(outcomes[1], ScriptOutcome::Error(_)));
}

#[test]
fn script_parse_error() {
    let db = CitadelDb::create("secret").unwrap();

    let outcomes = db.execute_script("GARBAGE NOT SQL");
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(outcomes[0], ScriptOutcome::Error(_)));
}

#[test]
fn script_transaction_persists_across_calls() {
    let db = CitadelDb::create("secret").unwrap();

    db.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    let open = db.execute_script("BEGIN; INSERT INTO t VALUES (1)");
    assert_eq!(open.len(), 2);
    assert!(matches!(open[0], ScriptOutcome::Ok));
    assert!(matches!(open[1], ScriptOutcome::Rows(1)));

    let more = db.execute_script("INSERT INTO t VALUES (2)");
    assert_eq!(more.len(), 1);
    assert!(matches!(more[0], ScriptOutcome::Rows(1)));

    let close = db.execute_script("COMMIT");
    assert_eq!(close.len(), 1);
    assert!(matches!(close[0], ScriptOutcome::Ok));

    let verify = db.execute_script("SELECT id FROM t ORDER BY id");
    match &verify[0] {
        ScriptOutcome::Query(qr) => assert_eq!(qr.rows.len(), 2),
        other => panic!("expected Query, got {other:?}"),
    }
}
