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
fn at_question_tz_basic_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":1}' AS JSONB) @?_tz '$.x'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_question_tz_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":1}' AS JSONB) @?_tz '$.missing'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn at_at_tz_basic_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":5}' AS JSONB) @@_tz '$.x > 3'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_at_tz_false_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":5}' AS JSONB) @@_tz '$.x > 10'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn at_question_tz_null_left_yields_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST(NULL AS JSONB) @?_tz '$.x'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn at_question_tz_null_right_yields_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":1}' AS JSONB) @?_tz NULL")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn at_at_tz_null_propagation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST(NULL AS JSONB) @@_tz '$.x > 0'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn at_question_tz_with_timestamp_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT CAST('\"2025-01-01T00:00:00+00:00\"' AS JSONB) \
             @?_tz '$.datetime() ? (@ < \"2030-01-01T00:00:00+00:00\".datetime())'",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_at_tz_with_timestamp_predicate_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT CAST('\"2024-06-01T12:00:00+00:00\"' AS JSONB) \
             @@_tz '$.datetime() > \"2020-01-01T00:00:00+00:00\".datetime()'",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_at_tz_with_timestamp_predicate_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT CAST('\"2018-06-01T12:00:00+00:00\"' AS JSONB) \
             @@_tz '$.datetime() > \"2020-01-01T00:00:00+00:00\".datetime()'",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn where_clause_with_at_question_tz() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"a\":3,\"b\":4}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE data @?_tz '$.a' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn where_clause_with_at_at_tz() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"score\":10}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"score\":50}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"score\":90}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE data @@_tz '$.score > 25' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn at_tz_combined_with_other_json_ops() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1,\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE data @? '$.a' AND data @?_tz '$.b' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_in_or_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"c\":3}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE data @?_tz '$.a' OR data @?_tz '$.b' ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn at_tz_with_not() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE NOT (data @?_tz '$.a') ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn at_tz_inside_case_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"score\":50}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"score\":5}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query(
            "SELECT id, CASE WHEN data @@_tz '$.score > 25' THEN 'HIGH' ELSE 'LOW' END AS bucket \
             FROM t ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Text("HIGH".into()));
    assert_eq!(qr.rows[1][1], Value::Text("LOW".into()));
}

#[test]
fn at_tz_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO other VALUES (1), (2)").unwrap();
    let qr = conn
        .query(
            "SELECT id FROM other WHERE id IN (SELECT id FROM t WHERE data @?_tz '$.a') ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_in_cte() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"a\":99}' AS JSONB))")
        .unwrap();
    let qr = conn
        .query(
            "WITH matched AS (SELECT id FROM t WHERE data @@_tz '$.a > 50') \
             SELECT COUNT(*) FROM matched",
        )
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_inside_view_definition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("CREATE VIEW vmatched AS SELECT id FROM t WHERE data @?_tz '$.a'")
        .unwrap();
    let qr = conn.query("SELECT id FROM vmatched").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_in_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("CREATE TABLE matched (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO matched SELECT id FROM src WHERE data @?_tz '$.a'")
        .unwrap();
    let qr = conn.query("SELECT COUNT(*) FROM matched").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_in_update_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB, flag INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB), 0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB), 0)")
        .unwrap();
    conn.execute("UPDATE t SET flag = 1 WHERE data @?_tz '$.a'")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE flag = 1 ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_in_delete_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"score\":10}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"score\":50}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"score\":90}' AS JSONB))")
        .unwrap();
    conn.execute("DELETE FROM t WHERE data @@_tz '$.score > 25'")
        .unwrap();
    let qr = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_question_tz_on_empty_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{}' AS JSONB) @?_tz '$.anything'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(false));
}

#[test]
fn at_question_tz_on_array_element() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('[1,2,3]' AS JSONB) @?_tz '$[1]'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_at_tz_on_nested_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query(
            "SELECT CAST('{\"items\":[{\"q\":1},{\"q\":5},{\"q\":10}]}' AS JSONB) \
             @@_tz '$.items[*].q > 3'",
        )
        .unwrap();
    // True because at least one item has q > 3.
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_tz_invalid_path_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let r = conn.query("SELECT CAST('{}' AS JSONB) @?_tz 'not-a-path'");
    assert!(r.is_err());
}

#[test]
fn at_tz_literal_string_not_an_operator() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    // The string `'@?_tz'` is just text, not an operator.
    let qr = conn.query("SELECT '@?_tz' AS s").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("@?_tz".into()));
}

#[test]
fn plain_at_question_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":1}' AS JSONB) @? '$.x'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn plain_at_at_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = conn
        .query("SELECT CAST('{\"x\":5}' AS JSONB) @@ '$.x > 3'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_tz_with_parameterized_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"b\":2}' AS JSONB))")
        .unwrap();
    let stmt = conn
        .prepare("SELECT id FROM t WHERE data @?_tz $1 ORDER BY id")
        .unwrap();
    let qr = stmt.query_collect(&[Value::Text("$.a".into())]).unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn at_tz_with_parameterized_jsonb() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let stmt = conn.prepare("SELECT CAST($1 AS JSONB) @?_tz $2").unwrap();
    let qr = stmt
        .query_collect(&[Value::Text("{\"k\":42}".into()), Value::Text("$.k".into())])
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}

#[test]
fn at_tz_filters_correctly_with_index_on_other_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT, data JSONB)")
        .unwrap();
    conn.execute("CREATE INDEX t_tag ON t(tag)").unwrap();
    for i in 0..30 {
        let data = if i % 2 == 0 {
            format!("{{\"a\":{i}}}")
        } else {
            format!("{{\"b\":{i}}}")
        };
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'tag{i}', CAST('{data}' AS JSONB))"
        ))
        .unwrap();
    }
    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE data @?_tz '$.a'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(15));
}

#[test]
fn at_tz_filters_many_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB)")
        .unwrap();
    for i in 0..100 {
        let data = format!("{{\"score\":{i}}}");
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, CAST('{data}' AS JSONB))"
        ))
        .unwrap();
    }
    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE data @@_tz '$.score > 50'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(49));
}

#[test]
fn at_question_tz_on_json_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSON))")
        .unwrap();
    let qr = conn
        .query("SELECT id FROM t WHERE data @?_tz '$.a'")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn at_tz_chained_with_and_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSONB, flag INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST('{\"a\":1}' AS JSONB), 1)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST('{\"a\":1}' AS JSONB), 0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, CAST('{\"b\":2}' AS JSONB), 1)")
        .unwrap();
    // `data @?_tz '$.a' AND flag = 1` should bind correctly.
    let qr = conn
        .query("SELECT id FROM t WHERE data @?_tz '$.a' AND flag = 1 ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}
