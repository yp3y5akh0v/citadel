use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, QueryResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn query(conn: &mut Connection, sql: &str) -> QueryResult {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(qr) => qr,
        other => panic!("expected Query, got {other:?}"),
    }
}

fn scalar(conn: &mut Connection, sql: &str) -> Value {
    let qr = query(conn, sql);
    assert_eq!(qr.rows.len(), 1, "expected 1 row for: {sql}");
    assert_eq!(qr.rows[0].len(), 1, "expected 1 col for: {sql}");
    qr.rows[0][0].clone()
}

fn setup(conn: &mut Connection) {
    assert_ok(
        conn.execute(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, val INTEGER, score REAL)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO t VALUES (1, 'alice', 10, 1.5)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'bob', 20, 2.5)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'charlie', 30, 3.5)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'diana', NULL, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (5, NULL, 50, 5.5)")
        .unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// BETWEEN torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn between_all_three_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT NULL BETWEEN NULL AND NULL"),
        Value::Null
    );
}

#[test]
fn between_null_low() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 5 >= NULL is NULL, 5 <= 10 is TRUE → NULL AND TRUE = NULL
    assert_eq!(
        scalar(&mut conn, "SELECT 5 BETWEEN NULL AND 10"),
        Value::Null
    );
}

#[test]
fn between_null_high() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT 5 BETWEEN 1 AND NULL"),
        Value::Null
    );
}

#[test]
fn between_null_val_definite_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // 5 >= 10 is FALSE → FALSE AND anything = FALSE
    assert_eq!(
        scalar(&mut conn, "SELECT 5 BETWEEN 10 AND NULL"),
        Value::Boolean(false)
    );
}

#[test]
fn between_mixed_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN 9.5 AND 20.5 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1)); // val=10
    assert_eq!(qr.rows[1][0], Value::Integer(2)); // val=20
}

#[test]
fn between_with_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN 5 + 5 AND 15 * 2 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 3); // 10, 20, 30
}

#[test]
fn between_in_having() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(conn.execute(
        "CREATE TABLE sales (id INTEGER NOT NULL PRIMARY KEY, region TEXT NOT NULL, amount INTEGER NOT NULL)"
    ).unwrap());
    conn.execute("INSERT INTO sales VALUES (1, 'north', 100)")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (2, 'north', 200)")
        .unwrap();
    conn.execute("INSERT INTO sales VALUES (3, 'south', 50)")
        .unwrap();

    let qr = query(&mut conn,
        "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) BETWEEN 100 AND 400"
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("north".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(300));
}

#[test]
fn not_between_null_three_valued() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // NOT BETWEEN: NOT(NULL) = NULL
    assert_eq!(
        scalar(&mut conn, "SELECT 5 NOT BETWEEN NULL AND 10"),
        Value::Null
    );
    // NOT BETWEEN: NOT(FALSE) = TRUE
    assert_eq!(
        scalar(&mut conn, "SELECT 5 NOT BETWEEN 10 AND NULL"),
        Value::Boolean(true)
    );
}

// ═══════════════════════════════════════════════════════════════════
// LIKE torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn like_empty_pattern_empty_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT '' LIKE ''"), Value::Boolean(true));
}

#[test]
fn like_empty_pattern_nonempty_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT 'abc' LIKE ''"),
        Value::Boolean(false)
    );
}

#[test]
fn like_percent_matches_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT '' LIKE '%'"),
        Value::Boolean(true)
    );
}

#[test]
fn like_underscore_needs_one_char() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT '' LIKE '_'"),
        Value::Boolean(false)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT 'a' LIKE '_'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT 'ab' LIKE '_'"),
        Value::Boolean(false)
    );
}

#[test]
fn like_consecutive_percent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT 'abc' LIKE '%%'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT '' LIKE '%%'"),
        Value::Boolean(true)
    );
}

#[test]
fn like_percent_underscore_percent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // %_% matches one or more characters
    assert_eq!(
        scalar(&mut conn, "SELECT 'a' LIKE '%_%'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT '' LIKE '%_%'"),
        Value::Boolean(false)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT 'ab' LIKE '%_%'"),
        Value::Boolean(true)
    );
}

#[test]
fn like_backtracking_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // This pattern requires backtracking: 'aaaaaaaaa' does NOT contain 'b'
    assert_eq!(
        scalar(&mut conn, "SELECT 'aaaaaaaaa' LIKE '%a%a%a%a%b'"),
        Value::Boolean(false)
    );
}

#[test]
fn like_backtracking_success() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT 'aaxaayaazb' LIKE '%a%a%a%b'"),
        Value::Boolean(true)
    );
}

#[test]
fn like_unicode_underscore() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // _ matches one Unicode code point
    assert_eq!(
        scalar(&mut conn, "SELECT '\u{00e9}' LIKE '_'"),
        Value::Boolean(true)
    );
}

#[test]
fn like_escape_percent_and_underscore() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT '50%' LIKE '%!%' ESCAPE '!'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT 'a_b' LIKE 'a!_b' ESCAPE '!'"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT 'axb' LIKE 'a!_b' ESCAPE '!'"),
        Value::Boolean(false)
    );
}

#[test]
fn like_pattern_only_percent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    // % matches everything including NULL name (but NULL LIKE '%' is NULL, filtered out)
    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE name LIKE '%' ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 4); // alice, bob, charlie, diana (not NULL id=5)
}

#[test]
fn like_in_join_on_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE patterns (id INTEGER NOT NULL PRIMARY KEY, pat TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(
        conn.execute("CREATE TABLE data (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO patterns VALUES (1, 'a%')")
        .unwrap();
    conn.execute("INSERT INTO data VALUES (1, 'alice')")
        .unwrap();
    conn.execute("INSERT INTO data VALUES (2, 'bob')").unwrap();

    let qr = query(
        &mut conn,
        "SELECT d.val FROM data d JOIN patterns p ON d.val LIKE p.pat",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

// ═══════════════════════════════════════════════════════════════════
// CASE WHEN torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn case_nested() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN 1 > 0 THEN \
            CASE WHEN 2 > 1 THEN 'deep' ELSE 'shallow' END \
         ELSE 'none' END",
    );
    assert_eq!(v, Value::Text("deep".into()));
}

#[test]
fn case_in_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name FROM t WHERE name IS NOT NULL \
         ORDER BY CASE name WHEN 'charlie' THEN 1 WHEN 'alice' THEN 2 ELSE 3 END",
    );
    assert_eq!(qr.rows[0][0], Value::Text("charlie".into()));
    assert_eq!(qr.rows[1][0], Value::Text("alice".into()));
}

#[test]
fn case_many_branches() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT CASE val \
            WHEN 10 THEN 'ten' \
            WHEN 20 THEN 'twenty' \
            WHEN 30 THEN 'thirty' \
            WHEN 40 THEN 'forty' \
            WHEN 50 THEN 'fifty' \
            ELSE 'other' \
         END FROM t WHERE val IS NOT NULL ORDER BY id",
    );
    assert_eq!(qr.rows[0][0], Value::Text("ten".into()));
    assert_eq!(qr.rows[1][0], Value::Text("twenty".into()));
    assert_eq!(qr.rows[2][0], Value::Text("thirty".into()));
    assert_eq!(qr.rows[3][0], Value::Text("fifty".into()));
}

#[test]
fn case_with_aggregate_in_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN MAX(val) > 40 THEN 'big' ELSE 'small' END FROM t",
    );
    assert_eq!(v, Value::Text("big".into()));
}

#[test]
fn case_short_circuit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // First branch TRUE → second (which would divide by zero) never evaluated
    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN 1 = 1 THEN 'ok' WHEN 1/0 = 1 THEN 'boom' END",
    );
    assert_eq!(v, Value::Text("ok".into()));
}

#[test]
fn case_in_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT CASE WHEN val <= 20 THEN 'low' ELSE 'high' END AS bucket, COUNT(*) \
         FROM t WHERE val IS NOT NULL \
         GROUP BY CASE WHEN val <= 20 THEN 'low' ELSE 'high' END \
         ORDER BY bucket",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("high".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(2)); // 30, 50
    assert_eq!(qr.rows[1][0], Value::Text("low".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(2)); // 10, 20
}

// ═══════════════════════════════════════════════════════════════════
// COALESCE torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn coalesce_nested() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT COALESCE(NULL, COALESCE(NULL, 42))");
    assert_eq!(v, Value::Integer(42));
}

#[test]
fn coalesce_many_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT COALESCE(NULL, NULL, NULL, NULL, NULL, 99)",
    );
    assert_eq!(v, Value::Integer(99));
}

#[test]
fn coalesce_short_circuit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // First non-null found, division by zero never evaluated
    let v = scalar(&mut conn, "SELECT COALESCE(1, 1/0)");
    assert_eq!(v, Value::Integer(1));
}

#[test]
fn coalesce_with_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT COALESCE(name, 'unknown') FROM t ORDER BY id",
    );
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[4][0], Value::Text("unknown".into())); // id=5 has NULL name
}

#[test]
fn coalesce_in_aggregate_context() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT SUM(COALESCE(val, 0)) FROM t");
    assert_eq!(v, Value::Integer(110)); // 10+20+30+0+50
}

// ═══════════════════════════════════════════════════════════════════
// NULLIF/IIF torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn nullif_both_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // NULL = NULL is not TRUE in CASE → returns first arg (NULL)
    let v = scalar(&mut conn, "SELECT NULLIF(NULL, NULL)");
    assert_eq!(v, Value::Null);
}

#[test]
fn nullif_first_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT NULLIF(NULL, 5)");
    assert_eq!(v, Value::Null);
}

#[test]
fn nullif_with_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT NULLIF(val, 20) FROM t WHERE val IS NOT NULL ORDER BY id",
    );
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Null); // val=20 → NULL
    assert_eq!(qr.rows[2][0], Value::Integer(30));
}

#[test]
fn iif_null_condition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // NULL is not truthy → returns else branch
    let v = scalar(&mut conn, "SELECT IIF(NULL, 'yes', 'no')");
    assert_eq!(v, Value::Text("no".into()));
}

// ═══════════════════════════════════════════════════════════════════
// CAST torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn cast_bool_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(TRUE AS INTEGER)"),
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(FALSE AS INTEGER)"),
        Value::Integer(0)
    );
}

#[test]
fn cast_integer_to_bool() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(1 AS BOOLEAN)"),
        Value::Boolean(true)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(0 AS BOOLEAN)"),
        Value::Boolean(false)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(42 AS BOOLEAN)"),
        Value::Boolean(true)
    );
}

#[test]
fn cast_text_float_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // '3.7' → parse as float → truncate to 3
    let v = scalar(&mut conn, "SELECT CAST('3.7' AS INTEGER)");
    assert_eq!(v, Value::Integer(3));
}

#[test]
fn cast_empty_string_to_integer_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);
    let err = conn
        .execute("SELECT CAST('' AS INTEGER) FROM t WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn cast_bool_to_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(TRUE AS REAL)"),
        Value::Real(1.0)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(FALSE AS REAL)"),
        Value::Real(0.0)
    );
}

#[test]
fn cast_real_to_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CAST(3.14 AS TEXT)");
    assert_eq!(v, Value::Text("3.14".into()));
}

#[test]
fn cast_integer_whole_real_to_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Whole-number reals display with .0
    let v = scalar(&mut conn, "SELECT CAST(5.0 AS TEXT)");
    assert_eq!(v, Value::Text("5.0".into()));
}

#[test]
fn cast_chained() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // CAST(CAST('42' AS INTEGER) AS TEXT) → "42"
    let v = scalar(&mut conn, "SELECT CAST(CAST('42' AS INTEGER) AS TEXT)");
    assert_eq!(v, Value::Text("42".into()));
}

#[test]
fn cast_text_to_blob() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TYPEOF(CAST('hello' AS BLOB))");
    assert_eq!(v, Value::Text("blob".into()));
}

// ═══════════════════════════════════════════════════════════════════
// String concat (||) torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn concat_op_null_propagation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Standard SQL: || propagates NULL
    assert_eq!(scalar(&mut conn, "SELECT NULL || 'b'"), Value::Null);
    assert_eq!(scalar(&mut conn, "SELECT 'a' || NULL"), Value::Null);
}

#[test]
fn concat_op_integer_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT 42 || ' items'");
    assert_eq!(v, Value::Text("42 items".into()));
}

#[test]
fn concat_op_boolean_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TRUE || ' story'");
    assert_eq!(v, Value::Text("TRUE story".into()));
}

#[test]
fn concat_op_chain_ten() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT 'a' || 'b' || 'c' || 'd' || 'e' || 'f' || 'g' || 'h' || 'i' || 'j'",
    );
    assert_eq!(v, Value::Text("abcdefghij".into()));
}

#[test]
fn concat_fn_vs_operator_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // CONCAT() treats NULL as empty; || propagates NULL
    assert_eq!(
        scalar(&mut conn, "SELECT CONCAT('a', NULL, 'b')"),
        Value::Text("ab".into())
    );
    assert_eq!(scalar(&mut conn, "SELECT 'a' || NULL || 'b'"), Value::Null);
}

// ═══════════════════════════════════════════════════════════════════
// SUBSTR torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn substr_start_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Start=0 with length: treated like start=1 but length reduced by 1
    let v = scalar(&mut conn, "SELECT SUBSTR('hello', 0, 3)");
    // 0-based: absorb 1 char of length, take 2 from start
    assert_eq!(v, Value::Text("he".into()));
}

#[test]
fn substr_start_beyond_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT SUBSTR('abc', 10, 5)");
    assert_eq!(v, Value::Text(String::new()));
}

#[test]
fn substr_length_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT SUBSTR('abc', 1, 0)");
    assert_eq!(v, Value::Text(String::new()));
}

#[test]
fn substr_very_large_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT SUBSTR('abc', 1, 99999)");
    assert_eq!(v, Value::Text("abc".into()));
}

#[test]
fn substr_negative_start_with_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Start=-1 counts from right: last 1 char
    let v = scalar(&mut conn, "SELECT SUBSTR('hello', -1)");
    assert_eq!(v, Value::Text("o".into()));

    let v2 = scalar(&mut conn, "SELECT SUBSTR('hello', -3)");
    assert_eq!(v2, Value::Text("llo".into()));
}

#[test]
fn substr_null_args() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SUBSTR(NULL, 1, 2)"), Value::Null);
    assert_eq!(
        scalar(&mut conn, "SELECT SUBSTR('abc', NULL, 2)"),
        Value::Null
    );
    assert_eq!(
        scalar(&mut conn, "SELECT SUBSTR('abc', 1, NULL)"),
        Value::Null
    );
}

// ═══════════════════════════════════════════════════════════════════
// TRIM torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn trim_multi_char_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Trim chars 'a' and 'b' from both ends
    let v = scalar(&mut conn, "SELECT TRIM('abcba', 'ab')");
    assert_eq!(v, Value::Text("c".into()));
}

#[test]
fn trim_nothing_to_trim() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TRIM('hello')");
    assert_eq!(v, Value::Text("hello".into()));
}

#[test]
fn trim_all_chars() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT TRIM('aaa', 'a')");
    assert_eq!(v, Value::Text(String::new()));
}

#[test]
fn ltrim_rtrim_specific() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT LTRIM('xxhello', 'x')"),
        Value::Text("hello".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT RTRIM('helloxx', 'x')"),
        Value::Text("hello".into())
    );
}

#[test]
fn trim_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT TRIM(NULL)"), Value::Null);
    assert_eq!(scalar(&mut conn, "SELECT TRIM('abc', NULL)"), Value::Null);
}

// ═══════════════════════════════════════════════════════════════════
// ROUND torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn round_negative_places() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT ROUND(1234.0, -2)");
    assert_eq!(v, Value::Real(1200.0));
}

#[test]
fn round_half_away_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT ROUND(0.5)"), Value::Real(1.0));
    assert_eq!(scalar(&mut conn, "SELECT ROUND(-0.5)"), Value::Real(-1.0));
    assert_eq!(scalar(&mut conn, "SELECT ROUND(2.5)"), Value::Real(3.0));
    assert_eq!(scalar(&mut conn, "SELECT ROUND(-2.5)"), Value::Real(-3.0));
}

#[test]
fn round_integer_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Integer input → still returns Real (ROUND always returns float)
    let v = scalar(&mut conn, "SELECT ROUND(5)");
    assert_eq!(v, Value::Real(5.0));
}

#[test]
fn round_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT ROUND(NULL)"), Value::Null);
    assert_eq!(scalar(&mut conn, "SELECT ROUND(3.14, NULL)"), Value::Null);
}

// ═══════════════════════════════════════════════════════════════════
// CEIL / FLOOR torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn ceil_negative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // CEIL rounds toward positive infinity
    assert_eq!(scalar(&mut conn, "SELECT CEIL(-2.1)"), Value::Integer(-2));
    assert_eq!(scalar(&mut conn, "SELECT CEIL(-2.9)"), Value::Integer(-2));
}

#[test]
fn floor_negative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // FLOOR rounds toward negative infinity
    assert_eq!(scalar(&mut conn, "SELECT FLOOR(-2.1)"), Value::Integer(-3));
    assert_eq!(scalar(&mut conn, "SELECT FLOOR(-2.9)"), Value::Integer(-3));
}

#[test]
fn ceil_floor_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT CEIL(0.0)"), Value::Integer(0));
    assert_eq!(scalar(&mut conn, "SELECT FLOOR(0.0)"), Value::Integer(0));
}

#[test]
fn ceil_floor_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT CEIL(NULL)"), Value::Null);
    assert_eq!(scalar(&mut conn, "SELECT FLOOR(NULL)"), Value::Null);
}

#[test]
fn ceil_floor_very_small() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT FLOOR(0.0001)"), Value::Integer(0));
    assert_eq!(scalar(&mut conn, "SELECT CEIL(0.0001)"), Value::Integer(1));
}

// ═══════════════════════════════════════════════════════════════════
// ABS / SIGN edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn abs_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT ABS(0)"), Value::Integer(0));
    assert_eq!(scalar(&mut conn, "SELECT ABS(0.0)"), Value::Real(0.0));
}

#[test]
fn abs_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT ABS(NULL)"), Value::Null);
}

#[test]
fn sign_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SIGN(-3.14)"), Value::Integer(-1));
    assert_eq!(scalar(&mut conn, "SELECT SIGN(0.0)"), Value::Integer(0));
    assert_eq!(scalar(&mut conn, "SELECT SIGN(3.14)"), Value::Integer(1));
}

#[test]
fn sign_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SIGN(NULL)"), Value::Null);
}

// ═══════════════════════════════════════════════════════════════════
// FROM-less SELECT torture
// ═══════════════════════════════════════════════════════════════════

#[test]
fn select_no_from_multiple_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let qr = query(&mut conn, "SELECT 1, 'hello', TRUE, NULL");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));
    assert_eq!(qr.rows[0][2], Value::Boolean(true));
    assert_eq!(qr.rows[0][3], Value::Null);
}

#[test]
fn select_no_from_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT 2 + 3 * 4"), Value::Integer(14));
}

#[test]
fn select_no_from_nested_functions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT ROUND(SQRT(144), 0)");
    assert_eq!(v, Value::Real(12.0));
}

#[test]
fn select_no_from_case() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN 1 + 1 = 2 THEN 'math works' ELSE 'broken' END",
    );
    assert_eq!(v, Value::Text("math works".into()));
}

#[test]
fn select_no_from_coalesce_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT COALESCE(NULL, CAST(42 AS TEXT))");
    assert_eq!(v, Value::Text("42".into()));
}

// ═══════════════════════════════════════════════════════════════════
// eval_const_expr improvements
// ═══════════════════════════════════════════════════════════════════

#[test]
fn insert_with_arithmetic_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE calc (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO calc VALUES (1, 2 + 3 * 4)")
        .unwrap();
    let v = scalar(&mut conn, "SELECT val FROM calc WHERE id = 1");
    assert_eq!(v, Value::Integer(14));
}

#[test]
fn insert_with_function_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE calc (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO calc VALUES (1, UPPER('hello'))")
        .unwrap();
    let v = scalar(&mut conn, "SELECT name FROM calc WHERE id = 1");
    assert_eq!(v, Value::Text("HELLO".into()));
}

#[test]
fn insert_with_cast_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE calc (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO calc VALUES (1, CAST('42' AS INTEGER))")
        .unwrap();
    let v = scalar(&mut conn, "SELECT val FROM calc WHERE id = 1");
    assert_eq!(v, Value::Integer(42));
}

#[test]
fn limit_with_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT id FROM t ORDER BY id LIMIT 1 + 1");
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn offset_with_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1 + 1",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(3));
    assert_eq!(qr.rows[1][0], Value::Integer(4));
}

// ═══════════════════════════════════════════════════════════════════
// eval_aggregate_expr with new variants
// ═══════════════════════════════════════════════════════════════════

#[test]
fn aggregate_with_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // COALESCE in aggregate projection
    let v = scalar(
        &mut conn,
        "SELECT COALESCE(SUM(val), 0) FROM t WHERE val > 1000",
    );
    // No matching rows → SUM returns NULL → COALESCE → 0
    assert_eq!(v, Value::Integer(0));
}

#[test]
fn aggregate_with_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST(COUNT(*) AS TEXT) FROM t");
    assert_eq!(v, Value::Text("5".into()));
}

#[test]
fn aggregate_with_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // Is the average between 20 and 30?
    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN AVG(val) BETWEEN 20.0 AND 30.0 THEN 'yes' ELSE 'no' END FROM t",
    );
    // AVG(10,20,30,50) = 27.5, which is between 20 and 30
    assert_eq!(v, Value::Text("yes".into()));
}

#[test]
fn aggregate_with_scalar_function() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT ABS(MIN(val) - MAX(val)) FROM t");
    // |10 - 50| = 40
    assert_eq!(v, Value::Integer(40));
}

#[test]
fn aggregate_with_unary_and_isnull() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT -SUM(val) FROM t");
    assert_eq!(v, Value::Integer(-110));

    let v2 = scalar(
        &mut conn,
        "SELECT CASE WHEN SUM(val) IS NOT NULL THEN 'has data' ELSE 'empty' END FROM t",
    );
    assert_eq!(v2, Value::Text("has data".into()));
}

// ═══════════════════════════════════════════════════════════════════
// is_aggregate_expr with new variants
// ═══════════════════════════════════════════════════════════════════

#[test]
fn aggregate_in_case_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // SUM inside CASE → should be recognized as aggregate
    let qr = query(
        &mut conn,
        "SELECT CASE WHEN SUM(val) > 50 THEN 'yes' ELSE 'no' END FROM t",
    );
    assert_eq!(qr.rows[0][0], Value::Text("yes".into()));
}

#[test]
fn aggregate_in_coalesce_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT COALESCE(AVG(score), 0.0) FROM t");
    // AVG(1.5, 2.5, 3.5, 5.5) = 13.0/4 = 3.25
    match v {
        Value::Real(r) => assert!((r - 3.25).abs() < 0.001),
        _ => panic!("expected Real, got {v:?}"),
    }
}

#[test]
fn aggregate_in_cast_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST(SUM(val) AS REAL) FROM t");
    assert_eq!(v, Value::Real(110.0));
}

// ═══════════════════════════════════════════════════════════════════
// materialize_expr / has_subquery with new variants
// ═══════════════════════════════════════════════════════════════════

#[test]
fn subquery_in_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN \
            (SELECT MIN(val) FROM t) AND (SELECT MAX(val) FROM t) ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 4); // all non-NULL vals between 10 and 50
}

#[test]
fn subquery_in_case() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id, CASE WHEN val > (SELECT AVG(val) FROM t) THEN 'above' ELSE 'below' END \
         FROM t WHERE val IS NOT NULL ORDER BY id",
    );
    // AVG = 27.5: 10→below, 20→below, 30→above, 50→above
    assert_eq!(qr.rows[0][1], Value::Text("below".into()));
    assert_eq!(qr.rows[1][1], Value::Text("below".into()));
    assert_eq!(qr.rows[2][1], Value::Text("above".into()));
    assert_eq!(qr.rows[3][1], Value::Text("above".into()));
}

#[test]
fn subquery_in_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(
        &mut conn,
        "SELECT COALESCE((SELECT val FROM t WHERE id = 999), -1)",
    );
    // No row with id=999 → scalar subquery returns NULL → COALESCE → -1
    assert_eq!(v, Value::Integer(-1));
}

#[test]
fn subquery_in_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST((SELECT COUNT(*) FROM t) AS TEXT)");
    assert_eq!(v, Value::Text("5".into()));
}

// ═══════════════════════════════════════════════════════════════════
// REPLACE / INSTR edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn replace_empty_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Empty search string → return unchanged
    let v = scalar(&mut conn, "SELECT REPLACE('hello', '', 'x')");
    assert_eq!(v, Value::Text("hello".into()));
}

#[test]
fn replace_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT REPLACE(NULL, 'a', 'b')"),
        Value::Null
    );
    assert_eq!(
        scalar(&mut conn, "SELECT REPLACE('abc', NULL, 'b')"),
        Value::Null
    );
    assert_eq!(
        scalar(&mut conn, "SELECT REPLACE('abc', 'a', NULL)"),
        Value::Null
    );
}

#[test]
fn instr_1_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT INSTR('abcdef', 'a')"),
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT INSTR('abcdef', 'f')"),
        Value::Integer(6)
    );
}

// ═══════════════════════════════════════════════════════════════════
// SQRT edge cases
// ═══════════════════════════════════════════════════════════════════

#[test]
fn sqrt_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SQRT(0)"), Value::Real(0.0));
}

#[test]
fn sqrt_perfect_square() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SQRT(144)"), Value::Real(12.0));
    assert_eq!(scalar(&mut conn, "SELECT SQRT(1)"), Value::Real(1.0));
}

// ═══════════════════════════════════════════════════════════════════
// Complex combinations
// ═══════════════════════════════════════════════════════════════════

#[test]
fn complex_case_between_like_combo() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name, \
            CASE \
                WHEN val BETWEEN 1 AND 15 AND name LIKE 'a%' THEN 'low-a' \
                WHEN val BETWEEN 16 AND 100 THEN 'mid-high' \
                ELSE 'other' \
            END AS category \
         FROM t WHERE name IS NOT NULL ORDER BY id",
    );
    assert_eq!(qr.rows[0][1], Value::Text("low-a".into())); // alice, val=10
    assert_eq!(qr.rows[1][1], Value::Text("mid-high".into())); // bob, val=20
    assert_eq!(qr.rows[2][1], Value::Text("mid-high".into())); // charlie, val=30
    assert_eq!(qr.rows[3][1], Value::Text("other".into())); // diana, val=NULL
}

#[test]
fn complex_nested_everything() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Combine: COALESCE + CAST + CASE + UPPER + || + BETWEEN
    let v = scalar(
        &mut conn,
        "SELECT UPPER(COALESCE(CAST(\
            CASE WHEN 5 BETWEEN 1 AND 10 THEN 42 ELSE 0 END \
         AS TEXT), 'none')) || '!'",
    );
    assert_eq!(v, Value::Text("42!".into()));
}

#[test]
fn complex_function_chain_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // WHERE with nested functions
    let qr = query(
        &mut conn,
        "SELECT name FROM t WHERE INSTR(LOWER(COALESCE(name, '')), 'li') > 0 ORDER BY name",
    );
    assert_eq!(qr.rows.len(), 2); // alice, charlie
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
    assert_eq!(qr.rows[1][0], Value::Text("charlie".into()));
}

#[test]
fn complex_all_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (6, 'eve', 60, 6.5)")
        .unwrap();

    let qr = query(
        &mut conn,
        "SELECT UPPER(name), ABS(val - 35), \
            CASE WHEN val > 35 THEN 'high' ELSE 'low' END \
         FROM t WHERE val BETWEEN 10 AND 60 AND name LIKE '%e%' ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 3); // alice(10), charlie(30), eve(60)
    assert_eq!(qr.rows[0][0], Value::Text("ALICE".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(25));
    assert_eq!(qr.rows[0][2], Value::Text("low".into()));
    assert_eq!(qr.rows[1][0], Value::Text("CHARLIE".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(5));
    assert_eq!(qr.rows[1][2], Value::Text("low".into()));
    assert_eq!(qr.rows[2][0], Value::Text("EVE".into()));
    assert_eq!(qr.rows[2][1], Value::Integer(25));
    assert_eq!(qr.rows[2][2], Value::Text("high".into()));

    conn.execute("ROLLBACK").unwrap();

    // After rollback, eve is gone
    let qr2 = query(&mut conn, "SELECT COUNT(*) FROM t");
    assert_eq!(qr2.rows[0][0], Value::Integer(5));
}

#[test]
fn length_unicode() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // LENGTH counts Unicode code points, not bytes
    assert_eq!(
        scalar(&mut conn, "SELECT LENGTH('\u{00e9}')"),
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&mut conn, "SELECT LENGTH('\u{1f600}')"),
        Value::Integer(1)
    );
}

#[test]
fn typeof_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(1)"),
        Value::Text("integer".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(1.5)"),
        Value::Text("real".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF('hi')"),
        Value::Text("text".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(TRUE)"),
        Value::Text("boolean".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(NULL)"),
        Value::Text("null".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(CAST('abc' AS BLOB))"),
        Value::Text("blob".into())
    );
}

#[test]
fn wrong_arg_count_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    assert!(conn.execute("SELECT ABS() FROM t WHERE id = 1").is_err());
    assert!(conn
        .execute("SELECT ABS(1, 2) FROM t WHERE id = 1")
        .is_err());
    assert!(conn.execute("SELECT LENGTH() FROM t WHERE id = 1").is_err());
    assert!(conn.execute("SELECT SQRT() FROM t WHERE id = 1").is_err());
    assert!(conn
        .execute("SELECT SIGN(1, 2) FROM t WHERE id = 1")
        .is_err());
}

#[test]
fn unknown_function_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let err = conn
        .execute("SELECT FOOBAR(1) FROM t WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn hex_blob_input() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE blobtest (id INTEGER NOT NULL PRIMARY KEY, data BLOB)")
            .unwrap(),
    );
    conn.execute("INSERT INTO blobtest VALUES (1, CAST('hello' AS BLOB))")
        .unwrap();

    let v = scalar(&mut conn, "SELECT HEX(data) FROM blobtest WHERE id = 1");
    assert_eq!(v, Value::Text("68656C6C6F".into()));
}

#[test]
fn concat_fn_zero_args() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CONCAT()");
    assert_eq!(v, Value::Text(String::new()));
}

#[test]
fn concat_fn_single_arg() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(&mut conn, "SELECT CONCAT('only')");
    assert_eq!(v, Value::Text("only".into()));
}

// ═══════════════════════════════════════════════════════════════════
// Coverage gap tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn ilike_keyword() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT name FROM t WHERE name ILIKE 'ALICE'");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));

    let qr2 = query(&mut conn, "SELECT name FROM t WHERE name ILIKE 'al%'");
    assert_eq!(qr2.rows.len(), 1);

    let qr3 = query(&mut conn, "SELECT name FROM t WHERE name ILIKE '%LIE'");
    assert_eq!(qr3.rows.len(), 1);
}

#[test]
fn ilike_not() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT name FROM t WHERE name NOT ILIKE 'ALICE'");
    // bob, charlie, diana — (NULL name excluded by NULL LIKE → NULL)
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn concat_op_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Real with fractional part
    let v = scalar(&mut conn, "SELECT 3.14 || ' units'");
    assert_eq!(v, Value::Text("3.14 units".into()));
    // Real with whole value displays as X.0
    let v2 = scalar(&mut conn, "SELECT 5.0 || ' items'");
    assert_eq!(v2, Value::Text("5.0 items".into()));
}

#[test]
fn concat_op_blob() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Blob in value_to_text produces hex
    let v = scalar(&mut conn, "SELECT CAST('AB' AS BLOB) || ' data'");
    assert_eq!(v, Value::Text("4142 data".into()));
}

#[test]
fn cast_integer_to_blob_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SELECT CAST(42 AS BLOB)").unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn cast_bool_to_blob_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SELECT CAST(TRUE AS BLOB)").unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn cast_real_to_blob_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SELECT CAST(3.14 AS BLOB)").unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn cast_negative_real_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Truncates toward zero
    let v = scalar(&mut conn, "SELECT CAST(-3.7 AS INTEGER)");
    assert_eq!(v, Value::Integer(-3));
    let v2 = scalar(&mut conn, "SELECT CAST(-0.9 AS INTEGER)");
    assert_eq!(v2, Value::Integer(0));
}

#[test]
fn cast_blob_to_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // CAST blob to text uses value_to_text (hex encoding)
    let v = scalar(&mut conn, "SELECT CAST(CAST('hi' AS BLOB) AS TEXT)");
    assert_eq!(v, Value::Text("6869".into()));
}

#[test]
fn cast_real_to_boolean_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn.execute("SELECT CAST(3.14 AS BOOLEAN)").unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn cast_blob_to_integer_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("SELECT CAST(CAST('x' AS BLOB) AS INTEGER)")
        .unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn between_real_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Real value between integer bounds
    let v = scalar(&mut conn, "SELECT 3.5 BETWEEN 2 AND 4");
    assert_eq!(v, Value::Boolean(true));
    // Integer value between real bounds
    let v2 = scalar(&mut conn, "SELECT 3 BETWEEN 2.5 AND 4.5");
    assert_eq!(v2, Value::Boolean(true));
    // Just outside range
    let v3 = scalar(&mut conn, "SELECT 4.5 BETWEEN 2 AND 4");
    assert_eq!(v3, Value::Boolean(false));
}

#[test]
fn case_all_null_conditions_no_else() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v = scalar(
        &mut conn,
        "SELECT CASE WHEN NULL THEN 'x' WHEN NULL THEN 'y' END",
    );
    assert_eq!(v, Value::Null);
}

#[test]
fn case_simple_null_operand() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // NULL doesn't match NULL in simple CASE
    let v = scalar(
        &mut conn,
        "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END",
    );
    assert_eq!(v, Value::Text("no match".into()));
}

#[test]
fn select_no_from_count_star_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // COUNT(*) without FROM is unsupported (non-aggregate context)
    let err = conn.execute("SELECT COUNT(*)").unwrap_err();
    assert!(matches!(err, SqlError::Unsupported(_)));
}

#[test]
fn concat_fn_mixed_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // CONCAT with 3+ mixed-type args, NULL treated as empty
    let v = scalar(&mut conn, "SELECT CONCAT('x', 42, NULL, TRUE, 3.14)");
    assert_eq!(v, Value::Text("x42TRUE3.14".into()));
}

#[test]
fn sqrt_negative_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&mut conn, "SELECT SQRT(-1.0)"), Value::Null);
    assert_eq!(scalar(&mut conn, "SELECT SQRT(-100)"), Value::Null);
}

#[test]
fn coalesce_error_not_short_circuited() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // First arg is NULL, so second arg IS evaluated → division by zero
    let err = conn.execute("SELECT COALESCE(NULL, 1/0)").unwrap_err();
    assert!(matches!(err, SqlError::DivisionByZero));
}

#[test]
fn like_empty_escape_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("SELECT 'test' LIKE 'test' ESCAPE ''")
        .unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

#[test]
fn random_returns_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    let v1 = scalar(&mut conn, "SELECT RANDOM()");
    assert!(matches!(v1, Value::Integer(_)));
    let v2 = scalar(&mut conn, "SELECT RANDOM()");
    assert!(matches!(v2, Value::Integer(_)));
    // Extremely unlikely to be equal
    if let (Value::Integer(a), Value::Integer(b)) = (&v1, &v2) {
        assert_ne!(a, b, "two RANDOM() calls should produce different values");
    }
}

#[test]
fn concat_op_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    // Boolean
    let v = scalar(&mut conn, "SELECT TRUE || FALSE");
    assert_eq!(v, Value::Text("TRUEFALSE".into()));
    // NULL propagation
    let v2 = scalar(&mut conn, "SELECT 'a' || NULL || 'b'");
    assert_eq!(v2, Value::Null);
}

#[test]
fn value_to_text_via_cast_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(42 AS TEXT)"),
        Value::Text("42".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(3.14 AS TEXT)"),
        Value::Text("3.14".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(TRUE AS TEXT)"),
        Value::Text("TRUE".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT CAST(FALSE AS TEXT)"),
        Value::Text("FALSE".into())
    );
    assert_eq!(scalar(&mut conn, "SELECT CAST(NULL AS TEXT)"), Value::Null);
}
