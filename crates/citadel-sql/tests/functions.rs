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
    assert_eq!(qr.rows.len(), 1, "expected 1 row, got {}", qr.rows.len());
    assert_eq!(qr.rows[0].len(), 1, "expected 1 column");
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

// ── BETWEEN ─────────────────────────────────────────────────────────

#[test]
fn between_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN 15 AND 35 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
}

#[test]
fn between_inclusive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN 10 AND 30 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[2][0], Value::Integer(3));
}

#[test]
fn not_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val NOT BETWEEN 15 AND 35 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(5));
}

#[test]
fn between_null_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // NULL val BETWEEN 15 AND 35 → NULL → filtered out
    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN 15 AND 35 ORDER BY id",
    );
    for row in &qr.rows {
        assert_ne!(row[0], Value::Integer(4));
    }
}

#[test]
fn between_reversed_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // BETWEEN 30 AND 10 → always FALSE (no auto-swap)
    let qr = query(&mut conn, "SELECT id FROM t WHERE val BETWEEN 30 AND 10");
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn between_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name FROM t WHERE name BETWEEN 'b' AND 'd' ORDER BY name",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Text("bob".into()));
    assert_eq!(qr.rows[1][0], Value::Text("charlie".into()));
}

// ── LIKE ────────────────────────────────────────────────────────────

#[test]
fn like_percent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT name FROM t WHERE name LIKE 'a%'");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn like_underscore() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT name FROM t WHERE name LIKE 'bo_'");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("bob".into()));
}

#[test]
fn like_case_insensitive_ascii() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT name FROM t WHERE name LIKE 'ALICE'");
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn not_like() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name FROM t WHERE name NOT LIKE '%ob' ORDER BY name",
    );
    // alice, charlie, diana
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn like_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // NULL LIKE '%' → NULL → not included
    let qr = query(&mut conn, "SELECT id FROM t WHERE name LIKE '%'");
    assert_eq!(qr.rows.len(), 4); // all except id=5 which has NULL name
}

#[test]
fn like_escape() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE esc (id INTEGER NOT NULL PRIMARY KEY, txt TEXT)")
            .unwrap(),
    );
    conn.execute("INSERT INTO esc VALUES (1, '100%')").unwrap();
    conn.execute("INSERT INTO esc VALUES (2, '100 percent')")
        .unwrap();
    conn.execute("INSERT INTO esc VALUES (3, '1000')").unwrap();

    let qr = query(
        &mut conn,
        "SELECT id FROM esc WHERE txt LIKE '100!%' ESCAPE '!'",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn like_complex_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // Pattern: starts with any char, has 'l' somewhere, ends with 'e'
    let qr = query(&mut conn, "SELECT name FROM t WHERE name LIKE '%l%e'");
    assert_eq!(qr.rows.len(), 2); // alice, charlie
}

// ── CASE WHEN ───────────────────────────────────────────────────────

#[test]
fn case_searched() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn,
        "SELECT id, CASE WHEN val > 25 THEN 'high' WHEN val > 15 THEN 'mid' ELSE 'low' END AS label \
         FROM t WHERE val IS NOT NULL ORDER BY id"
    );
    assert_eq!(qr.rows.len(), 4);
    assert_eq!(qr.rows[0][1], Value::Text("low".into())); // val=10
    assert_eq!(qr.rows[1][1], Value::Text("mid".into())); // val=20
    assert_eq!(qr.rows[2][1], Value::Text("high".into())); // val=30
    assert_eq!(qr.rows[3][1], Value::Text("high".into())); // val=50
}

#[test]
fn case_simple() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT CASE name WHEN 'alice' THEN 'A' WHEN 'bob' THEN 'B' ELSE '?' END \
         FROM t WHERE name IS NOT NULL ORDER BY id",
    );
    assert_eq!(qr.rows[0][0], Value::Text("A".into()));
    assert_eq!(qr.rows[1][0], Value::Text("B".into()));
    assert_eq!(qr.rows[2][0], Value::Text("?".into()));
}

#[test]
fn case_no_else() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT CASE WHEN val > 100 THEN 'big' END FROM t WHERE id = 1",
    );
    assert_eq!(qr.rows[0][0], Value::Null);
}

#[test]
fn case_null_operand() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // NULL = NULL is not TRUE, so CASE NULL WHEN NULL → no match → ELSE
    let qr = query(
        &mut conn,
        "SELECT CASE val WHEN NULL THEN 'matched' ELSE 'no match' END FROM t WHERE id = 4",
    );
    assert_eq!(qr.rows[0][0], Value::Text("no match".into()));
}

// ── COALESCE ────────────────────────────────────────────────────────

#[test]
fn coalesce_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT COALESCE(val, -1) FROM t WHERE id = 4");
    assert_eq!(qr.rows[0][0], Value::Integer(-1));
}

#[test]
fn coalesce_first_non_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT COALESCE(NULL, NULL, 42)");
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn coalesce_all_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(&mut conn, "SELECT COALESCE(NULL, NULL)");
    assert_eq!(qr.rows[0][0], Value::Null);
}

// ── NULLIF ──────────────────────────────────────────────────────────

#[test]
fn nullif_equal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT NULLIF(10, 10)");
    assert_eq!(v, Value::Null);
}

#[test]
fn nullif_not_equal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT NULLIF(10, 20)");
    assert_eq!(v, Value::Integer(10));
}

// ── IIF ─────────────────────────────────────────────────────────────

#[test]
fn iif_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT IIF(1 > 0, 'yes', 'no')");
    assert_eq!(v, Value::Text("yes".into()));
}

#[test]
fn iif_false() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT IIF(1 > 100, 'yes', 'no')");
    assert_eq!(v, Value::Text("no".into()));
}

// ── CAST ────────────────────────────────────────────────────────────

#[test]
fn cast_text_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST('42' AS INTEGER)");
    assert_eq!(v, Value::Integer(42));
}

#[test]
fn cast_integer_to_text() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST(42 AS TEXT)");
    assert_eq!(v, Value::Text("42".into()));
}

#[test]
fn cast_real_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST(3.7 AS INTEGER)");
    assert_eq!(v, Value::Integer(3));
}

#[test]
fn cast_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST(NULL AS INTEGER)");
    assert_eq!(v, Value::Null);
}

#[test]
fn cast_text_to_boolean() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CAST('true' AS BOOLEAN)");
    assert_eq!(v, Value::Boolean(true));
    let v2 = scalar(&mut conn, "SELECT CAST('false' AS BOOLEAN)");
    assert_eq!(v2, Value::Boolean(false));
}

#[test]
fn cast_invalid_text_to_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let err = conn
        .execute("SELECT CAST('abc' AS INTEGER) FROM t WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::InvalidValue(_)));
}

// ── String concatenation (||) ───────────────────────────────────────

#[test]
fn concat_operator() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT 'hello' || ' ' || 'world'");
    assert_eq!(v, Value::Text("hello world".into()));
}

#[test]
fn concat_operator_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // || propagates NULL per SQL standard (unlike CONCAT() which treats NULL as empty)
    let v = scalar(&mut conn, "SELECT 'a' || NULL || 'b'");
    assert_eq!(v, Value::Null);
}

#[test]
fn concat_operator_with_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name || ' #' || CAST(id AS TEXT) FROM t WHERE id = 1",
    );
    assert_eq!(qr.rows[0][0], Value::Text("alice #1".into()));
}

// ── String functions ────────────────────────────────────────────────

#[test]
fn fn_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT LENGTH('hello')");
    assert_eq!(v, Value::Integer(5));
}

#[test]
fn fn_length_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT LENGTH(NULL)");
    assert_eq!(v, Value::Null);
}

#[test]
fn fn_upper_lower() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT UPPER('hello')");
    assert_eq!(v, Value::Text("HELLO".into()));

    let v2 = scalar(&mut conn, "SELECT LOWER('HELLO')");
    assert_eq!(v2, Value::Text("hello".into()));
}

#[test]
fn fn_substr_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT SUBSTR('hello', 2, 3)");
    assert_eq!(v, Value::Text("ell".into()));
}

#[test]
fn fn_substr_no_length() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT SUBSTR('hello', 3)");
    assert_eq!(v, Value::Text("llo".into()));
}

#[test]
fn fn_substr_negative_start() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // Negative start counts from right
    let v = scalar(&mut conn, "SELECT SUBSTR('hello', -2)");
    assert_eq!(v, Value::Text("lo".into()));
}

#[test]
fn fn_trim() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT TRIM('  hello  ')");
    assert_eq!(v, Value::Text("hello".into()));
}

#[test]
fn fn_trim_chars() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT TRIM('xxhelloxx', 'x')");
    assert_eq!(v, Value::Text("hello".into()));
}

#[test]
fn fn_ltrim_rtrim() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT LTRIM('  hello  ')");
    assert_eq!(v, Value::Text("hello  ".into()));

    let v2 = scalar(&mut conn, "SELECT RTRIM('  hello  ')");
    assert_eq!(v2, Value::Text("  hello".into()));
}

#[test]
fn fn_replace() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT REPLACE('hello world', 'world', 'rust')");
    assert_eq!(v, Value::Text("hello rust".into()));
}

#[test]
fn fn_replace_all_occurrences() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT REPLACE('aabaa', 'a', 'x')");
    assert_eq!(v, Value::Text("xxbxx".into()));
}

#[test]
fn fn_instr() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT INSTR('hello world', 'world')");
    assert_eq!(v, Value::Integer(7));
}

#[test]
fn fn_instr_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT INSTR('hello', 'xyz')");
    assert_eq!(v, Value::Integer(0));
}

#[test]
fn fn_concat() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    // CONCAT treats NULL as empty string
    let v = scalar(&mut conn, "SELECT CONCAT('a', NULL, 'b')");
    assert_eq!(v, Value::Text("ab".into()));
}

// ── Math functions ──────────────────────────────────────────────────

#[test]
fn fn_abs_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT ABS(-42)");
    assert_eq!(v, Value::Integer(42));
}

#[test]
fn fn_abs_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT ABS(-3.15)");
    assert_eq!(v, Value::Real(3.15));
}

#[test]
fn fn_abs_min_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE minval (id INTEGER NOT NULL PRIMARY KEY, v INTEGER NOT NULL)")
            .unwrap(),
    );
    // i64::MIN = -9223372036854775808 = -9223372036854775807 - 1
    conn.execute("INSERT INTO minval VALUES (1, -9223372036854775807 - 1)")
        .unwrap();

    let err = conn
        .execute("SELECT ABS(v) FROM minval WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::IntegerOverflow));
}

#[test]
fn fn_round() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT ROUND(3.15159, 2)");
    assert_eq!(v, Value::Real(3.15));
}

#[test]
fn fn_round_no_places() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT ROUND(3.7)");
    assert_eq!(v, Value::Real(4.0));
}

#[test]
fn fn_ceil_floor() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CEIL(3.2)");
    assert_eq!(v, Value::Integer(4));

    let v2 = scalar(&mut conn, "SELECT FLOOR(3.8)");
    assert_eq!(v2, Value::Integer(3));
}

#[test]
fn fn_ceil_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT CEIL(5)");
    assert_eq!(v, Value::Integer(5));
}

#[test]
fn fn_sign() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    assert_eq!(scalar(&mut conn, "SELECT SIGN(-10)"), Value::Integer(-1));
    assert_eq!(scalar(&mut conn, "SELECT SIGN(0)"), Value::Integer(0));
    assert_eq!(scalar(&mut conn, "SELECT SIGN(10)"), Value::Integer(1));
}

#[test]
fn fn_sqrt() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT SQRT(16)");
    assert_eq!(v, Value::Real(4.0));
}

#[test]
fn fn_sqrt_negative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT SQRT(-1)");
    assert_eq!(v, Value::Null);
}

#[test]
fn fn_random() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT RANDOM()");
    assert!(matches!(v, Value::Integer(_)));
}

// ── Type/utility functions ──────────────────────────────────────────

#[test]
fn fn_typeof() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(42)"),
        Value::Text("integer".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(3.14)"),
        Value::Text("real".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF('hi')"),
        Value::Text("text".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(NULL)"),
        Value::Text("null".into())
    );
    assert_eq!(
        scalar(&mut conn, "SELECT TYPEOF(TRUE)"),
        Value::Text("boolean".into())
    );
}

#[test]
fn fn_hex() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT HEX('ABC')");
    assert_eq!(v, Value::Text("414243".into()));
}

#[test]
fn fn_hex_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    assert_eq!(scalar(&mut conn, "SELECT HEX(NULL)"), Value::Null);
}

// ── Scalar MIN/MAX (2-arg) ──────────────────────────────────────────

#[test]
fn fn_min_scalar() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT MIN(10, 20)");
    assert_eq!(v, Value::Integer(10));
}

#[test]
fn fn_max_scalar() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT MAX(10, 20)");
    assert_eq!(v, Value::Integer(20));
}

#[test]
fn fn_min_max_scalar_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    assert_eq!(scalar(&mut conn, "SELECT MIN(NULL, 5)"), Value::Integer(5));
    assert_eq!(scalar(&mut conn, "SELECT MAX(3, NULL)"), Value::Integer(3));
}

// ── Aggregate MIN/MAX (1-arg) still works ───────────────────────────

#[test]
fn aggregate_min_max_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT MIN(val) FROM t");
    assert_eq!(v, Value::Integer(10));

    let v2 = scalar(&mut conn, "SELECT MAX(val) FROM t");
    assert_eq!(v2, Value::Integer(50));
}

// ── Combinations ────────────────────────────────────────────────────

#[test]
fn case_with_between() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id, CASE WHEN val BETWEEN 1 AND 15 THEN 'low' \
                         WHEN val BETWEEN 16 AND 35 THEN 'mid' \
                         ELSE 'high' END \
         FROM t WHERE val IS NOT NULL ORDER BY id",
    );
    assert_eq!(qr.rows[0][1], Value::Text("low".into())); // 10
    assert_eq!(qr.rows[1][1], Value::Text("mid".into())); // 20
    assert_eq!(qr.rows[2][1], Value::Text("mid".into())); // 30
    assert_eq!(qr.rows[3][1], Value::Text("high".into())); // 50
}

#[test]
fn coalesce_with_cast() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT COALESCE(NULL, CAST(42 AS TEXT))");
    assert_eq!(v, Value::Text("42".into()));
}

#[test]
fn function_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE LENGTH(name) > 4 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 3); // alice (5), charlie (7), diana (5)
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(3));
    assert_eq!(qr.rows[2][0], Value::Integer(4));
}

#[test]
fn function_in_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT name FROM t WHERE name IS NOT NULL ORDER BY LENGTH(name)",
    );
    assert_eq!(qr.rows[0][0], Value::Text("bob".into())); // 3
    assert_eq!(qr.rows[1][0], Value::Text("alice".into())); // 5
    assert_eq!(qr.rows[2][0], Value::Text("diana".into())); // 5
    assert_eq!(qr.rows[3][0], Value::Text("charlie".into())); // 7
}

#[test]
fn nested_functions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let v = scalar(&mut conn, "SELECT UPPER(SUBSTR('hello world', 1, 5))");
    assert_eq!(v, Value::Text("HELLO".into()));
}

#[test]
fn case_in_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT CASE WHEN SUM(val) > 100 THEN 'big' ELSE 'small' END FROM t",
    );
    // SUM(10+20+30+50) = 110 > 100
    assert_eq!(qr.rows[0][0], Value::Text("big".into()));
}

#[test]
fn function_with_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(conn.execute(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, category TEXT NOT NULL, amount INTEGER NOT NULL)"
    ).unwrap());
    conn.execute("INSERT INTO orders VALUES (1, 'food', 100)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 'food', 200)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (3, 'drink', 50)")
        .unwrap();

    let qr = query(
        &mut conn,
        "SELECT UPPER(category), SUM(amount) FROM orders GROUP BY category ORDER BY category",
    );
    assert_eq!(qr.rows[0][0], Value::Text("DRINK".into()));
    assert_eq!(qr.rows[0][1], Value::Integer(50));
    assert_eq!(qr.rows[1][0], Value::Text("FOOD".into()));
    assert_eq!(qr.rows[1][1], Value::Integer(300));
}

#[test]
fn like_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    assert_ok(
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap(),
    );
    assert_ok(conn.execute(
        "CREATE TABLE emails (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, addr TEXT NOT NULL)"
    ).unwrap());

    conn.execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    conn.execute("INSERT INTO emails VALUES (1, 1, 'alice@gmail.com')")
        .unwrap();
    conn.execute("INSERT INTO emails VALUES (2, 2, 'bob@yahoo.com')")
        .unwrap();

    let qr = query(
        &mut conn,
        "SELECT u.name, e.addr FROM users u JOIN emails e ON u.id = e.user_id \
         WHERE e.addr LIKE '%gmail%'",
    );
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("alice".into()));
}

#[test]
fn between_with_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup(&mut conn);

    let qr = query(
        &mut conn,
        "SELECT id FROM t WHERE val BETWEEN (SELECT MIN(val) FROM t) AND 20 ORDER BY id",
    );
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
}

#[test]
fn functions_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    {
        let mut conn = Connection::open(&db).unwrap();
        assert_ok(
            conn.execute("CREATE TABLE p (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
                .unwrap(),
        );
        conn.execute("INSERT INTO p VALUES (1, 'hello')").unwrap();
    }
    drop(db);

    let db2 = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let mut conn = Connection::open(&db2).unwrap();
    let v = scalar(&mut conn, "SELECT UPPER(name) FROM p WHERE id = 1");
    assert_eq!(v, Value::Text("HELLO".into()));
}
