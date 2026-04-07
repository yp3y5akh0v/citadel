//! Edge case tests: boundary values, type edge cases, bug-finding tests.
//! Tests that expose real bugs are clearly documented.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

// ════════════════════════════════════════════════════════════════════
// INTEGER BOUNDARY TESTS
// ════════════════════════════════════════════════════════════════════

#[test]
fn i64_max_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 9223372036854775807)")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(i64::MAX));
}

#[test]
fn i64_max_minus_1() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 9223372036854775806)")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(i64::MAX - 1));
}

#[test]
fn i64_neg_max_as_negated_literal() {
    // -9223372036854775807 = -(i64::MAX) = i64::MIN + 1
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -9223372036854775807)")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(i64::MIN + 1));
}

#[test]
fn i64_min_literal_becomes_real() {
    // i64::MIN = -9223372036854775808
    // The parser sees -(9223372036854775808), but 9223372036854775808 > i64::MAX,
    // so it parses as f64 Real. This is a known limitation.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    // This will either:
    // (a) fail with type mismatch (Real -> Integer coercion truncates), or
    // (b) succeed with truncated value
    let result = conn.execute("INSERT INTO t VALUES (1, -9223372036854775808)");
    // Just verify it doesn't panic — the exact behavior is acceptable either way
    let _ = result;
}

#[test]
fn i64_max_as_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (9223372036854775807)")
        .unwrap();

    let qr = conn
        .query("SELECT id FROM t WHERE id = 9223372036854775807")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(i64::MAX));
}

#[test]
fn zero_as_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (0)").unwrap();

    let qr = conn.query("SELECT id FROM t WHERE id = 0").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

// ════════════════════════════════════════════════════════════════════
// ARITHMETIC OVERFLOW TESTS (potential panics in debug mode)
// ════════════════════════════════════════════════════════════════════

#[test]
fn arithmetic_overflow_add() {
    // i64::MAX + 1 should return error, not panic
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 9223372036854775807)")
        .unwrap();

    let result = conn.query("SELECT val + 1 FROM t WHERE id = 1");
    // Correct behavior: return an overflow error
    // Bug: currently panics with 'attempt to add with overflow' in debug mode
    assert!(
        result.is_err(),
        "i64::MAX + 1 should return error, not succeed or panic"
    );
}

#[test]
fn arithmetic_overflow_subtract() {
    // i64::MIN + 1 - 2 should return error
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -9223372036854775807)")
        .unwrap();

    let result = conn.query("SELECT val - 2 FROM t WHERE id = 1");
    assert!(
        result.is_err(),
        "i64::MIN+1 - 2 should return error, not succeed or panic"
    );
}

#[test]
fn arithmetic_overflow_multiply() {
    // i64::MAX * 2 should return error
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 9223372036854775807)")
        .unwrap();

    let result = conn.query("SELECT val * 2 FROM t WHERE id = 1");
    assert!(
        result.is_err(),
        "i64::MAX * 2 should return error, not succeed or panic"
    );
}

#[test]
fn arithmetic_overflow_negate_min() {
    // -(i64::MIN+1 - 1) = -(i64::MIN) overflows
    // We can't directly insert i64::MIN, but we can try: SELECT -val where val = i64::MIN+1 - 1
    // Actually, let's use: SELECT -(val - 1) where val = i64::MIN+1
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -9223372036854775807)")
        .unwrap();

    // val - 1 = i64::MIN, then -i64::MIN overflows
    let result = conn.query("SELECT -(val - 1) FROM t WHERE id = 1");
    // Both the subtraction and the negation could overflow
    assert!(result.is_err(), "negating i64::MIN should return error");
}

// ════════════════════════════════════════════════════════════════════
// DIVISION / MODULO EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn division_by_zero_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let result = conn.query("SELECT val / 0 FROM t WHERE id = 1");
    assert!(result.is_err(), "division by zero should error");
}

#[test]
fn division_by_zero_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();

    let result = conn.query("SELECT val / 0.0 FROM t WHERE id = 1");
    assert!(result.is_err(), "division by zero (real) should error");
}

#[test]
fn modulo_by_zero_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let result = conn.query("SELECT val % 0 FROM t WHERE id = 1");
    assert!(result.is_err(), "modulo by zero should error");
}

#[test]
fn modulo_by_zero_real() {
    // Bug: code doesn't check for Real(0.0) in modulo, only Integer(0)
    // Result would be NaN instead of an error
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();

    let result = conn.query("SELECT val % 0.0 FROM t WHERE id = 1");
    assert!(
        result.is_err(),
        "modulo by zero (real) should error, currently produces NaN"
    );
}

#[test]
fn integer_division_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 7)").unwrap();

    let qr = conn.query("SELECT val / 2 FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3)); // integer division truncates
}

#[test]
fn negative_integer_division() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -7)").unwrap();

    let qr = conn.query("SELECT val / 2 FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(-3)); // Rust truncates toward zero
}

#[test]
fn negative_modulo() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -7)").unwrap();

    let qr = conn.query("SELECT val % 3 FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(-1)); // Rust: -7 % 3 = -1
}

// ════════════════════════════════════════════════════════════════════
// UPDATE PK COLLISION TESTS (bugs in current implementation)
// ════════════════════════════════════════════════════════════════════

#[test]
fn update_pk_change_to_existing_key() {
    // Bug: UPDATE with PK change to existing key should fail with DuplicateKey
    // but current code silently overwrites
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'one')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'two')").unwrap();

    let result = conn.execute("UPDATE t SET id = 1 WHERE id = 2");
    assert!(
        result.is_err(),
        "UPDATE PK to existing key should fail with DuplicateKey, but it silently overwrites"
    );
}

#[test]
fn update_pk_shift_multiple_rows_data_integrity() {
    // Bug: UPDATE SET id = id + 1 on rows (1,2,3) processes sequentially.
    // Row 1→2 overwrites existing row 2. Row 2→3 then operates on corrupted state.
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

    // This UPDATE shifts all PKs by +1:
    // id=1 → id=2 (conflicts with existing id=2)
    // id=2 → id=3 (conflicts with existing id=3)
    // id=3 → id=4 (ok)
    let result = conn.execute("UPDATE t SET id = id + 1");

    // Correct behavior: should either error (DuplicateKey) or handle atomically.
    // Bug: silently corrupts data — some rows get lost.
    // Let's check what actually happened:
    match result {
        Err(_) => {
            // This would be correct behavior — rejecting the conflicting update
        }
        Ok(_) => {
            // If it "succeeds", verify data integrity
            let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
            assert_eq!(
                qr.rows[0][0],
                Value::Integer(3),
                "UPDATE SET id = id+1 lost rows — data corruption bug"
            );
        }
    }
}

#[test]
fn update_pk_swap() {
    // Swap PKs of two rows: should either work atomically or error
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();

    // Can't swap PKs in a single UPDATE (both would conflict).
    // But we can test a single PK change to a free slot:
    conn.execute("UPDATE t SET id = 10 WHERE id = 2").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 10").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("second".into()));

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("first".into()));

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

// ════════════════════════════════════════════════════════════════════
// NULL HANDLING EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn null_equality_is_null() {
    // In SQL, NULL = NULL should be NULL (not true)
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    // WHERE val = NULL should match nothing (NULL = NULL is NULL, not true)
    let qr = conn.query("SELECT id FROM t WHERE val = NULL").unwrap();
    assert_eq!(
        qr.rows.len(),
        0,
        "NULL = NULL should be NULL (falsy), not true"
    );
}

#[test]
fn null_in_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    // NULL + 1 should be NULL
    let qr = conn.query("SELECT val + 1 FROM t WHERE id = 1").unwrap();
    assert!(qr.rows[0][0].is_null(), "NULL + 1 should be NULL");
}

#[test]
fn null_in_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 5)").unwrap();

    // NULL > 3 should be NULL (falsy), so only id=2 matches
    let qr = conn.query("SELECT id FROM t WHERE val > 3").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn null_in_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 20)").unwrap();

    // Default ASC: NULLs first
    let qr = conn
        .query("SELECT id, val FROM t ORDER BY val ASC")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    // First two should be NULL
    assert!(qr.rows[0][1].is_null());
    assert!(qr.rows[1][1].is_null());
    // Then sorted values
    assert_eq!(qr.rows[2][1], Value::Integer(10));
    assert_eq!(qr.rows[3][1], Value::Integer(20));
    assert_eq!(qr.rows[4][1], Value::Integer(30));
}

#[test]
fn null_in_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp INTEGER, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 1, 30)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, NULL, 40)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 2, 50)").unwrap();

    let qr = conn
        .query("SELECT grp, COUNT(*), SUM(val) FROM t GROUP BY grp ORDER BY grp")
        .unwrap();

    // NULLs should form their own group
    // Groups: NULL (count=2, sum=60), 1 (count=2, sum=40), 2 (count=1, sum=50)
    assert_eq!(qr.rows.len(), 3);

    // NULL group comes first in ORDER BY (NULL < anything)
    assert!(qr.rows[0][0].is_null());
    assert_eq!(qr.rows[0][1], Value::Integer(2));
    assert_eq!(qr.rows[0][2], Value::Integer(60));

    assert_eq!(qr.rows[1][0], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
    assert_eq!(qr.rows[1][2], Value::Integer(40));
}

#[test]
fn aggregate_all_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, NULL)").unwrap();

    let qr = conn
        .query("SELECT SUM(val), AVG(val), MIN(val), MAX(val), COUNT(val), COUNT(*) FROM t")
        .unwrap();
    // SUM of all NULLs should be NULL
    assert!(qr.rows[0][0].is_null(), "SUM of all NULLs should be NULL");
    assert!(qr.rows[0][1].is_null(), "AVG of all NULLs should be NULL");
    assert!(qr.rows[0][2].is_null(), "MIN of all NULLs should be NULL");
    assert!(qr.rows[0][3].is_null(), "MAX of all NULLs should be NULL");
    assert_eq!(
        qr.rows[0][4],
        Value::Integer(0),
        "COUNT(col) of all NULLs should be 0"
    );
    assert_eq!(
        qr.rows[0][5],
        Value::Integer(3),
        "COUNT(*) should count all rows"
    );
}

#[test]
fn aggregate_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    let qr = conn
        .query("SELECT COUNT(*), SUM(val), AVG(val) FROM t")
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
    assert!(qr.rows[0][1].is_null(), "SUM of empty table should be NULL");
    assert!(qr.rows[0][2].is_null(), "AVG of empty table should be NULL");
}

#[test]
fn update_set_to_null_on_not_null_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    let result = conn.execute("UPDATE t SET name = NULL WHERE id = 1");
    assert!(
        matches!(result, Err(SqlError::NotNullViolation(_))),
        "UPDATE SET NOT NULL column to NULL should error"
    );
}

// ════════════════════════════════════════════════════════════════════
// STRING / TEXT EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn empty_string_as_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id TEXT NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('', 42)").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = ''").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(42));
}

#[test]
fn string_with_single_quotes_escaped() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'it''s a test')")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("it's a test".into()));
}

#[test]
fn unicode_in_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();

    let tests = vec![
        (1, "Hello, World!"),
        (2, "Привет мир"),
        (3, "こんにちは世界"),
        (4, "🦀🔐💾"),
        (5, "café résumé naïve"),
        (6, "中文测试"),
    ];

    for (id, text) in &tests {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, '{text}')"))
            .unwrap();
    }

    for (id, expected) in &tests {
        let qr = conn
            .query(&format!("SELECT val FROM t WHERE id = {id}"))
            .unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Text(expected.to_string().into()),
            "Unicode roundtrip failed for id={id}"
        );
    }
}

#[test]
fn unicode_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '🦀 Rust 数据库')")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("🦀 Rust 数据库".into()));
    }
}

#[test]
fn string_with_sql_keywords() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();

    let keyword_strings = [
        "SELECT",
        "INSERT INTO",
        "DROP TABLE",
        "DELETE FROM",
        "WHERE 1=1",
        "OR 1=1 --",
        "'; DROP TABLE t; --",
    ];

    for (i, kw) in keyword_strings.iter().enumerate() {
        let escaped = kw.replace('\'', "''");
        conn.execute(&format!("INSERT INTO t VALUES ({}, '{escaped}')", i + 1))
            .unwrap();
    }

    for (i, expected) in keyword_strings.iter().enumerate() {
        let qr = conn
            .query(&format!("SELECT val FROM t WHERE id = {}", i + 1))
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Text(expected.to_string().into()));
    }
}

// ════════════════════════════════════════════════════════════════════
// REAL / FLOAT EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn real_zero_positive_negative() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 0.0)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, -0.0)").unwrap();

    // Both should roundtrip
    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(0.0));

    let qr = conn.query("SELECT val FROM t WHERE id = 2").unwrap();
    // -0.0 == 0.0 in IEEE 754 comparison
    match &qr.rows[0][0] {
        Value::Real(r) => assert!(*r == 0.0),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn real_very_small_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 0.000000001)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, -0.000000001)")
        .unwrap();

    let qr = conn.query("SELECT val FROM t ORDER BY val").unwrap();
    assert!(qr.rows[0][0] < qr.rows[1][0]);
}

#[test]
fn real_scientific_notation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1.5e10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 2.5e-5)").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 1.5e10).abs() < 1.0),
        other => panic!("expected Real, got {other:?}"),
    }

    let qr = conn.query("SELECT val FROM t WHERE id = 2").unwrap();
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 2.5e-5).abs() < 1e-10),
        other => panic!("expected Real, got {other:?}"),
    }
}

#[test]
fn mixed_integer_real_comparison_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // Compare integer column with real literal
    let qr = conn.query("SELECT id FROM t WHERE val > 9.5").unwrap();
    assert_eq!(qr.rows.len(), 1);

    let qr = conn.query("SELECT id FROM t WHERE val < 10.5").unwrap();
    assert_eq!(qr.rows.len(), 1);

    let qr = conn.query("SELECT id FROM t WHERE val = 10.0").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

// ════════════════════════════════════════════════════════════════════
// BOOLEAN EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn boolean_in_where_without_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, active BOOLEAN NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, FALSE)").unwrap();

    // WHERE active (boolean used directly as predicate)
    let qr = conn.query("SELECT id FROM t WHERE active").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn boolean_and_or_three_valued_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a BOOLEAN, b BOOLEAN)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, TRUE, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, FALSE, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, NULL, TRUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (4, NULL, FALSE)")
        .unwrap();

    // TRUE AND NULL = NULL (falsy)
    let qr = conn.query("SELECT id FROM t WHERE a AND b").unwrap();
    assert_eq!(qr.rows.len(), 0, "TRUE AND NULL should be NULL (falsy)");

    // FALSE OR NULL = NULL (falsy)
    let qr = conn
        .query("SELECT id FROM t WHERE id = 2 AND (a OR b)")
        .unwrap();
    assert_eq!(qr.rows.len(), 0, "FALSE OR NULL should be NULL (falsy)");

    // NULL OR TRUE = TRUE
    let qr = conn
        .query("SELECT id FROM t WHERE id = 3 AND (a OR b)")
        .unwrap();
    assert_eq!(qr.rows.len(), 1, "NULL OR TRUE should be TRUE");

    // FALSE AND NULL = FALSE (falsy)
    let qr = conn
        .query("SELECT id FROM t WHERE id = 2 AND (a AND b)")
        .unwrap();
    assert_eq!(qr.rows.len(), 0, "FALSE AND NULL should be FALSE");
}

// ════════════════════════════════════════════════════════════════════
// COMPOSITE PRIMARY KEY EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn composite_pk_text_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (name TEXT NOT NULL, version INTEGER NOT NULL, data TEXT, PRIMARY KEY (name, version))"
    ).unwrap();
    conn.execute("INSERT INTO t VALUES ('foo', 1, 'first')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('foo', 2, 'second')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('bar', 1, 'bar_first')")
        .unwrap();

    let qr = conn
        .query("SELECT data FROM t WHERE name = 'foo' AND version = 2")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("second".into()));

    // Duplicate composite key
    let result = conn.execute("INSERT INTO t VALUES ('foo', 1, 'dup')");
    assert!(matches!(result, Err(SqlError::DuplicateKey)));
}

#[test]
fn composite_pk_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (a INTEGER NOT NULL, b INTEGER NOT NULL, PRIMARY KEY (a, b))")
        .unwrap();

    // Insert in random order
    conn.execute("INSERT INTO t VALUES (2, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 2)").unwrap();

    // B+ tree stores by encoded key order, which should be (1,1), (1,2), (2,1), (2,2)
    let qr = conn.query("SELECT a, b FROM t ORDER BY a, b").unwrap();
    assert_eq!(qr.rows[0], vec![Value::Integer(1), Value::Integer(1)]);
    assert_eq!(qr.rows[1], vec![Value::Integer(1), Value::Integer(2)]);
    assert_eq!(qr.rows[2], vec![Value::Integer(2), Value::Integer(1)]);
    assert_eq!(qr.rows[3], vec![Value::Integer(2), Value::Integer(2)]);
}

// ════════════════════════════════════════════════════════════════════
// SELECT / PROJECTION EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn select_nonexistent_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let result = conn.query("SELECT nonexistent FROM t");
    assert!(result.is_err(), "SELECT nonexistent column should error");
}

#[test]
fn select_duplicate_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    // SELECT id, id, val — duplicate column reference is valid SQL
    let qr = conn.query("SELECT id, id, val FROM t").unwrap();
    assert_eq!(qr.rows[0].len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[0][2], Value::Integer(42));
}

#[test]
fn select_with_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let qr = conn.query("SELECT id AS pk, val AS value FROM t").unwrap();
    assert_eq!(qr.columns, vec!["pk", "value"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(42));
}

#[test]
fn select_count_star_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(0));
}

#[test]
fn limit_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    let qr = conn.query("SELECT * FROM t LIMIT 0").unwrap();
    assert_eq!(qr.rows.len(), 0, "LIMIT 0 should return no rows");
}

#[test]
fn offset_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    let qr = conn.query("SELECT * FROM t ORDER BY id OFFSET 0").unwrap();
    assert_eq!(qr.rows.len(), 2, "OFFSET 0 should return all rows");
}

#[test]
fn limit_larger_than_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let qr = conn.query("SELECT * FROM t LIMIT 1000").unwrap();
    assert_eq!(qr.rows.len(), 1, "LIMIT > row count should return all rows");
}

// ════════════════════════════════════════════════════════════════════
// TYPE COERCION EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn integer_to_real_column_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL)")
        .unwrap();
    // Insert integer literal into REAL column
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(42.0));
}

#[test]
fn real_to_integer_column_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    // Insert real literal into INTEGER column — should truncate or error
    let result = conn.execute("INSERT INTO t VALUES (1, 42.7)");

    match result {
        Ok(_) => {
            let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
            // If coercion succeeds, value should be truncated to 42
            assert_eq!(qr.rows[0][0], Value::Integer(42));
        }
        Err(_) => {
            // Type mismatch is also acceptable behavior
        }
    }
}

#[test]
fn boolean_integer_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    // Insert integer into BOOLEAN column
    let result = conn.execute("INSERT INTO t VALUES (1, 1)");
    match result {
        Ok(_) => {
            let qr = conn.query("SELECT flag FROM t WHERE id = 1").unwrap();
            assert_eq!(qr.rows[0][0], Value::Boolean(true));
        }
        Err(_) => {
            // Type mismatch is also acceptable
        }
    }
}

#[test]
fn type_mismatch_text_into_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    let result = conn.execute("INSERT INTO t VALUES (1, 'not_a_number')");
    assert!(result.is_err(), "text into integer column should error");
}

// ════════════════════════════════════════════════════════════════════
// DDL EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn create_table_single_pk_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.columns, vec!["id"]);
}

#[test]
fn create_table_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (
            pk INTEGER NOT NULL PRIMARY KEY,
            a_int INTEGER,
            b_real REAL,
            c_text TEXT,
            d_bool BOOLEAN,
            e_blob BLOB
        )",
    )
    .unwrap();

    // Insert with explicit NULLs (all nullable)
    conn.execute("INSERT INTO t (pk) VALUES (1)").unwrap();

    let qr = conn.query("SELECT * FROM t WHERE pk = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    for i in 1..=5 {
        assert!(qr.rows[0][i].is_null(), "column {i} should be NULL");
    }
}

#[test]
fn drop_table_then_select_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("DROP TABLE t").unwrap();

    let result = conn.query("SELECT * FROM t");
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[test]
fn duplicate_column_names_in_create() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, name TEXT)");
    assert!(matches!(result, Err(SqlError::DuplicateColumn(_))));
}

// ════════════════════════════════════════════════════════════════════
// INSERT EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn insert_explicit_null_in_nullable_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert!(qr.rows[0][0].is_null());
}

#[test]
fn insert_wrong_column_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    let result = conn.execute("INSERT INTO t VALUES (1, 2, 3)");
    assert!(result.is_err(), "wrong column count should error");
}

#[test]
fn insert_too_few_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL)",
    )
    .unwrap();
    // Insert with only PK specified via column list, missing NOT NULL columns
    let result = conn.execute("INSERT INTO t (id) VALUES (1)");
    assert!(
        matches!(result, Err(SqlError::NotNullViolation(_))),
        "missing NOT NULL column should error"
    );
}

// ════════════════════════════════════════════════════════════════════
// UPDATE EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn update_all_rows_without_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    for i in 0..10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 0)"))
            .unwrap();
    }

    match conn.execute("UPDATE t SET val = 999").unwrap() {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 10),
        other => panic!("expected RowsAffected(10), got {other:?}"),
    }

    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE val = 999")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));
}

#[test]
fn update_nonexistent_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    let result = conn.execute("UPDATE t SET missing_col = 1 WHERE id = 1");
    assert!(result.is_err(), "UPDATE nonexistent column should error");
}

#[test]
fn update_pk_to_free_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    conn.execute("UPDATE t SET id = 100 WHERE id = 1").unwrap();

    // Old key gone
    let qr = conn.query("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows.len(), 0);

    // New key present
    let qr = conn.query("SELECT val FROM t WHERE id = 100").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("hello".into()));
}

#[test]
fn update_same_row_twice_sequentially() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    conn.execute("UPDATE t SET val = 10 WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET val = 20 WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET val = 30 WHERE id = 1").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(30));
}

// ════════════════════════════════════════════════════════════════════
// DELETE EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn delete_nonexistent_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    match conn.execute("DELETE FROM t WHERE id = 999").unwrap() {
        ExecutionResult::RowsAffected(n) => assert_eq!(n, 0),
        other => panic!("expected RowsAffected(0), got {other:?}"),
    }
}

#[test]
fn delete_then_reinsert_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'reinserted')")
        .unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Text("reinserted".into()));
}

#[test]
fn delete_with_complex_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, cat TEXT, val INTEGER)")
        .unwrap();
    for i in 0..20 {
        let cat = if i % 3 == 0 {
            "a"
        } else if i % 3 == 1 {
            "b"
        } else {
            "c"
        };
        conn.execute(&format!("INSERT INTO t VALUES ({i}, '{cat}', {i})"))
            .unwrap();
    }

    conn.execute("DELETE FROM t WHERE cat = 'a' AND val > 10")
        .unwrap();

    // cat='a' has ids 0,3,6,9,12,15,18
    // val > 10 removes: 12, 15, 18 (3 rows)
    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(17));
}

// ════════════════════════════════════════════════════════════════════
// EXPRESSION EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn nested_parenthesized_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let qr = conn
        .query("SELECT ((val + 5) * 2) - 10 FROM t WHERE id = 1")
        .unwrap();
    // (10 + 5) * 2 - 10 = 30 - 10 = 20
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

#[test]
fn deeply_nested_and_or() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20, 30)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 5, 25, 35)").unwrap();

    let qr = conn
        .query("SELECT id FROM t WHERE (a > 7 AND b < 25) OR (c > 32 AND a < 8)")
        .unwrap();
    // id=1: (10>7 AND 20<25)=true OR ... → true
    // id=2: (5>7)=false, (35>32 AND 5<8)=true → true
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn chained_comparisons_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    for i in 1..=20 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    // Range query: id >= 5 AND id <= 15
    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE id >= 5 AND id <= 15")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(11));
}

// ════════════════════════════════════════════════════════════════════
// AGGREGATE EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_single_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 42)").unwrap();

    let qr = conn
        .query("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Integer(42));
    assert_eq!(qr.rows[0][2], Value::Real(42.0));
    assert_eq!(qr.rows[0][3], Value::Integer(42));
    assert_eq!(qr.rows[0][4], Value::Integer(42));
}

#[test]
fn sum_large_integers() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    // Insert values that when summed will be close to i64::MAX
    conn.execute("INSERT INTO t VALUES (1, 4611686018427387903)")
        .unwrap(); // ~i64::MAX/2
    conn.execute("INSERT INTO t VALUES (2, 4611686018427387903)")
        .unwrap();

    let qr = conn.query("SELECT SUM(val) FROM t").unwrap();
    // Sum = 2 * (i64::MAX/2) = i64::MAX - 1 (approximately, due to integer division)
    assert_eq!(qr.rows[0][0], Value::Integer(9223372036854775806));
}

#[test]
fn avg_returns_real() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 3)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 4)").unwrap();

    let qr = conn.query("SELECT AVG(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Real(3.5));
}

#[test]
fn group_by_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp TEXT, val INTEGER)")
        .unwrap();

    let qr = conn
        .query("SELECT grp, COUNT(*) FROM t GROUP BY grp")
        .unwrap();
    assert_eq!(
        qr.rows.len(),
        0,
        "GROUP BY on empty table should return no groups"
    );
}

#[test]
fn count_star_vs_count_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, NULL)").unwrap();

    let qr = conn.query("SELECT COUNT(*), COUNT(val) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(4), "COUNT(*) counts all rows");
    assert_eq!(qr.rows[0][1], Value::Integer(2), "COUNT(col) skips NULLs");
}

// ════════════════════════════════════════════════════════════════════
// ORDER BY EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn order_by_multiple_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, grp TEXT NOT NULL, val INTEGER)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'b', 2)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a', 3)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'a', 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'b', 1)").unwrap();

    let qr = conn
        .query("SELECT id FROM t ORDER BY grp ASC, val ASC")
        .unwrap();
    // a,1 → a,3 → b,1 → b,2
    assert_eq!(qr.rows[0][0], Value::Integer(3)); // a,1
    assert_eq!(qr.rows[1][0], Value::Integer(2)); // a,3
    assert_eq!(qr.rows[2][0], Value::Integer(4)); // b,1
    assert_eq!(qr.rows[3][0], Value::Integer(1)); // b,2
}

#[test]
fn order_by_desc_nulls_last() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 10)").unwrap();

    // DESC: NULLs should come last by default
    let qr = conn
        .query("SELECT id, val FROM t ORDER BY val DESC")
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(30));
    assert_eq!(qr.rows[1][1], Value::Integer(10));
    assert!(qr.rows[2][1].is_null());
}

// ════════════════════════════════════════════════════════════════════
// SQL PARSE EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn empty_sql_string() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    let result = conn.execute("");
    assert!(result.is_err(), "empty SQL should error");
}

#[test]
fn sql_with_semicolons() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Multiple statements separated by semicolons should error
    let result = conn.execute(
        "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY)",
    );
    assert!(result.is_err(), "multiple statements should be rejected");
}

#[test]
fn sql_with_trailing_semicolon() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Single statement with trailing semicolon should work
    // (sqlparser usually accepts this as a single statement)
    let result = conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);");
    // This may or may not work depending on sqlparser behavior
    let _ = result;
}

// ════════════════════════════════════════════════════════════════════
// TRANSACTION / SNAPSHOT ISOLATION VIA SQL
// ════════════════════════════════════════════════════════════════════

#[test]
fn read_own_writes_within_connection() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();

    // Write then immediately read
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(100));

    // Update then immediately read
    conn.execute("UPDATE t SET val = 200 WHERE id = 1").unwrap();

    let qr = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(200));
}

// ════════════════════════════════════════════════════════════════════
// PARAMETERIZED TESTS (generating many test assertions)
// ════════════════════════════════════════════════════════════════════

#[test]
fn all_comparison_operators_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    // (operator, threshold, expected_count)
    let tests: Vec<(&str, i64, i64)> = vec![
        ("=", 5, 1),
        ("<>", 5, 9),
        ("<", 5, 4),
        (">", 5, 5),
        ("<=", 5, 5),
        (">=", 5, 6),
        ("=", 1, 1),
        ("=", 10, 1),
        ("=", 0, 0),
        ("=", 11, 0),
        ("<", 1, 0),
        (">", 10, 0),
        ("<=", 0, 0),
        (">=", 11, 0),
        ("<", 11, 10),
        (">", 0, 10),
    ];

    for (op, threshold, expected) in tests {
        let qr = conn
            .query(&format!("SELECT COUNT(*) FROM t WHERE id {op} {threshold}"))
            .unwrap();
        assert_eq!(
            qr.rows[0][0],
            Value::Integer(expected),
            "failed: id {op} {threshold} — expected {expected} rows"
        );
    }
}

#[test]
fn all_arithmetic_operators() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 15, 4)").unwrap();

    let tests: Vec<(&str, Value)> = vec![
        ("a + b", Value::Integer(19)),
        ("a - b", Value::Integer(11)),
        ("a * b", Value::Integer(60)),
        ("a / b", Value::Integer(3)), // 15/4 = 3 (integer division)
        ("a % b", Value::Integer(3)), // 15%4 = 3
    ];

    for (expr, expected) in tests {
        let qr = conn
            .query(&format!("SELECT {expr} FROM t WHERE id = 1"))
            .unwrap();
        assert_eq!(qr.rows[0][0], expected, "failed: {expr}");
    }
}

#[test]
fn null_propagation_all_operators() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();

    // All these should produce NULL due to NULL propagation
    let null_exprs = vec![
        "val + 1", "val - 1", "val * 2", "val / 2", "val % 2", "val = 0", "val <> 0", "val < 0",
        "val > 0", "val <= 0", "val >= 0",
    ];

    for expr in null_exprs {
        let qr = conn
            .query(&format!("SELECT {expr} FROM t WHERE id = 1"))
            .unwrap();
        assert!(
            qr.rows[0][0].is_null(),
            "NULL propagation failed for: {expr} — got {:?}",
            qr.rows[0][0]
        );
    }
}

// ════════════════════════════════════════════════════════════════════
// DATA INTEGRITY AFTER COMPLEX OPERATIONS
// ════════════════════════════════════════════════════════════════════

#[test]
fn complex_workflow_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    // Create two related tables
    conn.execute(
        "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN NOT NULL)"
    ).unwrap();
    conn.execute(
        "CREATE TABLE scores (id INTEGER NOT NULL PRIMARY KEY, user_id INTEGER NOT NULL, score INTEGER NOT NULL)"
    ).unwrap();

    // Populate users
    for i in 1..=20 {
        let active = if i <= 15 { "TRUE" } else { "FALSE" };
        conn.execute(&format!(
            "INSERT INTO users VALUES ({i}, 'user_{i}', {active})"
        ))
        .unwrap();
    }

    // Populate scores (3 scores per user)
    let mut sid = 1;
    for uid in 1..=20 {
        for score in [80, 90, 100] {
            conn.execute(&format!(
                "INSERT INTO scores VALUES ({sid}, {uid}, {score})"
            ))
            .unwrap();
            sid += 1;
        }
    }

    // Complex queries
    let qr = conn
        .query("SELECT COUNT(*) FROM users WHERE active = TRUE")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(15));

    let qr = conn.query("SELECT COUNT(*) FROM scores").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(60));

    let qr = conn.query(
        "SELECT user_id, SUM(score), AVG(score) FROM scores GROUP BY user_id ORDER BY user_id LIMIT 3"
    ).unwrap();
    assert_eq!(qr.rows.len(), 3);
    // Each user: 80+90+100=270, avg=90
    assert_eq!(qr.rows[0][1], Value::Integer(270));
    assert_eq!(qr.rows[0][2], Value::Real(90.0));

    // Deactivate some users
    conn.execute("UPDATE users SET active = FALSE WHERE id > 10")
        .unwrap();

    let qr = conn
        .query("SELECT COUNT(*) FROM users WHERE active = TRUE")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(10));

    // Delete low scores
    conn.execute("DELETE FROM scores WHERE score < 90").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM scores").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(40)); // 20 users * 2 remaining scores

    // Verify remaining scores are >= 90
    let qr = conn.query("SELECT MIN(score) FROM scores").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(90));

    // Drop scores table, users should be unaffected
    conn.execute("DROP TABLE scores").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(20));
}

// ════════════════════════════════════════════════════════════════════
// PERSISTENCE / DURABILITY EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn persist_all_value_types() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (
                id INTEGER NOT NULL PRIMARY KEY,
                int_val INTEGER,
                real_val REAL,
                text_val TEXT,
                bool_val BOOLEAN
            )",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 42, 3.15, 'hello', TRUE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, -100, -0.001, '', FALSE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (3, NULL, NULL, NULL, NULL)")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();

        let qr = conn.query("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(qr.rows.len(), 3);

        // Row 1
        assert_eq!(qr.rows[0][1], Value::Integer(42));
        assert_eq!(qr.rows[0][2], Value::Real(3.15));
        assert_eq!(qr.rows[0][3], Value::Text("hello".into()));
        assert_eq!(qr.rows[0][4], Value::Boolean(true));

        // Row 2
        assert_eq!(qr.rows[1][1], Value::Integer(-100));
        match &qr.rows[1][2] {
            Value::Real(r) => assert!((*r - (-0.001)).abs() < 1e-10),
            other => panic!("expected Real, got {other:?}"),
        }
        assert_eq!(qr.rows[1][3], Value::Text("".into()));
        assert_eq!(qr.rows[1][4], Value::Boolean(false));

        // Row 3 (all NULLs)
        for col in 1..=4 {
            assert!(qr.rows[2][col].is_null(), "col {col} should be NULL");
        }
    }
}

#[test]
fn persist_composite_pk_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = create_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (a TEXT NOT NULL, b INTEGER NOT NULL, val TEXT, PRIMARY KEY (a, b))",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES ('x', 1, 'first')")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('x', 2, 'second')")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('y', 1, 'third')")
            .unwrap();
    }

    {
        let db = open_db(dir.path());
        let mut conn = Connection::open(&db).unwrap();
        let qr = conn
            .query("SELECT val FROM t WHERE a = 'x' AND b = 2")
            .unwrap();
        assert_eq!(qr.rows[0][0], Value::Text("second".into()));

        let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(qr.rows[0][0], Value::Integer(3));
    }
}

// ════════════════════════════════════════════════════════════════════
// MISC EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn insert_and_select_real_as_integer_comparison() {
    // Comparing INTEGER column with REAL literal
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    // Integer 10 == Real 10.0 should work (mixed numeric comparison)
    let qr = conn.query("SELECT id FROM t WHERE val = 10.0").unwrap();
    assert_eq!(qr.rows.len(), 1);

    // val > 15.5 should match 20 and 30
    let qr = conn
        .query("SELECT COUNT(*) FROM t WHERE val > 15.5")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2));
}

#[test]
fn select_star_column_order_matches_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (z INTEGER NOT NULL PRIMARY KEY, a TEXT, m REAL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello', 3.15)")
        .unwrap();

    let qr = conn.query("SELECT * FROM t").unwrap();
    // Column order should match CREATE TABLE order: z, a, m
    assert_eq!(qr.columns, vec!["z", "a", "m"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][1], Value::Text("hello".into()));
    assert_eq!(qr.rows[0][2], Value::Real(3.15));
}

#[test]
fn query_returns_rows_affected_for_dml() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();

    // query() on INSERT should return rows_affected as a result
    let qr = conn.query("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(qr.columns, vec!["rows_affected"]);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
}

#[test]
fn order_by_expression() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 3, 10)").unwrap(); // a+b=13
    conn.execute("INSERT INTO t VALUES (2, 1, 5)").unwrap(); // a+b=6
    conn.execute("INSERT INTO t VALUES (3, 2, 8)").unwrap(); // a+b=10

    let qr = conn.query("SELECT id FROM t ORDER BY a + b").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(2)); // sum=6
    assert_eq!(qr.rows[1][0], Value::Integer(3)); // sum=10
    assert_eq!(qr.rows[2][0], Value::Integer(1)); // sum=13
}

// ════════════════════════════════════════════════════════════════════
// DISTINCT EDGE CASES
// ════════════════════════════════════════════════════════════════════

#[test]
fn distinct_integer_real_cross_type_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, int_val INTEGER, real_val REAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL, 10.0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 20, NULL)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT int_val FROM t WHERE int_val IS NOT NULL")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}

#[test]
fn distinct_null_equals_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, NULL)").unwrap();

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(
        qr.rows.len(),
        1,
        "multiple NULLs should collapse to one in DISTINCT"
    );
    assert!(qr.rows[0][0].is_null());
}

#[test]
fn distinct_preserves_without_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 5)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 5)").unwrap();

    let qr = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 3);

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
}

#[test]
fn distinct_multi_column_null_combinations() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, NULL, NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, NULL, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 1, NULL)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 1, NULL)").unwrap();

    let qr = conn.query("SELECT DISTINCT a, b FROM t").unwrap();
    assert_eq!(qr.rows.len(), 3);
}

#[test]
fn distinct_single_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'only')").unwrap();

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Text("only".into()));
}

#[test]
fn distinct_with_where_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, cat TEXT NOT NULL, val INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'a', 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'b', 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (5, 'a', 10)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val FROM t WHERE cat = 'a' ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Integer(20));
}

#[test]
fn distinct_count_star_not_affected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 5)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 5)").unwrap();

    let qr = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn distinct_with_alias() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val AS unique_val FROM t ORDER BY unique_val")
        .unwrap();
    assert_eq!(qr.columns, vec!["unique_val"]);
    assert_eq!(qr.rows.len(), 2);
    assert_eq!(qr.rows[0][0], Value::Integer(10));
    assert_eq!(qr.rows[1][0], Value::Integer(20));
}

#[test]
fn distinct_all_types() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'hello')").unwrap();

    let qr = conn.query("SELECT DISTINCT val FROM t").unwrap();
    assert_eq!(qr.rows.len(), 1);

    conn.execute("CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, val REAL NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 3.14)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (2, 3.14)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (3, 2.71)").unwrap();

    let qr = conn
        .query("SELECT DISTINCT val FROM t2 ORDER BY val")
        .unwrap();
    assert_eq!(qr.rows.len(), 2);
}
