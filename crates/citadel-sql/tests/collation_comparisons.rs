//! A column collation has to reach every comparison form, not just `=`. `IN`, `NOT IN`,
//! `BETWEEN` and `CASE x WHEN` did not consult it, so `BETWEEN` disagreed with the
//! `>=`/`<=` it expands to. Each form now takes the collation `=` would.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn first_row(conn: &Connection, sql: &str) -> Vec<Value> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q.rows.into_iter().next().expect("one result row"),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// `b` is binary and `s` is NOCASE, in one table: the collation is resolved per column, so a
/// binary column must keep binary comparison even when a sibling column collates.
fn seeded(conn: &Connection) {
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, b TEXT, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'b','b'),(2,'A','A'),(3,'B','B'),(4,'a','a')")
        .unwrap();
}

#[test]
fn in_collates_like_the_equality_it_expands_to() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let eq = ids(&conn, "SELECT id FROM c WHERE s = 'A'");
    assert_eq!(eq, vec![2, 4], "NOCASE equality matches 'A' and 'a'");
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IN ('A')"),
        eq,
        "x IN (a) is x = a"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s NOT IN ('A')"),
        ids(&conn, "SELECT id FROM c WHERE s <> 'A'"),
        "and the negated form follows the negated equality"
    );
}

/// The subquery form holds a hash set of values rather than a list of expressions, so it
/// takes a different route to the same answer.
#[test]
fn in_a_subquery_collates_like_the_equality_it_expands_to() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IN (SELECT 'A')"),
        vec![2, 4]
    );
}

/// The collated column may be the one the SUBQUERY projects, not the one on the left.
/// Reading only the left operand made `'a' IN (SELECT s ...)` answer false where `'a' = s`
/// answered true, because materialized values carry no column with them.
#[test]
fn in_a_subquery_takes_the_collation_from_the_selected_column() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let truth = |sql: &str| match conn.execute(sql).unwrap() {
        ExecutionResult::Query(q) => q.rows[0][0].clone(),
        other => panic!("expected rows, got {other:?}"),
    };

    assert_eq!(
        truth("SELECT 'a' IN (SELECT s FROM c WHERE id = 2) AS r"),
        truth("SELECT 'a' = s AS r FROM c WHERE id = 2"),
        "the subquery form agrees with the equality it expands to"
    );
    assert_eq!(
        truth("SELECT 'a' IN (SELECT s FROM c WHERE id = 2) AS r"),
        Value::Boolean(true)
    );
    assert_eq!(
        truth("SELECT 'z' IN (SELECT s FROM c) AS r"),
        Value::Boolean(false),
        "and folding does not invent a match"
    );
    assert_eq!(
        truth("SELECT 'a' IN (SELECT 'A') AS r"),
        Value::Boolean(false),
        "with no column on either side there is no collation to inherit, so it is binary"
    );
}

#[test]
fn an_explicit_binary_collation_wins_over_the_subquery_column() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let result = first_row(
        &conn,
        "SELECT 'a' COLLATE BINARY = s, \
         'a' COLLATE BINARY IN (SELECT s FROM c WHERE id = 2) \
         FROM c WHERE id = 2",
    );
    assert_eq!(
        result,
        vec![Value::Boolean(false), Value::Boolean(false)],
        "IN (subquery) must preserve an explicit BINARY choice on its left operand"
    );
}

#[test]
fn between_agrees_with_the_two_comparisons_it_is_defined_as() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s BETWEEN 'A' AND 'A'"),
        ids(&conn, "SELECT id FROM c WHERE s >= 'A' AND s <= 'A'"),
        "BETWEEN is >= AND <=, collation included"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s BETWEEN 'A' AND 'A'"),
        vec![2, 4]
    );
}

#[test]
fn case_when_collates_like_the_equality_it_expands_to() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM c WHERE CASE s WHEN 'A' THEN 1 ELSE 0 END = 1"
        ),
        vec![2, 4]
    );
}

/// The guard that matters most: nothing above may leak onto a column that never asked for a
/// collation, including one sitting beside a column that did.
#[test]
fn a_binary_column_beside_a_collated_one_stays_binary() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    for sql in [
        "SELECT id FROM c WHERE b = 'A'",
        "SELECT id FROM c WHERE b IN ('A')",
        "SELECT id FROM c WHERE b IN (SELECT 'A')",
        "SELECT id FROM c WHERE b BETWEEN 'A' AND 'A'",
        "SELECT id FROM c WHERE CASE b WHEN 'A' THEN 1 ELSE 0 END = 1",
    ] {
        assert_eq!(ids(&conn, sql), vec![2], "{sql}");
    }
}

/// NOCASE is not the only collation, and RTRIM folds a different set of bytes together.
#[test]
fn rtrim_reaches_the_same_forms() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, t TEXT COLLATE RTRIM)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (1,'x'),(2,'x  '),(3,'y')")
        .unwrap();

    for sql in [
        "SELECT id FROM r WHERE t = 'x'",
        "SELECT id FROM r WHERE t IN ('x')",
        "SELECT id FROM r WHERE t BETWEEN 'x' AND 'x'",
        "SELECT id FROM r WHERE CASE t WHEN 'x' THEN 1 ELSE 0 END = 1",
    ] {
        assert_eq!(ids(&conn, sql), vec![1, 2], "{sql}");
    }
}

/// An explicit COLLATE has to win over the column's own, in the forms that previously
/// ignored collation entirely.
#[test]
fn an_explicit_collate_applies_to_the_expanded_forms() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE b COLLATE NOCASE IN ('A')"),
        vec![2, 4],
        "the binary column collates when the query says so"
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM c WHERE b COLLATE NOCASE BETWEEN 'A' AND 'A'"
        ),
        vec![2, 4]
    );
}

/// The collation may sit on the right of the comparison, which is the only side that names a
/// column here.
///
/// A deliberate divergence from SQLite: it narrows `x IN (y, ...)` to x's collation
/// while its own `=` takes either operand, so there `'A' IN (s)` and `'A' = s`
/// disagree. Both IN forms follow the equality they expand to.
#[test]
fn a_collated_column_on_the_right_still_applies() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(ids(&conn, "SELECT id FROM c WHERE 'A' IN (s)"), vec![2, 4]);
    assert_eq!(ids(&conn, "SELECT id FROM c WHERE 'A' = s"), vec![2, 4]);
}

/// IN is three-valued: an unmatched NULL in the list makes the answer unknown rather than
/// false, and a match still wins over a NULL. Collating the comparison must not disturb it.
#[test]
fn in_keeps_its_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IN ('A', NULL)"),
        vec![2, 4],
        "a collated match still beats the NULL"
    );
    assert!(
        ids(&conn, "SELECT id FROM c WHERE s NOT IN ('A', NULL)").is_empty(),
        "NOT IN with a NULL is unknown for every row, matched or not"
    );
    assert!(
        ids(&conn, "SELECT id FROM c WHERE s IN (NULL)").is_empty(),
        "no match and a NULL present is unknown, not false"
    );
}

/// BETWEEN's bounds are two separate comparisons, so a range wider than one value has to
/// collate at both ends, and the negated form has to follow.
#[test]
fn between_collates_at_both_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s BETWEEN 'a' AND 'a'"),
        vec![2, 4],
        "binary bounds would keep only 'a', since 'A' sorts below the lower bound"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s BETWEEN 'a' AND 'b'"),
        vec![1, 2, 3, 4],
        "binary bounds would keep only 'b' and 'a'"
    );
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s NOT BETWEEN 'A' AND 'A'"),
        vec![1, 3]
    );
}

/// A match that is not the first item, and a CASE whose earlier arm must not steal the row.
#[test]
fn the_matching_item_is_found_wherever_it_sits() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s IN ('z', 'y', 'A')"),
        vec![2, 4]
    );
    assert_eq!(
        ids(
            &conn,
            "SELECT id FROM c WHERE CASE s WHEN 'z' THEN 0 WHEN 'A' THEN 1 ELSE 0 END = 1"
        ),
        vec![2, 4],
        "the first arm does not match, the second collates"
    );
}

/// A bound parameter reaches the comparison as a value with no expression of its own, so the
/// collation has to come from the column side.
#[test]
fn a_bound_parameter_collates_against_the_column() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);

    let rows = |sql: &str, params: &[Value]| match conn.execute_params(sql, params).unwrap() {
        ExecutionResult::Query(q) => q
            .rows
            .iter()
            .map(|r| match r.first() {
                Some(Value::Integer(n)) => *n,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        other => panic!("expected rows, got {other:?}"),
    };
    let one = [Value::Text("A".into())];
    let two = [Value::Text("A".into()), Value::Text("A".into())];

    assert_eq!(rows("SELECT id FROM c WHERE s = $1", &one), vec![2, 4]);
    assert_eq!(rows("SELECT id FROM c WHERE s IN ($1)", &one), vec![2, 4]);
    assert_eq!(
        rows("SELECT id FROM c WHERE s BETWEEN $1 AND $2", &two),
        vec![2, 4]
    );
    assert_eq!(
        rows(
            "SELECT id FROM c WHERE CASE s WHEN $1 THEN 1 ELSE 0 END = 1",
            &one
        ),
        vec![2, 4]
    );
}

/// An index on the collated column takes a different lookup path to the same rows.
#[test]
fn an_index_does_not_change_the_answer() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    seeded(&conn);
    conn.execute("CREATE INDEX ix ON c(s)").unwrap();

    assert_eq!(ids(&conn, "SELECT id FROM c WHERE s = 'A'"), vec![2, 4]);
    assert_eq!(ids(&conn, "SELECT id FROM c WHERE s IN ('A')"), vec![2, 4]);
    assert_eq!(
        ids(&conn, "SELECT id FROM c WHERE s BETWEEN 'A' AND 'A'"),
        vec![2, 4]
    );
}

/// The write paths take their own fast lanes to the predicate, and getting these wrong
/// changes stored data rather than a result set.
#[test]
fn writes_match_the_same_rows_a_select_would() {
    let dir = tempfile::tempdir().unwrap();
    let database = db(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO w VALUES (1,'b',0),(2,'A',0),(3,'B',0),(4,'a',0)")
        .unwrap();

    conn.execute("UPDATE w SET v = 9 WHERE s IN ('A')").unwrap();
    assert_eq!(
        ids(&conn, "SELECT id FROM w WHERE v = 9"),
        vec![2, 4],
        "UPDATE ... WHERE IN reaches both cases"
    );

    conn.execute("DELETE FROM w WHERE s BETWEEN 'B' AND 'B'")
        .unwrap();
    assert_eq!(
        ids(&conn, "SELECT id FROM w"),
        vec![2, 4],
        "DELETE ... WHERE BETWEEN removes both cases"
    );
}
