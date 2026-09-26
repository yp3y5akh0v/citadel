//! Subqueries that read the row of the query around them, whatever the shape
//! of their correlation and wherever that row comes from.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database(directory: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(directory.join("scopes.db"))
        .passphrase(b"correlated-scopes")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

/// t1: (1, a 1, b NULL), (2, a 2, b 1), (3, a 1, b 2); t2: (1, e 1), (2, e 5).
fn setup(connection: &Connection<'_>) {
    for sql in [
        "CREATE TABLE t1 (id INTEGER NOT NULL PRIMARY KEY, a INTEGER, b INTEGER)",
        "CREATE TABLE t2 (id INTEGER NOT NULL PRIMARY KEY, e INTEGER)",
        "INSERT INTO t1 VALUES (1, 1, NULL), (2, 2, 1), (3, 1, 2)",
        "INSERT INTO t2 VALUES (1, 1), (2, 5)",
    ] {
        connection.execute(sql).unwrap();
    }
}

fn rows(connection: &Connection<'_>, sql: &str) -> Vec<Vec<Value>> {
    connection
        .query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

fn ints(values: &[&[i64]]) -> Vec<Vec<Value>> {
    values
        .iter()
        .map(|row| row.iter().map(|&value| Value::Integer(value)).collect())
        .collect()
}

fn with_setup(check: impl FnOnce(&Connection<'_>)) {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    setup(&connection);
    check(&connection);
}

#[test]
fn non_equality_correlation_reads_the_outer_row() {
    with_setup(|connection| {
        // b <= NULL matches nothing, so only the NULL row has no smaller b.
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE NOT EXISTS \
                 (SELECT 1 FROM t1 AS i WHERE i.b <= o.b) ORDER BY 1"
            ),
            ints(&[&[1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE EXISTS \
                 (SELECT 1 FROM t1 AS i WHERE i.a < o.a) ORDER BY 1"
            ),
            ints(&[&[2]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE o.a IN \
                 (SELECT i.b FROM t1 AS i WHERE i.a > o.b) ORDER BY 1"
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT MIN(s.e) FROM t2 AS s WHERE s.id <> o.id) \
                 FROM t1 AS o ORDER BY 1"
            ),
            ints(&[&[1, 5], &[2, 1], &[3, 1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT COUNT(*) FROM t1 AS i WHERE i.a <> o.a) \
                 FROM t1 AS o ORDER BY 1"
            ),
            ints(&[&[1, 1], &[2, 2], &[3, 1]])
        );
    });
}

#[test]
fn a_join_keeps_its_rows_when_a_subquery_reads_any_joined_table() {
    with_setup(|connection| {
        // The join pairs are (2, 1), (2, 3) and (3, 2).
        assert_eq!(
            rows(
                connection,
                "SELECT x.id, y.id FROM t1 AS x JOIN t1 AS y ON x.b = y.a \
                 WHERE EXISTS (SELECT 1 FROM t2 AS s WHERE s.e = y.id) ORDER BY 1, 2"
            ),
            ints(&[&[2, 1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT x.id, y.id FROM t1 AS x JOIN t1 AS y ON x.b = y.a \
                 WHERE EXISTS (SELECT 1 FROM t2 AS s WHERE s.e = x.id) ORDER BY 1, 2"
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            rows(
                connection,
                "SELECT x.id, (SELECT MIN(s.e) FROM t2 AS s WHERE s.id <> x.id) \
                 FROM t1 AS x JOIN t1 AS y ON x.b = y.a ORDER BY 1, 2"
            ),
            ints(&[&[2, 1], &[2, 1], &[3, 1]])
        );
    });
}

#[test]
fn aggregate_subqueries_over_no_rows_keep_their_empty_values() {
    with_setup(|connection| {
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT COUNT(*) FROM t2 AS s WHERE s.id = o.b) \
                 FROM t1 AS o ORDER BY 1"
            ),
            ints(&[&[1, 0], &[2, 1], &[3, 1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT COUNT(*) FROM t2 AS s WHERE s.e = o.a), \
                 (SELECT SUM(s.id) FROM t2 AS s WHERE s.e = o.a) FROM t1 AS o ORDER BY 1"
            ),
            vec![
                vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(0), Value::Null],
                vec![Value::Integer(3), Value::Integer(1), Value::Integer(1)],
            ]
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE 0 = \
                 (SELECT COUNT(*) FROM t2 AS s WHERE s.e = o.a) ORDER BY 1"
            ),
            ints(&[&[2]])
        );
    });
}

#[test]
fn a_scalar_subquery_is_its_one_row_null_or_an_error() {
    with_setup(|connection| {
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT s.e FROM t2 AS s WHERE s.id = o.id) FROM t1 AS o ORDER BY 1"
            ),
            vec![
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(5)],
                vec![Value::Integer(3), Value::Null],
            ]
        );
        // The projection reads the outer row, so each row computes its own value.
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT s.e + o.id FROM t2 AS s WHERE s.id = o.a) \
                 FROM t1 AS o ORDER BY 1"
            ),
            ints(&[&[1, 2], &[2, 7], &[3, 4]])
        );
        // a = 1 matches rows 1 and 3.
        for sql in [
            "SELECT o.id, (SELECT i.id FROM t1 AS i WHERE i.a = o.a) FROM t1 AS o",
            "SELECT o.id FROM t1 AS o WHERE o.id = (SELECT i.id FROM t1 AS i WHERE i.a = o.a)",
        ] {
            let error = connection.query(sql).unwrap_err();
            assert!(
                matches!(error, SqlError::SubqueryMultipleRows),
                "{sql}: {error}"
            );
        }
    });
}

#[test]
fn in_subqueries_keep_their_non_equality_conditions() {
    with_setup(|connection| {
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE o.a IN \
                 (SELECT s.e FROM t2 AS s WHERE s.id = o.a AND s.e > o.b) ORDER BY 1"
            ),
            Vec::<Vec<Value>>::new()
        );
    });
}

#[test]
fn views_ctes_and_derived_tables_supply_the_outer_row() {
    with_setup(|connection| {
        connection
            .execute("CREATE VIEW v AS SELECT id, a, b FROM t1")
            .unwrap();
        for sql in [
            "SELECT o.id FROM v AS o WHERE EXISTS \
             (SELECT 1 FROM t1 AS i WHERE i.a < o.a) ORDER BY 1",
            "WITH c AS (SELECT id, a FROM t1) SELECT c.id FROM c WHERE EXISTS \
             (SELECT 1 FROM t1 AS i WHERE i.a < c.a) ORDER BY 1",
            "SELECT d.k FROM (SELECT id AS k, a AS n FROM t1) AS d WHERE EXISTS \
             (SELECT 1 FROM t1 AS i WHERE i.a < d.n) ORDER BY 1",
        ] {
            assert_eq!(rows(connection, sql), ints(&[&[2]]), "{sql}");
        }
        // The join pairs are (1, 2), (2, 3) and (3, 2); only y = 2 has b = 1.
        assert_eq!(
            rows(
                connection,
                "WITH c AS (SELECT id, a FROM t1) SELECT c.id, y.id FROM c \
                 JOIN t1 AS y ON c.a = y.b WHERE EXISTS \
                 (SELECT 1 FROM t2 AS s WHERE s.e = y.b) ORDER BY 1, 2"
            ),
            ints(&[&[1, 2], &[3, 2]])
        );
    });
}

#[test]
fn a_transaction_reads_joined_rows_through_subqueries() {
    with_setup(|connection| {
        connection.execute("BEGIN").unwrap();
        assert_eq!(
            rows(
                connection,
                "SELECT x.id, y.id FROM t1 AS x JOIN t1 AS y ON x.b = y.a \
                 WHERE EXISTS (SELECT 1 FROM t2 AS s WHERE s.e = y.id) ORDER BY 1, 2"
            ),
            ints(&[&[2, 1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT x.id, y.id FROM t1 AS x JOIN t1 AS y ON x.b = y.a \
                 WHERE y.id IN (SELECT s.e FROM t2 AS s) ORDER BY 1, 2"
            ),
            ints(&[&[2, 1]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id, (SELECT COUNT(*) FROM t1 AS i WHERE i.a <> o.a) \
                 FROM t1 AS o ORDER BY 1"
            ),
            ints(&[&[1, 1], &[2, 2], &[3, 1]])
        );
        connection.execute("COMMIT").unwrap();
    });
}

#[test]
fn a_join_condition_cannot_read_a_joined_row_through_a_subquery() {
    with_setup(|connection| {
        let error = connection
            .query(
                "SELECT x.id FROM t1 AS x JOIN t1 AS y \
                 ON y.a IN (SELECT s.e FROM t2 AS s WHERE s.id = x.id)",
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("JOIN condition"),
            "unexpected error: {error}"
        );
    });
}

#[test]
fn a_captured_scalar_compares_with_the_other_operands_collation() {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    for sql in [
        "CREATE TABLE n (id INTEGER NOT NULL PRIMARY KEY, name TEXT COLLATE NOCASE)",
        "INSERT INTO n VALUES (1, 'abc'), (2, 'xyz')",
    ] {
        connection.execute(sql).unwrap();
    }
    // The scalar subquery is on the left and has no collation of its own, so
    // NOCASE comes from the right operand.
    assert_eq!(
        rows(
            &connection,
            "SELECT o.id FROM n AS o WHERE \
             (SELECT UPPER(i.name) FROM n AS i WHERE i.id = o.id) = o.name ORDER BY 1"
        ),
        ints(&[&[1], &[2]])
    );
}

#[test]
fn ordering_grouping_and_nesting_read_the_outer_row() {
    with_setup(|connection| {
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o ORDER BY \
                 (SELECT COUNT(*) FROM t1 AS i WHERE i.a < o.a), o.id"
            ),
            ints(&[&[1], &[3], &[2]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.a, COUNT(*) FROM t1 AS o GROUP BY o.a HAVING EXISTS \
                 (SELECT 1 FROM t2 AS s WHERE s.e = o.a) ORDER BY 1"
            ),
            ints(&[&[1, 2]])
        );
        assert_eq!(
            rows(
                connection,
                "SELECT o.id FROM t1 AS o WHERE EXISTS (SELECT 1 FROM t2 AS s WHERE EXISTS \
                 (SELECT 1 FROM t1 AS i WHERE i.a = o.a AND i.id <> o.id AND s.e = i.a)) \
                 ORDER BY 1"
            ),
            ints(&[&[1], &[3]])
        );
    });
}
