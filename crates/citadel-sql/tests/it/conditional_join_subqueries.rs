use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn check_modes(check: impl Fn(&Connection<'_>)) {
    let directory = tempfile::tempdir().unwrap();
    let database = DatabaseBuilder::new(directory.path().join("conditional-join.db"))
        .passphrase(b"conditional-join")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let connection = Connection::open(&database).unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY)",
        "CREATE TABLE s (id INTEGER PRIMARY KEY)",
        "CREATE TABLE q (id INTEGER PRIMARY KEY)",
        "INSERT INTO t VALUES (1),(2),(3)",
        "INSERT INTO s VALUES (1),(2),(4)",
        "INSERT INTO q VALUES (1),(2),(3)",
    ] {
        connection.execute(sql).unwrap();
    }
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            connection.execute(begin).unwrap();
        }
        check(&connection);
        if begin.is_some() {
            connection.execute("ROLLBACK").unwrap();
        }
    }
}

fn assert_rows(connection: &Connection<'_>, sql: &str, expected: Vec<Vec<Value>>) {
    for prepared in [false, true] {
        let result = if prepared {
            connection.prepare(sql).unwrap().query_collect(&[])
        } else {
            connection.query(sql)
        }
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(result.columns, ["left_id", "right_id"]);
        assert_eq!(result.rows, expected, "{sql}");
    }
}

fn pair(left: i64, right: Option<i64>) -> Vec<Value> {
    vec![
        Value::Integer(left),
        right.map_or(Value::Null, Value::Integer),
    ]
}

#[test]
fn conditional_join_subqueries_preserve_equi_residual_and_left_padding() {
    check_modes(|connection| {
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM t o JOIN s i \
             ON o.id = i.id AND CASE WHEN o.id < 3 THEN TRUE ELSE (SELECT id FROM q) > 0 END \
             ORDER BY o.id",
            vec![pair(1, Some(1)), pair(2, Some(2))],
        );
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM t o LEFT JOIN s i \
             ON o.id = i.id AND CASE WHEN o.id = 1 THEN TRUE \
             ELSE COALESCE(0, (SELECT id FROM q)) = 1 END ORDER BY o.id",
            vec![pair(1, Some(1)), pair(2, None), pair(3, None)],
        );
        let error = connection
            .query(
                "SELECT * FROM t o JOIN s i ON o.id = i.id AND \
             CASE WHEN o.id = 1 THEN (SELECT id FROM q) > 0 ELSE TRUE END",
            )
            .unwrap_err();
        assert!(matches!(error, SqlError::SubqueryMultipleRows));
    });
}

#[test]
fn conditional_join_subqueries_cover_derived_cte_and_empty_inputs() {
    check_modes(|connection| {
        for source in ["t o", "(SELECT id FROM t) o"] {
            assert_rows(
                connection,
                &format!(
                    "WITH rhs AS (SELECT id FROM s) \
                 SELECT o.id AS left_id, i.id AS right_id FROM {source} LEFT JOIN rhs i \
                 ON o.id = i.id AND COALESCE(TRUE, (SELECT id FROM q) > 0) ORDER BY o.id"
                ),
                vec![pair(1, Some(1)), pair(2, Some(2)), pair(3, None)],
            );
        }
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM t o \
             LEFT JOIN (SELECT id FROM s WHERE id < 0) i \
             ON CASE WHEN FALSE THEN TRUE ELSE (SELECT id FROM q) > 0 END ORDER BY o.id",
            vec![pair(1, None), pair(2, None), pair(3, None)],
        );
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM (SELECT id FROM t WHERE id < 0) o \
             JOIN s i ON CASE WHEN FALSE THEN TRUE ELSE (SELECT id FROM q) > 0 END",
            vec![],
        );
    });
}

#[test]
fn conditional_join_subqueries_cover_lateral_and_keep_correlation_rejection() {
    check_modes(|connection| {
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM t o \
             LEFT JOIN LATERAL (SELECT id FROM s WHERE s.id = o.id) i \
             ON COALESCE(TRUE, (SELECT id FROM q) > 0) ORDER BY o.id",
            vec![pair(1, Some(1)), pair(2, Some(2)), pair(3, None)],
        );
        assert_rows(
            connection,
            "SELECT o.id AS left_id, i.id AS right_id FROM t o \
             JOIN LATERAL (SELECT o.id AS id) l ON TRUE JOIN s i \
             ON l.id = i.id AND CASE WHEN o.id < 3 THEN TRUE ELSE (SELECT id FROM q) > 0 END \
             ORDER BY o.id",
            vec![pair(1, Some(1)), pair(2, Some(2))],
        );
        assert!(matches!(connection.query(
            "SELECT * FROM t o JOIN s i ON CASE WHEN TRUE THEN TRUE \
             ELSE (SELECT id FROM q WHERE q.id > o.id) > 0 END"
        ), Err(SqlError::Unsupported(message)) if message.contains("JOIN condition")));
    });
}
