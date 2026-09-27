//! Subqueries obey the same conditional evaluation as ordinary expressions.

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn with_database(check: impl FnOnce(&Connection<'_>)) {
    let directory = tempfile::tempdir().unwrap();
    let database = DatabaseBuilder::new(directory.path().join("conditional.db"))
        .passphrase(b"conditional-subqueries")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let connection = Connection::open(&database).unwrap();
    connection
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO t VALUES (1,NULL),(2,20),(3,30)")
        .unwrap();
    check(&connection);
}

fn assert_rows(connection: &Connection<'_>, sql: &str, expected: &[Vec<Value>]) {
    assert_eq!(
        connection
            .query(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .rows,
        expected,
        "{sql}"
    );
    assert_eq!(
        connection
            .prepare(sql)
            .unwrap()
            .query_collect(&[])
            .unwrap()
            .rows,
        expected,
        "prepared {sql}"
    );
}

fn integer(value: i64) -> Vec<Value> {
    vec![Value::Integer(value)]
}

#[test]
fn conditional_subqueries_skip_unchosen_correlated_branches() {
    with_database(|connection| {
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                connection.execute(begin).unwrap();
            }
            for expression in [
                "CASE WHEN o.id = 1 THEN 7 ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END",
                "CASE o.id WHEN 1 THEN 7 ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END",
                "COALESCE(o.id + 6, (SELECT i.id FROM t i WHERE i.id > o.id))",
                "CASE WHEN o.id = 1 THEN COALESCE(NULL, 7) WHEN (SELECT i.id FROM t i WHERE i.id > o.id) > 0 THEN 9 ELSE 10 END",
                "COALESCE(NULL, CASE WHEN o.id = 1 THEN 7 ELSE COALESCE(NULL, (SELECT i.id FROM t i WHERE i.id > o.id)) END)",
            ] {
                let sql = format!("SELECT {expression} FROM t o WHERE o.id = 1");
                assert_rows(connection, &sql, &[integer(7)]);
            }
            if begin.is_some() {
                connection.execute("ROLLBACK").unwrap();
            }
        }
    });
}

#[test]
fn conditional_subqueries_execute_chosen_branches_and_preserve_null() {
    with_database(|connection| {
        assert_rows(
            connection,
            "SELECT CASE WHEN o.id = 1 THEN 0 ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o ORDER BY o.id",
            &[integer(0), integer(3), vec![Value::Null]],
        );
        assert_rows(
            connection,
            "SELECT COALESCE(NULL, (SELECT i.id FROM t i WHERE i.id > o.id), 9) FROM t o WHERE o.id >= 2 ORDER BY o.id",
            &[integer(3), integer(9)],
        );
        for sql in [
            "SELECT CASE WHEN o.id = 1 THEN (SELECT i.id FROM t i WHERE i.id > o.id) ELSE 0 END FROM t o WHERE o.id = 1",
            "SELECT COALESCE(o.v, (SELECT i.id FROM t i WHERE i.id > o.id)) FROM t o WHERE o.id = 1",
        ] {
            assert!(matches!(connection.query(sql), Err(SqlError::SubqueryMultipleRows)), "{sql}");
        }
    });
}

#[test]
fn conditional_subqueries_skip_closed_queries_with_and_without_source_rows() {
    with_database(|connection| {
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                connection.execute(begin).unwrap();
            }
            for suffix in ["", " FROM t o WHERE o.id = 1"] {
                for expression in [
                    "CASE WHEN TRUE THEN 7 ELSE (SELECT id FROM t) END",
                    "CASE 'A' COLLATE NOCASE WHEN 'a' THEN 7 ELSE (SELECT id FROM t) END",
                    "COALESCE(NULL, 7, (SELECT id FROM t))",
                ] {
                    assert_rows(
                        connection,
                        &format!("SELECT {expression}{suffix}"),
                        &[integer(7)],
                    );
                }
            }
            assert!(matches!(
                connection.query("SELECT CASE WHEN FALSE THEN 7 ELSE (SELECT id FROM t) END"),
                Err(SqlError::SubqueryMultipleRows)
            ));
            if begin.is_some() {
                connection.execute("ROLLBACK").unwrap();
            }
        }
    });
}

#[test]
fn conditional_subqueries_follow_aggregate_and_window_phases() {
    with_database(|connection| {
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                connection.execute(begin).unwrap();
            }
            assert_rows(connection,
                "SELECT CASE WHEN SUM(o.id) = 6 THEN SUM(o.id) ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o",
                &[integer(6)]);
            assert_rows(connection,
                "SELECT CASE WHEN COUNT(*) = 0 THEN 7 ELSE (SELECT id FROM t) END FROM t WHERE id < 0",
                &[integer(7)]);
            assert_rows(connection,
                "SELECT CASE WHEN ROW_NUMBER() OVER (ORDER BY o.id) > 0 THEN 7 ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o ORDER BY o.id",
                &[integer(7), integer(7), integer(7)]);
            assert_rows(connection,
                "SELECT CASE WHEN SUM(o.id) = 6 THEN ROW_NUMBER() OVER () ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o",
                &[integer(1)]);
            assert_rows(connection,
                "SELECT SUM(CASE WHEN o.id = 1 THEN 0 ELSE COALESCE((SELECT i.id FROM t i WHERE i.id > o.id), 0) END) FROM t o",
                &[integer(3)]);
            for window in ["", ", ROW_NUMBER() OVER (ORDER BY o.id) AS rn"] {
                let sql = format!("SELECT COUNT(*) AS n, o.id AS key{window} FROM t o GROUP BY o.id HAVING CASE WHEN key = 1 THEN TRUE ELSE (SELECT i.id FROM t i WHERE i.id > o.id) = 3 END ORDER BY o.id");
                let mut expected = vec![
                    vec![Value::Integer(1), Value::Integer(1)],
                    vec![Value::Integer(1), Value::Integer(2)],
                ];
                if !window.is_empty() {
                    expected[0].push(Value::Integer(1));
                    expected[1].push(Value::Integer(2));
                }
                assert_rows(connection, &sql, &expected);
            }
            if begin.is_some() {
                connection.execute("ROLLBACK").unwrap();
            }
        }
    });
}

#[test]
fn conditional_subqueries_keep_dependent_inputs_and_repeated_keys_distinct() {
    with_database(|connection| {
        assert_rows(connection,
            "SELECT CASE WHEN o.id = 1 THEN FALSE ELSE (SELECT i.id FROM t i WHERE i.id > o.id) IN (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o ORDER BY o.id",
            &[vec![Value::Boolean(false)], vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
        assert_rows(connection,
            "SELECT COALESCE((SELECT i.id FROM t i WHERE i.id = o.id), 0) IN (SELECT i.id FROM t i WHERE i.id <= o.bucket) FROM (SELECT id, 1 AS bucket FROM t) o ORDER BY o.id",
            &[vec![Value::Boolean(true)], vec![Value::Boolean(false)], vec![Value::Boolean(false)]]);
    });
}

#[test]
fn conditional_subqueries_preserve_closed_and_correlated_volatile_scopes() {
    with_database(|connection| {
        let result = connection.query(
            "SELECT COALESCE(NULL, (SELECT RANDOM())), COALESCE(NULL, (SELECT RANDOM() FROM t i WHERE i.id = o.bucket)) FROM (SELECT id, 1 AS bucket FROM t) o ORDER BY o.id"
        ).unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(
            result.rows.iter().all(|row| row[0] == result.rows[0][0]),
            "a closed scalar query runs once per statement"
        );
        assert_ne!(
            result.rows[0][1], result.rows[1][1],
            "a correlated volatile query runs for each source row"
        );
    });
}

#[test]
fn conditional_subqueries_in_update_set_are_lazy_and_atomic() {
    with_database(|connection| {
        connection.execute(
            "UPDATE t AS o SET v = CASE WHEN o.id = 1 THEN 7 ELSE COALESCE((SELECT i.id FROM t i WHERE i.id > o.id), 9) END"
        ).unwrap();
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[integer(7), integer(3), integer(9)],
        );
        connection.execute("UPDATE t SET v = CASE WHEN id = 1 THEN 10 ELSE COALESCE(v, (SELECT id FROM t)) END").unwrap();
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[integer(10), integer(3), integer(9)],
        );
        let error = connection.execute(
            "UPDATE t AS o SET v = CASE WHEN o.id = 1 THEN (SELECT i.id FROM t i WHERE i.id > o.id) ELSE 0 END"
        ).unwrap_err();
        assert!(matches!(error, SqlError::SubqueryMultipleRows));
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[integer(10), integer(3), integer(9)],
        );
    });
}

#[test]
fn conditional_subqueries_in_mutation_predicates_and_values_are_lazy() {
    with_database(|connection| {
        connection.execute(
            "UPDATE t AS o SET v = 7 WHERE CASE WHEN o.id = 1 THEN TRUE ELSE COALESCE((SELECT i.id FROM t i WHERE i.id > o.id), 0) < 0 END"
        ).unwrap();
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[integer(7), integer(20), integer(30)],
        );
        connection
            .execute("INSERT INTO t VALUES (4, CASE WHEN TRUE THEN 40 ELSE (SELECT id FROM t) END)")
            .unwrap();
        connection.execute(
            "DELETE FROM t AS o WHERE CASE WHEN o.id < 4 THEN FALSE ELSE COALESCE((SELECT i.id FROM t i WHERE i.id > o.id), 0) = 0 END"
        ).unwrap();
        assert_rows(
            connection,
            "SELECT id FROM t ORDER BY id",
            &[integer(1), integer(2), integer(3)],
        );
    });
}

#[test]
fn conditional_subqueries_do_not_defer_aggregate_computation() {
    with_database(|connection| {
        let error = connection.query(
            "SELECT CASE WHEN TRUE THEN 1 ELSE SUM(1 / (o.id - 1)) END, COALESCE(1, (SELECT id FROM t)) FROM t o"
        ).unwrap_err();
        assert!(matches!(error, SqlError::DivisionByZero));
    });
}

#[test]
fn conditional_subquery_membership_keeps_aggregate_and_window_operands_visible() {
    with_database(|connection| {
        assert_rows(connection,
            "SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.id) IN (SELECT i.id + 3 FROM t i WHERE i.id = 3) ELSE FALSE END FROM t o",
            &[vec![Value::Boolean(true)]]);
        assert_rows(connection,
            "SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.id) NOT IN (SELECT i.id FROM t i) ELSE FALSE END FROM t o",
            &[vec![Value::Boolean(true)]]);
        assert_rows(connection,
            "SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.id) > ALL (SELECT i.id FROM t i) ELSE FALSE END FROM t o",
            &[vec![Value::Boolean(true)]]);
        assert_rows(connection,
            "SELECT CASE WHEN o.id > 0 THEN ROW_NUMBER() OVER (ORDER BY o.id) IN (SELECT i.id FROM t i WHERE i.id <= 2) ELSE FALSE END FROM t o ORDER BY o.id",
            &[vec![Value::Boolean(true)], vec![Value::Boolean(true)], vec![Value::Boolean(false)]]);
        assert_rows(connection,
            "SELECT CASE WHEN COUNT(*) > 0 THEN MIN('a') IN (SELECT 'A' COLLATE NOCASE) ELSE FALSE END FROM t",
            &[vec![Value::Boolean(true)]]);
    });
}

#[test]
fn conditional_subqueries_do_not_reuse_a_previous_prepared_branch() {
    with_database(|connection| {
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                connection.execute(begin).unwrap();
            }
            let statement = connection.prepare(
                "SELECT CASE WHEN $1 = 1 THEN o.id ELSE (SELECT i.id FROM t i WHERE i.id > o.id) END FROM t o WHERE o.id = 1"
            ).unwrap();
            for parameter in [1, 0, 1] {
                let result = statement.query_collect(&[Value::Integer(parameter)]);
                if parameter == 1 {
                    assert_eq!(result.unwrap().rows, vec![integer(1)]);
                } else {
                    assert!(matches!(result, Err(SqlError::SubqueryMultipleRows)));
                }
            }
            if begin.is_some() {
                connection.execute("ROLLBACK").unwrap();
            }
        }
    });
}

#[test]
fn conditional_set_first_demand_after_trigger_reads_the_statement_snapshot() {
    for explicit in [false, true] {
        with_database(|connection| {
            if explicit {
                connection.execute("BEGIN").unwrap();
            }
            connection
                .execute("CREATE TABLE source (id INTEGER PRIMARY KEY, n INTEGER)")
                .unwrap();
            connection
                .execute("INSERT INTO source VALUES (1,100)")
                .unwrap();
            if explicit {
                // Both the table and this earlier write are uncommitted.
                connection
                    .execute("UPDATE source SET n = 200 WHERE id = 1")
                    .unwrap();
            }
            connection.execute(
                "CREATE TRIGGER flip_later AFTER UPDATE ON t FOR EACH ROW WHEN NEW.id = 1 BEGIN UPDATE source SET n = 999 WHERE id = 1; UPDATE t SET v = NULL WHERE id = 2; END"
            ).unwrap();
            connection.execute(
                "UPDATE t SET v = CASE WHEN id = 1 THEN 11 WHEN v IS NOT NULL THEN v + 1 ELSE (SELECT n FROM source WHERE id = 1) END WHERE id <= 2"
            ).unwrap();
            let expected = [
                integer(11),
                integer(if explicit { 200 } else { 100 }),
                integer(30),
            ];
            assert_rows(connection, "SELECT v FROM t ORDER BY id", &expected);
            assert_rows(connection, "SELECT n FROM source", &[integer(999)]);
            if explicit {
                connection.execute("COMMIT").unwrap();
                assert_rows(connection, "SELECT v FROM t ORDER BY id", &expected);
                assert_rows(connection, "SELECT n FROM source", &[integer(999)]);
            }
        });
    }
}

#[test]
fn conditional_set_selected_error_on_refresh_obeys_transaction_recovery() {
    for explicit in [false, true] {
        with_database(|connection| {
            if explicit {
                connection.execute("BEGIN").unwrap();
            }
            connection
                .execute("CREATE TABLE source (id INTEGER PRIMARY KEY, n INTEGER)")
                .unwrap();
            connection
                .execute("INSERT INTO source VALUES (1,100),(2,200)")
                .unwrap();
            if explicit {
                connection.execute("UPDATE source SET n = n + 10").unwrap();
            }
            connection.execute(
                "CREATE TRIGGER fail_later AFTER UPDATE ON t FOR EACH ROW WHEN NEW.id = 1 BEGIN UPDATE source SET n = 999; UPDATE t SET v = NULL WHERE id = 2; END"
            ).unwrap();
            if explicit {
                connection.execute("SAVEPOINT before_update").unwrap();
            }
            let error = connection.execute(
                "UPDATE t SET v = CASE WHEN id = 1 THEN 11 WHEN v IS NOT NULL THEN v + 1 ELSE (SELECT n FROM source) END WHERE id <= 2"
            ).unwrap_err();
            assert!(matches!(error, SqlError::SubqueryMultipleRows));
            if explicit {
                // A partially applied explicit statement poisons its writer.
                // Recovery is explicit and must retain the earlier writes.
                assert!(matches!(
                    connection.query("SELECT v FROM t").unwrap_err(),
                    SqlError::Storage(citadel_core::Error::TransactionFailed)
                ));
                connection.execute("ROLLBACK TO before_update").unwrap();
                connection.execute("RELEASE before_update").unwrap();
            }
            let original_rows = [vec![Value::Null], integer(20), integer(30)];
            let source_rows = [
                integer(if explicit { 110 } else { 100 }),
                integer(if explicit { 210 } else { 200 }),
            ];
            assert_rows(connection, "SELECT v FROM t ORDER BY id", &original_rows);
            assert_rows(connection, "SELECT n FROM source ORDER BY id", &source_rows);
            if explicit {
                connection.execute("COMMIT").unwrap();
                assert_rows(connection, "SELECT v FROM t ORDER BY id", &original_rows);
                assert_rows(connection, "SELECT n FROM source ORDER BY id", &source_rows);
            }
        });
    }
}

#[test]
fn conditional_set_keeps_the_correlated_row_refresh_guard() {
    with_database(|connection| {
        connection.execute(
            "CREATE TRIGGER alter_later AFTER UPDATE ON t FOR EACH ROW WHEN NEW.id = 1 BEGIN UPDATE t SET v = v + 100 WHERE id = 2; END"
        ).unwrap();
        let error = connection
            .execute(
                "UPDATE t AS o SET v = COALESCE((SELECT MAX(i.id) FROM t i WHERE i.id <= o.id), 0)",
            )
            .unwrap_err();
        assert!(
            matches!(error, SqlError::Unsupported(ref message) if message.contains("SET subqueries that read the row"))
        );
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[vec![Value::Null], integer(20), integer(30)],
        );
    });
}

#[test]
fn conditional_set_snapshot_keeps_pending_vectors_out_of_committed_ann_cache() {
    with_database(|connection| {
        connection
            .execute("CREATE TABLE vec_source (id INTEGER PRIMARY KEY, embedding VECTOR(2))")
            .unwrap();
        connection
            .execute(
                "INSERT INTO vec_source VALUES (1,'[10,0]'::VECTOR(2)),(2,'[20,0]'::VECTOR(2))",
            )
            .unwrap();
        connection.execute("CREATE INDEX vec_source_ann ON vec_source USING ann (embedding) WITH (metric = 'l2')").unwrap();
        connection.execute(
            "CREATE TRIGGER closer_later AFTER UPDATE ON t FOR EACH ROW WHEN NEW.id = 1 BEGIN INSERT INTO vec_source VALUES (4,'[0,0]'::VECTOR(2)); UPDATE t SET v = NULL WHERE id = 2; END"
        ).unwrap();
        let nearest = "SELECT id FROM vec_source ORDER BY embedding <-> '[0,0]'::VECTOR(2) LIMIT 1";
        assert_rows(connection, nearest, &[integer(1)]);
        assert!(connection
            .ann_cache_status("vec_source", "embedding")
            .unwrap()
            .is_some());
        connection.execute("BEGIN").unwrap();
        connection
            .execute("INSERT INTO vec_source VALUES (3,'[1,0]'::VECTOR(2))")
            .unwrap();
        connection.execute(
            "UPDATE t SET v = CASE WHEN id = 1 THEN 11 WHEN v IS NOT NULL THEN v + 1 ELSE (SELECT id FROM vec_source ORDER BY embedding <-> '[0,0]'::VECTOR(2) LIMIT 1) END WHERE id <= 2"
        ).unwrap();
        // 1 is the committed cached answer; 4 is the later live answer. The
        // statement snapshot must include the earlier pending row, number 3.
        assert_rows(connection, "SELECT v FROM t WHERE id = 2", &[integer(3)]);
        assert_rows(connection, nearest, &[integer(4)]);
        connection.execute("ROLLBACK").unwrap();
        assert_rows(connection, nearest, &[integer(1)]);
        assert_rows(
            connection,
            "SELECT v FROM t ORDER BY id",
            &[vec![Value::Null], integer(20), integer(30)],
        );
    });
}
