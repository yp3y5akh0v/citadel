use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, QueryResult, SqlError, Value};

fn with_connection(check: impl FnOnce(&Connection<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DatabaseBuilder::new(dir.path().join("aliases.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (2, 200)").unwrap();
    check(&conn);
}

fn query(conn: &Connection<'_>, sql: &str, prepared: bool) -> QueryResult {
    if prepared {
        conn.prepare(sql).unwrap().query_collect(&[]).unwrap()
    } else {
        conn.query(sql).unwrap()
    }
}

#[test]
fn update_and_delete_aliases_bind_values_and_preserve_returning_names() {
    for prepared in [false, true] {
        with_connection(|conn| {
            let result = query(
                conn,
                "UPDATE t AS dst SET v = dst.v + 1 WHERE dst.id = 1 RETURNING dst.id, dst.v",
                prepared,
            );
            assert_eq!(result.columns, ["dst.id", "dst.v"]);
            assert_eq!(
                result.rows,
                vec![vec![Value::Integer(1), Value::Integer(11)]]
            );
            let result = query(
                conn,
                "DELETE FROM t AS dst WHERE dst.id = 2 RETURNING dst.*",
                prepared,
            );
            assert_eq!(result.columns, ["id", "v"]);
            assert_eq!(
                result.rows,
                vec![vec![Value::Integer(2), Value::Integer(20)]]
            );
            assert_eq!(
                conn.query("SELECT id FROM t ORDER BY id").unwrap().rows,
                vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
            );
        });
    }
}

#[test]
fn correlated_alias_does_not_capture_same_named_inner_table() {
    for prepared in [false, true] {
        with_connection(|conn| {
            let result = query(conn, "UPDATE t AS dst SET v = dst.v + 5 WHERE EXISTS (SELECT 1 FROM t WHERE t.id = dst.id AND t.v = 20) RETURNING dst.id, dst.v", prepared);
            assert_eq!(
                result.rows,
                vec![vec![Value::Integer(2), Value::Integer(25)]]
            );
            let result = query(conn, "DELETE FROM t AS dst WHERE EXISTS (SELECT 1 FROM t WHERE t.id = dst.id AND t.v = 25) RETURNING dst.id", prepared);
            assert_eq!(result.columns, ["dst.id"]);
            assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        });
    }
}

#[test]
fn local_alias_shadows_target_alias_in_subquery() {
    with_connection(|conn| {
        conn.execute(
            "UPDATE t AS dst SET v = 7 WHERE EXISTS (SELECT 1 FROM u AS dst WHERE dst.id = 2)",
        )
        .unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            vec![vec![Value::Integer(7)]; 3]
        );
    });
}

#[test]
fn missing_target_column_does_not_resolve_to_an_inner_column() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE refs (id INTEGER PRIMARY KEY, x INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO refs VALUES (1,1)").unwrap();
        conn.execute("BEGIN").unwrap();
        let error = conn
            .execute("UPDATE t AS dst SET v = 7 WHERE EXISTS (SELECT 1 FROM refs WHERE dst.x > 0)")
            .unwrap_err();
        assert!(
            matches!(&error, SqlError::ColumnNotFound(name) if name == "t.x"),
            "{error:?}"
        );
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
            vec![
                vec![Value::Integer(10)],
                vec![Value::Integer(20)],
                vec![Value::Integer(30)]
            ]
        );
    });
}

#[test]
fn set_subquery_local_alias_shadows_target_without_becoming_correlated() {
    for prepared in [false, true] {
        with_connection(|conn| {
            let result = query(conn, "UPDATE t AS dst SET v = (SELECT dst.v FROM u AS dst WHERE dst.id = 2) WHERE dst.id = 1 RETURNING dst.v", prepared);
            assert_eq!(result.rows, vec![vec![Value::Integer(200)]]);
            assert_eq!(result.columns, ["dst.v"]);
        });
    }
}

#[test]
fn renamed_inner_alias_preserves_derived_column_names() {
    with_connection(|conn| {
        let result = conn.query("UPDATE t AS dst SET v = 7 WHERE EXISTS (SELECT 1 FROM (SELECT t.id FROM u AS t) AS d WHERE d.\"t.id\" = dst.id) RETURNING dst.id").unwrap();
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(result.columns, ["dst.id"]);
    });
}

#[test]
fn qualified_derived_output_name_keeps_its_literal_dots() {
    with_connection(|conn| {
        let result = conn
            .query("SELECT d.\"t.id\" FROM (SELECT t.id FROM u AS t) AS d")
            .unwrap();
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
        assert_eq!(result.columns, ["d.t.id"]);
    });
}

#[test]
fn fresh_alias_does_not_collide_with_user_alias() {
    with_connection(|conn| {
        let result = conn.query("UPDATE t AS dst SET v = 7 WHERE EXISTS (SELECT 1 FROM u AS t JOIN u AS __citadel_dml_scope_0 ON t.id = __citadel_dml_scope_0.id WHERE t.id = dst.id) RETURNING dst.id").unwrap();
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
    });
}

#[test]
fn insert_alias_and_excluded_have_distinct_bindings() {
    for prepared in [false, true] {
        with_connection(|conn| {
            let result = query(conn, "INSERT INTO t AS dst VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = dst.v + excluded.v WHERE dst.id = excluded.id RETURNING dst.id, dst.v", prepared);
            assert_eq!(result.columns, ["dst.id", "dst.v"]);
            assert_eq!(
                result.rows,
                vec![vec![Value::Integer(1), Value::Integer(17)]]
            );
        });
    }
}

#[test]
fn target_aliases_named_old_or_new_do_not_become_pseudo_rows() {
    for alias in ["old", "new"] {
        with_connection(|conn| {
            let sql = format!("INSERT INTO t AS {alias} VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = {alias}.v + excluded.v RETURNING {alias}.*");
            assert_eq!(
                conn.query(&sql).unwrap().rows,
                vec![vec![Value::Integer(1), Value::Integer(17)]]
            );
        });
    }
}

#[test]
fn local_pseudo_named_alias_shadows_external_row_namespace() {
    with_connection(|conn| {
        conn.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TRIGGER changed AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id); END").unwrap();
        let result = conn.query("UPDATE t AS dst SET v = 7 WHERE EXISTS (SELECT 1 FROM u AS old WHERE old.id = dst.id) RETURNING dst.id").unwrap();
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);
    });
}

#[test]
fn invalid_qualifiers_and_unsupported_correlations_cannot_mutate_rows() {
    for sql in [
        "UPDATE t AS dst SET v = wrong.v WHERE dst.id = 1",
        "UPDATE t AS dst SET v = 7 WHERE t.id = 1",
        "DELETE FROM t AS dst WHERE EXISTS (SELECT 1 FROM u AS src WHERE wrong.id = dst.id)",
        "UPDATE t AS dst SET v = (SELECT src.v FROM u AS src WHERE src.id = dst.id)",
        "UPDATE t AS dst SET v = 7 RETURNING (SELECT 1)",
        "INSERT INTO t AS dst VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = (SELECT 1)",
        "INSERT INTO t AS excluded VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        "INSERT INTO t AS dst SELECT dst.id, src.v FROM u AS src",
    ] {
        with_connection(|conn| {
            conn.execute("BEGIN").unwrap();
            let error = conn.execute(sql).unwrap_err();
            assert!(
                matches!(
                    error,
                    SqlError::ColumnNotFound(_)
                        | SqlError::Unsupported(_)
                        | SqlError::AmbiguousColumn(_)
                ),
                "{sql}: {error:?}"
            );
            assert_eq!(
                conn.query("SELECT v FROM t ORDER BY id").unwrap().rows,
                vec![
                    vec![Value::Integer(10)],
                    vec![Value::Integer(20)],
                    vec![Value::Integer(30)]
                ]
            );
            conn.execute("COMMIT").unwrap();
        });
    }
}

#[test]
fn insert_source_local_alias_remains_independent_of_target_alias() {
    with_connection(|conn| {
        conn.execute("DELETE FROM t").unwrap();
        conn.execute("INSERT INTO t AS dst SELECT dst.id, dst.v FROM u AS dst")
            .unwrap();
        assert_eq!(
            conn.query("SELECT * FROM t").unwrap().rows,
            vec![vec![Value::Integer(2), Value::Integer(200)]]
        );
    });
}
