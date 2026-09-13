use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, DataType, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"lazy-default-coercion")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn assert_rows(conn: &Connection<'_>, sql: &str, expected: &[Vec<Value>]) {
    assert_eq!(conn.query(sql).unwrap().rows, expected, "{sql}");
    let prepared = conn.prepare(sql).unwrap();
    assert_eq!(prepared.query_collect(&[]).unwrap().rows, expected, "{sql}");
    assert_eq!(
        prepared.query(&[]).unwrap().collect().unwrap().rows,
        expected,
        "{sql}"
    );
}

#[test]
fn missing_defaults_have_insert_types_before_projection_and_virtual_evaluation() {
    for strict in [false, true] {
        for (declared, default, expected_type) in [
            ("REAL", "7", DataType::Real),
            ("BOOLEAN", "1", DataType::Boolean),
            ("JSON", "'{\"x\":1}'", DataType::Json),
            ("JSONB", "'{\"x\":1}'", DataType::Jsonb),
            ("DATE", "'2020-01-02'", DataType::Date),
            ("TIME", "'03:04:05'", DataType::Time),
            ("TIMESTAMP", "'2020-01-02 03:04:05'", DataType::Timestamp),
            ("INTERVAL", "'1 day'", DataType::Interval),
        ] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY){}",
                if strict { " STRICT" } else { "" }
            ))
            .unwrap();
            conn.execute("INSERT INTO t VALUES(1)").unwrap();
            conn.execute(&format!(
                "ALTER TABLE t ADD COLUMN d {declared} DEFAULT {default}"
            ))
            .unwrap();
            conn.execute(&format!(
                "ALTER TABLE t ADD COLUMN v {declared} GENERATED ALWAYS AS(d) VIRTUAL"
            ))
            .unwrap();
            conn.execute("INSERT INTO t(id) VALUES(2)").unwrap();
            conn.execute("INSERT INTO t(id,d) VALUES(3,NULL)").unwrap();
            let inserted = conn.query("SELECT d FROM t WHERE id=2").unwrap().rows[0][0].clone();
            assert_eq!(inserted.data_type(), expected_type);
            for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
                if let Some(begin) = begin {
                    conn.execute(begin).unwrap();
                }
                for sql in ["SELECT d,v FROM t", "SELECT d,v FROM t ORDER BY id"] {
                    let rows = conn.query(sql).unwrap().rows;
                    assert_eq!(
                        rows[0][0].data_type(),
                        expected_type,
                        "{declared}, strict={strict}, {sql}"
                    );
                    assert_eq!(rows[0][1].data_type(), expected_type);
                    assert_rows(
                        &conn,
                        sql,
                        &[
                            vec![inserted.clone(), inserted.clone()],
                            vec![inserted.clone(), inserted.clone()],
                            vec![Value::Null, Value::Null],
                        ],
                    );
                }
                assert_rows(
                    &conn,
                    "SELECT * FROM t WHERE id=1",
                    &[vec![Value::Integer(1), inserted.clone(), inserted.clone()]],
                );
                if expected_type == DataType::Jsonb {
                    for predicate in [
                        "d @> CAST('{\"x\":1}' AS JSONB)",
                        "COALESCE(d @> CAST('{\"x\":1}' AS JSONB),FALSE)",
                    ] {
                        assert_rows(
                            &conn,
                            &format!("SELECT id FROM t WHERE {predicate}"),
                            &[vec![Value::Integer(1)], vec![Value::Integer(2)]],
                        );
                    }
                }
                if begin.is_some() {
                    conn.execute("ROLLBACK").unwrap();
                }
            }
        }
    }
}

#[test]
fn numeric_defaults_agree_in_raw_predicates_aggregates_grouping_and_topk() {
    for strict in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY){}",
            if strict { " STRICT" } else { "" }
        ))
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1)").unwrap();
        let default = if strict { "'7'" } else { "7.9" };
        conn.execute(&format!(
            "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT {default}"
        ))
        .unwrap();
        conn.execute("INSERT INTO t VALUES(2,2),(3,NULL)").unwrap();
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            for filter in ["n=7", "n BETWEEN 7 AND 7", "COALESCE(n=7,FALSE)"] {
                assert_rows(
                    &conn,
                    &format!("SELECT id FROM t WHERE {filter}"),
                    &[vec![Value::Integer(1)]],
                );
            }
            assert_rows(
                &conn,
                "SELECT SUM(n),MIN(n),MAX(n),COUNT(n) FROM t",
                &[vec![
                    Value::Integer(9),
                    Value::Integer(2),
                    Value::Integer(7),
                    Value::Integer(2),
                ]],
            );
            assert_rows(
                &conn,
                "SELECT n,COUNT(*) FROM t GROUP BY n ORDER BY n",
                &[
                    vec![Value::Null, Value::Integer(1)],
                    vec![Value::Integer(2), Value::Integer(1)],
                    vec![Value::Integer(7), Value::Integer(1)],
                ],
            );
            assert_rows(
                &conn,
                "SELECT id,n FROM t ORDER BY n DESC NULLS LAST LIMIT 1",
                &[vec![Value::Integer(1), Value::Integer(7)]],
            );
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn interval_defaults_are_normalized_before_streamed_aggregation() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,g INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1,7)").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN v INTERVAL DEFAULT 1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(2,7,INTERVAL '1 day')")
        .unwrap();
    let expected = Value::Interval {
        months: 0,
        days: 2,
        micros: 0,
    };
    for sql in [
        "SELECT SUM(v) FROM t",
        "SELECT SUM(v) FROM t WHERE id>0",
        "SELECT SUM(v) FROM t HAVING COUNT(*)>0",
    ] {
        assert_rows(&conn, sql, &[vec![expected.clone()]]);
    }
    assert_rows(
        &conn,
        "SELECT g,SUM(v) FROM t GROUP BY g",
        &[vec![Value::Integer(7), expected]],
    );
}

#[test]
fn invalid_defaults_error_only_when_missing_values_are_needed() {
    for (strict, definition, not_null) in [
        (false, "INTEGER DEFAULT '7'", false),
        (true, "INTEGER DEFAULT 1.5", false),
        (false, "JSONB DEFAULT 'invalid json'", false),
        (false, "INTEGER NOT NULL DEFAULT NULL", true),
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY){}",
            if strict { " STRICT" } else { "" }
        ))
        .unwrap();
        conn.execute(&format!("ALTER TABLE t ADD COLUMN n {definition}"))
            .unwrap();
        // The invalid default exists in metadata, but no missing value needs it.
        for sql in [
            "SELECT SUM(n) FROM t",
            "SELECT n,COUNT(*) FROM t GROUP BY n",
            "SELECT n FROM t ORDER BY n LIMIT 1",
        ] {
            conn.query(sql).unwrap();
        }
        let explicit = if definition.starts_with("JSONB") {
            "CAST('1' AS JSONB)"
        } else {
            "2"
        };
        conn.execute(&format!("INSERT INTO t VALUES(1,{explicit})"))
            .unwrap();
        for sql in [
            "SELECT n FROM t",
            "SELECT COUNT(n) FROM t",
            "SELECT n,COUNT(*) FROM t GROUP BY n",
            "SELECT n FROM t ORDER BY n LIMIT 1",
        ] {
            conn.query(sql).unwrap();
        }
        let inserted_error = conn.execute("INSERT INTO t(id) VALUES(2)").unwrap_err();
        if not_null {
            assert!(matches!(inserted_error, SqlError::NotNullViolation(_)));
        } else {
            assert!(matches!(inserted_error, SqlError::TypeMismatch { .. }));
        }

        // A separate table supplies an actual pre-ALTER row.
        conn.execute(&format!(
            "CREATE TABLE missing(id INTEGER PRIMARY KEY){}",
            if strict { " STRICT" } else { "" }
        ))
        .unwrap();
        conn.execute("INSERT INTO missing VALUES(1)").unwrap();
        conn.execute(&format!("ALTER TABLE missing ADD COLUMN n {definition}"))
            .unwrap();
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            assert_rows(&conn, "SELECT id FROM missing", &[vec![Value::Integer(1)]]);
            assert_rows(
                &conn,
                "SELECT COUNT(*) FROM missing",
                &[vec![Value::Integer(1)]],
            );
            for sql in [
                "SELECT id FROM missing WHERE id=1",
                "SELECT id FROM missing WHERE id>0",
                "SELECT id FROM missing WHERE COALESCE(id>0,FALSE)",
                "SELECT id FROM missing ORDER BY id",
                "SELECT missing.id FROM missing JOIN t ON missing.id=t.id WHERE missing.id=1",
            ] {
                assert_rows(&conn, sql, &[vec![Value::Integer(1)]]);
            }
            for sql in [
                "SELECT n FROM missing",
                "SELECT n FROM missing ORDER BY n LIMIT 1",
                "SELECT COUNT(n) FROM missing",
            ] {
                let error = conn.query(sql).unwrap_err();
                if not_null {
                    assert!(
                        matches!(error, SqlError::NotNullViolation(_)),
                        "{sql}: {error:?}"
                    );
                } else {
                    assert!(
                        matches!(error, SqlError::TypeMismatch { .. }),
                        "{sql}: {error:?}"
                    );
                }
            }
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn default_coercion_preserves_context_parameters_and_schema_savepoints() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN doc JSONB DEFAULT $1")
        .unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN local_time TEXT DEFAULT JSONB_PATH_QUERY_FIRST_TZ(CAST('\"2023-08-15T12:34:56+05:30\"' AS JSONB),'$.time().string()')").unwrap();
    let prepared = conn
        .prepare("SELECT doc,local_time FROM t WHERE $1 IS NOT NULL")
        .unwrap();
    for (zone, local_time) in [
        ("America/New_York", "\"03:04:56\""),
        ("+10:00", "\"17:04:56\""),
    ] {
        conn.set_session_timezone(zone).unwrap();
        let expected_time = Value::Text(local_time.into());
        for param in ["{\"n\":1}", "{\"n\":2}"] {
            let params = [Value::Text(param.into())];
            let expected_json = conn
                .query_params("SELECT CAST($1 AS JSONB)", &params)
                .unwrap()
                .rows[0][0]
                .clone();
            let expected = vec![vec![expected_json, expected_time.clone()]];
            assert_eq!(prepared.query_collect(&params).unwrap().rows, expected);
            assert_eq!(
                prepared.query(&params).unwrap().collect().unwrap().rows,
                expected
            );
        }
    }
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_column").unwrap();
    let star = conn
        .prepare("SELECT * FROM t WHERE $1 IS NOT NULL")
        .unwrap();
    assert_eq!(
        star.query_collect(&[Value::Text("{}".into())])
            .unwrap()
            .columns
            .len(),
        3
    );
    conn.execute("ALTER TABLE t ADD COLUMN extra REAL DEFAULT 4")
        .unwrap();
    let changed = star.query_collect(&[Value::Text("{}".into())]).unwrap();
    assert_eq!(changed.rows[0][3].data_type(), DataType::Real);
    conn.execute("ROLLBACK TO before_column").unwrap();
    assert_eq!(
        star.query_collect(&[Value::Text("{}".into())])
            .unwrap()
            .columns
            .len(),
        3
    );
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn short_row_writes_normalize_defaults_before_checks_and_preserve_rollback() {
    for explicit in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,x INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7.9 CHECK(n=CAST(n AS INTEGER))")
            .unwrap();
        if explicit {
            conn.execute("BEGIN").unwrap();
            conn.execute("SAVEPOINT before_update").unwrap();
        }
        let update = conn
            .prepare("UPDATE t SET x=x+1 WHERE id=$1 RETURNING x,n")
            .unwrap();
        assert_eq!(
            update.query_collect(&[Value::Integer(1)]).unwrap().rows,
            vec![vec![Value::Integer(11), Value::Integer(7)]]
        );
        let upsert = conn.prepare("INSERT INTO t(id,x) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET x=excluded.x RETURNING x,n").unwrap();
        assert_eq!(
            upsert
                .query_collect(&[Value::Integer(2), Value::Integer(21)])
                .unwrap()
                .rows,
            vec![vec![Value::Integer(21), Value::Integer(7)]]
        );
        if explicit {
            conn.execute("ROLLBACK TO before_update").unwrap();
            assert_rows(
                &conn,
                "SELECT x,n FROM t ORDER BY id",
                &[
                    vec![Value::Integer(10), Value::Integer(7)],
                    vec![Value::Integer(20), Value::Integer(7)],
                ],
            );
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn invalid_missing_default_cannot_publish_an_updated_prefix() {
    for prepared in [false, true] {
        for explicit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("rollback.db");
            let build = || {
                DatabaseBuilder::new(&path)
                    .passphrase(b"defaults")
                    .argon2_profile(Argon2Profile::Iot)
            };
            {
                let db = build().create().unwrap();
                let conn = Connection::open(&db).unwrap();
                conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,x INTEGER)")
                    .unwrap();
                conn.execute("INSERT INTO t VALUES(2,20)").unwrap();
                conn.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 'not integer'")
                    .unwrap();
                conn.execute("INSERT INTO t VALUES(1,10,1)").unwrap();
                if explicit {
                    conn.execute("BEGIN").unwrap();
                    conn.execute("SAVEPOINT before_update").unwrap();
                }
                let sql = "UPDATE t SET x=x+1 WHERE id BETWEEN 1 AND 2";
                let error = if prepared {
                    conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
                } else {
                    conn.execute(sql).unwrap_err()
                };
                assert!(matches!(error, SqlError::TypeMismatch { .. }), "{error:?}");
                if explicit {
                    conn.execute("ROLLBACK TO before_update").unwrap();
                    conn.execute("COMMIT").unwrap();
                }
                assert_rows(
                    &conn,
                    "SELECT id,x FROM t ORDER BY id",
                    &[
                        vec![Value::Integer(1), Value::Integer(10)],
                        vec![Value::Integer(2), Value::Integer(20)],
                    ],
                );
            }
            let db = build().open().unwrap();
            let conn = Connection::open(&db).unwrap();
            assert_rows(
                &conn,
                "SELECT id,x FROM t ORDER BY id",
                &[
                    vec![Value::Integer(1), Value::Integer(10)],
                    vec![Value::Integer(2), Value::Integer(20)],
                ],
            );
        }
    }
}
