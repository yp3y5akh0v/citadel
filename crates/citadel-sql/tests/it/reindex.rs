use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::encoding::encode_composite_key;
use citadel_sql::{Connection, SqlError, TableSchema, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"reindex")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn empty_index(db: &Database, conn: &Connection<'_>, table: &str, index: &str) {
    let table = conn.table_schema(table).unwrap().name;
    let mut write = db.begin_write().unwrap();
    write
        .table_truncate(&TableSchema::index_table_name(&table, index))
        .unwrap();
    write.commit().unwrap();
}

fn ids(conn: &Connection<'_>, sql: &str) -> Vec<Value> {
    conn.query(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

#[test]
fn every_form_rebuilds_an_index_from_its_rows() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE INDEX t_v ON t (v)",
        "INSERT INTO t VALUES (1, 5), (2, 5), (3, 7)",
    ] {
        conn.execute(sql).unwrap();
    }
    let lookup = "SELECT id FROM t WHERE v = 5 ORDER BY id";
    for form in [
        "REINDEX INDEX t_v",
        "REINDEX TABLE t",
        "REINDEX t",
        "REINDEX t_v",
        "REINDEX",
        "REINDEX DATABASE",
        "reindex table T;",
    ] {
        empty_index(&db, &conn, "t", "t_v");
        assert!(ids(&conn, lookup).is_empty(), "{form}");
        conn.execute(form).unwrap();
        assert_eq!(
            ids(&conn, lookup),
            [Value::Integer(1), Value::Integer(2)],
            "{form}"
        );
    }
    empty_index(&db, &conn, "t", "t_v");
    conn.execute_batch("REINDEX TABLE t; SELECT 1").unwrap();
    assert_eq!(ids(&conn, lookup), [Value::Integer(1), Value::Integer(2)]);
}

#[test]
fn reindex_database_rebuilds_permanent_tables_shadowed_by_temp() {
    for form in ["REINDEX", "REINDEX DATABASE"] {
        let db = database();
        let temporary = Connection::open(&db).unwrap();
        temporary
            .execute("CREATE TEMP TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        temporary
            .execute("CREATE INDEX temporary_v ON t (v)")
            .unwrap();
        temporary.execute("INSERT INTO t VALUES (9, 90)").unwrap();

        // A separate connection may create a permanent name after this
        // connection's TEMP alias exists. Both physical tables must be rebuilt.
        let permanent = Connection::open(&db).unwrap();
        permanent
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        permanent
            .execute("CREATE INDEX permanent_v ON t (v)")
            .unwrap();
        permanent.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        empty_index(&db, &permanent, "t", "permanent_v");
        empty_index(&db, &temporary, "t", "temporary_v");
        let permanent_lookup = "SELECT id FROM t WHERE v = 10";
        let temporary_lookup = "SELECT id FROM t WHERE v = 90";
        assert!(ids(&permanent, permanent_lookup).is_empty());
        assert!(ids(&temporary, temporary_lookup).is_empty());

        temporary.execute(form).unwrap();

        assert_eq!(
            ids(&permanent, permanent_lookup),
            [Value::Integer(1)],
            "{form}"
        );
        assert_eq!(
            ids(&temporary, temporary_lookup),
            [Value::Integer(9)],
            "{form}"
        );
    }
}

#[test]
fn reindex_named_index_finds_permanent_table_shadowed_by_temp() {
    for form in ["REINDEX INDEX permanent_v", "REINDEX permanent_v"] {
        let db = database();
        let temporary = Connection::open(&db).unwrap();
        temporary
            .execute("CREATE TEMP TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        temporary.execute("INSERT INTO t VALUES (9, 90)").unwrap();
        let permanent = Connection::open(&db).unwrap();
        permanent
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        permanent
            .execute("CREATE INDEX permanent_v ON t (v)")
            .unwrap();
        permanent.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        empty_index(&db, &permanent, "t", "permanent_v");
        let lookup = "SELECT id FROM t WHERE v = 10";
        assert!(ids(&permanent, lookup).is_empty());

        temporary.execute(form).unwrap();

        assert_eq!(ids(&permanent, lookup), [Value::Integer(1)], "{form}");
        assert_eq!(
            ids(&temporary, "SELECT id FROM t"),
            [Value::Integer(9)],
            "{form}"
        );
    }
}

#[test]
fn drop_index_uses_its_permanent_table_when_shadowed_by_temp() {
    let db = database();
    let temporary = Connection::open(&db).unwrap();
    temporary
        .execute("CREATE TEMP TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    temporary
        .execute("CREATE INDEX temporary_v ON t (v)")
        .unwrap();
    temporary.execute("INSERT INTO t VALUES (9, 90)").unwrap();
    let permanent = Connection::open(&db).unwrap();
    permanent
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    permanent
        .execute("CREATE INDEX permanent_v ON t (v)")
        .unwrap();
    permanent.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    temporary.execute("DROP INDEX permanent_v").unwrap();

    // table_schema reads the connection's admitted catalog. Reopen to check
    // persisted metadata after another connection performed the DDL.
    let reopened = Connection::open(&db).unwrap();
    assert!(reopened.table_schema("t").unwrap().indices.is_empty());
    let temp_schema = temporary.table_schema("t").unwrap();
    assert!(temp_schema.index_by_name("temporary_v").is_some());
    assert_eq!(temp_schema.indices.len(), 1);
    assert_eq!(
        ids(&reopened, "SELECT id FROM t WHERE v = 10"),
        [Value::Integer(1)]
    );
    assert_eq!(
        ids(&temporary, "SELECT id FROM t WHERE v = 90"),
        [Value::Integer(9)]
    );
}

#[test]
fn reindex_drops_keys_no_row_holds() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER)",
        "CREATE UNIQUE INDEX t_u ON t (u)",
        "INSERT INTO t VALUES (1, 5)",
    ] {
        conn.execute(sql).unwrap();
    }
    let mut write = db.begin_write().unwrap();
    let stale = encode_composite_key(&[Value::Integer(42)]);
    write
        .table_insert(&TableSchema::index_table_name("t", "t_u"), &stale, &[])
        .unwrap();
    write.commit().unwrap();
    let insert = "INSERT INTO t VALUES (2, 42)";
    assert!(matches!(
        conn.execute(insert),
        Err(SqlError::UniqueViolation(_))
    ));
    conn.execute("REINDEX INDEX t_u").unwrap();
    conn.execute(insert).unwrap();
}

#[test]
fn reindex_rebuilds_temporary_materialized_and_inverted_indexes() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE INDEX scratch_v ON scratch (v)",
        "INSERT INTO scratch VALUES (1, 5), (2, 7)",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO source VALUES (1, 5), (2, 7)",
        "CREATE MATERIALIZED VIEW snapshot AS SELECT id, v FROM source",
        "CREATE INDEX snapshot_v ON snapshot (v)",
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, payload JSONB)",
        "CREATE INDEX docs_payload ON docs USING gin (payload)",
        "INSERT INTO docs VALUES (1, '{\"a\": 1}'), (2, '{\"a\": 2}')",
    ] {
        conn.execute(sql).unwrap();
    }
    for (table, index, reindex, lookup) in [
        (
            "scratch",
            "scratch_v",
            "REINDEX TABLE scratch",
            "SELECT id FROM scratch WHERE v = 5",
        ),
        (
            "snapshot",
            "snapshot_v",
            "REINDEX snapshot",
            "SELECT id FROM snapshot WHERE v = 5",
        ),
        (
            "docs",
            "docs_payload",
            "REINDEX INDEX docs_payload",
            "SELECT id FROM docs WHERE payload @> '{\"a\": 1}'::JSONB",
        ),
    ] {
        empty_index(&db, &conn, table, index);
        assert!(ids(&conn, lookup).is_empty(), "{lookup}");
        conn.execute(reindex).unwrap();
        assert_eq!(ids(&conn, lookup), [Value::Integer(1)], "{reindex}");
    }
}

#[test]
fn reindex_keeps_nearest_neighbour_results() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE points (id INTEGER PRIMARY KEY, v VECTOR(3))",
        "CREATE INDEX points_v ON points USING ann (v)",
        "INSERT INTO points VALUES (1, '[1, 0, 0]'::VECTOR(3)), (2, '[0, 1, 0]'::VECTOR(3)), \
         (3, '[0.9, 0.1, 0]'::VECTOR(3))",
    ] {
        conn.execute(sql).unwrap();
    }
    let nearest = "SELECT id FROM points ORDER BY v <-> '[1, 0, 0]'::VECTOR(3) LIMIT 2";
    let before = ids(&conn, nearest);
    assert_eq!(before, [Value::Integer(1), Value::Integer(3)]);
    for form in ["REINDEX INDEX points_v", "REINDEX TABLE points"] {
        conn.execute(form).unwrap();
        assert_eq!(ids(&conn, nearest), before, "{form}");
    }
}

#[test]
fn reindex_names_what_it_cannot_rebuild() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE VIEW w AS SELECT id FROM t",
    ] {
        conn.execute(sql).unwrap();
    }
    for sql in ["REINDEX TABLE nope", "REINDEX nope", "REINDEX TABLE w"] {
        assert!(
            matches!(conn.execute(sql), Err(SqlError::TableNotFound(_))),
            "{sql}"
        );
    }
    assert!(matches!(
        conn.execute("REINDEX INDEX nope"),
        Err(SqlError::IndexNotFound(_))
    ));
    for sql in [
        "REINDEX (VERBOSE) TABLE t",
        "REINDEX TABLE CONCURRENTLY t",
        "REINDEX CONCURRENTLY t",
        "REINDEX SCHEMA public",
        "REINDEX SYSTEM",
        "REINDEX DATABASE citadel",
        "REINDEX TABLE",
        "REINDEX INDEX",
    ] {
        assert!(
            matches!(conn.execute(sql), Err(SqlError::Unsupported(_))),
            "{sql}"
        );
    }
    assert!(matches!(
        conn.execute("REINDEX t extra"),
        Err(SqlError::Parse(_))
    ));
    conn.execute("BEGIN READ ONLY").unwrap();
    assert!(matches!(
        conn.execute("REINDEX"),
        Err(SqlError::Unsupported(_))
    ));
    conn.execute("ROLLBACK").unwrap();
}
