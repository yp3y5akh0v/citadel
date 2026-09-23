use super::*;
use citadel::{Argon2Profile, Database, DatabaseBuilder, SyncMode};

use crate::parser::{parse_sql, Statement};
use crate::Connection;

fn database(path: &std::path::Path) -> Database {
    DatabaseBuilder::new(path.join("copy.citadel"))
        .passphrase(b"copy-tests")
        .argon2_profile(Argon2Profile::Iot)
        .sync_mode(SyncMode::Off)
        .create()
        .unwrap()
}

fn insert(sql: &str) -> InsertStmt {
    let Statement::Insert(statement) = parse_sql(sql).unwrap() else {
        panic!("expected INSERT");
    };
    statement
}

fn setup(conn: &Connection<'_>) {
    for table in ["source", "copied", "fallback"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id INTEGER NOT NULL PRIMARY KEY, value TEXT)"
        ))
        .unwrap();
    }
}

#[test]
fn copy_plan_requires_an_identity_query_and_compatible_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for ddl in [
        "CREATE TABLE nonnull (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
        "CREATE TABLE coerced (id INTEGER NOT NULL PRIMARY KEY, value INTEGER)",
        "CREATE TABLE stricter (id INTEGER NOT NULL PRIMARY KEY, value TEXT) STRICT",
        "CREATE TABLE defaulted (id INTEGER NOT NULL PRIMARY KEY, value TEXT DEFAULT 'x')",
        "CREATE TABLE checked (id INTEGER NOT NULL PRIMARY KEY, value TEXT CHECK (value <> 'x'))",
        "CREATE TABLE indexed (id INTEGER NOT NULL PRIMARY KEY, value TEXT UNIQUE)",
        "CREATE TABLE generated (id INTEGER NOT NULL PRIMARY KEY, value TEXT GENERATED ALWAYS AS ('x') STORED)",
        "CREATE TABLE referenced (id INTEGER NOT NULL PRIMARY KEY, value TEXT, FOREIGN KEY (id) REFERENCES source(id))",
        "CREATE TABLE reordered (id INTEGER NOT NULL, value TEXT PRIMARY KEY)",
        "CREATE VIEW source_view AS SELECT * FROM source",
    ] {
        conn.execute(ddl).unwrap();
    }
    let schema = SchemaManager::load(&db).unwrap();
    let empty = CteContext::default();
    for sql in [
        "INSERT INTO copied SELECT * FROM source",
        "INSERT INTO copied(id, value) SELECT id, value FROM source",
        "INSERT INTO copied SELECT s.id AS other_id, s.value AS other_value FROM source AS s",
    ] {
        assert!(
            CopyPlan::new(&schema, &insert(sql), &empty).is_some(),
            "{sql}"
        );
    }
    for sql in [
        "INSERT INTO copied SELECT * FROM source WHERE TRUE",
        "INSERT INTO copied SELECT * FROM source ORDER BY id",
        "INSERT INTO copied SELECT * FROM source LIMIT 1",
        "INSERT INTO copied SELECT DISTINCT * FROM source",
        "INSERT INTO copied SELECT id + 1, value FROM source",
        "INSERT INTO copied SELECT value, id FROM source",
        "INSERT INTO copied(value, id) SELECT value, id FROM source",
        "INSERT INTO copied SELECT wrong.id, wrong.value FROM source AS s",
        "INSERT INTO copied SELECT * FROM source ON CONFLICT DO NOTHING",
        "INSERT INTO copied SELECT * FROM source RETURNING *",
        "INSERT INTO copied WITH s AS (SELECT * FROM source) SELECT * FROM s",
        "INSERT INTO copied SELECT * FROM source_view",
        "INSERT INTO source SELECT * FROM source",
        "INSERT INTO nonnull SELECT * FROM source",
        "INSERT INTO coerced SELECT * FROM source",
        "INSERT INTO stricter SELECT * FROM source",
        "INSERT INTO defaulted SELECT * FROM source",
        "INSERT INTO checked SELECT * FROM source",
        "INSERT INTO indexed SELECT * FROM source",
        "INSERT INTO generated SELECT * FROM source",
        "INSERT INTO referenced SELECT * FROM source",
        "INSERT INTO reordered SELECT * FROM source",
    ] {
        assert!(
            CopyPlan::new(&schema, &insert(sql), &empty).is_none(),
            "{sql}"
        );
    }
    let mut outer = CteContext::default();
    outer.insert(
        "source".into(),
        super::super::CteRows::binary(crate::QueryResult {
            columns: vec!["id".into(), "value".into()],
            rows: vec![],
        })
        .shared(),
    );
    assert!(CopyPlan::new(
        &schema,
        &insert("INSERT INTO copied SELECT * FROM source"),
        &outer,
    )
    .is_none());
}

#[test]
fn encoded_copy_matches_fallback_across_batches_overflow_and_execution_modes() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("BEGIN").unwrap();
    let seed = conn.prepare("INSERT INTO source VALUES ($1, $2)").unwrap();
    for id in -4100..20 {
        let value = if id >= 0 {
            Value::Text(format!("{id}:{}", "text".repeat(20_000)).into())
        } else if id % 7 == 0 {
            Value::Null
        } else {
            Value::Text(format!("row {id}").into())
        };
        seed.execute(&[Value::Integer(id), value]).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let expected = conn.query("SELECT * FROM source ORDER BY id").unwrap().rows;
    for (prepared, explicit) in [(false, false), (true, false), (false, true), (true, true)] {
        if explicit {
            conn.execute("BEGIN").unwrap();
        }
        let sql = "INSERT INTO copied SELECT s.id, s.value FROM source AS s";
        if prepared {
            assert_eq!(
                conn.prepare(sql).unwrap().execute(&[]).unwrap(),
                expected.len() as u64
            );
        } else {
            assert!(matches!(
                conn.execute(sql).unwrap(),
                ExecutionResult::RowsAffected(4120)
            ));
        }
        conn.execute("INSERT INTO fallback SELECT * FROM source WHERE TRUE")
            .unwrap();
        if explicit {
            conn.execute("COMMIT").unwrap();
        }
        assert_eq!(
            conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows,
            expected
        );
        assert_eq!(
            conn.query("SELECT * FROM fallback ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
        conn.execute("DELETE FROM copied").unwrap();
        conn.execute("DELETE FROM fallback").unwrap();
    }
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    conn.execute("DROP TABLE source").unwrap();
    drop(seed);
    drop(conn);
    drop(db);
    let db = DatabaseBuilder::new(dir.path().join("copy.citadel"))
        .passphrase(b"copy-tests")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows,
        expected
    );
}

#[test]
fn encoded_copy_empty_and_duplicate_leave_the_same_atomic_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    assert!(matches!(
        conn.execute("INSERT INTO copied SELECT * FROM source")
            .unwrap(),
        ExecutionResult::RowsAffected(0)
    ));
    conn.execute("INSERT INTO source VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    for table in ["copied", "fallback"] {
        conn.execute(&format!("INSERT INTO {table} VALUES (2, 'existing')"))
            .unwrap();
    }
    for (table, suffix) in [("copied", ""), ("fallback", " WHERE TRUE")] {
        assert!(matches!(
            conn.execute(&format!("INSERT INTO {table} SELECT * FROM source{suffix}")),
            Err(SqlError::DuplicateKey)
        ));
        assert_eq!(
            conn.query(&format!("SELECT * FROM {table}")).unwrap().rows,
            vec![vec![Value::Integer(2), Value::Text("existing".into())]]
        );
    }
}

#[test]
fn insert_select_with_triggers_keeps_the_materialized_source_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("INSERT INTO source VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    let expected = conn.query("SELECT * FROM source ORDER BY id").unwrap().rows;
    conn.execute("CREATE TRIGGER change_source AFTER INSERT ON copied FOR EACH ROW BEGIN UPDATE source SET value = 'changed' WHERE id = NEW.id + 1; END").unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    assert!(CopyPlan::new(
        &schema,
        &insert("INSERT INTO copied SELECT * FROM source"),
        &CteContext::default()
    )
    .is_none());
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    assert_eq!(
        conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows,
        expected
    );
    assert_ne!(
        conn.query("SELECT * FROM source ORDER BY id").unwrap().rows,
        expected
    );
}

#[test]
fn encoded_copy_handles_short_rows_and_falls_back_for_drop_and_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE source (id INTEGER NOT NULL PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO source VALUES (1), (2)").unwrap();
    conn.execute("ALTER TABLE source ADD COLUMN value TEXT")
        .unwrap();
    conn.execute("CREATE TABLE copied (id INTEGER NOT NULL PRIMARY KEY, value TEXT)")
        .unwrap();
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    assert_eq!(
        conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Null]
        ]
    );
    conn.execute("ALTER TABLE source ADD COLUMN added TEXT DEFAULT 'defaulted'")
        .unwrap();
    conn.execute("ALTER TABLE copied ADD COLUMN added TEXT")
        .unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    assert!(CopyPlan::new(
        &schema,
        &insert("INSERT INTO copied SELECT * FROM source"),
        &CteContext::default()
    )
    .is_none());
    conn.execute("DELETE FROM copied").unwrap();
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    assert_eq!(
        conn.query("SELECT added FROM copied ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![Value::Text("defaulted".into())]; 2]
    );
    conn.execute("ALTER TABLE source DROP COLUMN value")
        .unwrap();
    conn.execute("ALTER TABLE copied DROP COLUMN value")
        .unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    assert!(CopyPlan::new(
        &schema,
        &insert("INSERT INTO copied SELECT * FROM source"),
        &CteContext::default()
    )
    .is_none());
    conn.execute("DELETE FROM copied").unwrap();
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    assert_eq!(
        conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows,
        conn.query("SELECT * FROM source ORDER BY id").unwrap().rows
    );
}

#[test]
fn encoded_copy_charges_each_source_value_once_and_poisoning_survives_partial_failure() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("BEGIN").unwrap();
    let seed = conn.prepare("INSERT INTO source VALUES ($1, $2)").unwrap();
    let text = Value::Text("s".repeat(80_000).into());
    for id in 0..12 {
        seed.execute(&[Value::Integer(id), text.clone()]).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let schema = SchemaManager::load(&db).unwrap();
    let statement = insert("INSERT INTO copied SELECT * FROM source");
    assert!(CopyPlan::new(&schema, &statement, &CteContext::default()).is_some());
    let mut txn = db.begin_write().unwrap();
    let mut total = 0;
    txn.table_for_each(b"source", |_, value| {
        total += value.len();
        Ok(())
    })
    .unwrap();
    txn.table_entry_count(b"copied").unwrap();
    let budget = citadel_txn::ReadBudget::new(total, total);
    txn.set_read_budget(Some(budget.clone()));
    assert!(matches!(
        super::super::exec_insert_in_admitted_txn(&mut txn, &schema, &statement, &[]).unwrap(),
        ExecutionResult::RowsAffected(12)
    ));
    assert_eq!(budget.remaining(), 0);
    drop(txn);

    let mut txn = db.begin_write().unwrap();
    txn.set_read_budget(Some(citadel_txn::ReadBudget::new(total, total / 2)));
    let error =
        super::super::exec_insert_in_admitted_txn(&mut txn, &schema, &statement, &[]).unwrap_err();
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::ReadBudgetExceeded { .. })
    ));
    assert!(matches!(
        txn.commit(),
        Err(citadel_core::Error::TransactionFailed)
    ));
    assert!(conn.query("SELECT * FROM copied").unwrap().rows.is_empty());

    let mut txn = db.begin_write().unwrap();
    let cancel = citadel::CancelToken::new();
    cancel.cancel();
    txn.set_cancel(Some(cancel));
    assert!(matches!(
        super::super::exec_insert_in_admitted_txn(&mut txn, &schema, &statement, &[]),
        Err(SqlError::Storage(citadel_core::Error::Interrupted))
    ));
    txn.set_cancel(None);
    assert_eq!(txn.table_entry_count(b"copied").unwrap(), 0);
    txn.commit().unwrap();
}

#[test]
fn encoded_copy_coerces_legacy_stored_types_and_resumes_by_the_source_key() {
    use crate::encoding::{encode_composite_key, encode_row};

    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path());
    let conn = Connection::open(&db).unwrap();
    for table in ["source", "copied", "fallback"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id REAL NOT NULL PRIMARY KEY, value TEXT)"
        ))
        .unwrap();
    }
    // Older cascade paths could store values without applying the destination
    // column's coercion. Reproduce that physical state without weakening DDL.
    let mut txn = db.begin_write().unwrap();
    for id in 0..4100 {
        let key = encode_composite_key(&[Value::Integer(id)]);
        let value = encode_row(&[Value::Json(id.to_string().into())]);
        txn.table_insert(b"source", &key, &value).unwrap();
    }
    txn.commit().unwrap();
    conn.execute("INSERT INTO copied SELECT * FROM source")
        .unwrap();
    conn.execute("INSERT INTO fallback SELECT * FROM source WHERE TRUE")
        .unwrap();
    let copied = conn.query("SELECT * FROM copied ORDER BY id").unwrap().rows;
    let fallback = conn
        .query("SELECT * FROM fallback ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(copied, fallback);
    assert_eq!(copied.len(), 4100);
    assert_eq!(
        copied[4099],
        vec![Value::Real(4099.0), Value::Text("4099".into())]
    );

    conn.execute(
        "CREATE TABLE nonnull_source (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE nonnull_copy (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
    )
    .unwrap();
    let mut txn = db.begin_write().unwrap();
    txn.table_insert(
        b"nonnull_source",
        &encode_composite_key(&[Value::Integer(1)]),
        &encode_row(&[Value::Null]),
    )
    .unwrap();
    txn.commit().unwrap();
    for suffix in ["", " WHERE TRUE"] {
        assert!(matches!(
            conn.execute(&format!(
                "INSERT INTO nonnull_copy SELECT * FROM nonnull_source{suffix}"
            )),
            Err(SqlError::NotNullViolation(_))
        ));
    }
    assert!(conn
        .query("SELECT * FROM nonnull_copy")
        .unwrap()
        .rows
        .is_empty());

    for table in ["typed_source", "typed_copy"] {
        conn.execute(&format!(
            "CREATE TABLE {table} (id INTEGER NOT NULL PRIMARY KEY, value INTEGER)"
        ))
        .unwrap();
    }
    let mut txn = db.begin_write().unwrap();
    txn.table_insert(
        b"typed_source",
        &encode_composite_key(&[Value::Integer(1)]),
        &encode_row(&[Value::Text("7".into())]),
    )
    .unwrap();
    txn.commit().unwrap();
    // Non-STRICT SQL does not implicitly coerce TEXT to INTEGER.
    for suffix in ["", " WHERE TRUE"] {
        assert!(matches!(
            conn.execute(&format!(
                "INSERT INTO typed_copy SELECT * FROM typed_source{suffix}"
            )),
            Err(SqlError::TypeMismatch { .. })
        ));
    }
    assert!(conn
        .query("SELECT * FROM typed_copy")
        .unwrap()
        .rows
        .is_empty());
}
