use std::fmt::Write as _;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::schema::SchemaManager;
use citadel_sql::{Connection, SqlError, TableSchema, Value};

const MAX_PHYSICAL: usize = 32767;

fn database(dir: &std::path::Path, reopen: bool) -> citadel::Database {
    let builder = DatabaseBuilder::new(dir.join("count.db"))
        .passphrase(b"count-boundary-test")
        .argon2_profile(Argon2Profile::Iot);
    if reopen {
        builder.open().unwrap()
    } else {
        builder.create().unwrap()
    }
}

fn wide_create(name: &str, non_pk_count: usize, if_not_exists: bool) -> String {
    let mut sql = format!(
        "CREATE TABLE {}{name}(id INTEGER PRIMARY KEY",
        if if_not_exists { "IF NOT EXISTS " } else { "" }
    );
    for i in 0..non_pk_count {
        write!(sql, ", c{i} INTEGER").unwrap();
    }
    sql.push(')');
    sql
}

fn physical_error(result: Result<citadel_sql::ExecutionResult, SqlError>) {
    assert!(matches!(result, Err(SqlError::InvalidValue(message)) if message.contains("32767")));
}

#[test]
fn create_rejects_oversized_rows_in_both_transaction_modes_before_catalog_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path(), false);
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE stable(id INTEGER PRIMARY KEY)")
        .unwrap();
    let bad = wide_create("bad", MAX_PHYSICAL + 1, false);
    physical_error(conn.execute(&bad));
    assert!(matches!(
        conn.query("SELECT id FROM bad"),
        Err(SqlError::TableNotFound(_))
    ));
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_bad").unwrap();
    physical_error(conn.execute(&bad));
    conn.execute("ROLLBACK TO before_bad").unwrap();
    conn.execute("RELEASE before_bad").unwrap();
    conn.execute("INSERT INTO stable VALUES(7)").unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(matches!(
        conn.query("SELECT id FROM bad"),
        Err(SqlError::TableNotFound(_))
    ));
    // Existing-table no-op is resolved before validating a never-used definition.
    conn.execute(&wide_create("stable", MAX_PHYSICAL + 1, true))
        .unwrap();
    assert_eq!(
        conn.query("SELECT id FROM stable").unwrap().rows,
        vec![vec![Value::Integer(7)]]
    );
    drop(conn);
    drop(db);
    let db = database(dir.path(), true);
    let conn = Connection::open(&db).unwrap();
    assert!(matches!(
        conn.query("SELECT id FROM bad"),
        Err(SqlError::TableNotFound(_))
    ));
    assert_eq!(
        conn.query("SELECT id FROM stable").unwrap().rows,
        vec![vec![Value::Integer(7)]]
    );
}

#[test]
fn maximum_physical_row_roundtrips_and_transition_admission_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path(), false);
    let conn = Connection::open(&db).unwrap();
    conn.execute(&wide_create("wide", MAX_PHYSICAL, false))
        .unwrap();
    conn.execute("INSERT INTO wide(id,c0,c32766) VALUES(1,11,22)")
        .unwrap();
    assert_eq!(
        conn.query("SELECT c0,c32766 FROM wide").unwrap().rows,
        vec![vec![Value::Integer(11), Value::Integer(22)]]
    );
    conn.execute("CREATE TABLE captured(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER capture AFTER INSERT ON wide REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT BEGIN INSERT INTO captured SELECT id FROM new_rows; END").unwrap();
    // Transition storage includes the base PK, so this otherwise valid row is too wide.
    physical_error(conn.execute("INSERT INTO wide(id,c0,c32766) VALUES(2,33,44)"));
    assert_eq!(
        conn.query("SELECT id FROM wide").unwrap().rows,
        vec![vec![Value::Integer(1)]]
    );
    assert!(conn
        .query("SELECT id FROM captured")
        .unwrap()
        .rows
        .is_empty());
    conn.execute("DROP TRIGGER capture").unwrap();
    conn.execute("INSERT INTO wide(id,c0,c32766) VALUES(2,33,44)")
        .unwrap();
    // A projected duplicate PK still occupies a non-PK backing slot in a matview.
    physical_error(
        conn.execute("CREATE MATERIALIZED VIEW too_wide AS SELECT *, id AS extra FROM wide"),
    );
    drop(conn);
    drop(db);
    let db = database(dir.path(), true);
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT c0,c32766 FROM wide ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(11), Value::Integer(22)],
            vec![Value::Integer(33), Value::Integer(44)]
        ]
    );
    assert!(matches!(
        conn.query("SELECT id FROM too_wide"),
        Err(SqlError::TableNotFound(_))
    ));
}

#[test]
fn alter_counts_dropped_slots_preserves_noops_and_recovers_via_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path(), false);
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, anchor INTEGER)")
            .unwrap();
    }
    let loaded = SchemaManager::load(&db).unwrap();
    let original = loaded.get("t").unwrap();
    // Equivalent to prior DROP history; avoid 32765 SQL schema rewrites in the fixture.
    let schema = TableSchema::with_drops(
        original.name.clone(),
        original.columns.clone(),
        original.primary_key_columns.clone(),
        vec![],
        vec![],
        vec![],
        (0..32765u16).collect(),
    );
    let mut wtx = db.begin_write().unwrap();
    SchemaManager::save_schema(&mut wtx, &schema).unwrap();
    wtx.commit().unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("INSERT INTO t VALUES(1,11)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT room").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN last INTEGER")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(2,22,33)").unwrap();
    conn.execute("SAVEPOINT full_layout").unwrap();
    physical_error(conn.execute("ALTER TABLE t ADD COLUMN excessive INTEGER"));
    conn.execute("ROLLBACK TO full_layout").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN IF NOT EXISTS anchor TEXT")
        .unwrap();
    // VIRTUAL columns have a physical NULL slot too.
    physical_error(conn.execute(
        "ALTER TABLE t ADD COLUMN virtual_extra INTEGER GENERATED ALWAYS AS (anchor + 1) VIRTUAL",
    ));
    conn.execute("ROLLBACK TO full_layout").unwrap();
    assert_eq!(
        conn.query("SELECT anchor,last FROM t ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![Value::Integer(11), Value::Null],
            vec![Value::Integer(22), Value::Integer(33)]
        ]
    );
    conn.execute("ROLLBACK TO room").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN last INTEGER")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    drop(conn);
    drop(db);
    let db = database(dir.path(), true);
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query("SELECT anchor,last FROM t").unwrap().rows,
        vec![vec![Value::Integer(11), Value::Null]]
    );
    physical_error(conn.execute("ALTER TABLE t ADD COLUMN excessive INTEGER"));
}

#[test]
fn save_schema_rejects_publicly_mutated_holes_without_overwriting_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path(), false);
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, keep INTEGER, removed INTEGER)")
            .unwrap();
        conn.execute("ALTER TABLE t DROP COLUMN removed").unwrap();
    }
    let loaded = SchemaManager::load(&db).unwrap();
    let mut invalid = loaded.get("t").unwrap().clone();
    let original_bytes = invalid.serialize();
    invalid.primary_key_columns.push(1);
    let mut wtx = db.begin_write().unwrap();
    assert!(
        matches!(SchemaManager::save_schema(&mut wtx, &invalid), Err(SqlError::InvalidValue(message)) if message.contains("dropped physical"))
    );
    // The owner rejected before table_insert, so an otherwise successful txn is unchanged.
    wtx.commit().unwrap();
    assert_eq!(
        SchemaManager::load(&db)
            .unwrap()
            .get("t")
            .unwrap()
            .serialize(),
        original_bytes
    );
}

#[test]
fn refresh_rechecks_expanded_projection_before_truncate_or_concurrent_merge() {
    let dir = tempfile::tempdir().unwrap();
    let db = database(dir.path(), false);
    let conn = Connection::open(&db).unwrap();
    conn.execute(&wide_create("src", MAX_PHYSICAL - 1, false))
        .unwrap();
    conn.execute("INSERT INTO src(id,c0) VALUES(1,11)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT *, id AS extra FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX mv_id ON mv(id)").unwrap();
    conn.execute("ALTER TABLE src ADD COLUMN newest INTEGER")
        .unwrap();
    conn.execute("UPDATE src SET c0=22 WHERE id=1").unwrap();
    for concurrent in [false, true] {
        let sql = if concurrent {
            "REFRESH MATERIALIZED VIEW CONCURRENTLY mv"
        } else {
            "REFRESH MATERIALIZED VIEW mv"
        };
        physical_error(conn.execute(sql));
        assert_eq!(
            conn.query("SELECT c0 FROM mv").unwrap().rows,
            vec![vec![Value::Integer(11)]]
        );
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT before_refresh").unwrap();
        physical_error(conn.execute(sql));
        conn.execute("ROLLBACK TO before_refresh").unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            conn.query("SELECT c0 FROM mv").unwrap().rows,
            vec![vec![Value::Integer(11)]]
        );
    }
}
