use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

#[test]
fn create_matview_materializes_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Integer(1));
    assert_eq!(r.rows[2][1], Value::Integer(30));
}

#[test]
fn matview_does_not_reflect_underlying_changes_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn refresh_matview_picks_up_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn drop_matview_removes_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    conn.execute("DROP MATERIALIZED VIEW mv").unwrap();
    let err = conn
        .prepare("SELECT * FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap_err();
    assert!(matches!(err, SqlError::TableNotFound(_)));
}

#[test]
fn drop_matview_if_exists_swallows_missing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("DROP MATERIALIZED VIEW IF EXISTS does_not_exist")
        .unwrap();
}

#[test]
fn create_matview_duplicate_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap_err();
    assert!(err.to_string().contains("already"));
}

#[test]
fn create_matview_name_conflicts_with_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW foo AS SELECT id FROM foo")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("already"));
}

#[test]
fn matview_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 100), (2, 200)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
            .unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let r = conn
        .prepare("SELECT SUM(v) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(300));
}

#[test]
fn insert_into_matview_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let err = conn.execute("INSERT INTO mv VALUES (1, 10)").unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn update_matview_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let err = conn
        .execute("UPDATE mv SET v = 99 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn delete_matview_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let err = conn.execute("DELETE FROM mv WHERE id = 1").unwrap_err();
    assert!(matches!(err, SqlError::CannotModifyView(_)));
}

#[test]
fn create_matview_rejects_now() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, NOW() AS t FROM src")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("non-deterministic"));
}

#[test]
fn create_matview_rejects_current_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, CURRENT_TIMESTAMP() AS t FROM src")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("non-deterministic"));
}

#[test]
fn create_matview_rejects_random() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, RANDOM() AS r FROM src")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("non-deterministic"));
}

#[test]
fn create_matview_rejects_non_deterministic_in_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, ts INTEGER)")
        .unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src WHERE ts < NOW()")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("non-deterministic"));
}

#[test]
fn refresh_concurrently_diff_merges_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    // Source changes: delete row 1, update row 2, insert row 4.
    conn.execute("DELETE FROM src WHERE id = 1").unwrap();
    conn.execute("UPDATE src SET v = 222 WHERE id = 2").unwrap();
    conn.execute("INSERT INTO src VALUES (4, 40)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Integer(2));
    assert_eq!(r.rows[0][1], Value::Integer(222));
    assert_eq!(r.rows[1][0], Value::Integer(3));
    assert_eq!(r.rows[2][0], Value::Integer(4));
}

#[test]
fn refresh_concurrently_no_change_is_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Integer(1));
    assert_eq!(r.rows[1][1], Value::Integer(20));
}

#[test]
fn refresh_concurrently_from_populated_to_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("DELETE FROM src").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn refresh_concurrently_from_empty_to_populated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT id, v FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn refresh_concurrently_repeated_keeps_state_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    for i in 0..10 {
        conn.execute(&format!(
            "INSERT INTO src VALUES ({}, {})",
            3 + i,
            (3 + i) * 10
        ))
        .unwrap();
        conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
            .unwrap();
    }
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(12));
}

#[test]
fn refresh_concurrently_handles_null_non_pk_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, NULL), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("UPDATE src SET v = 100 WHERE id = 1").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT v FROM mv WHERE id = 1")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(100));
}

#[test]
fn matview_with_index_used_by_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
        .unwrap();
    for i in 0..50 {
        conn.execute(&format!(
            "INSERT INTO src VALUES ({}, 'k{}', {})",
            i,
            i,
            i * 10
        ))
        .unwrap();
    }
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, k, v FROM src")
        .unwrap();
    conn.execute("CREATE INDEX mv_k ON mv (k)").unwrap();
    let r = conn
        .prepare("SELECT v FROM mv WHERE k = 'k25'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(250));
}

#[test]
fn matview_with_index_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, k TEXT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, k FROM src")
            .unwrap();
        conn.execute("CREATE INDEX mv_k ON mv (k)").unwrap();
    }
    let db = DatabaseBuilder::new(dir.path().join("test.db"))
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    let r = conn
        .prepare("SELECT id FROM mv WHERE k = 'b'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn drop_matview_with_dependent_view_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id FROM mv").unwrap();
    let err = conn.execute("DROP MATERIALIZED VIEW mv").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("depended"));
}

#[test]
fn drop_matview_cascade_drops_dependent_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id FROM mv").unwrap();
    conn.execute("DROP MATERIALIZED VIEW mv CASCADE").unwrap();
    let err_mv = conn
        .prepare("SELECT * FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap_err();
    assert!(matches!(err_mv, SqlError::TableNotFound(_)));
    let err_v = conn
        .prepare("SELECT * FROM v")
        .unwrap()
        .query_collect(&[])
        .unwrap_err();
    assert!(matches!(
        err_v,
        SqlError::TableNotFound(_) | SqlError::ViewNotFound(_)
    ));
}

#[test]
fn drop_matview_blocked_by_dependent_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv_a AS SELECT id FROM src")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv_b AS SELECT id FROM mv_a")
        .unwrap();
    let err = conn.execute("DROP MATERIALIZED VIEW mv_a").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("depended"));
}

#[test]
fn drop_matview_cascade_chains_through_matviews() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv_a AS SELECT id FROM src")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv_b AS SELECT id FROM mv_a")
        .unwrap();
    conn.execute("DROP MATERIALIZED VIEW mv_a CASCADE").unwrap();
    let err_b = conn
        .prepare("SELECT * FROM mv_b")
        .unwrap()
        .query_collect(&[])
        .unwrap_err();
    assert!(matches!(err_b, SqlError::TableNotFound(_)));
}

#[test]
fn matview_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO other VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let r = conn
        .prepare(
            "SELECT mv.id, mv.v, other.label FROM mv JOIN other ON mv.id = other.id ORDER BY mv.id",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][2], Value::Text("a".into()));
}

#[test]
fn matview_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO other VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let r = conn
        .prepare(
            "SELECT other.id, mv.v FROM other LEFT JOIN mv ON other.id = mv.id ORDER BY other.id",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::Integer(10));
    assert_eq!(r.rows[1][1], Value::Null);
}

#[test]
fn matview_in_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("INSERT INTO other VALUES (1), (2), (3)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT id FROM other WHERE id IN (SELECT id FROM mv) ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn matview_in_cte() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let r = conn
        .prepare("WITH big AS (SELECT id FROM mv WHERE v > 15) SELECT COUNT(*) FROM big")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn matview_in_exists_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("INSERT INTO other VALUES (1), (2)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT id FROM other WHERE EXISTS (SELECT 1 FROM mv WHERE mv.id = other.id)")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn matview_with_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1,1,10), (2,1,20), (3,2,5)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS total FROM src GROUP BY k")
        .unwrap();
    let r = conn
        .prepare("SELECT k, total FROM mv ORDER BY k")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Integer(1));
    assert_eq!(r.rows[0][1], Value::Integer(30));
}

#[test]
fn matview_built_on_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM v")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn matview_built_on_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv_outer AS SELECT id, k, v FROM src")
        .unwrap();
    conn.execute(
        "CREATE MATERIALIZED VIEW mv_agg AS SELECT k, SUM(v) AS total FROM mv_outer GROUP BY k",
    )
    .unwrap();
    let r = conn
        .prepare("SELECT k, total FROM mv_agg ORDER BY k")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::Integer(30));
}

#[test]
fn refresh_matview_inside_transaction_commits_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO src VALUES (2, 20)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    conn.execute("COMMIT").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
}

#[test]
fn refresh_matview_inside_read_only_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn create_matview_inside_read_only_txn_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("read"));
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn matview_preserves_text_and_null() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'alice', NULL), (2, NULL, 42)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, name, age FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT name, age FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Text("alice".into()));
    assert_eq!(r.rows[0][1], Value::Null);
    assert_eq!(r.rows[1][0], Value::Null);
    assert_eq!(r.rows[1][1], Value::Integer(42));
}

#[test]
fn refresh_matview_on_empty_source_yields_empty_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("DELETE FROM src").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn create_matview_with_duplicate_first_column_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, k INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 100), (2, 100)")
        .unwrap();
    // Project k (which has duplicates) as the first column → DuplicateKey at populate.
    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT k, id FROM src")
        .unwrap_err();
    assert!(matches!(err, SqlError::DuplicateKey));
}

#[test]
fn refresh_concurrent_readers_see_consistent_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let writer = Connection::open(&db).unwrap();
    writer
        .execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    writer
        .execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    writer
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    writer
        .execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    // A separate reader opens a snapshot and reads the matview repeatedly. The reader
    // never sees intermediate state — only the committed snapshot before or after.
    let reader = Connection::open(&db).unwrap();
    let r_pre = reader
        .prepare("SELECT SUM(v) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r_pre.rows[0][0], Value::Integer(30));

    writer
        .execute("UPDATE src SET v = 999 WHERE id = 1")
        .unwrap();
    writer
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();

    let r_post = reader
        .prepare("SELECT SUM(v) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r_post.rows[0][0], Value::Integer(1019));
}

#[test]
fn pg_matviews_lists_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT matviewname, ispopulated FROM pg_matviews WHERE matviewname = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("mv".into()));
    assert_eq!(r.rows[0][1], Value::Boolean(true));
}

#[test]
fn pg_matviews_includes_definition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT definition FROM pg_matviews WHERE matviewname = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    if let Value::Text(s) = &r.rows[0][0] {
        assert!(s.to_uppercase().contains("SELECT"));
    } else {
        panic!("expected text, got {:?}", r.rows[0][0]);
    }
}

#[test]
fn information_schema_tables_lists_matview_with_type() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare(
            "SELECT table_type FROM information_schema.tables \
             WHERE table_name = 'mv'",
        )
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("MATERIALIZED VIEW".into()));
}

#[test]
fn information_schema_tables_does_not_double_list_matview() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    // mv must appear exactly once (as MATERIALIZED VIEW), not also as BASE TABLE.
    let r = conn
        .prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

#[test]
fn show_materialized_views_lists_them() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW alpha AS SELECT id FROM src")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW beta AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare("SHOW MATERIALIZED VIEWS")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Text("alpha".into()));
    assert_eq!(r.rows[1][0], Value::Text("beta".into()));
}

#[test]
fn create_matview_with_no_data_is_unpopulated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src WITH NO DATA")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
    let r = conn
        .prepare("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Boolean(false));
}

#[test]
fn create_matview_default_is_populated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1), (2)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Boolean(true));
}

#[test]
fn with_no_data_refresh_populates() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src WITH NO DATA")
        .unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
    let r = conn
        .prepare("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'mv'")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Boolean(true));
}

#[test]
fn with_no_data_concurrently_rejected_when_unpopulated() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src WITH NO DATA")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    let err = conn
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_ascii_lowercase().contains("not populated"),
        "expected 'not populated' error, got: {msg}"
    );
}

#[test]
fn with_no_data_concurrently_allowed_after_first_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src WITH NO DATA")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW mv").unwrap();
    conn.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn with_no_data_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src with no data")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn with_no_data_trailing_semicolon() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM src WITH NO DATA;")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(0));
}

#[test]
fn with_no_data_inside_string_literal_not_stripped() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'WITH NO DATA'), (2, 'other')")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, label FROM src")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(2));
    let r = conn
        .prepare("SELECT label FROM mv ORDER BY id")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Text("WITH NO DATA".into()));
}

#[test]
fn refresh_concurrently_without_unique_index_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    let err = conn
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("unique"),
        "expected UNIQUE index error, got: {msg}"
    );
}

#[test]
fn refresh_concurrently_with_non_unique_index_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE INDEX ix_mv ON mv (v)").unwrap();
    let err = conn
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("unique"),
        "non-unique index must not satisfy CONCURRENTLY validation, got: {msg}"
    );
}

#[test]
fn refresh_concurrently_with_unique_index_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    let r = conn
        .prepare("SELECT COUNT(*) FROM mv")
        .unwrap()
        .query_collect(&[])
        .unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(3));
}

#[test]
fn refresh_concurrently_completes_during_concurrent_reader() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());

    let setup = Connection::open(&db).unwrap();
    setup
        .execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup
        .execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    setup
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    setup
        .execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    let reader = Connection::open(&db).unwrap();
    reader.execute("BEGIN READ ONLY").unwrap();
    let before = reader.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(before.rows[0][0], Value::Integer(2));

    let writer = Connection::open(&db).unwrap();
    writer.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    writer
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();

    let during = reader.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(during.rows[0][0], Value::Integer(2));
    reader.execute("COMMIT").unwrap();

    let after = reader.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(after.rows[0][0], Value::Integer(3));
}

#[test]
fn refresh_concurrently_diff_merge_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, label TEXT, v INTEGER)")
        .unwrap();
    conn.execute(
        "INSERT INTO src (id, label, v) VALUES \
         (1, 'A', 100), (2, 'B', 200), (3, 'C', 300)",
    )
    .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, label, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    conn.execute("UPDATE src SET v = 111 WHERE id = 1").unwrap();
    conn.execute("DELETE FROM src WHERE id = 3").unwrap();
    conn.execute("INSERT INTO src (id, label, v) VALUES (4, 'D', 400)")
        .unwrap();

    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();

    let qr = conn
        .query("SELECT id, label, v FROM mv ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][0], Value::Integer(1));
    assert_eq!(qr.rows[0][2], Value::Integer(111));
    assert_eq!(qr.rows[1][0], Value::Integer(2));
    assert_eq!(qr.rows[1][2], Value::Integer(200));
    assert_eq!(qr.rows[2][0], Value::Integer(4));
    assert_eq!(qr.rows[2][2], Value::Integer(400));
}

#[test]
fn refresh_concurrently_failure_leaves_no_stale_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    let before = conn.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(before.rows[0][0], Value::Integer(2));

    conn.execute("DROP INDEX ux_mv").unwrap();
    let err = conn
        .execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(msg.contains("unique"), "got: {msg}");

    let after = conn.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(after.rows[0][0], Value::Integer(2));
    let qr = conn.query("SELECT id, v FROM mv ORDER BY id").unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(20));
}

#[test]
fn refresh_concurrently_inside_begin_block_uses_in_txn_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();

    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO src VALUES (3, 30)").unwrap();
    conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let qr = conn.query("SELECT id, v FROM mv ORDER BY id").unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[2][0], Value::Integer(3));
    assert_eq!(qr.rows[2][1], Value::Integer(30));
}

#[test]
fn refresh_in_middle_of_script_via_execute_script() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();

    let exec = conn.execute_script(
        "CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src; \
         REFRESH MATERIALIZED VIEW mv; \
         SELECT * FROM mv ORDER BY id;",
    );
    assert!(
        exec.error.is_none(),
        "execute_script with REFRESH-in-middle should succeed, got: {:?}",
        exec.error
    );
    assert_eq!(exec.completed.len(), 3);
}

#[test]
fn refresh_at_end_of_script_via_execute_script() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();

    let exec = conn.execute_script(
        "INSERT INTO src VALUES (3, 30); \
         REFRESH MATERIALIZED VIEW mv;",
    );
    assert!(exec.error.is_none(), "got: {:?}", exec.error);
    assert_eq!(exec.completed.len(), 2);

    let qr = conn.query("SELECT COUNT(*) FROM mv").unwrap();
    assert_eq!(qr.rows[0][0], Value::Integer(3));
}

#[test]
fn refresh_concurrently_in_script_via_execute_script() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX ux_mv ON mv (id)")
        .unwrap();

    let exec = conn.execute_script(
        "INSERT INTO src VALUES (3, 30); \
         REFRESH MATERIALIZED VIEW CONCURRENTLY mv; \
         SELECT COUNT(*) FROM mv;",
    );
    assert!(exec.error.is_none(), "got: {:?}", exec.error);
    assert_eq!(exec.completed.len(), 3);
}

#[test]
fn refresh_and_with_no_data_in_same_script() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();

    let exec = conn.execute_script(
        "CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM src WITH NO DATA; \
         REFRESH MATERIALIZED VIEW mv; \
         SELECT COUNT(*) FROM mv;",
    );
    assert!(exec.error.is_none(), "got: {:?}", exec.error);
    assert_eq!(exec.completed.len(), 3);

    let qr = conn
        .query("SELECT ispopulated FROM pg_matviews WHERE matviewname='mv'")
        .unwrap();
    assert_eq!(qr.rows[0][0], Value::Boolean(true));
}
