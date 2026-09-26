use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::Connection;

fn create_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn open_db(dir: &std::path::Path) -> citadel::Database {
    let db_path = dir.join("test.db");
    DatabaseBuilder::new(db_path)
        .passphrase(b"test-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .open()
        .unwrap()
}

#[test]
fn create_ann_index_default_metric() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v)")
        .unwrap();
}

#[test]
fn create_ann_index_metric_l2() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
}

#[test]
fn create_ann_index_metric_inner() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'inner')")
        .unwrap();
}

#[test]
fn create_ann_index_metric_cosine() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'cosine')")
        .unwrap();
}

#[test]
fn ann_index_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(8))")
            .unwrap();
        conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'cosine')")
            .unwrap();
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v)")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("exists"), "{err}");
}

#[test]
fn ann_index_rejects_non_vector_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (name)")
        .unwrap_err();
    assert!(err.to_string().contains("VECTOR"), "{err}");
}

#[test]
fn ann_index_rejects_unknown_metric() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'bogus')")
        .unwrap_err();
    assert!(err.to_string().contains("bogus"), "{err}");
}

#[test]
fn ann_index_rejects_unknown_option() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v) WITH (foo = 'bar')")
        .unwrap_err();
    assert!(err.to_string().contains("foo"), "{err}");
}

#[test]
fn create_ann_index_with_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, tag INTEGER, v VECTOR(4))",
    )
    .unwrap();
    conn.execute(
        "CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'cosine', filters = 'category,tag')",
    )
    .unwrap();
}

#[test]
fn ann_index_filters_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, v VECTOR(8))")
            .unwrap();
        conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (filters = 'category')")
            .unwrap();
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v)")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("exists"), "{err}");
}

#[test]
fn ann_index_rejects_unknown_filter_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v) WITH (filters = 'nope')")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("nope"), "{err}");
}

#[test]
fn ann_index_rejects_filter_on_vector_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    let err = conn
        .execute("CREATE INDEX ix_v ON t USING ann (v) WITH (filters = 'v')")
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("vector column"),
        "{err}"
    );
}

#[test]
fn ann_index_does_not_block_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("CREATE INDEX ix_v ON t USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 2.0, 3.0]'::VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, '[4.0, 5.0, 6.0]'::VECTOR(3))")
        .unwrap();
}
