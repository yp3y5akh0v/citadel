use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

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
fn vector_column_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 2.5, -3.0, 0.5]'::VECTOR(4))")
        .unwrap();
    let qr = match conn.execute("SELECT v FROM t").unwrap() {
        ExecutionResult::Query(qr) => qr,
        _ => panic!("expected query result"),
    };
    let val = &qr.rows[0][0];
    match val {
        Value::Vector(v) => {
            assert_eq!(v.len(), 4);
            assert!((v[0] - 1.0).abs() < 1e-6);
            assert!((v[1] - 2.5).abs() < 1e-6);
            assert!((v[2] - (-3.0)).abs() < 1e-6);
            assert!((v[3] - 0.5).abs() < 1e-6);
        }
        other => panic!("expected Vector, got {other:?}"),
    }
}

#[test]
fn vector_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = create_db(dir.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '[0.1, 0.2, 0.3]'::VECTOR(3))")
            .unwrap();
    }
    let db = open_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = match conn.execute("SELECT v FROM t WHERE id = 1").unwrap() {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Vector(v) => assert_eq!(v.len(), 3),
        other => panic!("expected Vector, got {other:?}"),
    }
}

#[test]
fn vector_l2_distance() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 0.0, 0.0]'::VECTOR(3))")
        .unwrap();
    let qr = match conn
        .execute("SELECT v <-> '[0.0, 0.0, 0.0]'::VECTOR(3) FROM t")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 1.0).abs() < 1e-6),
        other => panic!("expected Real 1.0, got {other:?}"),
    }
}

#[test]
fn vector_inner_product() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 2.0, 3.0]'::VECTOR(3))")
        .unwrap();
    let qr = match conn
        .execute("SELECT v <#> '[4.0, 5.0, 6.0]'::VECTOR(3) FROM t")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - (-32.0)).abs() < 1e-6),
        other => panic!("expected Real -32.0, got {other:?}"),
    }
}

#[test]
fn vector_cosine_distance_orthogonal() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(2))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 0.0]'::VECTOR(2))")
        .unwrap();
    let qr = match conn
        .execute("SELECT v <=> '[0.0, 1.0]'::VECTOR(2) FROM t")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Real(r) => assert!((*r - 1.0).abs() < 1e-6),
        other => panic!("expected Real 1.0, got {other:?}"),
    }
}

#[test]
fn vector_cosine_distance_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 2.0, 3.0]'::VECTOR(3))")
        .unwrap();
    let qr = match conn
        .execute("SELECT v <=> '[2.0, 4.0, 6.0]'::VECTOR(3) FROM t")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Real(r) => assert!(r.abs() < 1e-6),
        other => panic!("expected Real ~0.0, got {other:?}"),
    }
}

#[test]
fn vector_dim_mismatch_in_distance_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '[1.0, 2.0, 3.0]'::VECTOR(3))")
        .unwrap();
    let err = conn
        .execute("SELECT v <-> '[1.0, 2.0]'::VECTOR(2) FROM t")
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("dimension mismatch"), "got: {msg}");
}

#[test]
fn vector_typeof_reports_vector() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let qr = match conn
        .execute("SELECT TYPEOF('[1.0, 2.0]'::VECTOR(2))")
        .unwrap()
    {
        ExecutionResult::Query(qr) => qr,
        _ => panic!(),
    };
    match &qr.rows[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "vector"),
        other => panic!("expected Text 'vector', got {other:?}"),
    }
}

#[test]
fn vector_zero_dim_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(0))")
        .unwrap_err();
    assert!(err.to_string().contains("VECTOR dimension"), "{err}");
}
