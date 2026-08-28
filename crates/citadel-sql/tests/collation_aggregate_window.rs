use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("t.db"))
        .passphrase(b"x")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap() {
        ExecutionResult::Query(result) => result.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn nested_and_cast_collations_reach_comparison_and_key_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1,'A'),(2,'a'),(3,'B')")
        .unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT CAST(s AS TEXT) = 'a', \
                    (s COLLATE NOCASE || '') = 'a', \
                    ('A' COLLATE NOCASE || '') = 'a', \
                    ((s COLLATE NOCASE || '') COLLATE BINARY) = 'a', \
                    CAST(s AS TEXT) IS NOT DISTINCT FROM 'a', \
                    (s COLLATE NOCASE || '') IS DISTINCT FROM 'a' \
             FROM c WHERE id = 1",
        )[0],
        vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(false),
        ]
    );

    assert_eq!(
        rows(&conn, "SELECT DISTINCT CAST(s AS TEXT) AS x FROM c").len(),
        2,
        "DISTINCT must use the same CAST-preserved collation as comparison"
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT CAST(s AS TEXT), COUNT(*) FROM c GROUP BY CAST(s AS TEXT)",
        )
        .len(),
        2,
        "GROUP BY must use the same CAST-preserved collation as comparison"
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT COUNT(*) FROM (SELECT CAST(s AS TEXT) AS x FROM c) d WHERE x = 'a'",
        )[0][0],
        Value::Integer(2),
        "derived-column metadata must preserve the inferred collation"
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT id FROM c WHERE id IN (2,3) ORDER BY (s COLLATE NOCASE || '')",
        ),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]],
        "ORDER BY must see an explicit COLLATE nested inside its expression"
    );
}

#[test]
fn aggregate_min_max_use_the_argument_collation_in_every_lane() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, g INTEGER, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1,1,'B'),(2,1,'a'),(3,2,'D'),(4,2,'c')")
        .unwrap();

    let expected = vec![Value::Text("a".into()), Value::Text("D".into())];
    assert_eq!(rows(&conn, "SELECT MIN(s), MAX(s) FROM a")[0], expected);
    assert_eq!(
        rows(
            &conn,
            "SELECT MIN(s COLLATE NOCASE), MAX(s COLLATE NOCASE) FROM a",
        )[0],
        expected,
        "the general aggregate lane must agree with the streaming lane"
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT g, MIN(s), MAX(s) FROM a GROUP BY g ORDER BY g",
        ),
        vec![
            vec![
                Value::Integer(1),
                Value::Text("a".into()),
                Value::Text("B".into()),
            ],
            vec![
                Value::Integer(2),
                Value::Text("c".into()),
                Value::Text("D".into()),
            ],
        ],
        "the fused GROUP BY aggregate lane must carry the source collation"
    );
}

#[test]
fn window_min_max_use_the_argument_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO w VALUES (1,'B'),(2,'a'),(3,'D'),(4,'c')")
        .unwrap();

    let full = rows(
        &conn,
        "SELECT id, MIN(s) OVER (), MAX(s) OVER () FROM w ORDER BY id",
    );
    for row in full {
        assert_eq!(
            &row[1..],
            &[Value::Text("a".into()), Value::Text("D".into())]
        );
    }

    let compared = rows(
        &conn,
        "SELECT (MIN(s COLLATE NOCASE) OVER ()) = 'A' FROM w ORDER BY id",
    );
    assert!(
        compared.iter().all(|row| row == &[Value::Boolean(true)]),
        "the extracted window-result slot must retain the argument collation"
    );

    assert_eq!(
        rows(
            &conn,
            "SELECT MIN(s COLLATE NOCASE) OVER (PARTITION BY id % 2) AS m \
             FROM w ORDER BY 1",
        ),
        vec![
            vec![Value::Text("a".into())],
            vec![Value::Text("a".into())],
            vec![Value::Text("B".into())],
            vec![Value::Text("B".into())],
        ],
        "ORDER BY the window output must use its explicit NOCASE collation"
    );

    assert_eq!(
        rows(
            &conn,
            "SELECT id, \
                    MIN(s) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
                    MAX(s) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM w ORDER BY id",
        ),
        vec![
            vec![
                Value::Integer(1),
                Value::Text("B".into()),
                Value::Text("B".into()),
            ],
            vec![
                Value::Integer(2),
                Value::Text("a".into()),
                Value::Text("B".into()),
            ],
            vec![
                Value::Integer(3),
                Value::Text("a".into()),
                Value::Text("D".into()),
            ],
            vec![
                Value::Integer(4),
                Value::Text("c".into()),
                Value::Text("D".into()),
            ],
        ]
    );
}

#[test]
fn a_later_explicit_window_argument_overrides_the_first_arguments_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, s TEXT COLLATE BINARY)")
        .unwrap();
    conn.execute("INSERT INTO w VALUES (1,'a'),(2,'b')")
        .unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT id, \
                    (LAG(s, 1, 'x' COLLATE NOCASE) OVER (ORDER BY id)) = 'A' \
             FROM w ORDER BY id",
        ),
        vec![
            vec![Value::Integer(1), Value::Boolean(false)],
            vec![Value::Integer(2), Value::Boolean(true)],
        ],
        "explicit COLLATE in any window argument must outrank an earlier implicit collation"
    );
}

#[test]
fn aggregate_fast_lanes_apply_limit_offset_and_distinct_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, g INTEGER, s TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1,1,'B'),(2,1,'a'),(3,2,'D'),(4,2,'c')")
        .unwrap();

    assert!(rows(&conn, "SELECT MIN(s) FROM a ORDER BY 1 LIMIT 0").is_empty());
    assert!(rows(&conn, "SELECT COUNT(*) FROM a ORDER BY 1 OFFSET 1").is_empty());
    assert_eq!(
        rows(&conn, "SELECT DISTINCT COUNT(*) FROM a GROUP BY g").len(),
        1,
        "the streaming GROUP BY lane must not bypass DISTINCT"
    );
    assert_eq!(
        rows(&conn, "SELECT g, MIN(s) FROM a GROUP BY g OFFSET 1").len(),
        1,
        "the streaming GROUP BY lane must not bypass OFFSET"
    );
}

#[test]
fn window_partitions_ranks_and_range_frames_share_collated_peers() {
    let dir = tempfile::tempdir().unwrap();
    let database = database(dir.path());
    let conn = Connection::open(&database).unwrap();
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO p VALUES (1,'A',10),(2,'a',20),(3,'B',30)")
        .unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT id, \
                    COUNT(*) OVER (PARTITION BY s), \
                    RANK() OVER (ORDER BY s), \
                    DENSE_RANK() OVER (ORDER BY s), \
                    SUM(v) OVER (ORDER BY s RANGE BETWEEN CURRENT ROW AND CURRENT ROW) \
             FROM p ORDER BY id",
        ),
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(30),
            ],
            vec![
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(30),
            ],
            vec![
                Value::Integer(3),
                Value::Integer(1),
                Value::Integer(3),
                Value::Integer(2),
                Value::Integer(30),
            ],
        ]
    );
}
