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

fn assert_ok(result: ExecutionResult) {
    match result {
        ExecutionResult::Ok => {}
        other => panic!("expected Ok, got {other:?}"),
    }
}

fn setup_employees(conn: &Connection) {
    assert_ok(
        conn.execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, \
             dept TEXT NOT NULL, salary INTEGER NOT NULL)",
        )
        .unwrap(),
    );
    conn.execute(
        "INSERT INTO employees (id, name, dept, salary) VALUES \
         (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 90), \
         (3, 'Carol', 'sales', 80), (4, 'Dave', 'sales', 70), \
         (5, 'Eve', 'eng', 100)",
    )
    .unwrap();
}

#[test]
fn error_rank_no_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    let err = conn
        .query("SELECT RANK() OVER () FROM employees")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("requires ORDER BY"), "unexpected error: {msg}");
}

#[test]
fn error_ntile_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    let err = conn
        .query("SELECT NTILE(0) OVER (ORDER BY id) FROM employees")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("invalid") || msg.contains("NTILE"),
        "unexpected error: {msg}"
    );
}

#[test]
fn ntile_large_and_null_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for buckets in [Value::Integer(1_i64 << 32), Value::Integer(i64::MAX)] {
        let qr = conn
            .query_params(
                "SELECT id, NTILE($1) OVER (ORDER BY id DESC) FROM employees ORDER BY id",
                std::slice::from_ref(&buckets),
            )
            .unwrap();
        assert_eq!(qr.rows.len(), 5);
        for (i, row) in qr.rows.iter().enumerate() {
            assert_eq!(row[1], Value::Integer(5 - i as i64), "{buckets:?}");
        }
    }

    let qr = conn
        .query("SELECT NTILE(NULL) OVER (PARTITION BY dept ORDER BY id) FROM employees")
        .unwrap();
    assert_eq!(qr.rows, vec![vec![Value::Null]; 5]);

    let qr = conn
        .query(
            "SELECT NTILE(CASE WHEN id = 1 THEN NULL WHEN id = 2 THEN 2 \
             WHEN id = 3 THEN NULL ELSE 99 END) OVER (ORDER BY id) FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Null],
            vec![Value::Integer(1)],
            vec![Value::Integer(1)],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]
    );
}

#[test]
fn ntile_uses_first_sorted_row_of_each_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE tiles (id INTEGER PRIMARY KEY, grp TEXT, position INTEGER, buckets INTEGER)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO tiles VALUES (1, 'a', 30, 99), (2, 'b', 20, 99), \
         (3, 'a', 10, 2), (4, 'b', 10, 3), (5, 'a', 20, 99), (6, 'b', 30, 99)",
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT id, NTILE(buckets) OVER (PARTITION BY grp ORDER BY position) \
             FROM tiles ORDER BY id",
        )
        .unwrap();
    let expected = [2, 2, 1, 1, 1, 3];
    assert_eq!(qr.rows.len(), expected.len());
    for (row, tile) in qr.rows.iter().zip(expected) {
        assert_eq!(row[1], Value::Integer(tile), "row {row:?}");
    }
}

#[test]
fn ntile_rejects_invalid_buckets_and_arity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for args in ["-1", "1.5", "'invalid'", "", "1, 2"] {
        let sql = format!("SELECT NTILE({args}) OVER (ORDER BY id) FROM employees");
        assert!(conn.query(&sql).is_err(), "expected error for {sql}");
    }
}

#[test]
fn window_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE empty (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM empty")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

#[test]
fn window_single_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE one (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO one VALUES (1, 42)").unwrap();

    let qr = conn
        .query(
            "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, \
             SUM(val) OVER (ORDER BY id) AS s, \
             LAG(val, 1, -1) OVER (ORDER BY id) AS prev \
             FROM one",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1);
    assert_eq!(qr.rows[0][0], Value::Integer(1)); // rn
    assert_eq!(qr.rows[0][1], Value::Integer(42)); // sum
    assert_eq!(qr.rows[0][2], Value::Integer(-1)); // lag default
}

#[test]
fn window_all_same_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    // All 5 rows in one partition — ROW_NUMBER should be 1..5
    let qr = conn
        .query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM employees ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[1], Value::Integer(i as i64 + 1));
    }
}

#[test]
fn window_each_own_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    // PARTITION BY id → each row is its own partition
    let qr = conn
        .query(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for row in &qr.rows {
        assert_eq!(row[1], Value::Integer(1)); // each partition has only 1 row
    }
}

#[test]
fn lag_beyond_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    // LAG(salary, 10) — offset larger than any partition
    let qr = conn
        .query(
            "SELECT id, LAG(salary, 10) OVER (PARTITION BY dept ORDER BY id) AS prev \
             FROM employees ORDER BY id",
        )
        .unwrap();
    for row in &qr.rows {
        assert_eq!(row[1], Value::Null); // all NULL
    }
}

#[test]
fn lead_beyond_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    let qr = conn
        .query(
            "SELECT id, LEAD(salary, 10, -1) OVER (PARTITION BY dept ORDER BY id) AS nxt \
             FROM employees ORDER BY id",
        )
        .unwrap();
    for row in &qr.rows {
        assert_eq!(row[1], Value::Integer(-1)); // all default
    }
}

#[test]
fn lag_lead_use_current_row_offsets_and_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE offsets (id INTEGER PRIMARY KEY, position INTEGER, val INTEGER, step INTEGER)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO offsets VALUES (1, 40, 40, 1), (2, 10, 10, -1), \
         (3, 30, NULL, 0), (4, 20, 20, 2), (5, 50, 50, NULL), (6, 60, 60, -1)",
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT id, LAG(val, step, id + 100) OVER (ORDER BY position), \
             LEAD(val, step, id + 100) OVER (ORDER BY position) \
             FROM offsets ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Integer(1), Value::Null, Value::Integer(50)],
            vec![Value::Integer(2), Value::Integer(20), Value::Integer(102)],
            vec![Value::Integer(3), Value::Null, Value::Null],
            vec![Value::Integer(4), Value::Integer(104), Value::Integer(40)],
            vec![Value::Integer(5), Value::Null, Value::Null],
            vec![Value::Integer(6), Value::Integer(106), Value::Integer(50)],
        ]
    );

    for function in ["LAG", "LEAD"] {
        let sql = format!(
            "SELECT id, {function}(val, 100, id + 100) OVER (ORDER BY position) \
             FROM offsets ORDER BY id"
        );
        let qr = conn.query(&sql).unwrap();
        for (i, row) in qr.rows.iter().enumerate() {
            assert_eq!(row[1], Value::Integer(i as i64 + 101), "{function}");
        }
    }
}

#[test]
fn lag_lead_extreme_offsets_return_current_row_default() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for function in ["LAG", "LEAD"] {
        for offset in [i64::MIN, i64::MAX] {
            let sql = format!(
                "SELECT id, {function}(salary, $1, id + 100) \
                 OVER (PARTITION BY dept ORDER BY salary DESC, id) FROM employees ORDER BY id"
            );
            let qr = conn.query_params(&sql, &[Value::Integer(offset)]).unwrap();
            assert_eq!(qr.rows.len(), 5);
            for (i, row) in qr.rows.iter().enumerate() {
                assert_eq!(
                    row[1],
                    Value::Integer(i as i64 + 101),
                    "{function} offset {offset}"
                );
            }
        }
    }
}

#[test]
fn lag_lead_optional_arguments_and_validation() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for function in ["LAG", "LEAD"] {
        let sql = format!(
            "SELECT {function}(salary) OVER (ORDER BY id), \
             {function}(salary, 1) OVER (ORDER BY id), \
             {function}(salary, 1, NULL) OVER (ORDER BY id) FROM employees ORDER BY id"
        );
        let qr = conn.query(&sql).unwrap();
        assert_eq!(qr.rows.len(), 5);
        for row in qr.rows {
            assert_eq!(row[0], row[1], "{function}");
            assert_eq!(row[0], row[2], "{function}");
        }

        for args in ["", "salary, 1, 0, 0", "salary, 1.5", "salary, 'invalid'"] {
            let sql = format!("SELECT {function}({args}) OVER (ORDER BY id) FROM employees");
            assert!(conn.query(&sql).is_err(), "expected error for {sql}");
        }
    }
}

#[test]
fn window_nulls_in_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE np (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO np VALUES (1, NULL, 10), (2, NULL, 20), (3, 'a', 30), (4, 'a', 40)")
        .unwrap();

    // NULL group should form one partition (SQL standard)
    let qr = conn
        .query(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn \
             FROM np ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(1)); // id=1 first in NULL partition
    assert_eq!(qr.rows[1][1], Value::Integer(2)); // id=2 second in NULL partition
    assert_eq!(qr.rows[2][1], Value::Integer(1)); // id=3 first in 'a' partition
    assert_eq!(qr.rows[3][1], Value::Integer(2)); // id=4 second in 'a' partition
}

#[test]
fn window_nulls_in_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE nv (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    conn.execute("INSERT INTO nv VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50)")
        .unwrap();

    let qr = conn
        .query(
            "SELECT id, SUM(val) OVER (ORDER BY id) AS running, \
             COUNT(val) OVER (ORDER BY id) AS cnt \
             FROM nv ORDER BY id",
        )
        .unwrap();
    // Running SUM skips NULLs: 10, 10, 40, 40, 90
    assert_eq!(qr.rows[0][1], Value::Integer(10));
    assert_eq!(qr.rows[1][1], Value::Integer(10)); // NULL skipped
    assert_eq!(qr.rows[2][1], Value::Integer(40));
    assert_eq!(qr.rows[3][1], Value::Integer(40)); // NULL skipped
    assert_eq!(qr.rows[4][1], Value::Integer(90));
    // COUNT(val) skips NULLs: 1, 1, 2, 2, 3
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(1));
    assert_eq!(qr.rows[2][2], Value::Integer(2));
    assert_eq!(qr.rows[3][2], Value::Integer(2));
    assert_eq!(qr.rows[4][2], Value::Integer(3));
}

#[test]
fn sliding_min_max_large() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    for batch in 0..10 {
        let mut sql = String::from("INSERT INTO big VALUES ");
        for i in 0..100 {
            let id = batch * 100 + i + 1;
            let val = (id * 7 + 13) % 997; // pseudo-random
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({id}, {val})"));
        }
        conn.execute(&sql).unwrap();
    }

    // Sliding MIN/MAX with window of 10
    let qr = conn
        .query(
            "SELECT id, val, \
             MIN(val) OVER (ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS wmin, \
             MAX(val) OVER (ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS wmax \
             FROM big ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 1000);

    assert_eq!(qr.rows[0][2], qr.rows[0][1]);
    assert_eq!(qr.rows[0][3], qr.rows[0][1]);

    let vals: Vec<i64> = qr
        .rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    for i in 0usize..1000 {
        let start = i.saturating_sub(9);
        let expected_min = vals[start..=i].iter().copied().min().unwrap();
        let expected_max = vals[start..=i].iter().copied().max().unwrap();
        assert_eq!(
            qr.rows[i][2],
            Value::Integer(expected_min),
            "MIN mismatch at row {i}"
        );
        assert_eq!(
            qr.rows[i][3],
            Value::Integer(expected_max),
            "MAX mismatch at row {i}"
        );
    }
}

#[test]
fn running_sum_vs_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE rs (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    conn.execute(
        "INSERT INTO rs VALUES (1, 5), (2, 3), (3, 8), (4, 1), (5, 7), \
         (6, 2), (7, 9), (8, 4), (9, 6), (10, 10)",
    )
    .unwrap();

    let qr = conn
        .query("SELECT id, SUM(val) OVER (ORDER BY id) AS running FROM rs ORDER BY id")
        .unwrap();
    let expected = [5, 8, 16, 17, 24, 26, 35, 39, 45, 55];
    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(
            qr.rows[i][1],
            Value::Integer(*exp),
            "running sum mismatch at row {i}"
        );
    }
}

#[test]
fn sort_sharing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    // Two window functions with the same OVER spec
    let qr = conn
        .query(
            "SELECT id, \
             ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS rn, \
             RANK() OVER (ORDER BY salary DESC, id) AS rnk \
             FROM employees ORDER BY id",
        )
        .unwrap();
    // With (salary DESC, id), unique ordering → RANK = ROW_NUMBER
    for row in &qr.rows {
        assert_eq!(
            row[1], row[2],
            "ROW_NUMBER and RANK should match with unique ordering"
        );
    }
}

#[test]
fn window_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE depts (name TEXT PRIMARY KEY, budget INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO depts VALUES ('eng', 1000), ('sales', 500)")
        .unwrap();
    setup_employees(&conn);

    let qr = conn
        .query(
            "SELECT e.id, e.name, \
             ROW_NUMBER() OVER (PARTITION BY e.dept ORDER BY e.id) AS rn \
             FROM employees e JOIN depts d ON e.dept = d.name \
             ORDER BY e.id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    // eng partition: Alice(rn=1), Bob(rn=2), Eve(rn=3)
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(2));
    // sales partition: Carol(rn=1), Dave(rn=2)
    assert_eq!(qr.rows[2][2], Value::Integer(1));
    assert_eq!(qr.rows[3][2], Value::Integer(2));
    assert_eq!(qr.rows[4][2], Value::Integer(3));
}

#[test]
fn unbounded_frame() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING = whole partition
    let qr = conn
        .query(
            "SELECT id, \
             SUM(salary) OVER (PARTITION BY dept ORDER BY id \
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total \
             FROM employees ORDER BY id",
        )
        .unwrap();
    // eng total: 100+90+100=290, sales total: 80+70=150
    assert_eq!(qr.rows[0][1], Value::Integer(290)); // Alice eng
    assert_eq!(qr.rows[1][1], Value::Integer(290)); // Bob eng
    assert_eq!(qr.rows[2][1], Value::Integer(150)); // Carol sales
    assert_eq!(qr.rows[3][1], Value::Integer(150)); // Dave sales
    assert_eq!(qr.rows[4][1], Value::Integer(290)); // Eve eng
}

#[test]
fn rows_frames_preserve_empty_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE frames (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER, amount REAL, label TEXT)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO frames VALUES (1, 'a', 10, 10.5, 'first'), (2, 'a', NULL, NULL, NULL), \
         (3, 'a', 30, 30.5, 'last'), (4, 'b', 40, 40.5, 'only')",
    )
    .unwrap();

    let functions = [
        "COUNT(*)",
        "COUNT(label)",
        "SUM(val)",
        "SUM(amount)",
        "AVG(val)",
        "MIN(val)",
        "MAX(val)",
        "FIRST_VALUE(label)",
        "LAST_VALUE(label)",
    ];
    let empty = vec![
        Value::Integer(0),
        Value::Integer(0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ];
    let singleton_results = [
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(10),
            Value::Real(10.5),
            Value::Real(10.0),
            Value::Integer(10),
            Value::Integer(10),
            Value::Text("first".into()),
            Value::Text("first".into()),
        ],
        vec![
            Value::Integer(1),
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(30),
            Value::Real(30.5),
            Value::Real(30.0),
            Value::Integer(30),
            Value::Integer(30),
            Value::Text("last".into()),
            Value::Text("last".into()),
        ],
    ];
    for (frame, sources) in [
        (
            "1 PRECEDING AND 1 PRECEDING",
            [None, Some(0), Some(1), None],
        ),
        (
            "1 FOLLOWING AND 1 FOLLOWING",
            [Some(1), Some(2), None, None],
        ),
        ("2 FOLLOWING AND 1 FOLLOWING", [None; 4]),
        ("1 PRECEDING AND 2 PRECEDING", [None; 4]),
        (
            "9223372036854775807 FOLLOWING AND 9223372036854775807 FOLLOWING",
            [None; 4],
        ),
        (
            "9223372036854775807 PRECEDING AND 9223372036854775807 PRECEDING",
            [None; 4],
        ),
    ] {
        let projection = functions
            .iter()
            .map(|function| {
                format!("{function} OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN {frame})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let qr = conn
            .query(&format!("SELECT {projection} FROM frames ORDER BY id"))
            .unwrap();
        assert_eq!(qr.rows.len(), sources.len(), "{frame}");
        for (i, (row, source)) in qr.rows.iter().zip(sources).enumerate() {
            let expected = source
                .map(|index| &singleton_results[index])
                .unwrap_or(&empty);
            assert_eq!(row, expected, "{frame}, row {}", i + 1);
        }
    }
}

#[test]
fn sliding_count_text_and_sum_after_real_expires() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE sliding (id INTEGER PRIMARY KEY, val REAL, label TEXT)")
        .unwrap();
    conn.execute(
        "INSERT INTO sliding VALUES (1, 1.5, 'first'), (2, NULL, 'second'), (3, NULL, NULL)",
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT COUNT(label) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
             SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
             AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM sliding ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Integer(1), Value::Real(1.5), Value::Real(1.5)],
            vec![Value::Integer(2), Value::Real(1.5), Value::Real(1.5)],
            vec![Value::Integer(1), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn rows_frames_reject_invalid_offsets_and_bound_categories() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for offset in ["-1", "NULL", "1.5", "'invalid'"] {
        for frame in [
            format!("{offset} PRECEDING AND CURRENT ROW"),
            format!("CURRENT ROW AND {offset} FOLLOWING"),
        ] {
            let sql = format!(
                "SELECT SUM(salary) OVER (ORDER BY id ROWS BETWEEN {frame}) FROM employees"
            );
            assert!(conn.query(&sql).is_err(), "expected error for {sql}");
        }
    }
    for frame in [
        "UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING",
        "UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING",
        "CURRENT ROW AND 1 PRECEDING",
        "1 FOLLOWING AND CURRENT ROW",
        "1 FOLLOWING AND 1 PRECEDING",
    ] {
        let sql =
            format!("SELECT SUM(salary) OVER (ORDER BY id ROWS BETWEEN {frame}) FROM employees");
        assert!(conn.query(&sql).is_err(), "expected error for {sql}");
    }
}

#[test]
fn window_integer_sum_overflow_does_not_overflow_avg() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE large_values (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for value in [1_i64 << 62, -(1_i64 << 62)] {
        conn.execute("DELETE FROM large_values").unwrap();
        conn.execute_params(
            "INSERT INTO large_values VALUES (1, $1), (2, $1), (3, $1)",
            &[Value::Integer(value)],
        )
        .unwrap();
        for frame in [
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        ] {
            let sql = format!("SELECT SUM(val) OVER (ORDER BY id {frame}) FROM large_values");
            let err = conn.query(&sql).unwrap_err();
            assert!(err.to_string().contains("overflow"), "{sql}: {err}");

            let sql =
                format!("SELECT AVG(val) OVER (ORDER BY id {frame}) FROM large_values ORDER BY id");
            let qr = conn.query(&sql).unwrap();
            assert_eq!(qr.rows, vec![vec![Value::Real(value as f64)]; 3], "{sql}");
        }
    }
}

#[test]
fn numeric_range_is_ignored_only_by_non_frame_functions() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);
    let frame = "RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING";

    for function in [
        "LAG(salary)",
        "LEAD(salary)",
        "ROW_NUMBER()",
        "RANK()",
        "DENSE_RANK()",
        "NTILE(2)",
    ] {
        let query = |frame: &str| {
            format!("SELECT {function} OVER (ORDER BY salary {frame}) FROM employees ORDER BY id")
        };
        assert_eq!(
            conn.query(&query(frame)).unwrap().rows,
            conn.query(&query("")).unwrap().rows,
            "{function}"
        );
    }
    for function in [
        "COUNT(*)",
        "SUM(salary)",
        "AVG(salary)",
        "MIN(salary)",
        "MAX(salary)",
        "FIRST_VALUE(salary)",
        "LAST_VALUE(salary)",
    ] {
        let sql = format!("SELECT {function} OVER (ORDER BY salary {frame}) FROM employees");
        let err = conn.query(&sql).unwrap_err();
        assert!(
            matches!(err, citadel_sql::SqlError::Unsupported(_)),
            "{sql}: {err}"
        );
    }
}

#[test]
fn suffix_real_aggregates_and_large_integer_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE magnitudes (id INTEGER PRIMARY KEY, val REAL, big INTEGER)")
        .unwrap();
    conn.execute_params(
        "INSERT INTO magnitudes VALUES (1, $1, $2), (2, 1.0, $2)",
        &[Value::Real(1e20), Value::Integer(i64::MAX)],
    )
    .unwrap();

    let qr = conn
        .query(
            "SELECT SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), \
             AVG(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
             FROM magnitudes ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        qr.rows,
        vec![
            vec![Value::Real(1e20), Value::Real(5e19)],
            vec![Value::Real(1.0), Value::Real(1.0)],
        ]
    );
    for (frame, counts) in [
        ("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING", [2, 1]),
        ("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", [1, 2]),
        (
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            [2, 2],
        ),
    ] {
        let sql =
            format!("SELECT COUNT(big) OVER (ORDER BY id {frame}) FROM magnitudes ORDER BY id");
        let expected: Vec<_> = counts
            .into_iter()
            .map(|count| vec![Value::Integer(count)])
            .collect();
        assert_eq!(conn.query(&sql).unwrap().rows, expected, "{frame}");
    }
}

#[test]
fn empty_input_still_validates_window_frames_and_arity() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE empty_frames (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for function in ["SUM(val)", "ROW_NUMBER()"] {
        for frame in [
            "-1 PRECEDING AND CURRENT ROW",
            "NULL PRECEDING AND CURRENT ROW",
            "1.5 PRECEDING AND CURRENT ROW",
            "CURRENT ROW AND 1 PRECEDING",
            "1 FOLLOWING AND CURRENT ROW",
            "UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING",
            "UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING",
        ] {
            let sql = format!(
                "SELECT {function} OVER (ORDER BY id ROWS BETWEEN {frame}) FROM empty_frames"
            );
            assert!(conn.query(&sql).is_err(), "expected error for {sql}");
        }
    }
    for function in [
        "LAG()",
        "LEAD()",
        "LAG(val, 1, 0, 0)",
        "LEAD(val, 1, 0, 0)",
        "NTILE()",
        "NTILE(1, 2)",
        "ROW_NUMBER(1)",
        "RANK(1)",
        "DENSE_RANK(1)",
        "COUNT(val, val)",
        "SUM()",
        "AVG()",
        "MIN()",
        "MAX()",
        "FIRST_VALUE()",
        "LAST_VALUE()",
    ] {
        let sql = format!("SELECT {function} OVER (ORDER BY id) FROM empty_frames");
        assert!(conn.query(&sql).is_err(), "expected error for {sql}");
    }
}

#[test]
fn rows_frame_offsets_accept_bound_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let conn = Connection::open(&db).unwrap();
    setup_employees(&conn);

    for (frame, offset, counts) in [
        ("$1 PRECEDING AND CURRENT ROW", 1, [1, 2, 2, 2, 2]),
        ("$1 PRECEDING AND CURRENT ROW", i64::MAX, [1, 2, 3, 4, 5]),
        ("CURRENT ROW AND $1 FOLLOWING", 1, [2, 2, 2, 2, 1]),
        ("CURRENT ROW AND $1 FOLLOWING", i64::MAX, [5, 4, 3, 2, 1]),
    ] {
        let sql = format!(
            "SELECT COUNT(*) OVER (ORDER BY id ROWS BETWEEN {frame}) FROM employees ORDER BY id"
        );
        let qr = conn.query_params(&sql, &[Value::Integer(offset)]).unwrap();
        let expected: Vec<_> = counts
            .into_iter()
            .map(|count| vec![Value::Integer(count)])
            .collect();
        assert_eq!(qr.rows, expected, "{frame}, offset {offset}");
    }
    for frame in [
        "$1 PRECEDING AND CURRENT ROW",
        "CURRENT ROW AND $1 FOLLOWING",
    ] {
        for offset in [Value::Null, Value::Integer(-1), Value::Real(1.5)] {
            let sql =
                format!("SELECT COUNT(*) OVER (ORDER BY id ROWS BETWEEN {frame}) FROM employees");
            assert!(
                conn.query_params(&sql, std::slice::from_ref(&offset))
                    .is_err(),
                "expected error for {sql}, offset {offset:?}"
            );
        }
    }
}
