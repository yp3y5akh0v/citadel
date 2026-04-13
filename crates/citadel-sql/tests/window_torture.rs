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

fn setup_employees(conn: &mut Connection) {
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

// ── 1. RANK without ORDER BY → error ────────────────────────────────

#[test]
fn error_rank_no_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let err = conn
        .query("SELECT RANK() OVER () FROM employees")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("requires ORDER BY"), "unexpected error: {msg}");
}

// ── 2. NTILE(0) → error ────────────────────────────────────────────

#[test]
fn error_ntile_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let err = conn
        .query("SELECT NTILE(0) OVER (ORDER BY id) FROM employees")
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("invalid") || msg.contains("NTILE"),
        "unexpected error: {msg}"
    );
}

// ── 3. Window over empty table ──────────────────────────────────────

#[test]
fn window_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE empty (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );

    let qr = conn
        .query("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM empty")
        .unwrap();
    assert_eq!(qr.rows.len(), 0);
}

// ── 4. Window over single row ───��──────────────────────────────────��

#[test]
fn window_single_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
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

// ── 5. All same partition (single partition) ────────────────────────

#[test]
fn window_all_same_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    // All 5 rows in one partition — ROW_NUMBER should be 1..5
    let qr = conn
        .query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM employees ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[1], Value::Integer(i as i64 + 1));
    }
}

// ── 6. Each row its own partition ───────────────────────────────────

#[test]
fn window_each_own_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

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

// ── 7. LAG beyond partition boundary ────────────────────────────────

#[test]
fn lag_beyond_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

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

// ── 8. LEAD beyond partition boundary ───────────────────────────────

#[test]
fn lead_beyond_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

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

// ── 9. NULL partition keys ────────────────────────────��─────────────

#[test]
fn window_nulls_in_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
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

// ── 10. NULL values in aggregated column ────────────��───────────────

#[test]
fn window_nulls_in_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
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

// ── 11. Sliding MIN/MAX over large dataset ──────────────────────────

#[test]
fn sliding_min_max_large() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap(),
    );
    // Insert 1000 rows
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

    // Verify first few and last few results manually
    // Row 1 (id=1): window = [row 1], val=(1*7+13)%997=20
    assert_eq!(qr.rows[0][2], qr.rows[0][1]); // min=val for first row
    assert_eq!(qr.rows[0][3], qr.rows[0][1]); // max=val for first row

    // Verify all MIN/MAX are correct against brute force
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

// ── 12. Running SUM verified against manual computation ─────────────

#[test]
fn running_sum_vs_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
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

// ── 13. Sort sharing (same OVER spec) ───────────────────────────────

#[test]
fn sort_sharing() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

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

// ── 14. Window with JOIN ────────────────────────────────────────────

#[test]
fn window_with_join() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    assert_ok(
        conn.execute("CREATE TABLE depts (name TEXT PRIMARY KEY, budget INTEGER NOT NULL)")
            .unwrap(),
    );
    conn.execute("INSERT INTO depts VALUES ('eng', 1000), ('sales', 500)")
        .unwrap();
    setup_employees(&mut conn);

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

// ── 15. UNBOUNDED frame ───────────────────────────────────���─────────

#[test]
fn unbounded_frame() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

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
