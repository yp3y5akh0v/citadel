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
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept TEXT NOT NULL, salary INTEGER NOT NULL)",
        )
        .unwrap(),
    );
    conn.execute("INSERT INTO employees (id, name, dept, salary) VALUES (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 90), (3, 'Carol', 'sales', 80), (4, 'Dave', 'sales', 70), (5, 'Eve', 'eng', 100)")
        .unwrap();
}

// ── 1. ROW_NUMBER basic ─────────────────────────────────────────────

#[test]
fn row_number_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM employees ORDER BY id")
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    for (i, row) in qr.rows.iter().enumerate() {
        assert_eq!(row[1], Value::Integer(i as i64 + 1));
    }
}

// ── 2. ROW_NUMBER with PARTITION BY ─────────────────────────────────

#[test]
fn row_number_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn \
             FROM employees ORDER BY dept, id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 5);
    // eng: Alice(1), Bob(2), Eve(3)
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(2));
    assert_eq!(qr.rows[2][2], Value::Integer(3));
    // sales: Carol(1), Dave(2)
    assert_eq!(qr.rows[3][2], Value::Integer(1));
    assert_eq!(qr.rows[4][2], Value::Integer(2));
}

// ── 3. RANK with ties ───────────────────────────────────────────────

#[test]
fn rank_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, RANK() OVER (ORDER BY salary DESC) AS rnk \
             FROM employees ORDER BY salary DESC, id",
        )
        .unwrap();
    // salary: 100, 100, 90, 80, 70
    // rank:   1,   1,   3,  4,  5
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(1));
    assert_eq!(qr.rows[2][2], Value::Integer(3));
    assert_eq!(qr.rows[3][2], Value::Integer(4));
    assert_eq!(qr.rows[4][2], Value::Integer(5));
}

// ── 4. DENSE_RANK ───────────────────────────────────────────────────

#[test]
fn dense_rank_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk \
             FROM employees ORDER BY salary DESC, id",
        )
        .unwrap();
    // salary: 100, 100, 90, 80, 70
    // dense:  1,   1,   2,  3,  4
    assert_eq!(qr.rows[0][2], Value::Integer(1));
    assert_eq!(qr.rows[1][2], Value::Integer(1));
    assert_eq!(qr.rows[2][2], Value::Integer(2));
    assert_eq!(qr.rows[3][2], Value::Integer(3));
    assert_eq!(qr.rows[4][2], Value::Integer(4));
}

// ── 5. NTILE ────────────────────────────────────────────────────────

#[test]
fn ntile_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query("SELECT id, NTILE(3) OVER (ORDER BY id) AS tile FROM employees ORDER BY id")
        .unwrap();
    // 5 rows / 3 tiles: sizes 2, 2, 1
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(1));
    assert_eq!(qr.rows[2][1], Value::Integer(2));
    assert_eq!(qr.rows[3][1], Value::Integer(2));
    assert_eq!(qr.rows[4][1], Value::Integer(3));
}

// ── 6. LAG basic ────────────────────────────────────────────────────

#[test]
fn lag_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, LAG(salary, 1) OVER (ORDER BY id) AS prev_sal \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][2], Value::Null); // no previous
    assert_eq!(qr.rows[1][2], Value::Integer(100)); // Alice's salary
    assert_eq!(qr.rows[2][2], Value::Integer(90)); // Bob's salary
}

// ── 7. LAG with default ─────────────────────────────────────────────

#[test]
fn lag_default() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev_sal \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(0)); // default
    assert_eq!(qr.rows[1][1], Value::Integer(100));
}

// ── 8. LEAD basic ───────────────────────────────────────────────────

#[test]
fn lead_basic() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, LEAD(salary, 1) OVER (ORDER BY id) AS next_sal \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][2], Value::Integer(90)); // Bob's salary
    assert_eq!(qr.rows[4][2], Value::Null); // no next
}

// ── 9. FIRST_VALUE ──────────────────────────────────────────────────

#[test]
fn first_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, dept, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY id) AS first_name \
             FROM employees ORDER BY dept, id",
        )
        .unwrap();
    // eng partition: first = Alice
    assert_eq!(qr.rows[0][2], Value::Text("Alice".into()));
    assert_eq!(qr.rows[1][2], Value::Text("Alice".into()));
    assert_eq!(qr.rows[2][2], Value::Text("Alice".into()));
    // sales partition: first = Carol
    assert_eq!(qr.rows[3][2], Value::Text("Carol".into()));
    assert_eq!(qr.rows[4][2], Value::Text("Carol".into()));
}

// ── 10. LAST_VALUE ──────────────────────────────────────────────────

#[test]
fn last_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, dept, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_name \
             FROM employees ORDER BY dept, id",
        )
        .unwrap();
    // eng: last = Eve (id=5)
    assert_eq!(qr.rows[0][2], Value::Text("Eve".into()));
    assert_eq!(qr.rows[1][2], Value::Text("Eve".into()));
    assert_eq!(qr.rows[2][2], Value::Text("Eve".into()));
    // sales: last = Dave (id=4)
    assert_eq!(qr.rows[3][2], Value::Text("Dave".into()));
    assert_eq!(qr.rows[4][2], Value::Text("Dave".into()));
}

// ── 11. SUM running total ───────────────────────────────────────────

#[test]
fn sum_running() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, SUM(salary) OVER (ORDER BY id) AS running \
             FROM employees ORDER BY id",
        )
        .unwrap();
    // Default frame with ORDER BY: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
    assert_eq!(qr.rows[0][2], Value::Integer(100));
    assert_eq!(qr.rows[1][2], Value::Integer(190));
    assert_eq!(qr.rows[2][2], Value::Integer(270));
    assert_eq!(qr.rows[3][2], Value::Integer(340));
    assert_eq!(qr.rows[4][2], Value::Integer(440));
}

// ── 12. COUNT(*) OVER partition ─────────────────────────────────────

#[test]
fn count_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_count \
             FROM employees ORDER BY dept, id",
        )
        .unwrap();
    // eng: 3 employees
    assert_eq!(qr.rows[0][2], Value::Integer(3));
    assert_eq!(qr.rows[1][2], Value::Integer(3));
    assert_eq!(qr.rows[2][2], Value::Integer(3));
    // sales: 2 employees
    assert_eq!(qr.rows[3][2], Value::Integer(2));
    assert_eq!(qr.rows[4][2], Value::Integer(2));
}

// ── 13. AVG sliding window ──────────────────────────────────────────

#[test]
fn avg_sliding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, AVG(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg2 \
             FROM employees ORDER BY id",
        )
        .unwrap();
    // id1: avg(100) = 100.0
    assert_eq!(qr.rows[0][2], Value::Real(100.0));
    // id2: avg(100,90) = 95.0
    assert_eq!(qr.rows[1][2], Value::Real(95.0));
    // id3: avg(90,80) = 85.0
    assert_eq!(qr.rows[2][2], Value::Real(85.0));
}

// ── 14. MIN sliding ─────────────────────────────────────────────────

#[test]
fn min_sliding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, MIN(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min2 \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][2], Value::Integer(100)); // min(100)
    assert_eq!(qr.rows[1][2], Value::Integer(90)); // min(100,90)
    assert_eq!(qr.rows[2][2], Value::Integer(80)); // min(90,80)
    assert_eq!(qr.rows[3][2], Value::Integer(70)); // min(80,70)
    assert_eq!(qr.rows[4][2], Value::Integer(70)); // min(70,100)
}

// ── 15. MAX sliding ─────────────────────────────────────────────────

#[test]
fn max_sliding() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, salary, MAX(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max2 \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][2], Value::Integer(100)); // max(100)
    assert_eq!(qr.rows[1][2], Value::Integer(100)); // max(100,90)
    assert_eq!(qr.rows[2][2], Value::Integer(90)); // max(90,80)
    assert_eq!(qr.rows[3][2], Value::Integer(80)); // max(80,70)
    assert_eq!(qr.rows[4][2], Value::Integer(100)); // max(70,100)
}

// ── 16. Multiple different windows ──────────────────────────────────

#[test]
fn multiple_windows() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, \
                    ROW_NUMBER() OVER (ORDER BY id) AS rn, \
                    SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum \
             FROM employees ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[0][2], Value::Integer(100));
    assert_eq!(qr.rows[4][1], Value::Integer(5));
    assert_eq!(qr.rows[4][2], Value::Integer(440));
}

// ── 17. Window with WHERE ───────────────────────────────────────────

#[test]
fn window_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn \
             FROM employees WHERE dept = 'eng' ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[1][1], Value::Integer(2));
    assert_eq!(qr.rows[2][1], Value::Integer(3));
}

// ── 18. Outer ORDER BY differs from window ──────────────────────────

#[test]
fn window_with_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn \
             FROM employees ORDER BY id",
        )
        .unwrap();
    // Outer order is by id, window order is by salary desc
    // id=1 salary=100 -> rn depends on salary ordering
    assert_eq!(qr.rows.len(), 5);
    // id=1 (salary=100) is rank 1 or 2 (tied with id=5)
    let rn1 = &qr.rows[0][1];
    let rn5 = &qr.rows[4][1];
    // Both should be 1 or 2 (ties, stable sort gives id=1 -> rn=1, id=5 -> rn=2)
    assert!(matches!(rn1, Value::Integer(1) | Value::Integer(2)));
    assert!(matches!(rn5, Value::Integer(1) | Value::Integer(2)));
}

// ── 19. Window with LIMIT ───────────────────────────────────────────

#[test]
fn window_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn \
             FROM employees ORDER BY id LIMIT 3",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
    assert_eq!(qr.rows[0][1], Value::Integer(1));
    assert_eq!(qr.rows[2][1], Value::Integer(3));
}

// ── 20. Window over CTE ─────────────────────────────────────────────

#[test]
fn window_over_cte() {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(dir.path());
    let mut conn = Connection::open(&db).unwrap();
    setup_employees(&mut conn);

    let qr = conn
        .query(
            "WITH eng AS (SELECT * FROM employees WHERE dept = 'eng') \
             SELECT id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM eng ORDER BY id",
        )
        .unwrap();
    assert_eq!(qr.rows.len(), 3);
}
