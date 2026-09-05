use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, PreparedStatement, Value};

const ROWS: i64 = 50_000;
const PAGE: usize = 20;
const WARMUP: usize = 8;
const ROUNDS: usize = 5;
const QUERIES_PER_ROUND: usize = 32;
const TIME_LIMIT: Duration = Duration::from_secs(90);

fn cursor(sequence: usize) -> i64 {
    49_000 + (sequence as i64 * 37) % 900
}

fn check_page(rows: &[Vec<Value>], after: i64) {
    assert_eq!(rows.len(), PAGE);
    for (offset, row) in rows.iter().enumerate() {
        assert_eq!(row.as_slice(), &[Value::Integer(after + offset as i64 + 1)]);
    }
}

fn main() {
    let started = Instant::now();
    let db = DatabaseBuilder::new("")
        .passphrase(b"pk-range-benchmark")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, marker INTEGER NOT NULL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1, 0)").unwrap();
    for id in 0..ROWS {
        insert.execute(&[Value::Integer(id)]).unwrap();
        if id % 1_000 == 0 {
            assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
        }
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM items").unwrap().rows,
        vec![vec![Value::Integer(ROWS)]]
    );
    let query = conn
        .prepare("SELECT id FROM items WHERE id >= $1 AND id > $2 LIMIT 20")
        .unwrap();
    for sequence in 0..WARMUP {
        run_query(&db, &query, cursor(sequence), started);
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    let mut total_scanned = 0;
    for round in 0..ROUNDS {
        let mut elapsed = Duration::ZERO;
        for position in 0..QUERIES_PER_ROUND {
            let sequence = WARMUP + round * QUERIES_PER_ROUND + position;
            let (duration, scanned) = run_query(&db, &query, cursor(sequence), started);
            elapsed += duration;
            total_scanned += scanned;
        }
        let micros = elapsed.as_secs_f64() * 1_000_000.0 / QUERIES_PER_ROUND as f64;
        samples.push(micros);
        println!(
            "round={} queries={} us_per_query={micros:.3}",
            round + 1,
            QUERIES_PER_ROUND
        );
    }
    samples.sort_by(f64::total_cmp);
    println!(
        "rows={ROWS} page={PAGE} measured_queries={} rows_scanned={total_scanned} median_us={:.3} wall_seconds={:.3}",
        ROUNDS * QUERIES_PER_ROUND,
        samples[ROUNDS / 2],
        started.elapsed().as_secs_f64(),
    );
}

fn run_query(
    db: &citadel::Database,
    query: &PreparedStatement<'_, '_>,
    after: i64,
    started: Instant,
) -> (Duration, u64) {
    assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
    let measurement = db.measure_scans();
    let begin = Instant::now();
    let result = black_box(
        query
            .query_collect(&[Value::Integer(0), Value::Integer(after)])
            .unwrap(),
    );
    let elapsed = begin.elapsed();
    let scanned = measurement.rows_scanned();
    check_page(&result.rows, after);
    assert!(
        scanned == PAGE as u64 + 1 || scanned == after as u64 + PAGE as u64 + 1,
        "unexpected scan count at {after}: {scanned}"
    );
    (elapsed, scanned)
}
