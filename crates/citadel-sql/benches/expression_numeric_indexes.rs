use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, PreparedStatement, Value};

const ROWS: i64 = 50_000;
const WARMUP: usize = 8;
const ROUNDS: usize = 5;
const QUERIES: usize = 32;
const TIME_LIMIT: Duration = Duration::from_secs(90);

fn main() {
    let started = Instant::now();
    let db = DatabaseBuilder::new("")
        .passphrase(b"expression-index-benchmark")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, external_id TEXT NOT NULL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn.prepare("INSERT INTO events VALUES ($1, $2)").unwrap();
    for id in 0..ROWS {
        insert
            .execute(&[Value::Integer(id), Value::Text(id.to_string().into())])
            .unwrap();
        if id % 1_000 == 0 {
            assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
        }
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX by_external_id ON events (CAST(external_id AS INTEGER))")
        .unwrap();
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM events").unwrap().rows,
        vec![vec![Value::Integer(ROWS)]]
    );
    let query = conn
        .prepare("SELECT id FROM events WHERE CAST(external_id AS INTEGER) = $1")
        .unwrap();
    let key = |sequence: usize| 49_000 + (sequence as i64 * 37) % 900;
    for sequence in 0..WARMUP {
        run_query(&db, &query, key(sequence), started);
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    let mut total_scanned = 0;
    for round in 0..ROUNDS {
        let mut elapsed = Duration::ZERO;
        for position in 0..QUERIES {
            let sequence = WARMUP + round * QUERIES + position;
            let (duration, scanned) = run_query(&db, &query, key(sequence), started);
            elapsed += duration;
            total_scanned += scanned;
        }
        let micros = elapsed.as_secs_f64() * 1_000_000.0 / QUERIES as f64;
        samples.push(micros);
        println!(
            "round={} queries={QUERIES} us_per_query={micros:.3}",
            round + 1
        );
    }
    samples.sort_by(f64::total_cmp);
    println!("rows={ROWS} measured_queries={} rows_scanned={total_scanned} median_us={:.3} wall_seconds={:.3}", ROUNDS * QUERIES, samples[ROUNDS / 2], started.elapsed().as_secs_f64());
}

fn run_query(
    db: &citadel::Database,
    query: &PreparedStatement<'_, '_>,
    key: i64,
    started: Instant,
) -> (Duration, u64) {
    assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
    let measurement = db.measure_scans();
    let begin = Instant::now();
    let result = black_box(query.query_collect(&[Value::Integer(key)]).unwrap());
    let elapsed = begin.elapsed();
    let scanned = measurement.rows_scanned();
    assert_eq!(result.rows, vec![vec![Value::Integer(key)]]);
    assert!(
        scanned == ROWS as u64 || scanned == 2,
        "unexpected scan count at {key}: {scanned}"
    );
    (elapsed, scanned)
}
