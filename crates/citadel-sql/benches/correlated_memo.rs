//! Reexecuted correlated IN with one shared RHS and changing outer operands.
//! Times query_collect only; result validation and destruction are excluded.

use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, Database, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, PreparedStatement, Value};

const OUTER_ROWS: usize = 2_048;
const RHS_ROWS: usize = 2_048;
const WARMUP: usize = 8;
const ROUNDS: usize = 5;
const QUERIES_PER_ROUND: usize = 8;
const TIME_LIMIT: Duration = Duration::from_secs(90);
const QUERY: &str = "SELECT p.id AS id FROM facts p WHERE p.id >= $1 AND p.needle IN \
    (SELECT c.value FROM children c WHERE c.bucket <= p.bucket) ORDER BY p.id";

fn text_value(id: usize) -> String {
    format!(
        "memo-value-{id:05}-{}",
        "materialized-owned-text-".repeat(4)
    )
}

fn check_runtime(started: Instant) {
    // Bound the whole probe, not the latency accepted for an individual query.
    assert!(started.elapsed() < TIME_LIMIT, "benchmark runtime limit");
}

fn seed(connection: &Connection<'_>, started: Instant) {
    connection
        .execute("CREATE TABLE facts (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, needle TEXT NOT NULL)")
        .unwrap();
    connection
        .execute("CREATE TABLE children (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, value TEXT NOT NULL)")
        .unwrap();
    let outer = connection
        .prepare("INSERT INTO facts VALUES ($1,1,$2)")
        .unwrap();
    let inner = connection
        .prepare("INSERT INTO children VALUES ($1,1,$2)")
        .unwrap();
    connection.execute("BEGIN").unwrap();
    for id in 0..RHS_ROWS {
        assert_eq!(
            inner
                .execute(&[
                    Value::Integer(id as i64),
                    Value::Text(text_value(id).into())
                ])
                .unwrap(),
            1
        );
        if id % 256 == 0 {
            check_runtime(started);
        }
    }
    for id in 0..OUTER_ROWS {
        let needle = if id % 3 == 0 {
            id % RHS_ROWS
        } else {
            RHS_ROWS + id
        };
        assert_eq!(
            outer
                .execute(&[
                    Value::Integer(id as i64),
                    Value::Text(text_value(needle).into()),
                ])
                .unwrap(),
            1
        );
        if id % 256 == 0 {
            check_runtime(started);
        }
    }
    connection.execute("COMMIT").unwrap();
}

fn run_query(
    database: &Database,
    query: &PreparedStatement<'_, '_>,
    threshold: usize,
    expected: &[Vec<Value>],
    started: Instant,
) -> (Duration, u64) {
    check_runtime(started);
    let parameters = [Value::Integer(threshold as i64)];
    let measurement = database.measure_scans();
    let begin = Instant::now();
    let result = black_box(query.query_collect(black_box(&parameters)).unwrap());
    let elapsed = begin.elapsed();
    let scanned = measurement.rows_scanned();
    drop(measurement);
    assert_eq!(result.columns, ["id"]);
    assert_eq!(result.rows, expected);
    assert!(scanned > 0, "query returned from the result cache");
    check_runtime(started);
    (elapsed, scanned)
}

fn main() {
    let started = Instant::now();
    let database = DatabaseBuilder::new("")
        .passphrase(b"correlated-memo-benchmark")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(4096)
        .sync_mode(SyncMode::Off)
        .create_in_memory()
        .unwrap();
    let connection = Connection::open(&database).unwrap();
    seed(&connection, started);
    let query = connection.prepare(QUERY).unwrap();
    let expected: [Vec<Vec<Value>>; 2] = std::array::from_fn(|threshold| {
        (threshold..OUTER_ROWS)
            .filter(|id| id % 3 == 0)
            .map(|id| vec![Value::Integer(id as i64)])
            .collect()
    });

    // Keep the sequence continuous through warmup and all measured rounds.
    // Alternating the threshold changes both the parameter and the answer.
    let mut sequence = 0;
    for _ in 0..WARMUP {
        let threshold = sequence % 2;
        run_query(&database, &query, threshold, &expected[threshold], started);
        sequence += 1;
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    let mut total_scanned = 0;
    for round in 1..=ROUNDS {
        let mut elapsed = Duration::ZERO;
        for _ in 0..QUERIES_PER_ROUND {
            let threshold = sequence % 2;
            let (duration, scanned) =
                run_query(&database, &query, threshold, &expected[threshold], started);
            sequence += 1;
            elapsed += duration;
            total_scanned += scanned;
        }
        let micros = elapsed.as_secs_f64() * 1e6 / QUERIES_PER_ROUND as f64;
        samples.push(micros);
        println!("round={round} queries={QUERIES_PER_ROUND} us_per_query={micros:.3}");
    }
    samples.sort_by(f64::total_cmp);
    println!(
        "case=repeated_key_correlated_in outer_rows={OUTER_ROWS} rhs_rows={RHS_ROWS} \
         measured_queries={} rows_scanned={total_scanned} median_us={:.3} \
         timing=query_collect wall_seconds={:.3}",
        ROUNDS * QUERIES_PER_ROUND,
        samples[ROUNDS / 2],
        started.elapsed().as_secs_f64(),
    );
}
