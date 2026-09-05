use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

const ROWS: i64 = 6_000;
const ROUNDS: usize = 5;
const QUERIES: usize = 8;
const TIME_LIMIT: Duration = Duration::from_secs(90);

fn main() {
    let started = Instant::now();
    for case in ["text", "integer-composite", "integer-specialized"] {
        let db = DatabaseBuilder::new("")
            .passphrase(b"join-probe-benchmark")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        let key_type = if case == "text" { "TEXT" } else { "INTEGER" };
        for table in ["a", "b"] {
            conn.execute(&format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, k {key_type}, guard INTEGER)"
            ))
            .unwrap();
            conn.execute("BEGIN").unwrap();
            let insert = conn
                .prepare(&format!("INSERT INTO {table} VALUES ($1, $2, 0)"))
                .unwrap();
            for id in 0..ROWS {
                let key = if case == "text" {
                    Value::Text(format!("customer-{id:08}-{}", "x".repeat(80)).into())
                } else {
                    Value::Integer(id)
                };
                insert.execute(&[Value::Integer(id), key]).unwrap();
                if id % 1_000 == 0 {
                    assert!(started.elapsed() < TIME_LIMIT);
                }
            }
            conn.execute("COMMIT").unwrap();
        }
        let extra = if case == "integer-composite" {
            " AND a.guard = b.guard"
        } else {
            ""
        };
        let query = conn
            .prepare(&format!(
                "SELECT a.id, b.id FROM a JOIN b ON a.k = b.k{extra}"
            ))
            .unwrap();
        let run = || {
            assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
            let begin = Instant::now();
            let result = black_box(query.query_collect(&[]).unwrap());
            let elapsed = begin.elapsed();
            assert_eq!(result.rows.len(), ROWS as usize);
            for (id, row) in result.rows.iter().enumerate() {
                assert_eq!(
                    row.as_slice(),
                    [Value::Integer(id as i64), Value::Integer(id as i64)]
                );
            }
            elapsed
        };
        for _ in 0..4 {
            run();
        }
        let mut samples = Vec::with_capacity(ROUNDS);
        for round in 1..=ROUNDS {
            let elapsed: Duration = (0..QUERIES).map(|_| run()).sum();
            let micros = elapsed.as_secs_f64() * 1e6 / QUERIES as f64;
            samples.push(micros);
            println!("case={case} round={round} queries={QUERIES} us_per_query={micros:.3}");
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "case={case} rows={ROWS} measured_queries={} median_us={:.3}",
            ROUNDS * QUERIES,
            samples[ROUNDS / 2]
        );
    }
}
