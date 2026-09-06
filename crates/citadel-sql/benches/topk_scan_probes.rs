use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

const ROUNDS: usize = 5;
const QUERIES: usize = 4;
const TIME_LIMIT: Duration = Duration::from_secs(90);

fn main() {
    let cache_pages = std::env::var_os("CITADEL_BENCH_CACHE_PAGES")
        .map(|value| {
            value
                .into_string()
                .expect("non-Unicode cache page count")
                .parse::<usize>()
                .expect("invalid cache page count")
        })
        .unwrap_or(citadel_core::constants::DEFAULT_BUFFER_POOL_SIZE);
    assert!((1..=16_384).contains(&cache_pages));
    println!("cache_pages={cache_pages}");
    let started = Instant::now();
    for rows in [8_192i64, 32_768] {
        let db = DatabaseBuilder::new("")
            .passphrase(b"topk-scan-benchmark")
            .argon2_profile(Argon2Profile::Iot)
            .cache_size(cache_pages)
            .create_in_memory()
            .unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, bucket INTEGER, score INTEGER, \
             name TEXT, payload1 TEXT, payload2 TEXT, payload3 TEXT, payload4 TEXT)",
        )
        .unwrap();
        let payload = Value::Text("record-payload-".repeat(20).into());
        let mut expected = Vec::with_capacity(rows as usize);
        conn.execute("BEGIN").unwrap();
        let insert = conn
            .prepare("INSERT INTO events VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
            .unwrap();
        for id in 0..rows {
            let score = (id * 4051) % rows;
            let name = format!("item-{score:08}-{}", "value-".repeat(24));
            let name = if id % 2 == 0 {
                name.to_ascii_uppercase()
            } else {
                name
            };
            insert
                .execute(&[
                    Value::Integer(id),
                    Value::Integer(id % 100),
                    Value::Integer(score),
                    Value::Text(name.clone().into()),
                    payload.clone(),
                    payload.clone(),
                    payload.clone(),
                    payload.clone(),
                ])
                .unwrap();
            expected.push((id, score, name));
            if id % 512 == 0 {
                assert!(started.elapsed() < TIME_LIMIT);
            }
        }
        conn.execute("COMMIT").unwrap();

        for (case, predicate, order) in [
            ("filtered-numeric", "WHERE bucket >= 20", "score DESC"),
            (
                "filtered-text",
                "WHERE bucket BETWEEN 20 AND 79",
                "name COLLATE NOCASE DESC",
            ),
            ("unfiltered-numeric", "", "score DESC"),
            ("unfiltered-text", "", "name COLLATE NOCASE DESC"),
        ] {
            let mut expected: Vec<_> = expected
                .iter()
                .filter(|(id, _, _)| match case {
                    "filtered-numeric" => id % 100 >= 20,
                    "filtered-text" => (20..=79).contains(&(id % 100)),
                    _ => true,
                })
                .collect();
            expected.sort_by_key(|(_, score, _)| std::cmp::Reverse(*score));
            let query = conn
                .prepare(&format!(
                    "SELECT id, name FROM events {predicate} ORDER BY {order} LIMIT $1 OFFSET 3"
                ))
                .unwrap();
            let mut invocation = 0;
            let mut run = || {
                assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
                let limit = 10 + invocation % 4;
                invocation += 1;
                let begin = Instant::now();
                let result = black_box(query.query_collect(&[Value::Integer(limit)]).unwrap());
                let elapsed = begin.elapsed();
                assert_eq!(result.rows.len(), limit as usize);
                for (actual, expected) in result.rows.iter().zip(&expected[3..]) {
                    assert_eq!(
                        actual.as_slice(),
                        [
                            Value::Integer(expected.0),
                            Value::Text(expected.2.clone().into())
                        ]
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
                println!("case={case} rows={rows} round={round} queries={QUERIES} us_per_query={micros:.3}");
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "case={case} rows={rows} measured_queries={} median_us={:.3}",
                ROUNDS * QUERIES,
                samples[ROUNDS / 2]
            );
        }
    }
}
