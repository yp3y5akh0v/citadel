use std::hint::black_box;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

const ROUNDS: usize = 5;
const QUERIES: usize = 4;
const TIME_LIMIT: Duration = Duration::from_secs(90);

fn main() {
    let started = Instant::now();
    for case in [
        "timestamp-text",
        "date-timestamp",
        "temporal-composite",
        "timestamp-integer",
        "temporal-anticorrelated",
        "temporal-collated-text",
    ] {
        let sizes = if case == "temporal-anticorrelated" {
            [128, 1024]
        } else {
            [512, 4096]
        };
        for rows in sizes {
            let db = DatabaseBuilder::new("")
                .passphrase(b"temporal-join-benchmark")
                .argon2_profile(Argon2Profile::Iot)
                .create_in_memory()
                .unwrap();
            let conn = Connection::open(&db).unwrap();
            for (table, key_type) in match case {
                "date-timestamp" => [("a", "DATE"), ("b", "TIMESTAMP")],
                "timestamp-integer" => [("a", "TIMESTAMP"), ("b", "INTEGER")],
                _ => [("a", "TIMESTAMP"), ("b", "TEXT")],
            } {
                let seq_type = if case == "temporal-collated-text" {
                    "TEXT COLLATE NOCASE"
                } else {
                    "INTEGER"
                };
                conn.execute(&format!(
                    "CREATE TABLE {table} (id INTEGER PRIMARY KEY, k {key_type}, seq {seq_type})"
                ))
                .unwrap();
                conn.execute("BEGIN").unwrap();
                let insert = conn
                    .prepare(&format!("INSERT INTO {table} VALUES ($1, $2, $3)"))
                    .unwrap();
                for id in 0..rows {
                    let key = match (case, table) {
                        ("date-timestamp", "a") => Value::Date(id as i32),
                        ("date-timestamp", _) => Value::Timestamp(id * 86_400_000_000),
                        ("temporal-composite" | "temporal-collated-text", "a") => {
                            Value::Timestamp(0)
                        }
                        ("temporal-composite" | "temporal-collated-text", _) => {
                            Value::Text("1970-01-01 00:00:00".into())
                        }
                        ("temporal-anticorrelated", "a") => Value::Timestamp(0),
                        ("temporal-anticorrelated", _) => Value::Text(
                            if id < rows / 2 {
                                "1970-01-01 00:00:00"
                            } else {
                                "1970-01-02 00:00:00"
                            }
                            .into(),
                        ),
                        ("timestamp-integer", "b") => Value::Integer(id),
                        (_, "a") => Value::Timestamp(id * 1_000_000),
                        _ => Value::Text(Value::Timestamp(id * 1_000_000).to_string().into()),
                    };
                    let seq = match case {
                        "temporal-collated-text" => {
                            let text = format!("tenant-{id:08}-{}", "entry-".repeat(24));
                            assert!(text.len() > 128);
                            Value::Text(
                                if table == "a" {
                                    text.to_ascii_uppercase()
                                } else {
                                    text
                                }
                                .into(),
                            )
                        }
                        "temporal-anticorrelated" => {
                            Value::Integer(i64::from(table == "a" || id >= rows / 2))
                        }
                        _ => Value::Integer(id),
                    };
                    insert.execute(&[Value::Integer(id), key, seq]).unwrap();
                    if id % 512 == 0 {
                        assert!(started.elapsed() < TIME_LIMIT);
                    }
                }
                conn.execute("COMMIT").unwrap();
            }
            let extra = if matches!(
                case,
                "temporal-composite" | "temporal-anticorrelated" | "temporal-collated-text"
            ) {
                " AND a.seq = b.seq"
            } else {
                ""
            };
            let query = conn
                .prepare(&format!(
                    "SELECT a.id, b.id FROM a JOIN b ON a.k = b.k{extra} WHERE $1 >= 0"
                ))
                .unwrap();
            let mut parameter = 0;
            let mut run = || {
                assert!(started.elapsed() < TIME_LIMIT, "benchmark time limit");
                parameter += 1;
                let begin = Instant::now();
                let result = black_box(query.query_collect(&[Value::Integer(parameter)]).unwrap());
                let elapsed = begin.elapsed();
                let expected = if case == "temporal-anticorrelated" {
                    0
                } else {
                    rows as usize
                };
                assert_eq!(result.rows.len(), expected);
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
