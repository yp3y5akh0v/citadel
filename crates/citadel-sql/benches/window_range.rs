use std::hint::black_box;
use std::time::Duration;

use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, Value};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Copy)]
enum Frame {
    Prefix,
    Peers,
}

impl Frame {
    fn name(self) -> &'static str {
        match self {
            Self::Prefix => "prefix",
            Self::Peers => "peers",
        }
    }

    fn query(self) -> String {
        let spec = match self {
            Self::Prefix => "ORDER BY id",
            Self::Peers => "ORDER BY peer_key RANGE BETWEEN CURRENT ROW AND CURRENT ROW",
        };
        let aggregates = [
            "SUM(v)", "COUNT(*)", "COUNT(v)", "AVG(v)", "MIN(v)", "MAX(v)",
        ]
        .map(|aggregate| format!("{aggregate} OVER ({spec})"))
        .join(", ");
        format!("SELECT id, payload, {aggregates} FROM t ORDER BY id")
    }
}

fn input_value(id: i64) -> Option<i64> {
    (id % 11 != 0).then_some(id % 97 - 48)
}

#[derive(Clone, Copy, Default)]
struct Totals {
    rows: i64,
    nonnull: i64,
    sum: i64,
    min: Option<i64>,
    max: Option<i64>,
}

impl Totals {
    fn add(&mut self, value: Option<i64>) {
        self.rows += 1;
        if let Some(value) = value {
            self.nonnull += 1;
            self.sum += value;
            self.min = Some(self.min.map_or(value, |old| old.min(value)));
            self.max = Some(self.max.map_or(value, |old| old.max(value)));
        }
    }

    fn result(self) -> [Value; 6] {
        [
            if self.nonnull == 0 {
                Value::Null
            } else {
                Value::Integer(self.sum)
            },
            Value::Integer(self.rows),
            Value::Integer(self.nonnull),
            if self.nonnull == 0 {
                Value::Null
            } else {
                Value::Real(self.sum as f64 / self.nonnull as f64)
            },
            self.min.map_or(Value::Null, Value::Integer),
            self.max.map_or(Value::Null, Value::Integer),
        ]
    }
}

fn expected_rows(frame: Frame, count: i64, payload: &str) -> Vec<Vec<Value>> {
    let mut peers = [Totals::default(); 4];
    for id in 0..count {
        let peer = id as usize % peers.len();
        peers[peer].add(input_value(id));
    }
    let mut prefix = Totals::default();
    (0..count)
        .map(|id| {
            prefix.add(input_value(id));
            let totals = match frame {
                Frame::Prefix => prefix,
                Frame::Peers => peers[id as usize % peers.len()],
            };
            let mut row = vec![Value::Integer(id), Value::Text(payload.into())];
            row.extend(totals.result());
            row
        })
        .collect()
}

fn sqlite_collect(statement: &mut rusqlite::Statement<'_>) -> Vec<Vec<rusqlite::types::Value>> {
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| row.get::<_, rusqlite::types::Value>(column))
                .collect()
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn bench(c: &mut Criterion) {
    let payload = "p".repeat(512);
    for count in [1000_i64, 3000] {
        let citadel_dir = tempfile::tempdir().unwrap();
        let database = DatabaseBuilder::new(citadel_dir.path().join("bench.citadel"))
            .passphrase(b"bench-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .cache_size(4096)
            .sync_mode(SyncMode::Off)
            .create()
            .unwrap();
        let connection = Connection::open(&database).unwrap();
        let sqlite_dir = tempfile::tempdir().unwrap();
        let sqlite = rusqlite::Connection::open(sqlite_dir.path().join("bench.db")).unwrap();
        sqlite.execute_batch("PRAGMA page_size=8192; PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF; PRAGMA cache_size=4096;").unwrap();
        let create = "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, payload TEXT, peer_key INTEGER, v INTEGER)";
        connection.execute(create).unwrap();
        sqlite.execute_batch(create).unwrap();
        connection.execute("BEGIN").unwrap();
        sqlite.execute_batch("BEGIN").unwrap();
        let insert = connection
            .prepare("INSERT INTO t VALUES ($1, $2, $3, $4)")
            .unwrap();
        let mut sqlite_insert = sqlite
            .prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        for id in 0..count {
            insert
                .execute(&[
                    Value::Integer(id),
                    Value::Text(payload.as_str().into()),
                    Value::Integer(id % 4),
                    input_value(id).map_or(Value::Null, Value::Integer),
                ])
                .unwrap();
            sqlite_insert
                .execute(rusqlite::params![id, payload, id % 4, input_value(id)])
                .unwrap();
        }
        connection.execute("COMMIT").unwrap();
        sqlite.execute_batch("COMMIT").unwrap();

        for frame in [Frame::Prefix, Frame::Peers] {
            let query_text = frame.query();
            let query = connection.prepare(&query_text).unwrap();
            let mut sqlite_query = sqlite.prepare(&query_text).unwrap();
            let expected = expected_rows(frame, count, &payload);
            // The smallest output owns 500 KiB of TEXT, beyond the 128 KiB
            // result-cache cap. Verify both prepared executions really scan.
            for _ in 0..2 {
                let measurement = database.measure_scans();
                let result = query.query_collect(&[]).unwrap();
                assert!(measurement.rows_scanned() >= count as u64);
                assert_eq!(result.rows, expected);
            }
            let sqlite_rows: Vec<Vec<Value>> = sqlite_collect(&mut sqlite_query)
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| match value {
                            rusqlite::types::Value::Null => Value::Null,
                            rusqlite::types::Value::Integer(value) => Value::Integer(value),
                            rusqlite::types::Value::Real(value) => Value::Real(value),
                            rusqlite::types::Value::Text(value) => Value::Text(value.into()),
                            rusqlite::types::Value::Blob(value) => Value::Blob(value),
                        })
                        .collect()
                })
                .collect();
            assert_eq!(sqlite_rows, expected);

            let mut group = c.benchmark_group(format!("window_range_{}_{count}", frame.name()));
            group.warm_up_time(Duration::from_secs(1));
            group.measurement_time(Duration::from_secs(2));
            group.sample_size(30);
            group.bench_function(BenchmarkId::new("citadel", ""), |b| {
                b.iter(|| black_box(query.query_collect(&[]).unwrap()));
            });
            group.bench_function(BenchmarkId::new("sqlite", ""), |b| {
                b.iter(|| black_box(sqlite_collect(&mut sqlite_query)));
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
