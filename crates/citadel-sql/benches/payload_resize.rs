//! Committed, self-restoring inline payload updates over a fixed 1,000-row table.
//! Each cycle shrinks every row, grows it back, and commits. Seeding at the
//! maximum width keeps the steady-state workload bounded; this measures page
//! compaction in SQL updates, not a continuously growing or splitting tree.

use std::hint::black_box;
use std::time::Duration;

use citadel::{Argon2Profile, Database, DatabaseBuilder, SyncMode};
use citadel_sql::{Connection, PreparedStatement, Value};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS: usize = 1_000;
const UPDATES_PER_CYCLE: u64 = 2 * ROWS as u64;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

fn bindings(width: usize, fill: char) -> Vec<[Value; 2]> {
    let suffix = fill.to_string().repeat(width - 4);
    (0..ROWS)
        .map(|id| {
            [
                Value::Text(format!("{id:04}{suffix}").into()),
                Value::Integer(id as i64),
            ]
        })
        .collect()
}

fn update_rows(update: &PreparedStatement<'_, '_>, bindings: &[[Value; 2]]) -> u64 {
    bindings
        .iter()
        .map(|params| update.execute(params).unwrap())
        .sum()
}

fn run_cycle(
    connection: &Connection<'_>,
    update: &PreparedStatement<'_, '_>,
    narrow: &[[Value; 2]],
    wide: &[[Value; 2]],
) -> u64 {
    connection.execute("BEGIN").unwrap();
    let affected = update_rows(update, narrow) + update_rows(update, wide);
    connection.execute("COMMIT").unwrap();
    affected
}

fn check_rows(connection: &Connection<'_>, expected: &[[Value; 2]]) {
    let rows = connection
        .query("SELECT id, payload FROM t ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), ROWS);
    for (id, (row, params)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], params[1], "key at position {id}");
        assert_eq!(row[1], params[0], "payload at position {id}");
    }
}

fn check_storage(database: &Database) {
    // A generous, fixed untimed guard catches runaway physical growth while
    // allowing different free-page placement between implementations. Even
    // this bound fits inside the configured 4,096-page (32 MiB) cache.
    let bytes = std::fs::metadata(database.data_path()).unwrap().len();
    assert!(
        bytes <= MAX_FILE_BYTES,
        "fixed-size fixture grew to {bytes} bytes"
    );
    let report = database.integrity_check_quiet().unwrap();
    assert!(report.is_ok(), "{report:?}");
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_resize");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(30);
    group.throughput(Throughput::Elements(UPDATES_PER_CYCLE));

    for (small, large) in [(8, 32), (128, 512)] {
        let directory = tempfile::tempdir().unwrap();
        // Match the existing SQL controls: a full commit is timed, with fsync
        // disabled. No read transaction remains open across update cycles.
        let database = DatabaseBuilder::new(directory.path().join("bench.citadel"))
            .passphrase(b"bench-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .cache_size(4096)
            .sync_mode(SyncMode::Off)
            .create()
            .unwrap();
        let connection = Connection::open(&database).unwrap();
        connection
            .execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)")
            .unwrap();
        // Payload bindings and UPDATE preparation stay outside timing.
        // The fixed-width key prefix makes each row's expected payload unique.
        let narrow = bindings(small, 's');
        let wide = bindings(large, 'w');
        let insert = connection
            .prepare("INSERT INTO t (payload, id) VALUES ($1, $2)")
            .unwrap();
        connection.execute("BEGIN").unwrap();
        for params in &wide {
            assert_eq!(insert.execute(params).unwrap(), 1);
        }
        connection.execute("COMMIT").unwrap();
        let update = connection
            .prepare("UPDATE t SET payload = $1 WHERE id = $2")
            .unwrap();
        check_rows(&connection, &wide);

        // Validate both intermediate states, then warm page reuse and the
        // prepared mutation path before Criterion's own warm-up begins.
        connection.execute("BEGIN").unwrap();
        assert_eq!(update_rows(&update, &narrow), ROWS as u64);
        check_rows(&connection, &narrow);
        assert_eq!(update_rows(&update, &wide), ROWS as u64);
        check_rows(&connection, &wide);
        connection.execute("COMMIT").unwrap();
        for _ in 0..4 {
            assert_eq!(
                run_cycle(&connection, &update, &narrow, &wide),
                UPDATES_PER_CYCLE
            );
        }
        check_rows(&connection, &wide);
        check_storage(&database);

        let mut cycles = 0u64;
        let mut affected = 0u64;
        group.bench_function(BenchmarkId::new(format!("{small}_{large}"), ROWS), |b| {
            b.iter(|| {
                let count = run_cycle(&connection, &update, &narrow, &wide);
                affected += count;
                cycles += 1;
                black_box(count)
            });
        });
        assert_eq!(affected, cycles * UPDATES_PER_CYCLE);
        check_rows(&connection, &wide);
        check_storage(&database);
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
