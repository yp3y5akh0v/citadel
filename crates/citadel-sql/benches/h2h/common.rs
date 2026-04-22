use citadel::{Argon2Profile, DatabaseBuilder, SyncMode};
use citadel_sql::Connection;

pub fn citadel_db(dir: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(dir.join("bench.citadel"))
        .passphrase(b"bench-passphrase")
        .argon2_profile(Argon2Profile::Iot)
        .cache_size(4096)
        .sync_mode(SyncMode::Off)
        .create()
        .unwrap()
}

pub fn citadel_100k(conn: &Connection) {
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..100_000i64 {
        conn.execute(&format!(
            "INSERT INTO t (id, name, age) VALUES ({i}, 'user_{i}', {})",
            i % 100
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

pub fn citadel_join_tables(conn: &Connection) {
    conn.execute("CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER, data TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..1_000i64 {
        conn.execute(&format!("INSERT INTO a (id, val) VALUES ({i}, 'a_{i}')"))
            .unwrap();
    }
    for i in 0..1_000i64 {
        conn.execute(&format!(
            "INSERT INTO b (id, a_id, data) VALUES ({i}, {}, 'b_{i}')",
            i % 1_000
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

pub fn sqlite_db(dir: &std::path::Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(dir.join("bench.db")).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=8192; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=4096;",
    )
    .unwrap();
    conn
}

pub fn sqlite_100k(conn: &rusqlite::Connection) {
    conn.execute(
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)",
        [],
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..100_000i64 {
        conn.execute(
            "INSERT INTO t (id, name, age) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, format!("user_{i}"), i % 100],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

pub fn sqlite_join_tables(conn: &rusqlite::Connection) {
    conn.execute(
        "CREATE TABLE a (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE b (id INTEGER NOT NULL PRIMARY KEY, a_id INTEGER, data TEXT)",
        [],
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..1_000i64 {
        conn.execute(
            "INSERT INTO a (id, val) VALUES (?1, ?2)",
            rusqlite::params![i, format!("a_{i}")],
        )
        .unwrap();
    }
    for i in 0..1_000i64 {
        conn.execute(
            "INSERT INTO b (id, a_id, data) VALUES (?1, ?2, ?3)",
            rusqlite::params![i, i % 1_000, format!("b_{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

pub fn sqlite_collect_stmt(stmt: &mut rusqlite::Statement<'_>) -> Vec<Vec<rusqlite::types::Value>> {
    let col_count = stmt.column_count();
    stmt.query_map([], |row| {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            vals.push(
                row.get::<_, rusqlite::types::Value>(i)
                    .unwrap_or(rusqlite::types::Value::Null),
            );
        }
        Ok(vals)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

pub const DATE_ROWS: i64 = 100_000;

pub fn citadel_date_table(conn: &Connection) {
    conn.execute("CREATE TABLE events (id INTEGER NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..DATE_ROWS {
        let day_of_year = (i % 365) + 1;
        let month = (day_of_year / 31) + 1;
        let day = (day_of_year % 28) + 1;
        let hour = i % 24;
        let sql = format!(
            "INSERT INTO events VALUES ({i}, DATE '2024-{:02}-{:02}', TIMESTAMP '2024-{:02}-{:02} {:02}:00:00')",
            month, day, month, day, hour
        );
        conn.execute(&sql).unwrap();
    }
    conn.execute("COMMIT").unwrap();
}
