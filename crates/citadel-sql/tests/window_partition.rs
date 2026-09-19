use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database(directory: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(directory.join("partition.db"))
        .passphrase(b"whole-partition")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn matches_sqlite(connection: &Connection<'_>, sqlite: &rusqlite::Connection, sql: &str) {
    let actual = connection
        .prepare(sql)
        .unwrap()
        .query_collect(&[])
        .unwrap()
        .rows;
    let mut statement = sqlite.prepare(sql).unwrap();
    let width = statement.column_count();
    let expected = statement
        .query_map([], |row| {
            (0..width)
                .map(|index| {
                    use rusqlite::types::ValueRef;
                    Ok(match row.get_ref(index)? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(value) => Value::Integer(value),
                        ValueRef::Real(value) => Value::Real(value),
                        ValueRef::Text(value) => {
                            Value::Text(std::str::from_utf8(value).unwrap().into())
                        }
                        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(actual, expected, "{sql}");
}

#[test]
fn full_and_changing_partition_frames_match_sqlite_with_nulls_and_collations() {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, g TEXT COLLATE NOCASE, \
         v INTEGER, label TEXT COLLATE NOCASE)",
        "INSERT INTO t VALUES \
         (1, 'A', NULL, NULL), (2, 'A', 7, 'a_long_owned_text_value_for_collation'), \
         (3, 'a', -3, 'A_long_owned_text_value_for_collation'), \
         (4, 'B', NULL, NULL), (5, 'b', NULL, NULL), \
         (6, 'C', 9, 'z_long_owned_text_value_for_collation'), \
         (7, NULL, 1, 'null_partition')",
    ] {
        connection.execute(sql).unwrap();
        sqlite.execute_batch(sql).unwrap();
    }
    let full = "SELECT id, \
        SUM(v) OVER (PARTITION BY g), COUNT(*) OVER (PARTITION BY g), \
        COUNT(v) OVER (PARTITION BY g), AVG(v) OVER (PARTITION BY g), \
        MIN(label) OVER (PARTITION BY g ORDER BY id DESC \
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), \
        MAX(label) OVER (PARTITION BY g ORDER BY id \
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
        FROM t ORDER BY id";
    matches_sqlite(&connection, &sqlite, full);
    // SQLite can choose a different spelling for NOCASE ties in moving
    // extrema. Use binary ordering here; unit tests preserve our exact ties.
    matches_sqlite(
        &connection,
        &sqlite,
        "SELECT id, \
         SUM(v) OVER (PARTITION BY g ORDER BY id), \
         COUNT(*) OVER (PARTITION BY g ORDER BY v \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
         AVG(v) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), \
         MIN(label) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), \
         MAX(label COLLATE BINARY) OVER (PARTITION BY g ORDER BY id \
             RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
         FROM t ORDER BY id",
    );
    connection.execute("DELETE FROM t").unwrap();
    sqlite.execute_batch("DELETE FROM t").unwrap();
    matches_sqlite(&connection, &sqlite, full);
}

#[test]
fn moving_real_sum_and_avg_match_sqlite_after_large_values_expire() {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v REAL)",
        "INSERT INTO t VALUES \
         (1, 'a', 1e20), (2, 'a', 1.0), (3, 'a', 1.0), (4, 'a', 2.0), \
         (5, 'a', NULL), (6, 'a', 4.0), \
         (7, 'b', -1e20), (8, 'b', -1.0), (9, 'b', -1.0), (10, 'b', -2.0)",
    ] {
        connection.execute(sql).unwrap();
        sqlite.execute_batch(sql).unwrap();
    }
    for frame in ["1 PRECEDING AND CURRENT ROW", "1 PRECEDING AND 1 FOLLOWING"] {
        let spec = format!("PARTITION BY g ORDER BY id ROWS BETWEEN {frame}");
        matches_sqlite(
            &connection,
            &sqlite,
            &format!("SELECT id, SUM(v) OVER ({spec}), AVG(v) OVER ({spec}) FROM t ORDER BY id"),
        );
    }
}
