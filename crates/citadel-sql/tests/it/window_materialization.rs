use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database(directory: &std::path::Path) -> citadel::Database {
    DatabaseBuilder::new(directory.join("windows.db"))
        .passphrase(b"window-materialization")
        .argon2_profile(Argon2Profile::Iot)
        .create()
        .unwrap()
}

fn seed(connection: &Connection<'_>, sqlite: &rusqlite::Connection) {
    for sql in [
        "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, \
         g TEXT COLLATE NOCASE, score INTEGER, label TEXT, v INTEGER, step INTEGER)",
        "INSERT INTO t VALUES \
         (1, 'A', 2, 'first_heap_text_value_longer_than_inline_storage', 10, 1), \
         (2, 'a', 1, 'second_heap_text_value_longer_than_inline_storage', NULL, 2), \
         (3, 'B', 1, 'third_heap_text_value_longer_than_inline_storage', 7, 0), \
         (4, 'b', NULL, NULL, 3, NULL), \
         (5, 'A', 2, 'fifth_heap_text_value_longer_than_inline_storage', -5, 1), \
         (6, NULL, 1, 'sixth_heap_text_value_longer_than_inline_storage', 20, 2), \
         (7, 'B', 1, 'seventh_heap_text_value_longer_than_inline_storage', NULL, 1)",
    ] {
        connection.execute(sql).unwrap();
        sqlite.execute_batch(sql).unwrap();
    }
}

fn assert_matches_sqlite(connection: &Connection<'_>, sqlite: &rusqlite::Connection, sql: &str) {
    let actual = connection
        .prepare(sql)
        .unwrap()
        .query_collect(&[])
        .unwrap()
        .rows;
    let mut statement = sqlite.prepare(sql).unwrap();
    let column_count = statement.column_count();
    let expected = statement
        .query_map([], |row| {
            (0..column_count)
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
    assert_eq!(actual, expected, "query: {sql}");
}

#[test]
fn mixed_window_arities_and_independent_orders_preserve_owned_values() {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    seed(&connection, &sqlite);
    // NULL offsets intentionally differ from SQLite; window_torture covers
    // that contract. Keep this allocation-focused comparison in shared SQL.
    let query = "SELECT id, \
        ROW_NUMBER() OVER (ORDER BY id), \
        COUNT(*) OVER (), \
        SUM(v) OVER (PARTITION BY g ORDER BY score NULLS FIRST, id \
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
        MIN(label) OVER (PARTITION BY g ORDER BY score NULLS FIRST, id \
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), \
        LAG(label, COALESCE(step, 1), 'default_heap_text_value_longer_than_inline_storage') \
            OVER (PARTITION BY g ORDER BY id), \
        LEAD(v, 2, -999) OVER (ORDER BY id DESC), \
        LAG(label, 1) OVER (ORDER BY id), \
        LAST_VALUE(label) OVER (ORDER BY id \
            ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) \
        FROM t ORDER BY id";
    assert_matches_sqlite(&connection, &sqlite, query);

    // Re-run after changing values used by different argument widths and
    // window orders; then cover the zero-row result path with the same query.
    for sql in [
        "UPDATE t SET label = NULL, v = 42, score = -1, step = 0 WHERE id = 2",
        "DELETE FROM t",
    ] {
        connection.execute(sql).unwrap();
        sqlite.execute_batch(sql).unwrap();
        assert_matches_sqlite(&connection, &sqlite, query);
    }
}

#[test]
fn window_key_rows_preserve_collated_partitions_null_peers_and_empty_frames() {
    let directory = tempfile::tempdir().unwrap();
    let database = database(directory.path());
    let connection = Connection::open(&database).unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    seed(&connection, &sqlite);
    assert_matches_sqlite(
        &connection,
        &sqlite,
        "SELECT id, \
         RANK() OVER (PARTITION BY g ORDER BY score NULLS FIRST), \
         DENSE_RANK() OVER (PARTITION BY g ORDER BY score DESC NULLS LAST), \
         COUNT(*) OVER (PARTITION BY g ORDER BY score NULLS FIRST \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
         FIRST_VALUE(label) OVER (PARTITION BY g ORDER BY score NULLS FIRST, id \
             ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), \
         COUNT(*) OVER (PARTITION BY g), \
         NTILE(2) OVER (ORDER BY id) \
         FROM t ORDER BY id",
    );
}
