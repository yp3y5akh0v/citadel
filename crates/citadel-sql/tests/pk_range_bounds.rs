use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn database(first: i64) -> citadel::Database {
    let db = DatabaseBuilder::new("")
        .passphrase(b"range-bound-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, marker INTEGER NOT NULL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn.prepare("INSERT INTO items VALUES ($1, 0)").unwrap();
    for id in first..first + 1_024 {
        insert.execute(&[Value::Integer(id)]).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    drop(insert);
    drop(conn);
    db
}

fn select_page(
    db: &citadel::Database,
    where_and_order: &str,
    params: &[Value],
    expected: &[i64],
    scanned: u64,
) {
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        for prepared in [false, true] {
            let conn = Connection::open(db).unwrap();
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            let sql = format!("SELECT id FROM items WHERE {where_and_order} LIMIT 8");
            let statement = prepared.then(|| conn.prepare(&sql).unwrap());
            let measurement = db.measure_scans();
            let rows = match &statement {
                Some(statement) => statement.query_collect(params).unwrap().rows,
                None => conn.query_params(&sql, params).unwrap().rows,
            };
            let expected: Vec<_> = expected
                .iter()
                .map(|&id| vec![Value::Integer(id)])
                .collect();
            assert_eq!(
                rows, expected,
                "{where_and_order}, {begin:?}, prepared={prepared}"
            );
            assert_eq!(
                measurement.rows_scanned(),
                scanned,
                "{where_and_order}, {begin:?}, prepared={prepared}"
            );
            drop(measurement);
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn primary_key_pages_seek_the_strongest_lower_bound_in_either_conjunct_order() {
    let db = database(0);
    for predicate in ["id >= 0 AND id > 900", "id > 900 AND id >= 0"] {
        select_page(&db, predicate, &[], &(901..909).collect::<Vec<_>>(), 9);
    }
    for predicate in ["id > 0 AND id >= 900", "id >= 900 AND id > 0"] {
        select_page(&db, predicate, &[], &(900..908).collect::<Vec<_>>(), 8);
    }
    select_page(
        &db,
        "id >= 900 AND id > 900",
        &[],
        &(901..909).collect::<Vec<_>>(),
        9,
    );
}

#[test]
fn ordered_primary_key_pages_preserve_ascending_and_descending_results() {
    let db = database(0);
    select_page(
        &db,
        "id >= 0 AND id > 900 ORDER BY id",
        &[],
        &(901..909).collect::<Vec<_>>(),
        9,
    );
    select_page(
        &db,
        "id >= 0 AND id > 900 ORDER BY id DESC",
        &[],
        &(1_016..1_024).rev().collect::<Vec<_>>(),
        124,
    );
}

#[test]
fn contradictory_primary_key_bounds_stop_at_the_range_boundary() {
    let db = database(0);
    for (predicate, scanned) in [
        ("id >= 0 AND id > 900 AND id <= 900", 2),
        ("id <= 900 AND id > 900 AND id >= 0", 2),
        ("id >= 0 AND id >= 900 AND id < 900", 1),
    ] {
        select_page(&db, predicate, &[], &[], scanned);
    }
}

#[test]
fn parameterized_primary_key_pages_seek_negative_and_large_integer_bounds() {
    for first in [-1_024, 1i64 << 53, i64::MAX - 1_024] {
        let db = database(first);
        let params = [Value::Integer(first), Value::Integer(first + 900)];
        for predicate in ["id >= $1 AND id > $2", "id > $2 AND id >= $1"] {
            select_page(
                &db,
                predicate,
                &params,
                &(first + 901..first + 909).collect::<Vec<_>>(),
                9,
            );
        }
    }
}

#[test]
fn binary_text_primary_key_pages_seek_the_strongest_lower_bound() {
    let db = DatabaseBuilder::new("")
        .passphrase(b"range-bound-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, marker INTEGER)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..1_024 {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, $2)",
            &[
                Value::Text(format!("key-{id:04}").into()),
                Value::Integer(id),
            ],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    for predicate in [
        "id >= 'key-0000' AND id > 'key-0900'",
        "id > 'key-0900' AND id >= 'key-0000'",
    ] {
        let conn = Connection::open(&db).unwrap();
        let statement = conn
            .prepare(&format!(
                "SELECT marker FROM items WHERE {predicate} LIMIT 8"
            ))
            .unwrap();
        let measurement = db.measure_scans();
        assert_eq!(
            statement.query_collect(&[]).unwrap().rows,
            (901..909)
                .map(|id| vec![Value::Integer(id)])
                .collect::<Vec<_>>()
        );
        assert_eq!(measurement.rows_scanned(), 9);
    }
}

fn execute(conn: &Connection<'_>, sql: &str, prepared: bool) -> u64 {
    if prepared {
        conn.prepare(sql).unwrap().execute(&[]).unwrap()
    } else {
        match conn.execute(sql).unwrap() {
            ExecutionResult::RowsAffected(count) => count,
            other => panic!("expected affected row count: {other:?}"),
        }
    }
}

#[test]
fn primary_key_update_and_delete_seek_tightly_and_preserve_rollback() {
    for prepared in [false, true] {
        for operation in ["UPDATE items SET marker = 1", "DELETE FROM items"] {
            for predicate in [
                "id >= 0 AND id > 900 AND id <= 904",
                "id <= 904 AND id > 900 AND id >= 0",
                "id > 0 AND id >= 901 AND id < 905",
            ] {
                let db = database(0);
                let conn = Connection::open(&db).unwrap();
                let original = conn
                    .query("SELECT id, marker FROM items ORDER BY id")
                    .unwrap();
                conn.execute("BEGIN").unwrap();
                conn.execute("INSERT INTO items VALUES (2000, 2)").unwrap();
                conn.execute("SAVEPOINT before_mutation").unwrap();
                let before = conn
                    .query("SELECT id, marker FROM items ORDER BY id")
                    .unwrap();
                let sql = format!("{operation} WHERE {predicate}");
                let measurement = db.measure_scans();
                assert_eq!(execute(&conn, &sql, prepared), 4);
                let expected_scans = if predicate.contains("id >= 901") {
                    5
                } else {
                    6
                };
                assert_eq!(measurement.rows_scanned(), expected_scans, "{sql}");
                drop(measurement);

                let mut expected = before.rows.clone();
                if operation.starts_with("UPDATE") {
                    for row in &mut expected {
                        if matches!(row[0], Value::Integer(901..=904)) {
                            row[1] = Value::Integer(1);
                        }
                    }
                } else {
                    expected.retain(|row| !matches!(row[0], Value::Integer(901..=904)));
                }
                assert_eq!(
                    conn.query("SELECT id, marker FROM items ORDER BY id")
                        .unwrap()
                        .rows,
                    expected
                );
                conn.execute("ROLLBACK TO before_mutation").unwrap();
                assert_eq!(
                    conn.query("SELECT id, marker FROM items ORDER BY id")
                        .unwrap()
                        .rows,
                    before.rows
                );
                conn.execute("ROLLBACK").unwrap();
                assert_eq!(
                    conn.query("SELECT id, marker FROM items ORDER BY id")
                        .unwrap()
                        .rows,
                    original.rows
                );
                let measurement = db.measure_scans();
                assert_eq!(execute(&conn, &sql, prepared), 4);
                assert_eq!(
                    measurement.rows_scanned(),
                    expected_scans,
                    "autocommit {sql}"
                );
            }
        }
    }
}
