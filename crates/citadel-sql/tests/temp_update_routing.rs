use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, ExecutionResult, Value};

fn expected_rows(values: [i64; 3]) -> Vec<Vec<Value>> {
    values
        .into_iter()
        .enumerate()
        .map(|(id, n)| vec![Value::Integer(id as i64 + 1), Value::Integer(n)])
        .collect()
}

fn check_update(shadow: bool, sql: &str, prepared: bool, affected: u64, values: [i64; 3]) {
    let db = DatabaseBuilder::new("")
        .passphrase(b"temp-update-routing-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TEMP TABLE items (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
        .unwrap();
    if shadow {
        // Creating TEMP over an existing name is rejected. A second connection
        // has no TEMP alias and may create the same user-facing base name.
        let other = Connection::open(&db).unwrap();
        other
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
            .unwrap();
        other
            .execute("INSERT INTO items VALUES (1, 100), (2, 200), (3, 300)")
            .unwrap();
    }
    // Use ordinary explicit-transaction INSERT to keep the fixture independent
    // of the separately tested compiled INSERT routing bug.
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let statement = prepared.then(|| conn.prepare(sql).unwrap());
    conn.execute("BEGIN").unwrap();
    if let Some(statement) = statement {
        let result = statement.execute(&[]);
        assert!(
            matches!(result, Ok(count) if count == affected),
            "prepared TEMP UPDATE: {sql}; shadow={shadow}: {result:?}"
        );
    } else {
        let result = conn.execute(sql);
        assert!(
            matches!(result, Ok(ExecutionResult::RowsAffected(count)) if count == affected),
            "TEMP UPDATE: {sql}; shadow={shadow}: {result:?}"
        );
    }
    conn.execute("COMMIT").unwrap();
    let temp_rows = conn
        .query("SELECT id, n FROM items ORDER BY id")
        .unwrap()
        .rows;
    if shadow {
        conn.execute("DROP TABLE items").unwrap();
        assert_eq!(
            conn.query("SELECT id, n FROM items ORDER BY id")
                .unwrap()
                .rows,
            expected_rows([100, 200, 300]),
            "TEMP UPDATE changed the shadowed base table: {sql}"
        );
    }
    assert_eq!(temp_rows, expected_rows(values), "{sql}; shadow={shadow}");
}

#[test]
fn explicit_updates_find_temp_without_a_base_table() {
    for (sql, affected, values) in [
        ("UPDATE items SET n = n + 1 WHERE id = 2", 1, [10, 21, 30]),
        ("UPDATE items SET n = n + 1 WHERE id >= 2", 2, [10, 21, 31]),
        ("UPDATE items SET n = n + 1", 3, [11, 21, 31]),
    ] {
        check_update(false, sql, false, affected, values);
    }
}

#[test]
fn explicit_point_update_preserves_temp_shadowed_base() {
    check_update(
        true,
        "UPDATE items SET n = n + 1 WHERE id = 2",
        false,
        1,
        [10, 21, 30],
    );
}

#[test]
fn explicit_range_update_preserves_temp_shadowed_base() {
    check_update(
        true,
        "UPDATE items SET n = n + 1 WHERE id >= 2",
        false,
        2,
        [10, 21, 31],
    );
}

#[test]
fn explicit_full_scan_update_preserves_temp_shadowed_base() {
    check_update(true, "UPDATE items SET n = n + 1", false, 3, [11, 21, 31]);
}

#[test]
fn prepared_update_subquery_fallback_uses_resolved_temp_storage() {
    for shadow in [true, false] {
        // The subquery excludes direct compiled UPDATE. Materialization then
        // produces a point predicate for the interpreted transaction fast path.
        check_update(
            shadow,
            "UPDATE items SET n = n + 1 WHERE id = (SELECT 2)",
            true,
            1,
            [10, 21, 30],
        );
    }
}
