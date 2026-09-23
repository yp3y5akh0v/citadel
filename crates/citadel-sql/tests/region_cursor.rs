use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"region-cursor-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn setup(conn: &Connection<'_>) {
    conn.execute("CREATE TABLE atoms (id INTEGER PRIMARY KEY, region INTEGER, flag INTEGER, content TEXT, expires INTEGER)").unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn
        .prepare("INSERT INTO atoms VALUES ($1, $2, $3, 'content', $4)")
        .unwrap();
    for id in 0..2048 {
        insert
            .execute(&[
                Value::Integer(id),
                if id % 32 == 31 {
                    Value::Null
                } else {
                    Value::Integer(id % 32)
                },
                Value::Integer((id / 32) % 3),
                if id % 5 == 0 {
                    Value::Integer(1)
                } else {
                    Value::Null
                },
            ])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    // A higher equality score must not hide the index that also covers the ID range.
    conn.execute("CREATE INDEX unrelated ON atoms (region, flag, expires)")
        .unwrap();
    conn.execute("CREATE INDEX region_id ON atoms (region, id)")
        .unwrap();
}

#[test]
fn ordered_region_cursor_filters_before_limit_in_every_transaction_mode() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    let sql = "SELECT id, content FROM atoms WHERE region = $1 AND id > $2 AND id < 1900 AND flag = 2 AND (expires IS NULL OR expires > 2) ORDER BY id LIMIT $3 OFFSET $4";
    let expected: Vec<_> = (0..1900)
        .filter(|id| id % 32 == 3 && *id > 100 && (id / 32) % 3 == 2 && id % 5 != 0)
        .skip(2)
        .take(4)
        .map(|id| vec![Value::Integer(id), Value::Text("content".into())])
        .collect();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        for prepared in [false, true] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            let measured = db.measure_scans();
            let params = [
                Value::Integer(3),
                Value::Integer(100),
                Value::Integer(4),
                Value::Integer(2),
            ];
            let actual = if prepared {
                conn.prepare(sql)
                    .unwrap()
                    .query_collect(&params)
                    .unwrap()
                    .rows
            } else {
                conn.query_params(sql, &params).unwrap().rows
            };
            assert_eq!(actual, expected, "{begin:?}, prepared={prepared}");
            assert!(
                measured.rows_scanned() < 100,
                "{} scanned for {begin:?}, prepared={prepared}",
                measured.rows_scanned()
            );
            drop(measured);
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn region_range_bounds_order_and_nulls_match_primary_scan() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    for predicate in [
        "region = 3 AND id > 100 AND id < 700",
        "region = 3 AND id >= 99 AND id >= 99 AND id <= 611",
        "region = 3 AND id > 700 AND id < 100",
        "region = 3 AND id BETWEEN 99 AND 611",
        "region = NULL AND id > 0",
        "region IS NULL AND id > 0",
        "region = 3 AND id > NULL",
    ] {
        for suffix in [
            "ORDER BY id LIMIT 5 OFFSET 2",
            "ORDER BY id DESC LIMIT 5",
            "ORDER BY flag, id LIMIT 5",
            "ORDER BY id LIMIT 0",
        ] {
            let query = format!("SELECT id, content FROM atoms WHERE {predicate} {suffix}");
            let indexed = conn.query(&query).unwrap().rows;
            conn.execute("DROP INDEX region_id").unwrap();
            let primary = conn.query(&query).unwrap().rows;
            conn.execute("CREATE INDEX region_id ON atoms (region, id)")
                .unwrap();
            assert_eq!(indexed, primary, "{query}");
        }
    }
}

#[test]
fn small_region_scan_work_is_independent_of_unrelated_table_suffix() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE atoms (id INTEGER PRIMARY KEY, region INTEGER, content TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..2048 {
        conn.execute_params(
            "INSERT INTO atoms VALUES ($1, $2, 'value')",
            &[Value::Integer(id), Value::Integer(i64::from(id >= 8))],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let sql = "SELECT id, content FROM atoms WHERE region = 0 AND id > -1 ORDER BY id LIMIT 256";
    let measured = db.measure_scans();
    let expected = conn.query(sql).unwrap().rows;
    let primary_work = measured.rows_scanned();
    drop(measured);
    conn.execute("CREATE INDEX region_id ON atoms (region, id)")
        .unwrap();
    let measured = db.measure_scans();
    assert_eq!(conn.query(sql).unwrap().rows, expected);
    let indexed_work = measured.rows_scanned();
    assert!(primary_work >= 2048);
    assert!(indexed_work <= 20, "{indexed_work} scanned index entries");
    assert!(primary_work > indexed_work * 100);
    eprintln!("small-region scan entries: primary={primary_work}, composite={indexed_work} (point lookups excluded)");
    drop(measured);
    let measured = db.measure_scans();
    assert!(conn
        .query("SELECT id, content FROM atoms WHERE region = 0 AND id > 7 ORDER BY id LIMIT 256")
        .unwrap()
        .rows
        .is_empty());
    assert!(measured.rows_scanned() <= 3);
}

#[test]
fn cursor_reads_uncommitted_rows_from_the_same_write_transaction() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO atoms VALUES (3000, 3, 2, 'pending', NULL)")
        .unwrap();
    assert_eq!(
        conn.query(
            "SELECT id, content FROM atoms WHERE region = 3 AND id > 2048 ORDER BY id LIMIT 1"
        )
        .unwrap()
        .rows,
        vec![vec![Value::Integer(3000), Value::Text("pending".into())]]
    );
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn index_batches_resume_after_256_keys_without_skips_or_duplicates() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, region INTEGER, accepted INTEGER, body TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX region_id ON items (region, id)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..1200 {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, $2, $3, 'body')",
            &[
                Value::Integer(id),
                Value::Integer(id % 2),
                Value::Integer(id % 7),
            ],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let expected: Vec<_> = (0..1200)
        .filter(|id| id % 2 == 0 && id % 7 != 0)
        .skip(19)
        .take(300)
        .map(|id| vec![Value::Integer(id), Value::Text("body".into())])
        .collect();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        assert_eq!(conn.query("SELECT id, body FROM items WHERE region = 0 AND id >= 0 AND accepted != 0 ORDER BY id LIMIT 300 OFFSET 19").unwrap().rows, expected);
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn unproven_index_order_does_not_stop_before_sorting() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    setup(&conn);
    conn.execute("DROP INDEX region_id").unwrap();
    let expected: Vec<_> = [3, 35, 67, 99]
        .into_iter()
        .map(|id| vec![Value::Integer(id), Value::Text("content".into())])
        .collect();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        assert_eq!(
            conn.query("SELECT id, content FROM atoms WHERE region = 3 ORDER BY id LIMIT 4")
                .unwrap()
                .rows,
            expected
        );
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn narrower_index_proof_rejects_unproven_definitions() {
    use citadel_sql::planner::{plan_select, ScanPlan};
    for definition in [
        "(region, id) WHERE flag = 1",
        "(region, id COLLATE NOCASE)",
        "(region, CAST(id AS INTEGER))",
    ] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, region INTEGER, flag INTEGER)")
            .unwrap();
        conn.execute(&format!("CREATE INDEX candidate ON items {definition}"))
            .unwrap();
        let predicate = citadel_sql::parser::parse_sql_expr("region = 2 AND id > 'a'").unwrap();
        let schema = conn.table_schema("items").unwrap();
        assert!(
            matches!(
                plan_select(&schema, &Some(predicate)),
                ScanPlan::PkRangeScan { .. }
            ),
            "{definition}"
        );
    }
}

#[test]
fn sparse_residual_with_limit_one_does_not_reseek_for_every_rejected_row() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, region INTEGER, accepted INTEGER, body TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX region_id ON items (region, id)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 0..1024 {
        conn.execute_params(
            "INSERT INTO items VALUES ($1, 0, $1, 'body')",
            &[Value::Integer(id)],
        )
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        let measured = db.measure_scans();
        assert_eq!(conn.query("SELECT id, body FROM items WHERE region = 0 AND id >= 0 AND accepted = 1023 ORDER BY id LIMIT 1").unwrap().rows, vec![vec![Value::Integer(1023), Value::Text("body".into())]]);
        assert!(
            measured.rows_scanned() <= 1100,
            "{} rows for {begin:?}",
            measured.rows_scanned()
        );
        drop(measured);
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}
