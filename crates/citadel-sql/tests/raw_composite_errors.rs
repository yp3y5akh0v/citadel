use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::encoding::{
    decode_column_raw, decode_row, encode_composite_key, encode_row, RawColumn,
};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"raw-composite-error-test")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn rows(vector: bool) -> (Vec<u8>, Vec<u8>) {
    let value = if vector {
        Value::Vector(vec![7.0].into())
    } else {
        Value::Array(vec![Value::Integer(7)].into())
    };
    let valid = encode_row(&[value]);
    let mut malformed = valid.clone();
    let body_len = match decode_column_raw(&valid, 0).unwrap() {
        RawColumn::Array(body) | RawColumn::Vector(body) => body.len(),
        other => panic!("unexpected fixture: {other:?}"),
    };
    let body_start = valid.len() - body_len;
    // Advertise two elements/components while retaining the complete outer
    // row envelope and the body for one. Both old decoders allocate at most
    // two elements, so this public regression is safe to run before the fix.
    malformed[body_start] = 2;
    assert!(decode_row(&valid).is_ok());
    assert!(matches!(
        decode_row(&malformed),
        Err(SqlError::InvalidValue(_))
    ));
    (valid, malformed)
}

fn put_rows(db: &Database, valid: &[u8], malformed: &[u8]) {
    let mut txn = db.begin_write().unwrap();
    for (id, bytes) in [(1, valid), (2, malformed)] {
        txn.table_insert(
            b"items",
            &encode_composite_key(&[Value::Integer(id)]),
            bytes,
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

fn assert_error(error: SqlError, expected: &str, sql: &str) {
    assert!(matches!(error, SqlError::InvalidValue(_)), "{sql}: {error}");
    assert_eq!(error.to_string(), expected, "{sql}");
}

#[test]
fn malformed_composites_fail_raw_topk_aggregates_and_distinct() {
    for vector in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        // Non-strict storage can contain runtime types different from the
        // declared affinity. Exercise both composite tags without adding an
        // ARRAY DDL feature solely for this corruption regression.
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        let (valid, malformed) = rows(vector);
        put_rows(&db, &valid, &malformed);
        let expected = decode_row(&malformed).unwrap_err().to_string();
        let queries = [
            "SELECT id FROM items ORDER BY v ASC NULLS FIRST LIMIT 1",
            "SELECT id FROM items ORDER BY v ASC NULLS LAST LIMIT 1",
            "SELECT id FROM items ORDER BY v DESC NULLS FIRST LIMIT 1",
            "SELECT id FROM items ORDER BY v DESC NULLS LAST LIMIT 1",
            "SELECT id FROM items WHERE id = 2 ORDER BY v LIMIT 3",
            "SELECT MIN(v) FROM items",
            "SELECT MAX(v) FROM items",
            "SELECT DISTINCT v FROM items",
            "SELECT v FROM items",
        ];
        for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            for sql in queries {
                assert_error(conn.query(sql).unwrap_err(), &expected, sql);
                let prepared = conn.prepare(sql).unwrap();
                assert_error(prepared.query_collect(&[]).unwrap_err(), &expected, sql);
            }
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

fn mutation_rows(vector: bool) -> (Vec<u8>, Vec<u8>) {
    let composite = if vector {
        Value::Vector(vec![7.0].into())
    } else {
        Value::Array(vec![Value::Integer(7)].into())
    };
    let valid = encode_row(&[Value::Integer(10), Value::Blob(vec![7])]);
    let mut malformed = encode_row(&[Value::Integer(20), composite]);
    let body_len = match decode_column_raw(&malformed, 1).unwrap() {
        RawColumn::Array(body) | RawColumn::Vector(body) => body.len(),
        other => panic!("unexpected fixture: {other:?}"),
    };
    let body_start = malformed.len() - body_len;
    // Keep the corruption bounded, as in the original public regression.
    malformed[body_start] = 2;
    assert!(matches!(
        decode_row(&malformed),
        Err(SqlError::InvalidValue(_))
    ));
    (valid, malformed)
}

fn assert_mutation_rows(db: &Database, counter: i64, malformed: &[u8]) {
    let expected_first = encode_row(&[Value::Integer(counter), Value::Blob(vec![7])]);
    let mut txn = db.begin_read();
    for (id, expected_bytes) in [(1, expected_first.as_slice()), (2, malformed)] {
        assert_eq!(
            txn.table_get(b"items", &encode_composite_key(&[Value::Integer(id)]))
                .unwrap()
                .as_deref(),
            Some(expected_bytes),
            "unexpected stored bytes for row {id}"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum MutationTransaction {
    Autocommit,
    Explicit,
    Savepoint,
}

#[test]
fn malformed_composites_fail_updates_and_preserve_transaction_failure_semantics() {
    for vector in [false, true] {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, v BLOB)")
            .unwrap();
        let (valid, malformed) = mutation_rows(vector);
        let expected = decode_row(&malformed).unwrap_err().to_string();
        for (sql, partial_range) in [
            ("UPDATE items SET n = n + 1, v = v WHERE id = 2", false),
            ("UPDATE items SET n = n + 1, v = v WHERE id >= 1", false),
            // Only the non-null fixed-width target is patched. The extra
            // composite RHS input is decoded inside each range callback.
            (
                "UPDATE items SET n = n + CASE WHEN v IS NULL THEN 0 ELSE 1 END WHERE id >= 1",
                true,
            ),
        ] {
            for prepared in [false, true] {
                for transaction in [
                    MutationTransaction::Autocommit,
                    MutationTransaction::Explicit,
                    MutationTransaction::Savepoint,
                ] {
                    put_rows(&db, &valid, &malformed);
                    if !matches!(transaction, MutationTransaction::Autocommit) {
                        conn.execute("BEGIN").unwrap();
                        // This earlier statement must survive a failed point
                        // update, or a rollback to the later savepoint.
                        conn.execute("UPDATE items SET n = n + 10 WHERE id = 1")
                            .unwrap();
                    }
                    if matches!(transaction, MutationTransaction::Savepoint) {
                        conn.execute("SAVEPOINT before_bad_update").unwrap();
                    }
                    let error = if prepared {
                        conn.prepare(sql).unwrap().execute(&[]).unwrap_err()
                    } else {
                        conn.execute(sql).unwrap_err()
                    };
                    assert_error(error, &expected, sql);
                    assert_eq!(
                        conn.in_transaction(),
                        !matches!(transaction, MutationTransaction::Autocommit)
                    );

                    let expected_counter = match transaction {
                        MutationTransaction::Autocommit => 10,
                        MutationTransaction::Explicit if partial_range => {
                            // The range changed row1 before rejecting row2.
                            // A partial explicit statement poisons the txn;
                            // a refused COMMIT ends it and drops all writes.
                            let commit = conn.execute("COMMIT");
                            assert!(
                                matches!(
                                    commit,
                                    Err(SqlError::Storage(citadel_core::Error::TransactionFailed))
                                ),
                                "{sql}, prepared={prepared}, vector={vector}: {commit:?}"
                            );
                            10
                        }
                        MutationTransaction::Explicit => {
                            // Point updates decode before staging; the BLOB-target
                            // range buffers every replacement before applying any.
                            // Neither failed path changes stored rows, so earlier
                            // successful work remains committable.
                            conn.execute("COMMIT").unwrap();
                            20
                        }
                        MutationTransaction::Savepoint => {
                            conn.execute("ROLLBACK TO before_bad_update").unwrap();
                            assert_eq!(
                                conn.query("SELECT n FROM items WHERE id = 1").unwrap().rows,
                                vec![vec![Value::Integer(20)]],
                                "range prefix survived savepoint rollback: {sql}"
                            );
                            // Prove recovery clears failure and permits fresh
                            // writes, while preserving the pre-savepoint +10.
                            conn.execute("UPDATE items SET n = n + 100 WHERE id = 1")
                                .unwrap();
                            conn.execute("COMMIT").unwrap();
                            120
                        }
                    };
                    assert!(!conn.in_transaction());
                    assert_mutation_rows(&db, expected_counter, &malformed);
                }
            }
        }
    }
}

#[test]
fn malformed_vectors_fail_ann_build_instead_of_becoming_null_entries() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, v VECTOR(1))")
        .unwrap();
    conn.execute("CREATE INDEX items_v ON items USING ann (v) WITH (metric = 'l2')")
        .unwrap();
    let (valid, malformed) = rows(true);
    put_rows(&db, &valid, &malformed);
    let expected = decode_row(&malformed).unwrap_err().to_string();
    let sql = "SELECT id FROM items ORDER BY v <-> '[0.0]'::VECTOR(1) LIMIT 1";
    assert_error(conn.query(sql).unwrap_err(), &expected, sql);
    assert_error(
        conn.prepare(sql).unwrap().query_collect(&[]).unwrap_err(),
        &expected,
        sql,
    );
}
