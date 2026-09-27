use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"correlated-partial-decode")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn assert_filter(
    connection: &Connection<'_>,
    projection: &str,
    predicate: &str,
    expected: Vec<Vec<Value>>,
) {
    let sql =
        format!("SELECT {projection} FROM outer_rows o WHERE {predicate} ORDER BY {projection}");
    // COALESCE leaves the correlated predicate in the generic row evaluator,
    // instead of the top-level IN/EXISTS partial-decode lane.
    let generic = format!(
        "SELECT {projection} FROM outer_rows o WHERE COALESCE({predicate}, FALSE) ORDER BY {projection}"
    );
    for mode in ["", "BEGIN READ ONLY", "BEGIN"] {
        if !mode.is_empty() {
            connection.execute(mode).unwrap();
        }
        assert_eq!(connection.query(&generic).unwrap().rows, expected);
        assert_eq!(
            connection.query(&sql).unwrap().rows,
            expected,
            "{sql}, {mode}"
        );
        assert_eq!(
            connection
                .prepare(&sql)
                .unwrap()
                .query_collect(&[])
                .unwrap()
                .rows,
            expected,
            "prepared {sql}, {mode}"
        );
        if !mode.is_empty() {
            connection.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn correlated_filters_decode_text_primary_keys() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (code TEXT PRIMARY KEY, probe INTEGER)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, owner TEXT, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES ('a', 10), ('b', 20), ('c', 30)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 'a', 10), (2, 'b', 99)")
        .unwrap();
    for predicate in [
        "o.probe IN (SELECT i.v FROM inner_rows i WHERE i.owner = o.code)",
        "EXISTS (SELECT 1 FROM inner_rows i WHERE i.owner = o.code AND i.v = o.probe)",
    ] {
        assert_filter(
            &connection,
            "o.code",
            predicate,
            vec![vec![Value::Text("a".into())]],
        );
    }
}

#[test]
fn correlated_filters_materialize_added_column_defaults() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY, probe INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES (1, 10), (2, 20)")
        .unwrap();
    connection
        .execute("ALTER TABLE outer_rows ADD COLUMN bucket INTEGER DEFAULT 7")
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES (3, 30, NULL), (4, 40, 9)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, bucket INTEGER, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 7, 10), (2, 7, 20), (3, 9, 99)")
        .unwrap();
    for predicate in [
        "o.probe IN (SELECT i.v FROM inner_rows i WHERE i.bucket = o.bucket)",
        "EXISTS (SELECT 1 FROM inner_rows i WHERE i.bucket = o.bucket AND i.v = o.probe)",
    ] {
        assert_filter(
            &connection,
            "o.id",
            predicate,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        );
    }
}

#[test]
fn correlated_filters_materialize_virtual_correlation_keys() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection.execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY, base INTEGER, probe INTEGER, bucket INTEGER GENERATED ALWAYS AS (base * 10) VIRTUAL)").unwrap();
    connection
        .execute(
            "INSERT INTO outer_rows (id, base, probe) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)",
        )
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, bucket INTEGER, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 10, 10), (2, 20, 20), (3, 30, 99)")
        .unwrap();
    for predicate in [
        "o.probe IN (SELECT i.v FROM inner_rows i WHERE i.bucket = o.bucket)",
        "EXISTS (SELECT 1 FROM inner_rows i WHERE i.bucket = o.bucket AND i.v = o.probe)",
    ] {
        assert_filter(
            &connection,
            "o.id",
            predicate,
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        );
    }
}

#[test]
fn correlated_filters_preserve_composite_primary_key_positions() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection.execute("CREATE TABLE outer_rows (payload TEXT, code TEXT, seq INTEGER, probe INTEGER, PRIMARY KEY (code, seq))").unwrap();
    connection.execute("INSERT INTO outer_rows VALUES ('one', 'a', 1, 10), ('two', 'a', 2, 20), ('three', 'b', 1, 30)").unwrap();
    connection
        .execute(
            "CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, owner TEXT, seq INTEGER, v INTEGER)",
        )
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 'a', 1, 10), (2, 'a', 2, 99), (3, 'b', 1, 30)")
        .unwrap();
    for predicate in [
        "o.probe IN (SELECT i.v FROM inner_rows i WHERE i.owner = o.code AND i.seq = o.seq)",
        "EXISTS (SELECT 1 FROM inner_rows i WHERE i.owner = o.code AND i.seq = o.seq AND i.v = o.probe)",
    ] {
        assert_filter(&connection, "o.code, o.seq", predicate, vec![vec![Value::Text("a".into()), Value::Integer(1)], vec![Value::Text("b".into()), Value::Integer(1)]]);
    }
}

#[test]
fn correlated_in_stored_operand_preserves_collation_and_dropped_slot_mapping() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection.execute("CREATE TABLE outer_rows (discarded TEXT, code TEXT COLLATE NOCASE PRIMARY KEY, probe TEXT COLLATE RTRIM)").unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES ('unused', 'A', 'hit '), ('unused', 'b', 'miss')")
        .unwrap();
    connection
        .execute(
            "CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, owner TEXT COLLATE NOCASE, v TEXT)",
        )
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 'a', 'hit'), (2, 'b', 'other')")
        .unwrap();
    for dropped in [false, true] {
        if dropped {
            connection
                .execute("ALTER TABLE outer_rows DROP COLUMN discarded")
                .unwrap();
        }
        for operand in ["probe", "o.probe"] {
            assert_filter(
                &connection,
                "o.code",
                &format!("{operand} IN (SELECT i.v FROM inner_rows i WHERE i.owner = o.code)"),
                vec![vec![Value::Text("A".into())]],
            );
        }
    }
}

#[test]
fn empty_correlated_rhs_does_not_materialize_failing_operand_columns() {
    for definition in [
        "probe INTEGER DEFAULT (1 / 0)",
        "probe INTEGER GENERATED ALWAYS AS (id / 0) VIRTUAL",
    ] {
        let db = database();
        let connection = Connection::open(&db).unwrap();
        connection
            .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)")
            .unwrap();
        connection
            .execute("INSERT INTO outer_rows VALUES (1)")
            .unwrap();
        connection
            .execute(&format!("ALTER TABLE outer_rows ADD COLUMN {definition}"))
            .unwrap();
        connection
            .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, owner INTEGER, v INTEGER)")
            .unwrap();
        let sql = "SELECT o.id FROM outer_rows o WHERE o.probe IN (SELECT i.v FROM inner_rows i WHERE i.owner = o.id)";
        for mode in ["", "BEGIN READ ONLY"] {
            if !mode.is_empty() {
                connection.execute(mode).unwrap();
            }
            assert!(
                connection.query(sql).unwrap().rows.is_empty(),
                "{definition}"
            );
            assert!(
                connection
                    .prepare(sql)
                    .unwrap()
                    .query_collect(&[])
                    .unwrap()
                    .rows
                    .is_empty(),
                "prepared {definition}"
            );
            if !mode.is_empty() {
                connection.execute("ROLLBACK").unwrap();
            }
        }
        connection
            .execute("INSERT INTO inner_rows VALUES (1, 1, 1)")
            .unwrap();
        assert!(
            matches!(
                connection.query(sql),
                Err(citadel_sql::SqlError::DivisionByZero)
            ),
            "a nonempty RHS must demand {definition}"
        );
    }
}

#[test]
fn correlated_key_completion_reuses_a_volatile_missing_default() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute("CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)")
        .unwrap();
    for id in 0..64 {
        connection
            .execute_params("INSERT INTO outer_rows VALUES ($1)", &[Value::Integer(id)])
            .unwrap();
    }
    connection
        .execute("ALTER TABLE outer_rows ADD COLUMN token INTEGER DEFAULT (RANDOM() % 3)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, bucket INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, -2), (2, -1), (3, 0), (4, 1), (5, 2)")
        .unwrap();
    // Every possible token has exactly one matching group and selected value.
    // Completing the row must retain the token already used as its key.
    let sql = "SELECT o.id, o.token FROM outer_rows o WHERE o.token IN (SELECT i.bucket FROM inner_rows i WHERE i.bucket = o.token) ORDER BY o.id";
    for mode in ["", "BEGIN READ ONLY"] {
        if !mode.is_empty() {
            connection.execute(mode).unwrap();
        }
        let result = connection.query(sql).unwrap();
        assert_eq!(result.rows.len(), 64);
        for (id, row) in result.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Integer(id as i64));
            assert!(matches!(row[1], Value::Integer(-2..=2)));
        }
        if !mode.is_empty() {
            connection.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn correlated_in_stored_operand_uses_the_qualified_resolver() {
    let db = database();
    let connection = Connection::open(&db).unwrap();
    connection
        .execute(
            "CREATE TABLE outer_rows (id INTEGER PRIMARY KEY, probe INTEGER, \"o.probe\" INTEGER)",
        )
        .unwrap();
    connection
        .execute("INSERT INTO outer_rows VALUES (1, 0, 7), (2, 7, 0)")
        .unwrap();
    connection
        .execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY, owner INTEGER, v INTEGER)")
        .unwrap();
    connection
        .execute("INSERT INTO inner_rows VALUES (1, 1, 7), (2, 2, 7)")
        .unwrap();
    for mode in ["", "BEGIN READ ONLY", "BEGIN"] {
        if !mode.is_empty() {
            connection.execute(mode).unwrap();
        }
        for operator in ["IN", "NOT IN"] {
            let predicate =
                format!("o.probe {operator} (SELECT i.v FROM inner_rows i WHERE i.owner = o.id)");
            let generic = format!(
                "SELECT o.id FROM outer_rows o WHERE COALESCE({predicate}, FALSE) ORDER BY o.id"
            );
            let optimized =
                format!("SELECT o.id FROM outer_rows o WHERE {predicate} ORDER BY o.id");
            let expected = connection.query(&generic).unwrap().rows;
            assert_eq!(expected.len(), 1, "the resolver comparison is not vacuous");
            assert_eq!(connection.query(&optimized).unwrap().rows, expected);
            assert_eq!(
                connection
                    .prepare(&optimized)
                    .unwrap()
                    .query_collect(&[])
                    .unwrap()
                    .rows,
                expected
            );
        }
        if !mode.is_empty() {
            connection.execute("ROLLBACK").unwrap();
        }
    }
}
