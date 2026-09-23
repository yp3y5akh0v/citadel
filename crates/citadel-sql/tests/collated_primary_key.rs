use citadel::{Argon2Profile, Database, DatabaseBuilder};
use citadel_sql::{Connection, Value};

fn database() -> Database {
    DatabaseBuilder::new("")
        .passphrase(b"collated-primary-key")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn collated_primary_key_equality_and_ranges_preserve_matches() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE NOCASE PRIMARY KEY, payload INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A', 1), ('b', 2), ('Z', 3)")
        .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for (predicate, expected) in [
            ("name = 'a'", vec![1]),
            ("'a' = name", vec![1]),
            ("name > 'a'", vec![2, 3]),
            ("name >= 'a' AND name < 'z'", vec![1, 2]),
            ("name BETWEEN 'a' AND 'z'", vec![1, 2, 3]),
        ] {
            let expected: Vec<_> = expected
                .into_iter()
                .map(|v| vec![Value::Integer(v)])
                .collect();
            for suffix in ["", " ORDER BY payload"] {
                let sql = format!("SELECT payload FROM names WHERE {predicate}{suffix}");
                let mut rows = conn.query(&sql).unwrap().rows;
                rows.sort_by_key(|r| match r[0] {
                    Value::Integer(v) => v,
                    _ => unreachable!(),
                });
                assert_eq!(rows, expected, "{begin:?}: {sql}");
            }
        }
        for predicate in ["name = $1", "$1 = name"] {
            let sql = format!("SELECT payload FROM names WHERE {predicate}");
            let statement = conn.prepare(&sql).unwrap();
            assert_eq!(
                statement
                    .query_collect(&[Value::Text("a".into())])
                    .unwrap()
                    .rows,
                vec![vec![Value::Integer(1)]],
                "{begin:?}: {sql}"
            );
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn composite_collated_primary_key_uses_comparison_semantics() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (tenant INTEGER, name TEXT COLLATE NOCASE, payload INTEGER, PRIMARY KEY (tenant, name))").unwrap();
    conn.execute("INSERT INTO names VALUES (1, 'A', 1), (2, 'A', 2)")
        .unwrap();
    for sql in [
        "SELECT payload FROM names WHERE tenant = 1 AND name = 'a'",
        "SELECT payload FROM names WHERE tenant = 1 AND name = 'a' ORDER BY payload",
    ] {
        assert_eq!(
            conn.query(sql).unwrap().rows,
            vec![vec![Value::Integer(1)]],
            "{sql}"
        );
    }
    assert_eq!(
        conn.prepare("SELECT payload FROM names WHERE tenant = $1 AND name = $2")
            .unwrap()
            .query_collect(&[Value::Integer(1), Value::Text("a".into())])
            .unwrap()
            .rows,
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn collated_primary_key_update_and_delete_find_equivalent_text() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE NOCASE PRIMARY KEY, payload INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A', 1), ('b', 2), ('Z', 3)")
        .unwrap();
    conn.execute("UPDATE names SET payload = 10 WHERE name = 'a'")
        .unwrap();
    assert_eq!(
        conn.query("SELECT payload FROM names WHERE name COLLATE NOCASE = 'a'")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(10)]]
    );
    conn.execute("DELETE FROM names WHERE name > 'b'").unwrap();
    assert_eq!(
        conn.query("SELECT payload FROM names ORDER BY payload")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(2)], vec![Value::Integer(10)]]
    );
}

#[test]
fn collated_outer_join_key_preserves_literal_and_prepared_matches() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE NOCASE PRIMARY KEY, payload INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE labels (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A', 1), ('b', 2), ('Z', 3)")
        .unwrap();
    conn.execute("INSERT INTO labels VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for predicate in ["n.name = 'a'", "'a' = n.name"] {
            let sql = format!(
                "SELECT l.label FROM names n JOIN labels l ON n.payload = l.id WHERE {predicate}"
            );
            assert_eq!(
                conn.query(&sql).unwrap().rows,
                vec![vec![Value::Text("one".into())]],
                "{begin:?}: {sql}"
            );
        }
        for predicate in ["n.name = $1", "$1 = n.name"] {
            let sql = format!(
                "SELECT l.label FROM names n JOIN labels l ON n.payload = l.id WHERE {predicate}"
            );
            let statement = conn.prepare(&sql).unwrap();
            for _ in 0..2 {
                assert_eq!(
                    statement
                        .query_collect(&[Value::Text("a".into())])
                        .unwrap()
                        .rows,
                    vec![vec![Value::Text("one".into())]],
                    "{begin:?}: {sql}"
                );
            }
        }
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn prepared_collated_mutations_preserve_point_and_range_matches() {
    for begin in [None, Some("BEGIN")] {
        for (collation, stored, equivalent) in [("NOCASE", "A", "a"), ("RTRIM", "A  ", "A")] {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE names (name TEXT COLLATE {collation} PRIMARY KEY, payload INTEGER)"
            ))
            .unwrap();
            conn.execute(&format!(
                "INSERT INTO names VALUES ('{stored}', 1), ('b', 2), ('Z', 3)"
            ))
            .unwrap();
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            for predicate in ["name = $1", "$1 = name"] {
                let sql = format!("UPDATE names SET payload = payload + 10 WHERE {predicate}");
                let statement = conn.prepare(&sql).unwrap();
                assert_eq!(
                    statement
                        .execute(&[Value::Text(equivalent.into())])
                        .unwrap(),
                    1,
                    "{begin:?}: {collation}: {sql}"
                );
            }
            assert_eq!(
                conn.query("SELECT payload FROM names ORDER BY payload")
                    .unwrap()
                    .rows,
                vec![
                    vec![Value::Integer(2)],
                    vec![Value::Integer(3)],
                    vec![Value::Integer(21)]
                ]
            );
            let point = conn.prepare("DELETE FROM names WHERE name = $1").unwrap();
            assert_eq!(point.execute(&[Value::Text(equivalent.into())]).unwrap(), 1);
            let range = conn
                .prepare("DELETE FROM names WHERE name > $1 AND name <= $2")
                .unwrap();
            let (lower, upper) = if collation == "NOCASE" {
                ("b", "z")
            } else {
                ("A", "Z")
            };
            assert_eq!(
                range
                    .execute(&[Value::Text(lower.into()), Value::Text(upper.into())])
                    .unwrap(),
                1
            );
            assert_eq!(
                conn.query("SELECT payload FROM names").unwrap().rows,
                vec![vec![Value::Integer(2)]]
            );
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn rtrim_primary_key_comparisons_preserve_padding_semantics() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE RTRIM PRIMARY KEY, payload INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A  ', 1), ('B ', 2), ('C', 3)")
        .unwrap();
    for begin in [None, Some("BEGIN READ ONLY"), Some("BEGIN")] {
        if let Some(begin) = begin {
            conn.execute(begin).unwrap();
        }
        for (predicate, expected) in [
            ("name = 'A'", vec![1]),
            ("name > 'A'", vec![2, 3]),
            ("name <= 'B'", vec![1, 2]),
            ("name BETWEEN 'A' AND 'B'", vec![1, 2]),
        ] {
            let sql = format!("SELECT payload FROM names WHERE {predicate} ORDER BY payload");
            assert_eq!(
                conn.query(&sql).unwrap().rows,
                expected
                    .into_iter()
                    .map(|v| vec![Value::Integer(v)])
                    .collect::<Vec<_>>(),
                "{begin:?}: {sql}"
            );
        }
        assert_eq!(
            conn.prepare("SELECT payload FROM names WHERE name = $1")
                .unwrap()
                .query_collect(&[Value::Text("A".into())])
                .unwrap()
                .rows,
            vec![vec![Value::Integer(1)]]
        );
        if begin.is_some() {
            conn.execute("ROLLBACK").unwrap();
        }
    }
}

#[test]
fn prepared_collated_delete_ranges_preserve_collated_bounds() {
    for begin in [None, Some("BEGIN")] {
        for (collation, stored, lower, upper, expected) in
            [("NOCASE", "Z", "b", "z", 3), ("RTRIM", "B  ", "A", "B", 2)]
        {
            let db = database();
            let conn = Connection::open(&db).unwrap();
            conn.execute(&format!(
                "CREATE TABLE names (name TEXT COLLATE {collation} PRIMARY KEY, payload INTEGER)"
            ))
            .unwrap();
            conn.execute(&format!(
                "INSERT INTO names VALUES ('{stored}', {expected})"
            ))
            .unwrap();
            if let Some(begin) = begin {
                conn.execute(begin).unwrap();
            }
            let statement = conn
                .prepare("DELETE FROM names WHERE name > $1 AND name <= $2")
                .unwrap();
            assert_eq!(
                statement
                    .execute(&[Value::Text(lower.into()), Value::Text(upper.into())])
                    .unwrap(),
                1,
                "{begin:?}: {collation}"
            );
            assert!(conn
                .query("SELECT payload FROM names")
                .unwrap()
                .rows
                .is_empty());
            if begin.is_some() {
                conn.execute("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn binary_override_and_collated_secondary_index_preserve_results() {
    let db = database();
    let conn = Connection::open(&db).unwrap();
    conn.execute("CREATE TABLE names (name TEXT COLLATE NOCASE PRIMARY KEY, payload INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO names VALUES ('A', 1), ('b', 2), ('Z', 3)")
        .unwrap();
    conn.execute("CREATE INDEX names_collated ON names(name)")
        .unwrap();
    assert_eq!(
        conn.query("SELECT payload FROM names WHERE name = 'a'")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(1)]]
    );
    assert!(conn
        .query("SELECT payload FROM names WHERE name COLLATE BINARY = 'a'")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        conn.query("SELECT payload FROM names WHERE name COLLATE BINARY > 'a'")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(2)]]
    );
}
