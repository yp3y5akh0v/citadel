use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_sql::{Connection, SqlError, Value};

fn database() -> citadel::Database {
    DatabaseBuilder::new("")
        .passphrase(b"parent-collation-fk")
        .argon2_profile(Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

#[test]
fn parent_collation_controls_prepared_integer_child_inserts_and_updates() {
    for (collation, stored, equivalent, different) in [
        ("NOCASE", "Alpha", "aLPHA", "Beta"),
        ("RTRIM", "Alpha  ", "Alpha", "alpha"),
    ] {
        let db = database();
        let c = Connection::open(&db).unwrap();
        c.execute(&format!(
            "CREATE TABLE p (id TEXT COLLATE {collation} PRIMARY KEY)"
        ))
        .unwrap();
        c.execute(
            "CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT COLLATE BINARY REFERENCES p(id))",
        )
        .unwrap();
        c.execute(&format!("INSERT INTO p VALUES ('{stored}')"))
            .unwrap();
        let insert = c.prepare("INSERT INTO c VALUES ($1, $2)").unwrap();
        insert
            .execute(&[Value::Integer(1), text(equivalent)])
            .unwrap();
        assert!(matches!(
            insert.execute(&[Value::Integer(2), text(different)]),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        insert.execute(&[Value::Integer(3), Value::Null]).unwrap();
        assert!(matches!(
            c.execute(&format!("UPDATE c SET p='{different}' WHERE id=1")),
            Err(SqlError::ForeignKeyViolation(_))
        ));
        assert_eq!(
            c.query("SELECT p FROM c WHERE id=1").unwrap().rows,
            vec![vec![text(equivalent)]]
        );
    }
}

#[test]
fn self_reference_uses_parent_equality_before_the_candidate_is_stored() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE t (a TEXT COLLATE NOCASE, b TEXT COLLATE RTRIM, x TEXT, y TEXT, PRIMARY KEY(a,b), FOREIGN KEY(x,y) REFERENCES t(a,b))").unwrap();
    c.execute("INSERT INTO t VALUES ('Alpha','tail  ','alpha','tail')")
        .unwrap();
    c.execute("INSERT INTO t VALUES ('Other','row',NULL,'missing')")
        .unwrap();
    assert!(matches!(
        c.execute("INSERT INTO t VALUES ('Bad','row','bad','ROW')"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
}

#[test]
fn deferred_checks_use_current_child_values_and_raw_child_identity() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("CREATE TABLE c (id TEXT COLLATE NOCASE PRIMARY KEY, p TEXT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
    c.execute("BEGIN").unwrap();
    c.execute("INSERT INTO c VALUES ('RawChild','alpha')")
        .unwrap();
    c.execute("SAVEPOINT before_bad").unwrap();
    c.execute("UPDATE c SET p='absent' WHERE id='rawchild'")
        .unwrap();
    c.execute("ROLLBACK TO SAVEPOINT before_bad").unwrap();
    c.execute("INSERT INTO p VALUES ('Alpha')").unwrap();
    c.execute("COMMIT").unwrap();
    c.execute("BEGIN").unwrap();
    c.execute("UPDATE c SET id='ChangedChild', p='missing' WHERE id='rawchild'")
        .unwrap();
    c.execute("DELETE FROM c WHERE id='changedchild'").unwrap();
    c.execute("COMMIT").unwrap();
    assert!(c.query("SELECT * FROM c").unwrap().rows.is_empty());
}

#[test]
fn equal_parent_spelling_change_does_not_trigger_any_referential_action() {
    for action in [
        "NO ACTION",
        "RESTRICT",
        "CASCADE",
        "SET NULL",
        "SET DEFAULT",
    ] {
        let db = database();
        let c = Connection::open(&db).unwrap();
        c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
            .unwrap();
        c.execute(&format!("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT DEFAULT 'fallback' REFERENCES p(id) ON UPDATE {action})")).unwrap();
        c.execute("INSERT INTO p VALUES ('Alpha'),('fallback')")
            .unwrap();
        c.execute("INSERT INTO c VALUES (1,'aLPHA')").unwrap();
        c.execute("UPDATE p SET id='ALPHA' WHERE id='Alpha'")
            .unwrap();
        assert_eq!(
            c.query("SELECT p FROM c").unwrap().rows,
            vec![vec![text("aLPHA")]],
            "{action}"
        );
        let outcome = c.execute("UPDATE p SET id='Beta' WHERE id='ALPHA'");
        let expected = match action {
            "NO ACTION" | "RESTRICT" => {
                assert!(matches!(outcome, Err(SqlError::ForeignKeyViolation(_))));
                text("aLPHA")
            }
            "CASCADE" => {
                outcome.unwrap();
                text("Beta")
            }
            "SET NULL" => {
                outcome.unwrap();
                Value::Null
            }
            "SET DEFAULT" => {
                outcome.unwrap();
                text("fallback")
            }
            _ => unreachable!(),
        };
        assert_eq!(
            c.query("SELECT p FROM c").unwrap().rows,
            vec![vec![expected]],
            "{action}"
        );
    }
}

#[test]
fn delete_actions_find_collation_equivalent_children() {
    for action in [
        "NO ACTION",
        "RESTRICT",
        "CASCADE",
        "SET NULL",
        "SET DEFAULT",
    ] {
        let db = database();
        let c = Connection::open(&db).unwrap();
        c.execute("CREATE TABLE p (id TEXT COLLATE RTRIM PRIMARY KEY)")
            .unwrap();
        c.execute(&format!("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT DEFAULT 'fallback' REFERENCES p(id) ON DELETE {action})")).unwrap();
        c.execute("INSERT INTO p VALUES ('Alpha  '),('fallback')")
            .unwrap();
        c.execute("INSERT INTO c VALUES (1,'Alpha')").unwrap();
        let outcome = c.execute("DELETE FROM p WHERE id='Alpha'");
        let expected = match action {
            "NO ACTION" | "RESTRICT" => {
                assert!(matches!(outcome, Err(SqlError::ForeignKeyViolation(_))));
                vec![vec![text("Alpha")]]
            }
            "CASCADE" => {
                outcome.unwrap();
                vec![]
            }
            "SET NULL" => {
                outcome.unwrap();
                vec![vec![Value::Null]]
            }
            "SET DEFAULT" => {
                outcome.unwrap();
                vec![vec![text("fallback")]]
            }
            _ => unreachable!(),
        };
        assert_eq!(
            c.query("SELECT p FROM c").unwrap().rows,
            expected,
            "{action}"
        );
    }
}

#[test]
fn parent_unique_index_must_cover_its_declared_equality() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE)")
        .unwrap();
    c.execute("CREATE UNIQUE INDEX narrower ON p(k COLLATE BINARY)")
        .unwrap();
    assert!(matches!(
        c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT REFERENCES p(k))"),
        Err(SqlError::Unsupported(_))
    ));
    c.execute("CREATE UNIQUE INDEX compatible ON p(k COLLATE NOCASE)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT REFERENCES p(k) ON DELETE CASCADE)")
        .unwrap();
    c.execute("INSERT INTO p VALUES (1,'Alpha')").unwrap();
    c.execute("INSERT INTO c VALUES (1,'alpha')").unwrap();
    assert!(matches!(
        c.execute("DROP INDEX compatible"),
        Err(SqlError::Unsupported(_))
    ));
    c.execute("DELETE FROM p WHERE id=1").unwrap();
    assert!(c.query("SELECT * FROM c").unwrap().rows.is_empty());
}

#[test]
fn child_index_coverage_rejects_narrower_or_incomparable_replacements() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT REFERENCES p(id))")
        .unwrap();
    c.execute("CREATE INDEX narrower ON c(p COLLATE BINARY)")
        .unwrap();
    c.execute("CREATE INDEX incomparable ON c(p COLLATE RTRIM)")
        .unwrap();
    assert!(matches!(
        c.execute("DROP INDEX __fk_c_0"),
        Err(SqlError::Unsupported(_))
    ));
    c.execute("CREATE INDEX compatible ON c(p COLLATE NOCASE)")
        .unwrap();
    c.execute("DROP INDEX __fk_c_0").unwrap();
    assert!(matches!(
        c.execute("DROP INDEX compatible"),
        Err(SqlError::Unsupported(_))
    ));
}

#[test]
fn binary_parent_rechecks_broader_unique_index_and_overrides_child_collation() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, k TEXT COLLATE BINARY)")
        .unwrap();
    c.execute("CREATE UNIQUE INDEX broader ON p(k COLLATE NOCASE)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT COLLATE NOCASE REFERENCES p(k))")
        .unwrap();
    c.execute("INSERT INTO p VALUES (1,'Alpha')").unwrap();
    c.execute("INSERT INTO c VALUES (1,'Alpha')").unwrap();
    assert!(matches!(
        c.execute("INSERT INTO c VALUES (2,'alpha')"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
}

#[test]
fn added_fk_column_backfills_existing_default_values_for_parent_actions() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("INSERT INTO p VALUES ('Alpha')").unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)")
        .unwrap();
    c.execute("BEGIN").unwrap();
    let insert = c.prepare("INSERT INTO c VALUES ($1)").unwrap();
    for id in 1..=257 {
        insert.execute(&[Value::Integer(id)]).unwrap();
    }
    c.execute("COMMIT").unwrap();
    c.execute("ALTER TABLE c ADD COLUMN p TEXT DEFAULT 'alpha' REFERENCES p(id) ON DELETE CASCADE")
        .unwrap();
    c.execute("DELETE FROM p WHERE id='Alpha'").unwrap();
    assert!(c.query("SELECT * FROM c").unwrap().rows.is_empty());
}

#[test]
fn added_fk_column_rejects_invalid_existing_defaults_atomically() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)")
        .unwrap();
    c.execute("INSERT INTO c VALUES (1),(2)").unwrap();
    assert!(matches!(
        c.execute("ALTER TABLE c ADD COLUMN p TEXT DEFAULT 'missing' REFERENCES p(id)"),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    assert_eq!(c.table_schema("c").unwrap().columns.len(), 1);
    assert!(c.table_schema("c").unwrap().indices.is_empty());
    c.execute("INSERT INTO p VALUES ('MISSING')").unwrap();
    c.execute("ALTER TABLE c ADD COLUMN p TEXT DEFAULT 'missing' REFERENCES p(id)")
        .unwrap();
}

#[test]
fn added_deferred_fk_column_can_be_satisfied_before_commit() {
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)")
        .unwrap();
    c.execute("INSERT INTO c VALUES (1)").unwrap();
    c.execute("BEGIN").unwrap();
    c.execute("ALTER TABLE c ADD COLUMN p TEXT DEFAULT 'alpha' REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED").unwrap();
    c.execute("INSERT INTO p VALUES ('Alpha')").unwrap();
    c.execute("COMMIT").unwrap();
    assert_eq!(
        c.query("SELECT p FROM c").unwrap().rows,
        vec![vec![text("alpha")]]
    );
}

#[test]
fn existing_child_index_is_upgraded_to_cover_parent_equality() {
    use citadel_sql::{schema::SchemaManager, types::TableSchema};
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id TEXT COLLATE NOCASE PRIMARY KEY)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT REFERENCES p(id) ON DELETE CASCADE)")
        .unwrap();
    c.execute("CREATE INDEX old_binary ON c(p COLLATE BINARY)")
        .unwrap();
    c.execute("INSERT INTO p VALUES ('Alpha')").unwrap();
    c.execute("INSERT INTO c VALUES (1,'alpha')").unwrap();
    let mut child = c.table_schema("c").unwrap();
    // Model a catalog written before FK equality was represented correctly.
    child.indices.retain(|index| index.name != "__fk_c_0");
    drop(c);
    let mut raw = db.begin_write().unwrap();
    raw.drop_table(&TableSchema::index_table_name("c", "__fk_c_0"))
        .unwrap();
    SchemaManager::save_schema(&mut raw, &child).unwrap();
    raw.commit().unwrap();
    let c = Connection::open(&db).unwrap();
    let child = c.table_schema("c").unwrap();
    assert_eq!(
        child.index_by_name("old_binary").unwrap().collation_at(0),
        citadel_sql::types::Collation::Binary
    );
    assert!(child
        .indices
        .iter()
        .any(|index| index.is_full_column_btree(&[1])
            && index.collation_at(0) == citadel_sql::types::Collation::NoCase));
    c.execute("DELETE FROM p WHERE id='Alpha'").unwrap();
    assert!(c.query("SELECT * FROM c").unwrap().rows.is_empty());
}

#[test]
fn existing_incompatible_parent_unique_constraint_is_not_silently_replaced() {
    use citadel_sql::{schema::SchemaManager, types::TableSchema};
    let db = database();
    let c = Connection::open(&db).unwrap();
    c.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE)")
        .unwrap();
    c.execute("CREATE UNIQUE INDEX compatible ON p(k COLLATE NOCASE)")
        .unwrap();
    c.execute("CREATE UNIQUE INDEX narrower ON p(k COLLATE BINARY)")
        .unwrap();
    c.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, p TEXT REFERENCES p(k))")
        .unwrap();
    c.execute("INSERT INTO p VALUES (1,'Alpha')").unwrap();
    c.execute("INSERT INTO c VALUES (1,'alpha')").unwrap();
    let mut parent = c.table_schema("p").unwrap();
    parent.indices.retain(|index| index.name != "compatible");
    drop(c);
    let mut raw = db.begin_write().unwrap();
    raw.drop_table(&TableSchema::index_table_name("p", "compatible"))
        .unwrap();
    SchemaManager::save_schema(&mut raw, &parent).unwrap();
    raw.commit().unwrap();
    assert!(matches!(
        Connection::open(&db),
        Err(SqlError::ForeignKeyViolation(_))
    ));
    let schema = SchemaManager::load(&db).unwrap();
    assert!(schema
        .get("p")
        .unwrap()
        .index_by_name("compatible")
        .is_none());
    assert_eq!(schema.get("p").unwrap().indices.len(), 1);
    assert_eq!(db.begin_read().table_entry_count(b"p").unwrap(), 1);
    assert_eq!(db.begin_read().table_entry_count(b"c").unwrap(), 1);
}
