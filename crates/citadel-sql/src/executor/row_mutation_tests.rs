use super::*;

fn database() -> citadel::Database {
    citadel::DatabaseBuilder::new("")
        .passphrase(b"cascade-coverage")
        .argon2_profile(citadel::Argon2Profile::Iot)
        .create_in_memory()
        .unwrap()
}

#[test]
fn complete_child_coverage_uses_no_value_budget_and_keeps_singletons_keyed() {
    for (count, unique) in [(0, false), (1, false), (2, false), (40, false), (1, true)] {
        let db = database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE, payload BLOB)").unwrap();
        if unique {
            conn.execute("CREATE UNIQUE INDEX by_parent ON child (p)")
                .unwrap();
            conn.execute("DROP INDEX __fk_child_0").unwrap();
        }
        conn.execute("INSERT INTO parent VALUES (1)").unwrap();
        conn.execute("BEGIN").unwrap();
        let insert = conn
            .prepare("INSERT INTO child VALUES ($1, 1, $2)")
            .unwrap();
        for id in 0..count {
            insert
                .execute(&[Value::Integer(id), Value::Blob(vec![0x5a; 4096])])
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        let schema = SchemaManager::load(&db).unwrap();
        let child = schema.get("child").unwrap();
        let reference = super::super::fk::ReferenceKey::new(
            schema.get("parent").unwrap(),
            &child.foreign_keys[0],
        )
        .unwrap();
        let index = find_cascading_idx(child, &child.foreign_keys[0], &reference).unwrap();
        let index_table = TableSchema::index_table_name("child", &index.name);
        let mut wtx = db.begin_write().unwrap();
        let mut hits = FkChildHits::default();
        scan_fk_index_keys(
            &mut wtx,
            child,
            index,
            &reference,
            &encode_composite_key(&[Value::Integer(1)]),
            &mut hits,
        )
        .unwrap();
        assert_eq!(hits.len(), count as usize);
        // Install after scanning: UNIQUE entries retain their original scan
        // charge; neither coverage checking nor tree freeing materializes rows.
        let budget = citadel_txn::ReadBudget::new(0, 0);
        wtx.set_read_budget(Some(budget.clone()));
        let marker = wtx.mutation_marker();
        let cleared =
            try_truncate_leaf_children(&mut wtx, child, index, &index_table, &hits).unwrap();
        assert_eq!(cleared, !unique && count >= 2);
        assert_eq!(wtx.mutated_since(marker), cleared);
        assert_eq!(
            wtx.table_entry_count(b"child").unwrap(),
            if cleared { 0 } else { count as u64 }
        );
        assert_eq!(
            wtx.table_entry_count(&index_table).unwrap(),
            if cleared { 0 } else { count as u64 }
        );
        assert_eq!(budget.remaining(), 0);
    }
}

#[test]
fn folded_child_coverage_keeps_exact_scan_budget_and_rechecks_folded_siblings() {
    for sibling in [false, true] {
        let db = database();
        let conn = crate::Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE parent (id TEXT PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p TEXT REFERENCES parent(id) ON DELETE CASCADE, payload TEXT)").unwrap();
        conn.execute("CREATE INDEX by_parent ON child (p COLLATE NOCASE)")
            .unwrap();
        conn.execute("DROP INDEX __fk_child_0").unwrap();
        conn.execute("INSERT INTO parent VALUES ('a'), ('A')")
            .unwrap();
        conn.execute("INSERT INTO child VALUES (1, 'a', 'one'), (2, 'a', 'second')")
            .unwrap();
        if sibling {
            conn.execute("INSERT INTO child VALUES (3, 'A', 'sibling')")
                .unwrap();
        }
        let schema = SchemaManager::load(&db).unwrap();
        let child = schema.get("child").unwrap();
        let reference = super::super::fk::ReferenceKey::new(
            schema.get("parent").unwrap(),
            &child.foreign_keys[0],
        )
        .unwrap();
        let index = find_cascading_idx(child, &child.foreign_keys[0], &reference).unwrap();
        let index_table = TableSchema::index_table_name("child", &index.name);
        let mut sizes = Vec::new();
        db.begin_read()
            .table_for_each(b"child", |_, value| {
                sizes.push(value.len());
                Ok(())
            })
            .unwrap();
        let total: usize = sizes.iter().sum();
        let parent_key = encode_composite_key(&[Value::Text("a".into())]);
        for allowance in [total - 1, total] {
            let mut wtx = db.begin_write().unwrap();
            let budget = citadel_txn::ReadBudget::new(*sizes.iter().max().unwrap(), allowance);
            wtx.set_read_budget(Some(budget.clone()));
            let marker = wtx.mutation_marker();
            let mut hits = FkChildHits::default();
            let scanned =
                scan_fk_index_keys(&mut wtx, child, index, &reference, &parent_key, &mut hits);
            if allowance < total {
                assert!(matches!(
                    scanned,
                    Err(SqlError::Storage(
                        citadel_core::Error::ReadBudgetExceeded { .. }
                    ))
                ));
                assert!(!wtx.mutated_since(marker));
                continue;
            }
            scanned.unwrap();
            assert_eq!(hits.len(), 2);
            assert_eq!(budget.remaining(), 0);
            assert_eq!(
                try_truncate_leaf_children(&mut wtx, child, index, &index_table, &hits).unwrap(),
                !sibling
            );
            assert_eq!(budget.remaining(), 0);
            assert_eq!(
                wtx.table_entry_count(b"child").unwrap(),
                if sibling { 3 } else { 0 }
            );
        }
    }
}
