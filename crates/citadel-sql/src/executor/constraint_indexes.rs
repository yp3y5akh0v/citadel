//! Constraint equality and complete child lookups backed by ordinary B-tree indexes.

use citadel::Database;
use citadel_txn::write_txn::WriteTxn;

use crate::error::{Result, SqlError};
use crate::schema::{SchemaManager, SchemaSnapshot};
use crate::types::{IndexDef, IndexKind, TableSchema};

use super::index_build::IndexBuildPlan;

pub(super) fn primary_key_index_to_add(
    table: &TableSchema,
    name_in_use: impl Fn(&str) -> bool,
) -> Option<IndexDef> {
    if table.primary_key_has_binary_collation() || table.primary_key_equality_index().is_some() {
        return None;
    }
    let prefix = format!("__pk_{}", table.name);
    let mut name = prefix.clone();
    let mut suffix = 0_u64;
    while table.index_by_name(&name).is_some() || name_in_use(&name) {
        suffix += 1;
        name = format!("{prefix}_{suffix}");
    }
    Some(IndexDef::from_column_lists(
        name,
        table.primary_key_columns.clone(),
        table
            .primary_key_columns
            .iter()
            .map(|&column| table.columns[column as usize].collation)
            .collect(),
        true,
        None,
        None,
        IndexKind::BTree,
    ))
}

/// Read-only admission is sufficient when the catalog already has the required
/// definitions. The rare upgrade uses the same admitted writer as SQL mutation.
pub(crate) fn reconcile_constraint_indexes(
    db: &Database,
    schema: &mut SchemaManager,
) -> Result<()> {
    if schema.missing_constraint_index().is_none() {
        return Ok(());
    }
    let mut wtx = db.begin_write().map_err(SqlError::Storage)?;
    let snapshot = schema.admit_write(db, &mut wtx)?;
    if let Some(snapshot) = snapshot {
        if let Err(error) = super::commit_with_ann_publication(wtx, schema) {
            schema.restore_snapshot(snapshot);
            return Err(error);
        }
    }
    Ok(())
}

/// Caller has already admitted this exact writer's catalog. Backfill is one
/// atomic unit even for public caller-owned writers. The returned catalog is
/// from that admitted snapshot, before any backfill; an owned writer's caller
/// restores it if a later statement or commit aborts the transaction.
pub(crate) fn reconcile_constraint_indexes_in_txn(
    wtx: &mut WriteTxn<'_>,
    schema: &mut SchemaManager,
) -> Result<Option<SchemaSnapshot>> {
    if schema.missing_constraint_index().is_none() {
        return Ok(None);
    }
    let snapshot = schema.save_snapshot();
    let savepoint = wtx.begin_savepoint();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut tables = schema.all_schemas().cloned().collect::<Vec<_>>();
        tables.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let cancel = wtx.cancel_token().cloned();
        let mut updated = Vec::new();
        for original in &tables {
            let mut table = original.clone();
            let mut additions = Vec::new();
            if let Some(index) = primary_key_index_to_add(&table, |candidate| {
                tables
                    .iter()
                    .chain(updated.iter())
                    .any(|other: &TableSchema| other.index_by_name(candidate).is_some())
            }) {
                table.indices.push(index.clone());
                additions.push(index);
            }
            let child_indexes = super::fk::child_indexes_to_add(schema, &table, |candidate| {
                updated
                    .iter()
                    .any(|other: &TableSchema| other.index_by_name(candidate).is_some())
            })?;
            table.indices.extend(child_indexes.iter().cloned());
            additions.extend(child_indexes);
            if additions.is_empty() {
                continue;
            }
            for index in &additions {
                let storage = TableSchema::index_table_name(&table.name, &index.name);
                let plan = IndexBuildPlan::new(&table, index, cancel.as_ref())?;
                let entries =
                    plan.collect(|visit| wtx.table_scan_from(table.name.as_bytes(), b"", visit))?;
                // A physical collision is corruption, never permission to adopt
                // or overwrite an undeclared tree.
                wtx.create_table(&storage).map_err(SqlError::Storage)?;
                if let Err(error) = plan.insert(wtx, &storage, entries) {
                    return Err(match error {
                        SqlError::UniqueViolation(_)
                            if table.is_primary_key_equality_index(index) =>
                        {
                            SqlError::UniqueViolation(format!(
                                "primary key of '{}' under its declared collation",
                                table.name
                            ))
                        }
                        other => other,
                    });
                }
            }
            SchemaManager::save_schema(wtx, &table)?;
            updated.push(table);
            #[cfg(test)]
            CANCEL_AFTER_BACKFILL.with(|slot| {
                if let Some(token) = slot.borrow_mut().take() {
                    token.cancel();
                }
            });
        }
        for table in updated {
            schema.register(table);
        }
        // Missing child trees can be built, but an incompatible declared
        // parent UNIQUE key is not authority to add a new uniqueness constraint.
        for table in schema.all_schemas() {
            for fk in &table.foreign_keys {
                let parent = schema
                    .get(&fk.foreign_table)
                    .ok_or_else(|| SqlError::TableNotFound(fk.foreign_table.clone()))?;
                super::fk::validate_parent_reference(parent, fk)?;
            }
        }
        require_constraint_indexes(schema)?;
        schema.bind_write_catalog(wtx)?;
        Ok(())
    }));
    match outcome {
        Ok(Ok(())) => Ok(Some(snapshot)),
        failure => {
            wtx.restore_snapshot(savepoint);
            schema.restore_snapshot(snapshot);
            match failure {
                Ok(Err(error)) => Err(error),
                Err(payload) => std::panic::resume_unwind(payload),
                Ok(Ok(())) => unreachable!(),
            }
        }
    }
}

pub(crate) fn require_constraint_indexes(schema: &SchemaManager) -> Result<()> {
    match schema.missing_constraint_index() {
        Some(table) => Err(SqlError::InvalidValue(format!(
            "constraints on '{table}' require equality indexes; use mutable SQL admission to build them before insertion"
        ))),
        None => Ok(()),
    }
}

/// Recomputed only when table metadata changes, not on the statement hot path.
pub(crate) fn first_missing_foreign_key_index(schema: &SchemaManager) -> Option<String> {
    schema
        .all_schemas()
        .find(|table| !super::fk::has_complete_foreign_key_indexes(schema, table))
        .map(|table| table.name.clone())
}

#[cfg(test)]
thread_local! {
    static CANCEL_AFTER_BACKFILL: std::cell::RefCell<Option<citadel::CancelToken>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Collation, IndexKey};
    use crate::Connection;
    use citadel::{Argon2Profile, DatabaseBuilder};

    fn database() -> Database {
        DatabaseBuilder::new("")
            .passphrase(b"pk-index-tests")
            .argon2_profile(Argon2Profile::Iot)
            .create_in_memory()
            .unwrap()
    }

    #[test]
    fn equality_proof_rejects_partial_expression_wrong_shape_and_collation() {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE names(tenant INTEGER,name TEXT COLLATE NOCASE,n INTEGER,PRIMARY KEY(tenant,name))").unwrap();
        let table = conn.table_schema("names").unwrap();
        let exact = table.indices[0].clone();
        assert!(table.is_primary_key_equality_index(&exact));
        for case in 0..7 {
            let mut index = exact.clone();
            match case {
                0 => index.unique = false,
                1 => index.predicate_sql = Some("n>0".into()),
                2 => index.predicate_expr = Some(crate::parser::parse_sql_expr("n>0").unwrap()),
                3 => index.keys.swap(0, 1),
                4 => {
                    index.keys[1] = IndexKey::Column {
                        idx: 1,
                        collate: Collation::Binary,
                    }
                }
                5 => {
                    index.keys[1] = IndexKey::Column {
                        idx: 1,
                        collate: Collation::Rtrim,
                    }
                }
                6 => {
                    index.keys[1] = IndexKey::Expr {
                        expr: crate::parser::parse_sql_expr("LOWER(name)").unwrap(),
                        original_sql: "LOWER(name)".into(),
                    }
                }
                _ => unreachable!(),
            }
            assert!(!table.is_primary_key_equality_index(&index), "case {case}");
        }
        let mut harmless = exact;
        harmless.keys[0] = IndexKey::Column {
            idx: 0,
            collate: Collation::Rtrim,
        };
        assert!(table.is_primary_key_equality_index(&harmless));
    }

    #[test]
    fn reserved_looking_name_is_not_constraint_proof_and_does_not_get_overwritten() {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE names(name TEXT COLLATE NOCASE PRIMARY KEY)")
            .unwrap();
        let mut table = conn.table_schema("names").unwrap();
        table.indices[0].unique = false;
        let new = primary_key_index_to_add(&table, |name| name == "__pk_names_1").unwrap();
        assert_eq!(new.name, "__pk_names_2");
        assert!(table.is_primary_key_equality_index(&new));
        assert!(!table.indices[0].unique);
    }

    #[test]
    fn cancelled_backfill_restores_catalog_rows_trees_and_derived_marker() {
        let db = database();
        let conn = Connection::open(&db).unwrap();
        for name in ["a", "b"] {
            conn.execute(&format!(
                "CREATE TABLE {name}(name TEXT COLLATE NOCASE PRIMARY KEY)"
            ))
            .unwrap();
            conn.execute(&format!("INSERT INTO {name} VALUES ('A')"))
                .unwrap();
        }
        let mut schema = SchemaManager::load(&db).unwrap();
        let mut raw = db.begin_write().unwrap();
        let mut storage = Vec::new();
        for name in ["a", "b"] {
            let mut table = schema.get(name).unwrap().clone();
            for index in table.indices.drain(..) {
                let path = TableSchema::index_table_name(name, &index.name);
                raw.drop_table(&path).unwrap();
                storage.push(path);
            }
            SchemaManager::save_schema(&mut raw, &table).unwrap();
        }
        raw.commit().unwrap();
        let token = citadel::CancelToken::new();
        let mut wtx = db.begin_write().unwrap();
        let before = wtx.table_get(b"_schema", b"a").unwrap();
        wtx.set_cancel(Some(token.clone()));
        CANCEL_AFTER_BACKFILL.with(|slot| *slot.borrow_mut() = Some(token));
        assert!(matches!(
            schema.admit_owned_write(&mut wtx),
            Err(SqlError::Storage(citadel_core::Error::Interrupted))
        ));
        wtx.set_cancel(None);
        assert_eq!(wtx.table_get(b"_schema", b"a").unwrap(), before);
        for path in storage {
            assert!(wtx.table_root_stamp(&path).unwrap().is_none());
        }
        assert!(schema.missing_constraint_index().is_some());
        assert!(schema.get("a").unwrap().indices.is_empty());
        wtx.commit().unwrap();
    }
}
