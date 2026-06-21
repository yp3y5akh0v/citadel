//! Schema manager: in-memory cache of table schemas.

use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use citadel::{Database, SqlCacheHandle};
use parking_lot::Mutex;

use crate::error::{Result, SqlError};
use crate::system_tables::{self, VirtualTable};
use crate::types::{ForeignKeySchemaEntry, TableSchema, ViewDef};

const SCHEMA_TABLE: &[u8] = b"_schema";
const VIEWS_TABLE: &[u8] = b"_views";
const TRIGGERS_TABLE: &[u8] = b"_triggers";
const MATVIEWS_TABLE: &[u8] = b"_matviews";

thread_local! {
    /// Stack of `(alias → storage_name)` frames pushed by FOR EACH STATEMENT trigger
    /// firings so `REFERENCING NEW TABLE AS new_t` resolves while the body runs.
    static TRANSITION_TABLES: std::cell::RefCell<Vec<FxHashMap<String, String>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn transition_table_lookup(name_lower: &str) -> Option<String> {
    TRANSITION_TABLES.with(|cell| {
        let stack = cell.borrow();
        for frame in stack.iter().rev() {
            if let Some(storage) = frame.get(name_lower) {
                return Some(storage.clone());
            }
        }
        None
    })
}

pub(crate) fn push_transition_tables(aliases: FxHashMap<String, String>) -> TransitionGuard {
    TRANSITION_TABLES.with(|cell| cell.borrow_mut().push(aliases));
    TransitionGuard
}

pub(crate) struct TransitionGuard;
impl Drop for TransitionGuard {
    fn drop(&mut self) {
        TRANSITION_TABLES.with(|cell| {
            cell.borrow_mut().pop();
        });
    }
}

/// Manages table schemas in memory, backed by the `_schema` table.
pub struct SchemaManager {
    tables: FxHashMap<String, TableSchema>,
    views: FxHashMap<String, ViewDef>,
    virtual_tables: FxHashMap<String, Arc<dyn VirtualTable>>,
    /// Within a `(target, timing, event)` group, triggers fire in name order.
    triggers: FxHashMap<String, Vec<crate::types::TriggerDef>>,
    /// Matview catalog. Backing table shares the matview's name in `tables`; this map
    /// also gates DML rejection (matviews are read-only outside REFRESH).
    matviews: FxHashMap<String, crate::types::MatviewDef>,
    /// Maps user-typed TEMP name to prefixed storage name (`__temp_<conn_id>_<name>`).
    temp_aliases: FxHashMap<String, String>,
    /// Each entry is leaked once via `Box::leak` so `get()` can hand out a `&TableSchema`
    /// from inside `&self` methods. Bounded by `(active triggers × transition aliases)`.
    transition_schemas: std::cell::RefCell<FxHashMap<String, &'static TableSchema>>,
    generation: u64,
    /// Per-Database shared cache (e.g. ANN indexes). Cloned from the Database
    /// when the Connection opens; all Connections to the same DB share entries.
    /// Tests created via `empty()` get their own isolated cache.
    pub sql_caches: SqlCacheHandle,
    /// Tables mutated (UPDATE/DELETE/upsert/DDL) since the last commit; their
    /// shared caches are hard-invalidated on commit.
    dml_dirty_tables: std::cell::RefCell<FxHashSet<String>>,
    /// Tables touched only by pure appends, mapped to the min inserted pk; an
    /// append above the index snapshot tail-merges instead of hard-invalidating.
    dml_append_tables: std::cell::RefCell<FxHashMap<String, i64>>,
}

/// DML since the last commit, classified for cache invalidation.
pub struct DmlDirty {
    pub mutating: Vec<String>,
    pub appends: Vec<(String, i64)>,
}

#[derive(Clone)]
pub struct SchemaSnapshot {
    tables: FxHashMap<String, TableSchema>,
    views: FxHashMap<String, ViewDef>,
    generation: u64,
}

impl SchemaManager {
    pub fn empty() -> Self {
        Self {
            tables: FxHashMap::default(),
            views: FxHashMap::default(),
            virtual_tables: FxHashMap::default(),
            triggers: FxHashMap::default(),
            matviews: FxHashMap::default(),
            temp_aliases: FxHashMap::default(),
            transition_schemas: std::cell::RefCell::new(FxHashMap::default()),
            generation: 0,
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            dml_dirty_tables: std::cell::RefCell::new(FxHashSet::default()),
            dml_append_tables: std::cell::RefCell::new(FxHashMap::default()),
        }
    }

    /// Mark a table mutated (UPDATE/DELETE/upsert/DDL); supersedes a pending append.
    pub fn mark_dml(&self, table_name: &str) {
        let lower = table_name.to_ascii_lowercase();
        self.dml_append_tables.borrow_mut().remove(&lower);
        self.dml_dirty_tables.borrow_mut().insert(lower);
    }

    /// Mark a pure append with the smallest inserted pk; no-op if already mutating.
    pub fn mark_dml_append(&self, table_name: &str, min_pk: i64) {
        let lower = table_name.to_ascii_lowercase();
        if self.dml_dirty_tables.borrow().contains(&lower) {
            return;
        }
        self.dml_append_tables
            .borrow_mut()
            .entry(lower)
            .and_modify(|m| *m = (*m).min(min_pk))
            .or_insert(min_pk);
    }

    /// Take the touched tables, classified into mutating vs pure-append.
    pub fn drain_dml_dirty(&self) -> DmlDirty {
        DmlDirty {
            mutating: self.dml_dirty_tables.borrow_mut().drain().collect(),
            appends: self.dml_append_tables.borrow_mut().drain().collect(),
        }
    }

    /// Forget pending DML markers without invalidating downstream caches.
    /// Used on rollback (uncommitted writes leave no caches stale).
    pub fn clear_dml_dirty(&self) {
        self.dml_dirty_tables.borrow_mut().clear();
        self.dml_append_tables.borrow_mut().clear();
    }

    pub fn register_temp_alias(&mut self, user_name: &str, prefixed_name: String) {
        self.temp_aliases
            .insert(user_name.to_ascii_lowercase(), prefixed_name);
        self.generation += 1;
    }

    pub fn unregister_temp_alias(&mut self, user_name: &str) -> Option<String> {
        let lower = user_name.to_ascii_lowercase();
        let removed = self.temp_aliases.remove(&lower);
        if removed.is_some() {
            self.generation += 1;
        }
        removed
    }

    pub fn temp_alias_iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.temp_aliases
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn resolve_temp(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        if let Some(prefixed) = self.temp_aliases.get(&lower) {
            return prefixed.clone();
        }
        name.to_string()
    }

    pub fn load(db: &Database) -> Result<Self> {
        let mut tables = FxHashMap::default();

        let mut rtx = db.begin_read();
        let mut parse_err: Option<crate::error::SqlError> = None;
        let scan_result = rtx.table_for_each(SCHEMA_TABLE, |_key, value| {
            match TableSchema::deserialize(value) {
                Ok(schema) => {
                    tables.insert(schema.name.clone(), schema);
                }
                Err(e) => {
                    parse_err = Some(e);
                }
            }
            Ok(())
        });

        match scan_result {
            Ok(()) => {}
            Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = parse_err {
            return Err(e);
        }

        let mut views = FxHashMap::default();
        let mut rtx2 = db.begin_read();
        let mut view_err: Option<crate::error::SqlError> = None;
        let view_scan = rtx2.table_for_each(VIEWS_TABLE, |_key, value| {
            match ViewDef::deserialize(value) {
                Ok(vd) => {
                    views.insert(vd.name.clone(), vd);
                }
                Err(e) => {
                    view_err = Some(e);
                }
            }
            Ok(())
        });

        match view_scan {
            Ok(()) => {}
            Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = view_err {
            return Err(e);
        }

        let mut triggers: FxHashMap<String, Vec<crate::types::TriggerDef>> = FxHashMap::default();
        let mut rtx3 = db.begin_read();
        let mut trig_err: Option<crate::error::SqlError> = None;
        let trig_scan = rtx3.table_for_each(TRIGGERS_TABLE, |_key, value| {
            match crate::types::TriggerDef::deserialize(value) {
                Ok(td) => {
                    triggers
                        .entry(td.target.to_ascii_lowercase())
                        .or_default()
                        .push(td);
                }
                Err(e) => {
                    trig_err = Some(e);
                }
            }
            Ok(())
        });
        match trig_scan {
            Ok(()) => {}
            Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = trig_err {
            return Err(e);
        }
        // PG-faithful: triggers fire in name order within a (target, timing, event) group.
        for v in triggers.values_mut() {
            v.sort_by(|a, b| a.name.cmp(&b.name));
        }

        let mut matviews: FxHashMap<String, crate::types::MatviewDef> = FxHashMap::default();
        let mut rtx4 = db.begin_read();
        let mut mv_err: Option<crate::error::SqlError> = None;
        let mv_scan = rtx4.table_for_each(MATVIEWS_TABLE, |_key, value| {
            match crate::types::MatviewDef::deserialize(value) {
                Ok(mv) => {
                    matviews.insert(mv.name.to_ascii_lowercase(), mv);
                }
                Err(e) => {
                    mv_err = Some(e);
                }
            }
            Ok(())
        });
        match mv_scan {
            Ok(()) => {}
            Err(citadel_core::Error::TableNotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(e) = mv_err {
            return Err(e);
        }

        let mut mgr = Self {
            tables,
            views,
            virtual_tables: FxHashMap::default(),
            triggers,
            matviews,
            temp_aliases: FxHashMap::default(),
            transition_schemas: std::cell::RefCell::new(FxHashMap::default()),
            generation: 0,
            sql_caches: db.sql_cache_handle(),
            dml_dirty_tables: std::cell::RefCell::new(FxHashSet::default()),
            dml_append_tables: std::cell::RefCell::new(FxHashMap::default()),
        };
        system_tables::register_builtins(&mut mgr);
        Ok(mgr)
    }

    pub fn get_virtual(&self, name: &str) -> Option<&Arc<dyn VirtualTable>> {
        self.virtual_tables.get(name)
    }

    pub fn register_virtual(&mut self, vt: Arc<dyn VirtualTable>) {
        let name = vt.name().to_ascii_lowercase();
        self.virtual_tables.insert(name, vt);
    }

    pub fn get(&self, name: &str) -> Option<&TableSchema> {
        let lower = name.to_ascii_lowercase();
        if let Some(prefixed) = transition_table_lookup(&lower) {
            if let Some(s) = self.tables.get(&prefixed) {
                return Some(s);
            }
            if let Some(&leaked) = self.transition_schemas.borrow().get(&prefixed) {
                return Some(leaked);
            }
        }
        if let Some(mv) = self.matviews.get(&lower) {
            return self.tables.get(&mv.backing_table);
        }
        if let Some(prefixed) = self.temp_aliases.get(&lower) {
            return self.tables.get(prefixed);
        }
        if let Some(s) = self.tables.get(name) {
            return Some(s);
        }
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.tables.get(&lower)
        } else {
            None
        }
    }

    pub fn register_transition_schema(&self, storage_name: String, schema: TableSchema) {
        let leaked: &'static TableSchema = Box::leak(Box::new(schema));
        self.transition_schemas
            .borrow_mut()
            .insert(storage_name, leaked);
    }

    pub fn unregister_transition_schema(&self, storage_name: &str) {
        self.transition_schemas.borrow_mut().remove(storage_name);
    }

    pub fn contains(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        if transition_table_lookup(&lower).is_some() {
            return true;
        }
        if self.matviews.contains_key(&lower) {
            return true;
        }
        if self.temp_aliases.contains_key(&lower) {
            return true;
        }
        if self.tables.contains_key(name) {
            return true;
        }
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.tables.contains_key(&lower)
        } else {
            false
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn register(&mut self, schema: TableSchema) {
        let lower = schema.name.to_ascii_lowercase();
        self.tables.insert(lower, schema);
        self.generation += 1;
    }

    pub fn remove(&mut self, name: &str) -> Option<TableSchema> {
        let lower = name.to_ascii_lowercase();
        let result = self.tables.remove(&lower);
        if result.is_some() {
            self.generation += 1;
        }
        result
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn all_schemas(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewDef> {
        if let Some(v) = self.views.get(name) {
            return Some(v);
        }
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.views.get(&name.to_ascii_lowercase())
        } else {
            None
        }
    }

    pub fn register_view(&mut self, view: ViewDef) {
        let lower = view.name.to_ascii_lowercase();
        self.views.insert(lower, view);
        self.generation += 1;
    }

    pub fn remove_view(&mut self, name: &str) -> Option<ViewDef> {
        let lower = name.to_ascii_lowercase();
        let result = self.views.remove(&lower);
        if result.is_some() {
            self.generation += 1;
        }
        result
    }

    pub fn view_names(&self) -> Vec<&str> {
        self.views.keys().map(|s| s.as_str()).collect()
    }

    pub fn triggers_for(&self, target: &str) -> &[crate::types::TriggerDef] {
        let key = target.to_ascii_lowercase();
        self.triggers.get(&key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn all_triggers(&self) -> impl Iterator<Item = &crate::types::TriggerDef> + '_ {
        self.triggers.values().flatten()
    }

    pub fn register_trigger(&mut self, trig: crate::types::TriggerDef) {
        let target = trig.target.to_ascii_lowercase();
        let bucket = self.triggers.entry(target).or_default();
        bucket.push(trig);
        bucket.sort_by(|a, b| a.name.cmp(&b.name));
        self.generation += 1;
    }

    pub fn remove_trigger(&mut self, name: &str) -> Option<crate::types::TriggerDef> {
        let lower = name.to_ascii_lowercase();
        let mut result = None;
        for bucket in self.triggers.values_mut() {
            if let Some(pos) = bucket
                .iter()
                .position(|t| t.name.eq_ignore_ascii_case(&lower))
            {
                result = Some(bucket.remove(pos));
                break;
            }
        }
        if result.is_some() {
            self.generation += 1;
        }
        result
    }

    /// Caller is responsible for dropping the returned triggers' on-disk catalog rows.
    pub fn remove_triggers_for(&mut self, target: &str) -> Vec<crate::types::TriggerDef> {
        let key = target.to_ascii_lowercase();
        let removed = self.triggers.remove(&key).unwrap_or_default();
        if !removed.is_empty() {
            self.generation += 1;
        }
        removed
    }

    pub fn find_trigger(&self, name: &str) -> Option<(&str, &crate::types::TriggerDef)> {
        let lower = name.to_ascii_lowercase();
        for (target, bucket) in &self.triggers {
            if let Some(t) = bucket.iter().find(|t| t.name.eq_ignore_ascii_case(&lower)) {
                return Some((target.as_str(), t));
            }
        }
        None
    }

    pub fn set_trigger_enabled(&mut self, name: &str, enabled: bool) -> bool {
        let lower = name.to_ascii_lowercase();
        for bucket in self.triggers.values_mut() {
            if let Some(t) = bucket
                .iter_mut()
                .find(|t| t.name.eq_ignore_ascii_case(&lower))
            {
                t.enabled = enabled;
                self.generation += 1;
                return true;
            }
        }
        false
    }

    pub fn set_all_triggers_enabled(&mut self, target: &str, enabled: bool) -> usize {
        let key = target.to_ascii_lowercase();
        let bucket = match self.triggers.get_mut(&key) {
            Some(b) => b,
            None => return 0,
        };
        let count = bucket.len();
        for t in bucket {
            t.enabled = enabled;
        }
        if count > 0 {
            self.generation += 1;
        }
        count
    }

    pub fn ensure_triggers_table(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        match wtx.create_table(TRIGGERS_TABLE) {
            Ok(()) => Ok(()),
            Err(citadel_core::Error::TableAlreadyExists(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save_trigger(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        trig: &crate::types::TriggerDef,
    ) -> Result<()> {
        Self::ensure_triggers_table(wtx)?;
        let data = trig.serialize();
        let lower = trig.name.to_ascii_lowercase();
        wtx.table_insert(TRIGGERS_TABLE, lower.as_bytes(), &data)
            .map_err(crate::error::SqlError::from)?;
        Ok(())
    }

    pub fn delete_trigger(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        name: &str,
    ) -> Result<()> {
        Self::ensure_triggers_table(wtx)?;
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(TRIGGERS_TABLE, lower.as_bytes())
            .map_err(crate::error::SqlError::from)?;
        Ok(())
    }

    pub fn save_view(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>, view: &ViewDef) -> Result<()> {
        let lower = view.name.to_ascii_lowercase();
        let data = view.serialize();
        wtx.table_insert(VIEWS_TABLE, lower.as_bytes(), &data)?;
        Ok(())
    }

    pub fn delete_view(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>, name: &str) -> Result<()> {
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(VIEWS_TABLE, lower.as_bytes())
            .map_err(|e| match e {
                citadel_core::Error::TableNotFound(_) => SqlError::ViewNotFound(name.into()),
                other => SqlError::Storage(other),
            })?;
        Ok(())
    }

    pub fn ensure_views_table(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        match wtx.create_table(VIEWS_TABLE) {
            Ok(()) => Ok(()),
            Err(citadel_core::Error::TableAlreadyExists(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_matview(&self, name: &str) -> Option<&crate::types::MatviewDef> {
        let lower = name.to_ascii_lowercase();
        self.matviews.get(&lower)
    }

    pub fn matview_names(&self) -> Vec<&str> {
        self.matviews.keys().map(|s| s.as_str()).collect()
    }

    pub fn all_matviews(&self) -> impl Iterator<Item = &crate::types::MatviewDef> + '_ {
        self.matviews.values()
    }

    pub fn register_matview(&mut self, mv: crate::types::MatviewDef) {
        let lower = mv.name.to_ascii_lowercase();
        self.matviews.insert(lower, mv);
        self.generation += 1;
    }

    pub fn remove_matview(&mut self, name: &str) -> Option<crate::types::MatviewDef> {
        let lower = name.to_ascii_lowercase();
        let removed = self.matviews.remove(&lower);
        if removed.is_some() {
            self.generation += 1;
        }
        removed
    }

    pub fn ensure_matviews_table(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        match wtx.create_table(MATVIEWS_TABLE) {
            Ok(()) => Ok(()),
            Err(citadel_core::Error::TableAlreadyExists(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save_matview(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        mv: &crate::types::MatviewDef,
    ) -> Result<()> {
        Self::ensure_matviews_table(wtx)?;
        let lower = mv.name.to_ascii_lowercase();
        let data = mv.serialize();
        wtx.table_insert(MATVIEWS_TABLE, lower.as_bytes(), &data)?;
        Ok(())
    }

    pub fn delete_matview(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        name: &str,
    ) -> Result<()> {
        Self::ensure_matviews_table(wtx)?;
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(MATVIEWS_TABLE, lower.as_bytes())
            .map_err(crate::error::SqlError::from)?;
        Ok(())
    }

    pub fn child_fks_for(&self, parent: &str) -> Vec<(&str, &ForeignKeySchemaEntry)> {
        self.tables
            .iter()
            .flat_map(|(name, schema)| {
                schema
                    .foreign_keys
                    .iter()
                    .filter(|fk| fk.foreign_table == parent)
                    .map(move |fk| (name.as_str(), fk))
            })
            .collect()
    }

    pub fn save_schema(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        schema: &TableSchema,
    ) -> Result<()> {
        let lower = schema.name.to_ascii_lowercase();
        let data = schema.serialize();
        wtx.table_insert(SCHEMA_TABLE, lower.as_bytes(), &data)?;
        Ok(())
    }

    pub fn delete_schema(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>, name: &str) -> Result<()> {
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(SCHEMA_TABLE, lower.as_bytes())
            .map_err(|e| match e {
                citadel_core::Error::TableNotFound(_) => SqlError::TableNotFound(name.into()),
                other => SqlError::Storage(other),
            })?;
        Ok(())
    }

    pub fn ensure_schema_table(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        match wtx.create_table(SCHEMA_TABLE) {
            Ok(()) => Ok(()),
            Err(citadel_core::Error::TableAlreadyExists(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save_snapshot(&self) -> SchemaSnapshot {
        SchemaSnapshot {
            tables: self.tables.clone(),
            views: self.views.clone(),
            generation: self.generation,
        }
    }

    pub fn restore_snapshot(&mut self, snap: SchemaSnapshot) {
        self.tables = snap.tables;
        self.views = snap.views;
        self.generation = snap.generation;
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
