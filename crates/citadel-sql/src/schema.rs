//! Schema manager: in-memory cache of table schemas.

use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use citadel::{Database, SqlCacheHandle};
use parking_lot::Mutex;

use crate::error::{Result, SqlError};
use crate::system_tables::{self, VirtualTable};
use crate::types::{ForeignKeySchemaEntry, TableSchema, ViewDef};

/// Reverse FK index (parent -> [(child table, fk idx)]), tagged with its generation.
type FkChildrenCache = std::cell::RefCell<Option<(u64, FxHashMap<String, Vec<(String, usize)>>)>>;

const SCHEMA_TABLE: &[u8] = b"_schema";
const VIEWS_TABLE: &[u8] = b"_views";
const TRIGGERS_TABLE: &[u8] = b"_triggers";
const MATVIEWS_TABLE: &[u8] = b"_matviews";

type CatalogStamps = [Option<(citadel_core::PageId, citadel_core::TxnId)>; 4];
type CatalogVisitor<'a> = dyn FnMut(&[u8], &[u8]) -> citadel_core::Result<()> + 'a;
const CATALOGS: [&[u8]; 4] = [SCHEMA_TABLE, VIEWS_TABLE, TRIGGERS_TABLE, MATVIEWS_TABLE];

trait CatalogReader {
    fn manager_id(&self) -> u64;
    fn stamp(
        &mut self,
        table: &[u8],
    ) -> citadel_core::Result<Option<(citadel_core::PageId, citadel_core::TxnId)>>;
    fn scan(&mut self, table: &[u8], visit: &mut CatalogVisitor<'_>) -> citadel_core::Result<()>;
}

macro_rules! catalog_reader {
    ($ty:ty) => {
        impl CatalogReader for $ty {
            fn manager_id(&self) -> u64 {
                self.manager_id()
            }
            fn stamp(
                &mut self,
                table: &[u8],
            ) -> citadel_core::Result<Option<(citadel_core::PageId, citadel_core::TxnId)>> {
                self.table_root_stamp(table)
            }
            fn scan(
                &mut self,
                table: &[u8],
                visit: &mut CatalogVisitor<'_>,
            ) -> citadel_core::Result<()> {
                self.table_for_each(table, visit)
            }
        }
    };
}
catalog_reader!(citadel_txn::read_txn::ReadTxn<'_>);
catalog_reader!(citadel_txn::write_txn::WriteTxn<'_>);

fn catalog_stamps(txn: &mut impl CatalogReader) -> Result<CatalogStamps> {
    let mut stamps = [None; 4];
    for (stamp, name) in stamps.iter_mut().zip(CATALOGS) {
        *stamp = txn.stamp(name)?;
    }
    Ok(stamps)
}

// A writer can prove the local schema and catalog roots before its commit
// generation is known. Keep that proof distinct from an unbound cache.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CatalogBinding {
    commit_generation: Option<u64>,
    local_generation: u64,
}

/// Whether a schema load may be stopped by the database's cancel token.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cancellable {
    Yes,
    No,
}

fn schema_read(db: &Database, cancellable: Cancellable) -> citadel_txn::read_txn::ReadTxn<'_> {
    let mut rtx = db.begin_read();
    if cancellable == Cancellable::No {
        rtx.set_cancel(None);
    }
    rtx
}

thread_local! {
    /// Stack of `(alias → storage_name)` frames pushed by FOR EACH STATEMENT trigger
    /// firings so `REFERENCING NEW TABLE AS new_t` resolves while the body runs.
    static TRANSITION_TABLES: std::cell::RefCell<Vec<FxHashMap<String, String>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Borrow already-lowercase names (the common hot-path case), allocate otherwise.
fn lower_cow(name: &str) -> std::borrow::Cow<'_, str> {
    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        std::borrow::Cow::Owned(name.to_ascii_lowercase())
    } else {
        std::borrow::Cow::Borrowed(name)
    }
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
    /// Persisted catalogs admitted from one exact storage snapshot. Local DDL
    /// invalidates the generation shortcut until the next admission.
    catalog_origin: Option<u64>,
    catalog_stamps: Option<CatalogStamps>,
    catalog_binding: Option<CatalogBinding>,
    /// First volatile persisted expression from a legacy catalog. Maintained
    /// with table registration/removal so executor entry points can reject in
    /// O(1) without rescanning every schema on the statement hot path.
    legacy_volatile_definition: Option<String>,
    /// Derived catalog invariant; sound writes do not rescan table metadata.
    missing_primary_key_index: Option<String>,
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
    /// Reverse FK index, rebuilt lazily whenever the schema generation changes.
    fk_children_cache: FkChildrenCache,
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
    triggers: FxHashMap<String, Vec<crate::types::TriggerDef>>,
    matviews: FxHashMap<String, crate::types::MatviewDef>,
    temp_aliases: FxHashMap<String, String>,
    dml_dirty_tables: FxHashSet<String>,
    dml_append_tables: FxHashMap<String, i64>,
    generation: u64,
    sql_caches: SqlCacheHandle,
    catalog_origin: Option<u64>,
    catalog_stamps: Option<CatalogStamps>,
    catalog_binding: Option<CatalogBinding>,
}

pub(crate) struct DmlSnapshot {
    dirty_tables: FxHashSet<String>,
    append_tables: FxHashMap<String, i64>,
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
            catalog_origin: None,
            catalog_stamps: None,
            catalog_binding: None,
            legacy_volatile_definition: None,
            missing_primary_key_index: None,
            sql_caches: Arc::new(Mutex::new(FxHashMap::default())),
            dml_dirty_tables: std::cell::RefCell::new(FxHashSet::default()),
            dml_append_tables: std::cell::RefCell::new(FxHashMap::default()),
            fk_children_cache: std::cell::RefCell::new(None),
        }
    }

    /// Mark a table mutated (UPDATE/DELETE/upsert/DDL); supersedes a pending append.
    pub fn mark_dml(&self, table_name: &str) {
        let key = lower_cow(table_name);
        // Dirty implies no pending append (mark_dml_append checks dirty first;
        // this fn removes the append entry before inserting dirty).
        if self.dml_dirty_tables.borrow().contains(key.as_ref()) {
            return;
        }
        self.dml_append_tables.borrow_mut().remove(key.as_ref());
        self.dml_dirty_tables.borrow_mut().insert(key.into_owned());
    }

    /// Mark a pure append with the smallest inserted pk; no-op if already mutating.
    pub fn mark_dml_append(&self, table_name: &str, min_pk: i64) {
        let key = lower_cow(table_name);
        if self.dml_dirty_tables.borrow().contains(key.as_ref()) {
            return;
        }
        let mut appends = self.dml_append_tables.borrow_mut();
        match appends.get_mut(key.as_ref()) {
            Some(m) => *m = (*m).min(min_pk),
            None => {
                appends.insert(key.into_owned(), min_pk);
            }
        }
    }

    /// Take the touched tables, classified into mutating vs pure-append.
    pub fn drain_dml_dirty(&self) -> DmlDirty {
        DmlDirty {
            mutating: self.dml_dirty_tables.borrow_mut().drain().collect(),
            appends: self.dml_append_tables.borrow_mut().drain().collect(),
        }
    }

    pub fn has_dml_dirty(&self) -> bool {
        !self.dml_dirty_tables.borrow().is_empty() || !self.dml_append_tables.borrow().is_empty()
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

    /// Carry TEMP aliases onto a freshly loaded manager.
    ///
    /// They live only in memory, so a reload would otherwise hide this connection's
    /// TEMP tables while their rows stay on disk under the prefixed name.
    /// Carry the prior schema's TEMP aliases onto a freshly loaded one.
    ///
    /// An alias whose backing table is absent from the reload is dropped: a TEMP
    /// table created in a rolled-back transaction has no physical table, and
    /// keeping its alias would make that name permanently unusable.
    pub fn adopt_temp_aliases(&mut self, prior: &SchemaManager) {
        for (name, prefixed) in prior.temp_alias_iter() {
            if self.tables.contains_key(&prefixed.to_ascii_lowercase()) {
                self.temp_aliases
                    .insert(name.to_string(), prefixed.to_string());
            }
        }
    }

    pub fn resolve_temp(&self, name: &str) -> String {
        let lower = name.to_ascii_lowercase();
        if let Some(prefixed) = self.temp_aliases.get(&lower) {
            return prefixed.clone();
        }
        name.to_string()
    }

    pub fn load(db: &Database) -> Result<Self> {
        Self::load_inner(db, Cancellable::Yes)
    }

    /// Load all SQL catalogs from an already-owned read snapshot.
    pub fn load_with_read(
        db: &Database,
        rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    ) -> Result<Self> {
        if rtx.manager_id() != db.manager().instance_id() {
            return Err(SqlError::InvalidValue(
                "read transaction belongs to a different database manager".into(),
            ));
        }
        let generation = rtx.commit_generation();
        Self::load_catalogs(db.sql_cache_handle(), rtx, Some(generation))
    }

    /// Load one table from the caller's transaction snapshot.
    pub(crate) fn load_table(
        name: &str,
        get: impl FnOnce(&[u8], &[u8]) -> citadel_core::Result<Option<Vec<u8>>>,
    ) -> Result<Option<TableSchema>> {
        let lower = lower_cow(name);
        match get(SCHEMA_TABLE, lower.as_bytes()) {
            Ok(Some(data)) => TableSchema::deserialize(&data).map(Some),
            Ok(None) | Err(citadel_core::Error::TableNotFound(_)) => Ok(None),
            Err(error) => Err(SqlError::Storage(error)),
        }
    }

    /// Loads the schema with cancellation suspended.
    ///
    /// Restoring the schema after a transaction ends is recovery, not the
    /// caller's query: a ROLLBACK under a tripped token must not fail before
    /// replacing the schema it just rolled back.
    pub(crate) fn load_ignoring_cancel(db: &Database) -> Result<Self> {
        Self::load_inner(db, Cancellable::No)
    }

    fn load_inner(db: &Database, cancellable: Cancellable) -> Result<Self> {
        let mut rtx = schema_read(db, cancellable);
        let generation = rtx.commit_generation();
        Self::load_catalogs(db.sql_cache_handle(), &mut rtx, Some(generation))
    }

    fn load_catalogs(
        sql_caches: SqlCacheHandle,
        txn: &mut impl CatalogReader,
        generation: Option<u64>,
    ) -> Result<Self> {
        let stamps = catalog_stamps(txn)?;
        let mut tables = FxHashMap::default();
        let mut parse_err: Option<crate::error::SqlError> = None;
        let scan_result = txn.scan(SCHEMA_TABLE, &mut |_key, value| {
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
        let mut view_err: Option<crate::error::SqlError> = None;
        let view_scan = txn.scan(VIEWS_TABLE, &mut |_key, value| {
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
        let mut trig_err: Option<crate::error::SqlError> = None;
        let trig_scan = txn.scan(TRIGGERS_TABLE, &mut |_key, value| {
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
        let mut mv_err: Option<crate::error::SqlError> = None;
        let mv_scan = txn.scan(MATVIEWS_TABLE, &mut |_key, value| {
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

        let legacy_volatile_definition = tables
            .values()
            .find_map(TableSchema::volatile_persisted_expression);
        let missing_primary_key_index = tables
            .values()
            .find(|table| {
                !table.primary_key_has_binary_collation()
                    && table.primary_key_equality_index().is_none()
            })
            .map(|table| table.name.clone());
        let mut mgr = Self {
            tables,
            views,
            virtual_tables: FxHashMap::default(),
            triggers,
            matviews,
            temp_aliases: FxHashMap::default(),
            transition_schemas: std::cell::RefCell::new(FxHashMap::default()),
            generation: 0,
            catalog_origin: Some(txn.manager_id()),
            catalog_stamps: Some(stamps),
            catalog_binding: Some(CatalogBinding {
                commit_generation: generation,
                local_generation: 0,
            }),
            legacy_volatile_definition,
            missing_primary_key_index,
            sql_caches,
            dml_dirty_tables: std::cell::RefCell::new(FxHashSet::default()),
            dml_append_tables: std::cell::RefCell::new(FxHashMap::default()),
            fk_children_cache: std::cell::RefCell::new(None),
        };
        system_tables::register_builtins(&mut mgr);
        Ok(mgr)
    }

    /// Admit an immutable read snapshot before compiling or executing SQL.
    pub(crate) fn admit_read(
        &mut self,
        db: &Database,
        rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    ) -> Result<()> {
        let generation = rtx.commit_generation();
        self.admit_catalogs(db, rtx, generation)
    }

    /// Called immediately after acquiring the exclusive writer, before any SQL
    /// mutation. Its committed generation cannot change while the writer is held.
    pub(crate) fn admit_write(
        &mut self,
        db: &Database,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    ) -> Result<Option<SchemaSnapshot>> {
        self.admit_catalogs(db, wtx, db.manager().commit_generation())?;
        crate::executor::constraint_indexes::reconcile_primary_key_indexes_in_txn(wtx, self)
    }

    fn admit_catalogs(
        &mut self,
        db: &Database,
        txn: &mut impl CatalogReader,
        generation: u64,
    ) -> Result<()> {
        if txn.manager_id() != db.manager().instance_id() {
            return Err(SqlError::InvalidValue(
                "transaction belongs to a different database manager".into(),
            ));
        }
        if self.catalog_origin == Some(txn.manager_id())
            && self.catalog_binding
                == Some(CatalogBinding {
                    commit_generation: Some(generation),
                    local_generation: self.generation,
                })
        {
            return Ok(());
        }
        let stamps = catalog_stamps(txn)?;
        if self.catalog_origin != Some(txn.manager_id())
            || self.catalog_stamps != Some(stamps)
            || self
                .catalog_binding
                .is_none_or(|binding| binding.local_generation != self.generation)
        {
            let mut fresh = Self::load_catalogs(db.sql_cache_handle(), txn, Some(generation))?;
            fresh.bump_generation_past(self.generation);
            if self.catalog_origin == Some(txn.manager_id()) {
                fresh.adopt_temp_aliases(self);
                fresh.restore_dml_snapshot(self.save_dml_snapshot());
                fresh.virtual_tables.extend(
                    self.virtual_tables
                        .iter()
                        .map(|(name, table)| (name.clone(), Arc::clone(table))),
                );
            } else if self.catalog_origin.is_none() {
                // User virtual tables registered before first admission have
                // no prior database provenance. Persisted/local dirty state
                // is never carried from a different manager.
                fresh.virtual_tables.extend(
                    self.virtual_tables
                        .iter()
                        .map(|(name, table)| (name.clone(), Arc::clone(table))),
                );
            }
            *self = fresh;
        }
        self.catalog_stamps = Some(stamps);
        self.catalog_binding = Some(CatalogBinding {
            commit_generation: Some(generation),
            local_generation: self.generation,
        });
        Ok(())
    }

    /// Publish only the generation returned by this admitted writer's commit.
    /// A later concurrent commit must still differ when its snapshot is admitted.
    pub(crate) fn bind_committed_catalog(&mut self, manager_id: u64, generation: u64) {
        if self.catalog_origin == Some(manager_id) && self.catalog_stamps.is_some() {
            if let Some(binding) = &mut self.catalog_binding {
                if binding.local_generation == self.generation {
                    binding.commit_generation = Some(generation);
                }
            }
        }
    }

    /// A public caller can alternate caches inside one writer, whose catalog
    /// pages can change in place without a new root stamp. Compare exact
    /// persisted definitions here; Connection's admitted route skips this scan.
    pub(crate) fn admit_owned_write(
        &mut self,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    ) -> Result<()> {
        self.check_writer_origin(wtx)?;
        let mut fresh = Self::load_catalogs(self.sql_caches.clone(), wtx, None)?;
        if self.catalog_origin.is_none() || self.catalog_records()? != fresh.catalog_records()? {
            fresh.bump_generation_past(self.generation);
            fresh.adopt_temp_aliases(self);
            fresh.virtual_tables.extend(
                self.virtual_tables
                    .iter()
                    .map(|(name, table)| (name.clone(), Arc::clone(table))),
            );
            fresh.restore_dml_snapshot(self.save_dml_snapshot());
            *self = fresh;
        } else {
            self.catalog_origin = fresh.catalog_origin;
            self.catalog_stamps = fresh.catalog_stamps;
        }
        self.catalog_binding = Some(CatalogBinding {
            commit_generation: None,
            local_generation: self.generation,
        });
        // The public caller owns the surrounding writer and its eventual
        // commit/rollback; backfill itself restores both snapshots on failure.
        let _ =
            crate::executor::constraint_indexes::reconcile_primary_key_indexes_in_txn(wtx, self)?;
        Ok(())
    }

    fn check_writer_origin(&self, wtx: &citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        if self
            .catalog_origin
            .is_some_and(|origin| origin != wtx.manager_id())
        {
            return Err(SqlError::InvalidValue(
                "SQL schema belongs to a different database manager".into(),
            ));
        }
        Ok(())
    }

    fn catalog_records(&self) -> Result<[std::collections::BTreeMap<String, Vec<u8>>; 4]> {
        let mut records: [std::collections::BTreeMap<String, Vec<u8>>; 4] = Default::default();
        for (name, table) in &self.tables {
            records[0].insert(name.clone(), table.try_serialize()?);
        }
        for (name, view) in &self.views {
            records[1].insert(name.clone(), view.try_serialize()?);
        }
        for trigger in self.triggers.values().flatten() {
            records[2].insert(trigger.name.clone(), trigger.try_serialize()?);
        }
        for (name, view) in &self.matviews {
            records[3].insert(name.clone(), view.try_serialize()?);
        }
        Ok(records)
    }

    pub(crate) fn bind_write_catalog(
        &mut self,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    ) -> Result<()> {
        self.catalog_stamps = Some(catalog_stamps(wtx)?);
        self.catalog_binding = Some(CatalogBinding {
            commit_generation: None,
            local_generation: self.generation,
        });
        Ok(())
    }

    pub(crate) fn validate_write_catalog(
        &self,
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    ) -> Result<()> {
        self.check_writer_origin(wtx)?;
        let fresh = Self::load_catalogs(self.sql_caches.clone(), wtx, None)?;
        if self.catalog_records()? == fresh.catalog_records()? {
            Ok(())
        } else {
            Err(SqlError::InvalidValue("SQL schema does not match the supplied write snapshot; execute_in_txn admits a mutable schema before mutation".into()))
        }
    }

    pub(crate) fn validate_read_catalog(
        &self,
        rtx: &mut citadel_txn::read_txn::ReadTxn<'_>,
    ) -> Result<()> {
        if self.catalog_origin != Some(rtx.manager_id()) {
            return Err(SqlError::InvalidValue(
                "SQL schema belongs to a different database manager".into(),
            ));
        }
        let generation = rtx.commit_generation();
        if let Some(binding) = self.catalog_binding {
            if binding.local_generation == self.generation
                && (binding.commit_generation == Some(generation)
                    || self.catalog_stamps == Some(catalog_stamps(rtx)?))
            {
                return Ok(());
            }
        }
        let fresh = Self::load_catalogs(self.sql_caches.clone(), rtx, Some(generation))?;
        if self.catalog_records()? == fresh.catalog_records()? {
            Ok(())
        } else {
            Err(SqlError::InvalidValue("SQL schema does not match the supplied read snapshot; load that snapshot before execution".into()))
        }
    }

    pub fn get_virtual(&self, name: &str) -> Option<&Arc<dyn VirtualTable>> {
        self.virtual_tables.get(name)
    }

    pub fn register_virtual(&mut self, vt: Arc<dyn VirtualTable>) {
        let name = vt.name().to_ascii_lowercase();
        self.virtual_tables.insert(name, vt);
    }

    pub fn get(&self, name: &str) -> Option<&TableSchema> {
        // Hot callers pass pre-lowered names; those skip the String alloc.
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.get_lower(&name.to_ascii_lowercase())
        } else {
            self.get_lower(name)
        }
    }

    /// Resolution precedence: transition > matview > temp alias > base table.
    fn get_lower(&self, lower: &str) -> Option<&TableSchema> {
        if let Some(prefixed) = transition_table_lookup(lower) {
            if let Some(s) = self.tables.get(&prefixed) {
                return Some(s);
            }
            if let Some(&leaked) = self.transition_schemas.borrow().get(&prefixed) {
                return Some(leaked);
            }
        }
        if !self.matviews.is_empty() {
            if let Some(mv) = self.matviews.get(lower) {
                return self.tables.get(&mv.backing_table);
            }
        }
        if !self.temp_aliases.is_empty() {
            if let Some(prefixed) = self.temp_aliases.get(lower) {
                return self.tables.get(prefixed);
            }
        }
        // Table keys are registered lowercase; one canonical probe suffices.
        self.tables.get(lower)
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
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.contains_lower(&name.to_ascii_lowercase())
        } else {
            self.contains_lower(name)
        }
    }

    fn contains_lower(&self, lower: &str) -> bool {
        transition_table_lookup(lower).is_some()
            || (!self.matviews.is_empty() && self.matviews.contains_key(lower))
            || (!self.temp_aliases.is_empty() && self.temp_aliases.contains_key(lower))
            || self.tables.contains_key(lower)
    }

    /// A false result proves schema-directed evaluation cannot observe an
    /// internal literal-binding vector. Scan all tables because FK validation
    /// can decode referenced rows; enabled trigger bodies remain conservative.
    /// Callers cache this proof alongside their existing schema generation.
    pub(crate) fn may_read_scoped_parameters(&self) -> bool {
        self.all_triggers().any(|trigger| trigger.enabled)
            || self
                .all_schemas()
                .any(TableSchema::may_read_scoped_parameters)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn legacy_volatile_definition(&self) -> Option<&str> {
        self.legacy_volatile_definition.as_deref()
    }

    fn refresh_table_invariants(&mut self) {
        self.legacy_volatile_definition = self
            .tables
            .values()
            .find_map(TableSchema::volatile_persisted_expression);
        self.missing_primary_key_index = self
            .tables
            .values()
            .find(|table| {
                !table.primary_key_has_binary_collation()
                    && table.primary_key_equality_index().is_none()
            })
            .map(|table| table.name.clone());
    }

    pub(crate) fn missing_primary_key_index(&self) -> Option<&str> {
        self.missing_primary_key_index.as_deref()
    }

    pub fn register(&mut self, schema: TableSchema) {
        let lower = schema.name.to_ascii_lowercase();
        self.tables.insert(lower, schema);
        self.refresh_table_invariants();
        self.generation += 1;
    }

    pub fn remove(&mut self, name: &str) -> Option<TableSchema> {
        let lower = name.to_ascii_lowercase();
        let result = self.tables.remove(&lower);
        if result.is_some() {
            self.refresh_table_invariants();
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
        if self.triggers.is_empty() {
            return &[];
        }
        // Keys are stored lowercased; skip the alloc when target already is.
        if !target.bytes().any(|b| b.is_ascii_uppercase()) {
            return self.triggers.get(target).map_or(&[], |v| v.as_slice());
        }
        let key = target.to_ascii_lowercase();
        self.triggers.get(&key).map_or(&[], |v| v.as_slice())
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
        let data = trig.try_serialize()?;
        Self::ensure_triggers_table(wtx)?;
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
        let data = view.try_serialize()?;
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
        let data = mv.try_serialize()?;
        Self::ensure_matviews_table(wtx)?;
        let lower = mv.name.to_ascii_lowercase();
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
        self.ensure_fk_children_cache();
        let cache = self.fk_children_cache.borrow();
        let Some((_, map)) = cache.as_ref() else {
            return Vec::new();
        };
        let Some(children) = map.get(parent) else {
            return Vec::new();
        };
        children
            .iter()
            .map(|(child, fk_idx)| {
                let schema = self.tables.get(child).expect("cached child table exists");
                (schema.name.as_str(), &schema.foreign_keys[*fk_idx])
            })
            .collect()
    }

    /// Rebuild the reverse FK index iff it is stale for the current generation.
    fn ensure_fk_children_cache(&self) {
        if matches!(self.fk_children_cache.borrow().as_ref(), Some((g, _)) if *g == self.generation)
        {
            return;
        }
        let mut map: FxHashMap<String, Vec<(String, usize)>> = FxHashMap::default();
        for (child_name, schema) in &self.tables {
            for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
                map.entry(fk.foreign_table.clone())
                    .or_default()
                    .push((child_name.clone(), fk_idx));
            }
        }
        *self.fk_children_cache.borrow_mut() = Some((self.generation, map));
    }

    pub fn save_schema(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        schema: &TableSchema,
    ) -> Result<()> {
        let lower = schema.name.to_ascii_lowercase();
        let data = schema.try_serialize()?;
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
            triggers: self.triggers.clone(),
            matviews: self.matviews.clone(),
            temp_aliases: self.temp_aliases.clone(),
            dml_dirty_tables: self.dml_dirty_tables.borrow().clone(),
            dml_append_tables: self.dml_append_tables.borrow().clone(),
            generation: self.generation,
            sql_caches: self.sql_caches.clone(),
            catalog_origin: self.catalog_origin,
            catalog_stamps: self.catalog_stamps,
            catalog_binding: self.catalog_binding,
        }
    }

    pub(crate) fn save_dml_snapshot(&self) -> DmlSnapshot {
        DmlSnapshot {
            dirty_tables: self.dml_dirty_tables.borrow().clone(),
            append_tables: self.dml_append_tables.borrow().clone(),
        }
    }

    pub(crate) fn restore_dml_snapshot(&self, snap: DmlSnapshot) {
        *self.dml_dirty_tables.borrow_mut() = snap.dirty_tables;
        *self.dml_append_tables.borrow_mut() = snap.append_tables;
    }

    pub fn restore_snapshot(&mut self, snap: SchemaSnapshot) {
        // Equal generations prove nothing changed since the snapshot: restore
        // is a no-op and plans stay valid. Otherwise never rewind - a plan
        // compiled after the snapshot must not revalidate against it.
        if self.generation != snap.generation {
            self.generation = self.generation.max(snap.generation) + 1;
        }
        self.sql_caches = snap.sql_caches;
        self.catalog_origin = snap.catalog_origin;
        self.catalog_stamps = snap.catalog_stamps;
        self.catalog_binding = snap.catalog_binding;
        self.tables = snap.tables;
        self.refresh_table_invariants();
        self.views = snap.views;
        self.triggers = snap.triggers;
        self.matviews = snap.matviews;
        self.temp_aliases = snap.temp_aliases;
        *self.dml_dirty_tables.borrow_mut() = snap.dml_dirty_tables;
        *self.dml_append_tables.borrow_mut() = snap.dml_append_tables;
    }

    /// Advance the generation strictly past `prior` so plans compiled against
    /// a predecessor manager can never revalidate against this one.
    pub fn bump_generation_past(&mut self, prior: u64) {
        if self.generation <= prior {
            self.generation = prior + 1;
        }
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
