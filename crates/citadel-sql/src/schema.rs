//! Schema manager: in-memory cache of table schemas.

use std::collections::HashMap;

use citadel::Database;

use crate::error::{Result, SqlError};
use crate::types::{ForeignKeySchemaEntry, TableSchema, ViewDef};

const SCHEMA_TABLE: &[u8] = b"_schema";
const VIEWS_TABLE: &[u8] = b"_views";

/// Manages table schemas in memory, backed by the `_schema` table.
pub struct SchemaManager {
    tables: HashMap<String, TableSchema>,
    views: HashMap<String, ViewDef>,
    generation: u64,
}

#[derive(Clone)]
pub struct SchemaSnapshot {
    tables: HashMap<String, TableSchema>,
    views: HashMap<String, ViewDef>,
    generation: u64,
}

impl SchemaManager {
    /// Load all schemas from the database's `_schema` table.
    pub fn load(db: &Database) -> Result<Self> {
        let mut tables = HashMap::new();

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

        // Load views from _views table
        let mut views = HashMap::new();
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

        Ok(Self {
            tables,
            views,
            generation: 0,
        })
    }

    pub fn get(&self, name: &str) -> Option<&TableSchema> {
        if let Some(s) = self.tables.get(name) {
            return Some(s);
        }
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.tables.get(&name.to_ascii_lowercase())
        } else {
            None
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            return true;
        }
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.tables.contains_key(&name.to_ascii_lowercase())
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

    /// Returns all table schemas.
    pub fn all_schemas(&self) -> impl Iterator<Item = &TableSchema> {
        self.tables.values()
    }

    // ── View management ────────────────────────────────────────────

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

    /// Find all FKs in other tables that reference `parent` table.
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

    /// Persist a schema to the _schema table (called within a write txn).
    pub fn save_schema(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        schema: &TableSchema,
    ) -> Result<()> {
        let lower = schema.name.to_ascii_lowercase();
        let data = schema.serialize();
        wtx.table_insert(SCHEMA_TABLE, lower.as_bytes(), &data)?;
        Ok(())
    }

    /// Remove a schema from the _schema table (called within a write txn).
    pub fn delete_schema(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>, name: &str) -> Result<()> {
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(SCHEMA_TABLE, lower.as_bytes())
            .map_err(|e| match e {
                citadel_core::Error::TableNotFound(_) => SqlError::TableNotFound(name.into()),
                other => SqlError::Storage(other),
            })?;
        Ok(())
    }

    /// Ensure the _schema table exists (called once per write).
    pub fn ensure_schema_table(wtx: &mut citadel_txn::write_txn::WriteTxn<'_>) -> Result<()> {
        // Try to create; ignore if already exists
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
