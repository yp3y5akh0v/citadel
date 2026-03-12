//! Schema manager: in-memory cache of table schemas.

use std::collections::HashMap;

use citadel::Database;

use crate::error::{Result, SqlError};
use crate::types::TableSchema;

const SCHEMA_TABLE: &[u8] = b"_schema";

/// Manages table schemas in memory, backed by the `_schema` table.
pub struct SchemaManager {
    tables: HashMap<String, TableSchema>,
}

impl SchemaManager {
    /// Load all schemas from the database's `_schema` table.
    pub fn load(db: &Database) -> Result<Self> {
        let mut tables = HashMap::new();

        let mut rtx = db.begin_read();
        let mut parse_err: Option<crate::error::SqlError> = None;
        let scan_result = rtx.table_for_each(SCHEMA_TABLE, |_key, value| {
            match TableSchema::deserialize(value) {
                Ok(schema) => { tables.insert(schema.name.clone(), schema); }
                Err(e) => { parse_err = Some(e); }
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

        Ok(Self { tables })
    }

    pub fn get(&self, name: &str) -> Option<&TableSchema> {
        let lower = name.to_ascii_lowercase();
        self.tables.get(&lower)
    }

    pub fn contains(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        self.tables.contains_key(&lower)
    }

    pub fn register(&mut self, schema: TableSchema) {
        let lower = schema.name.to_ascii_lowercase();
        self.tables.insert(lower, schema);
    }

    pub fn remove(&mut self, name: &str) -> Option<TableSchema> {
        let lower = name.to_ascii_lowercase();
        self.tables.remove(&lower)
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
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
    pub fn delete_schema(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
        name: &str,
    ) -> Result<()> {
        let lower = name.to_ascii_lowercase();
        wtx.table_delete(SCHEMA_TABLE, lower.as_bytes())
            .map_err(|e| match e {
                citadel_core::Error::TableNotFound(_) => {
                    SqlError::TableNotFound(name.into())
                }
                other => SqlError::Storage(other),
            })?;
        Ok(())
    }

    /// Ensure the _schema table exists (called once per write).
    pub fn ensure_schema_table(
        wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    ) -> Result<()> {
        // Try to create; ignore if already exists
        match wtx.create_table(SCHEMA_TABLE) {
            Ok(()) => Ok(()),
            Err(citadel_core::Error::TableAlreadyExists(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
