use wasm_bindgen::prelude::*;
use js_sys::{Array, Object, Reflect};
use serde::Serialize;

use crate::{CitadelDb as InnerDb, CellValue};

#[wasm_bindgen(js_name = "CitadelDb")]
pub struct JsCitadelDb {
    inner: InnerDb,
}

#[wasm_bindgen(js_class = "CitadelDb")]
impl JsCitadelDb {
    /// Create a new in-memory encrypted database.
    #[wasm_bindgen(constructor)]
    pub fn new(passphrase: &str) -> Result<JsCitadelDb, JsValue> {
        let inner = InnerDb::create(passphrase)
            .map_err(|e| JsValue::from_str(&e))?;
        Ok(Self { inner })
    }

    /// Execute a SQL statement (DDL or DML).
    /// Returns the number of rows affected.
    pub fn execute(&self, sql: &str) -> Result<f64, JsValue> {
        self.inner
            .execute(sql)
            .map(|n| n as f64)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Execute a SQL query and return results as a JS object.
    ///
    /// Returns `{ columns: string[], rows: any[][] }`.
    pub fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        let result = self.inner
            .query(sql)
            .map_err(|e| JsValue::from_str(&e))?;

        let obj = Object::new();

        // Build columns array
        let cols = Array::new();
        for name in &result.columns {
            cols.push(&JsValue::from_str(name));
        }
        Reflect::set(&obj, &JsValue::from_str("columns"), &cols)?;

        // Build rows array
        let rows = Array::new();
        for row in &result.rows {
            let js_row = Array::new();
            for cell in row {
                js_row.push(&cell_to_js(cell));
            }
            rows.push(&js_row);
        }
        Reflect::set(&obj, &JsValue::from_str("rows"), &rows)?;

        Ok(obj.into())
    }

    /// Execute multiple SQL statements separated by semicolons.
    #[wasm_bindgen(js_name = "executeBatch")]
    pub fn execute_batch(&self, sql: &str) -> Result<(), JsValue> {
        self.inner
            .execute_batch(sql)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Put a key-value pair into the default table.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
        self.inner
            .put(key, value)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Get a value by key from the default table.
    /// Returns null if not found.
    pub fn get(&self, key: &[u8]) -> Result<JsValue, JsValue> {
        match self.inner.get(key).map_err(|e| JsValue::from_str(&e))? {
            Some(v) => Ok(js_sys::Uint8Array::from(v.as_slice()).into()),
            None => Ok(JsValue::NULL),
        }
    }

    /// Delete a key from the default table.
    /// Returns true if the key existed.
    pub fn delete(&self, key: &[u8]) -> Result<bool, JsValue> {
        self.inner
            .delete(key)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Put a key-value pair into a named table.
    #[wasm_bindgen(js_name = "tablePut")]
    pub fn table_put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<(), JsValue> {
        self.inner
            .table_put(table, key, value)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Get a value by key from a named table.
    /// Returns null if not found.
    #[wasm_bindgen(js_name = "tableGet")]
    pub fn table_get(&self, table: &str, key: &[u8]) -> Result<JsValue, JsValue> {
        match self.inner.table_get(table, key).map_err(|e| JsValue::from_str(&e))? {
            Some(v) => Ok(js_sys::Uint8Array::from(v.as_slice()).into()),
            None => Ok(JsValue::NULL),
        }
    }

    /// Delete a key from a named table.
    /// Returns true if the key existed.
    #[wasm_bindgen(js_name = "tableDelete")]
    pub fn table_delete(&self, table: &str, key: &[u8]) -> Result<bool, JsValue> {
        self.inner
            .table_delete(table, key)
            .map_err(|e| JsValue::from_str(&e))
    }

    /// Get database statistics.
    /// Returns `{ entryCount: number, totalPages: number, treeDepth: number }`.
    pub fn stats(&self) -> Result<JsValue, JsValue> {
        let s = self.inner.stats();
        let obj = Object::new();
        Reflect::set(&obj, &JsValue::from_str("entryCount"), &JsValue::from_f64(s.entry_count as f64))?;
        Reflect::set(&obj, &JsValue::from_str("totalPages"), &JsValue::from_f64(s.total_pages as f64))?;
        Reflect::set(&obj, &JsValue::from_str("treeDepth"), &JsValue::from_f64(s.tree_depth as f64))?;
        Ok(obj.into())
    }
}

fn cell_to_js(cell: &CellValue) -> JsValue {
    match cell {
        CellValue::Null => JsValue::NULL,
        CellValue::Integer(i) => JsValue::from_f64(*i as f64),
        CellValue::Real(r) => JsValue::from_f64(*r),
        CellValue::Text(s) => JsValue::from_str(s),
        CellValue::Blob(b) => js_sys::Uint8Array::from(b.as_slice()).into(),
        CellValue::Boolean(b) => JsValue::from_bool(*b),
    }
}
