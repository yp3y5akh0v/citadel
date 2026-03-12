use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// SQL data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
    Boolean,
}

impl DataType {
    pub fn type_tag(self) -> u8 {
        match self {
            DataType::Null => 0,
            DataType::Blob => 1,
            DataType::Text => 2,
            DataType::Boolean => 3,
            DataType::Integer => 4,
            DataType::Real => 5,
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(DataType::Null),
            1 => Some(DataType::Blob),
            2 => Some(DataType::Text),
            3 => Some(DataType::Boolean),
            4 => Some(DataType::Integer),
            5 => Some(DataType::Real),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

/// SQL value.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Integer(_) => DataType::Integer,
            Value::Real(_) => DataType::Real,
            Value::Text(_) => DataType::Text,
            Value::Blob(_) => DataType::Blob,
            Value::Boolean(_) => DataType::Boolean,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Attempt to coerce this value to the target type.
    pub fn coerce_to(&self, target: DataType) -> Option<Value> {
        match (self, target) {
            (_, DataType::Null) => Some(Value::Null),
            (Value::Null, _) => Some(Value::Null),
            (Value::Integer(i), DataType::Integer) => Some(Value::Integer(*i)),
            (Value::Integer(i), DataType::Real) => Some(Value::Real(*i as f64)),
            (Value::Real(r), DataType::Real) => Some(Value::Real(*r)),
            (Value::Real(r), DataType::Integer) => Some(Value::Integer(*r as i64)),
            (Value::Text(s), DataType::Text) => Some(Value::Text(s.clone())),
            (Value::Blob(b), DataType::Blob) => Some(Value::Blob(b.clone())),
            (Value::Boolean(b), DataType::Boolean) => Some(Value::Boolean(*b)),
            (Value::Boolean(b), DataType::Integer) => Some(Value::Integer(if *b { 1 } else { 0 })),
            (Value::Integer(i), DataType::Boolean) => Some(Value::Boolean(*i != 0)),
            _ => None,
        }
    }

    /// Numeric ordering for Integer and Real values (promotes to f64 for mixed).
    fn numeric_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b),
            (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a == b,
            (Value::Integer(a), Value::Real(b)) => (*a as f64) == *b,
            (Value::Real(a), Value::Integer(b)) => *a == (*b as f64),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Integer(i) => {
                // Hash via f64 bits so Integer(n) and Real(n.0) produce the same hash,
                // matching the cross-type PartialEq contract.
                1u8.hash(state);
                (*i as f64).to_bits().hash(state);
            }
            Value::Real(r) => {
                1u8.hash(state);
                r.to_bits().hash(state);
            }
            Value::Text(s) => { 2u8.hash(state); s.hash(state); }
            Value::Blob(b) => { 3u8.hash(state); b.hash(state); }
            Value::Boolean(b) => { 4u8.hash(state); b.hash(state); }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // NULL < BOOLEAN < INTEGER/REAL (numeric) < TEXT < BLOB
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            (Value::Boolean(_), _) => Some(Ordering::Less),
            (_, Value::Boolean(_)) => Some(Ordering::Greater),

            // Numeric: Integer and Real are comparable
            (Value::Integer(_) | Value::Real(_), Value::Integer(_) | Value::Real(_)) => {
                self.numeric_cmp(other)
            }
            (Value::Integer(_) | Value::Real(_), _) => Some(Ordering::Less),
            (_, Value::Integer(_) | Value::Real(_)) => Some(Ordering::Greater),

            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Text(_), _) => Some(Ordering::Less),
            (_, Value::Text(_)) => Some(Ordering::Greater),

            (Value::Blob(a), Value::Blob(b)) => Some(a.cmp(b)),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Real(r) => {
                if r.fract() == 0.0 && r.is_finite() {
                    write!(f, "{r:.1}")
                } else {
                    write!(f, "{r}")
                }
            }
            Value::Text(s) => write!(f, "{s}"),
            Value::Blob(b) => write!(f, "X'{}'", hex_encode(b)),
            Value::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
        }
    }
}

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for byte in data {
        s.push_str(&format!("{byte:02X}"));
    }
    s
}

/// Column definition.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub position: u16,
}

/// Index definition stored as part of the table schema.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<u16>,
    pub unique: bool,
}

/// Table schema stored in the _schema table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key_columns: Vec<u16>,
    pub indices: Vec<IndexDef>,
}

const SCHEMA_VERSION: u8 = 2;

impl TableSchema {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(SCHEMA_VERSION);

        // Table name
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        // Column count
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        // Columns
        for col in &self.columns {
            let col_name = col.name.as_bytes();
            buf.extend_from_slice(&(col_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(col_name);
            buf.push(col.data_type.type_tag());
            buf.push(if col.nullable { 1 } else { 0 });
            buf.extend_from_slice(&col.position.to_le_bytes());
        }

        // Primary key columns
        buf.extend_from_slice(&(self.primary_key_columns.len() as u16).to_le_bytes());
        for &pk_idx in &self.primary_key_columns {
            buf.extend_from_slice(&pk_idx.to_le_bytes());
        }

        // Indices (v2+)
        buf.extend_from_slice(&(self.indices.len() as u16).to_le_bytes());
        for idx in &self.indices {
            let idx_name = idx.name.as_bytes();
            buf.extend_from_slice(&(idx_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(idx_name);
            buf.extend_from_slice(&(idx.columns.len() as u16).to_le_bytes());
            for &col_idx in &idx.columns {
                buf.extend_from_slice(&col_idx.to_le_bytes());
            }
            buf.push(if idx.unique { 1 } else { 0 });
        }

        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        let mut pos = 0;

        if data.is_empty() || (data[0] != SCHEMA_VERSION && data[0] != 1) {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid schema version".into(),
            ));
        }
        let version = data[0];
        pos += 1;

        // Table name
        let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let name = String::from_utf8_lossy(&data[pos..pos + name_len]).into_owned();
        pos += name_len;

        // Column count
        let col_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let col_name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let col_name = String::from_utf8_lossy(&data[pos..pos + col_name_len]).into_owned();
            pos += col_name_len;
            let data_type = DataType::from_tag(data[pos]).ok_or_else(|| {
                crate::error::SqlError::InvalidValue("unknown data type tag".into())
            })?;
            pos += 1;
            let nullable = data[pos] != 0;
            pos += 1;
            let position = u16::from_le_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            columns.push(ColumnDef {
                name: col_name,
                data_type,
                nullable,
                position,
            });
        }

        // Primary key columns
        let pk_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let mut primary_key_columns = Vec::with_capacity(pk_count);
        for _ in 0..pk_count {
            let pk_idx = u16::from_le_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            primary_key_columns.push(pk_idx);
        }
        let indices = if version >= 2 && pos + 2 <= data.len() {
            let idx_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let mut idxs = Vec::with_capacity(idx_count);
            for _ in 0..idx_count {
                let idx_name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                let idx_name = String::from_utf8_lossy(&data[pos..pos + idx_name_len]).into_owned();
                pos += idx_name_len;
                let col_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                let mut cols = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let col_idx = u16::from_le_bytes([data[pos], data[pos + 1]]);
                    pos += 2;
                    cols.push(col_idx);
                }
                let unique = data[pos] != 0;
                pos += 1;
                idxs.push(IndexDef { name: idx_name, columns: cols, unique });
            }
            idxs
        } else {
            vec![]
        };
        let _ = pos;

        Ok(Self {
            name,
            columns,
            primary_key_columns,
            indices,
        })
    }

    /// Get column index by name (case-insensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let lower = name.to_ascii_lowercase();
        self.columns.iter().position(|c| c.name.to_ascii_lowercase() == lower)
    }

    /// Get indices of non-PK columns (columns stored in the B+ tree value).
    pub fn non_pk_indices(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|i| !self.primary_key_columns.contains(&(*i as u16)))
            .collect()
    }

    /// Get the PK column indices as usize.
    pub fn pk_indices(&self) -> Vec<usize> {
        self.primary_key_columns.iter().map(|&i| i as usize).collect()
    }

    /// Get index definition by name (case-insensitive).
    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        let lower = name.to_ascii_lowercase();
        self.indices.iter().find(|i| i.name == lower)
    }

    /// Get the KV table name for an index.
    pub fn index_table_name(table_name: &str, index_name: &str) -> Vec<u8> {
        format!("__idx_{table_name}_{index_name}").into_bytes()
    }
}

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum ExecutionResult {
    RowsAffected(u64),
    Query(QueryResult),
    Ok,
}

/// Result of a SELECT query.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_ordering() {
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Boolean(false) < Value::Boolean(true));
        assert!(Value::Boolean(true) < Value::Integer(0));
        assert!(Value::Integer(-1) < Value::Integer(0));
        assert!(Value::Integer(0) < Value::Real(0.5));
        assert!(Value::Real(1.0) < Value::Text("".into()));
        assert!(Value::Text("a".into()) < Value::Text("b".into()));
        assert!(Value::Text("z".into()) < Value::Blob(vec![]));
        assert!(Value::Blob(vec![0]) < Value::Blob(vec![1]));
    }

    #[test]
    fn value_numeric_mixed() {
        assert_eq!(Value::Integer(1), Value::Real(1.0));
        assert!(Value::Integer(1) < Value::Real(1.5));
        assert!(Value::Real(0.5) < Value::Integer(1));
    }

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Real(3.14)), "3.14");
        assert_eq!(format!("{}", Value::Real(1.0)), "1.0");
        assert_eq!(format!("{}", Value::Text("hello".into())), "hello");
        assert_eq!(format!("{}", Value::Blob(vec![0xDE, 0xAD])), "X'DEAD'");
        assert_eq!(format!("{}", Value::Boolean(true)), "TRUE");
        assert_eq!(format!("{}", Value::Boolean(false)), "FALSE");
    }

    #[test]
    fn value_coerce() {
        assert_eq!(
            Value::Integer(42).coerce_to(DataType::Real),
            Some(Value::Real(42.0))
        );
        assert_eq!(
            Value::Boolean(true).coerce_to(DataType::Integer),
            Some(Value::Integer(1))
        );
        assert_eq!(
            Value::Null.coerce_to(DataType::Integer),
            Some(Value::Null)
        );
        assert_eq!(
            Value::Text("x".into()).coerce_to(DataType::Integer),
            None
        );
    }

    #[test]
    fn schema_roundtrip() {
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
                ColumnDef { name: "name".into(), data_type: DataType::Text, nullable: true, position: 1 },
                ColumnDef { name: "active".into(), data_type: DataType::Boolean, nullable: false, position: 2 },
            ],
            primary_key_columns: vec![0],
            indices: vec![],
        };

        let data = schema.serialize();
        let restored = TableSchema::deserialize(&data).unwrap();

        assert_eq!(restored.name, "users");
        assert_eq!(restored.columns.len(), 3);
        assert_eq!(restored.columns[0].name, "id");
        assert_eq!(restored.columns[0].data_type, DataType::Integer);
        assert!(!restored.columns[0].nullable);
        assert_eq!(restored.columns[1].name, "name");
        assert_eq!(restored.columns[1].data_type, DataType::Text);
        assert!(restored.columns[1].nullable);
        assert_eq!(restored.columns[2].name, "active");
        assert_eq!(restored.columns[2].data_type, DataType::Boolean);
        assert_eq!(restored.primary_key_columns, vec![0]);
    }

    #[test]
    fn schema_roundtrip_with_indices() {
        let schema = TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
                ColumnDef { name: "customer".into(), data_type: DataType::Text, nullable: false, position: 1 },
                ColumnDef { name: "amount".into(), data_type: DataType::Real, nullable: true, position: 2 },
            ],
            primary_key_columns: vec![0],
            indices: vec![
                IndexDef { name: "idx_customer".into(), columns: vec![1], unique: false },
                IndexDef { name: "idx_amount_uniq".into(), columns: vec![2], unique: true },
            ],
        };

        let data = schema.serialize();
        let restored = TableSchema::deserialize(&data).unwrap();

        assert_eq!(restored.indices.len(), 2);
        assert_eq!(restored.indices[0].name, "idx_customer");
        assert_eq!(restored.indices[0].columns, vec![1]);
        assert!(!restored.indices[0].unique);
        assert_eq!(restored.indices[1].name, "idx_amount_uniq");
        assert_eq!(restored.indices[1].columns, vec![2]);
        assert!(restored.indices[1].unique);
    }

    #[test]
    fn schema_v1_backward_compat() {
        let old_schema = TableSchema {
            name: "test".into(),
            columns: vec![
                ColumnDef { name: "id".into(), data_type: DataType::Integer, nullable: false, position: 0 },
            ],
            primary_key_columns: vec![0],
            indices: vec![],
        };
        let mut data = old_schema.serialize();
        // Patch to v1 format: replace version byte and truncate index data
        data[0] = 1;
        // Remove the last 2 bytes (index count = 0)
        data.truncate(data.len() - 2);

        let restored = TableSchema::deserialize(&data).unwrap();
        assert_eq!(restored.name, "test");
        assert!(restored.indices.is_empty());
    }

    #[test]
    fn data_type_display() {
        assert_eq!(format!("{}", DataType::Integer), "INTEGER");
        assert_eq!(format!("{}", DataType::Text), "TEXT");
        assert_eq!(format!("{}", DataType::Boolean), "BOOLEAN");
    }
}
