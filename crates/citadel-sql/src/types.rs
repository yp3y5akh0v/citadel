use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

pub use compact_str::CompactString;

use crate::parser::Expr;

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
#[derive(Debug, Clone, Default)]
pub enum Value {
    #[default]
    Null,
    Integer(i64),
    Real(f64),
    Text(CompactString),
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

    pub fn coerce_into(self, target: DataType) -> Option<Value> {
        if self.is_null() || target == DataType::Null {
            return Some(Value::Null);
        }
        if self.data_type() == target {
            return Some(self);
        }
        match (self, target) {
            (Value::Integer(i), DataType::Real) => Some(Value::Real(i as f64)),
            (Value::Real(r), DataType::Integer) => Some(Value::Integer(r as i64)),
            (Value::Boolean(b), DataType::Integer) => Some(Value::Integer(if b { 1 } else { 0 })),
            (Value::Integer(i), DataType::Boolean) => Some(Value::Boolean(i != 0)),
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
            Value::Text(s) => {
                2u8.hash(state);
                s.hash(state);
            }
            Value::Blob(b) => {
                3u8.hash(state);
                b.hash(state);
            }
            Value::Boolean(b) => {
                4u8.hash(state);
                b.hash(state);
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        // NULL < BOOLEAN < INTEGER/REAL (numeric) < TEXT < BLOB
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Boolean(_), _) => Ordering::Less,
            (_, Value::Boolean(_)) => Ordering::Greater,

            // Numeric: Integer and Real are comparable
            (Value::Integer(_) | Value::Real(_), Value::Integer(_) | Value::Real(_)) => {
                self.numeric_cmp(other).unwrap_or(Ordering::Equal)
            }
            (Value::Integer(_) | Value::Real(_), _) => Ordering::Less,
            (_, Value::Integer(_) | Value::Real(_)) => Ordering::Greater,

            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Text(_), _) => Ordering::Less,
            (_, Value::Text(_)) => Ordering::Greater,

            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
        }
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
    pub default_expr: Option<Expr>,
    pub default_sql: Option<String>,
    pub check_expr: Option<Expr>,
    pub check_sql: Option<String>,
    pub check_name: Option<String>,
}

/// Index definition stored as part of the table schema.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<u16>,
    pub unique: bool,
}

/// View definition stored in the _views metadata table.
#[derive(Debug, Clone)]
pub struct ViewDef {
    pub name: String,
    pub sql: String,
    pub column_aliases: Vec<String>,
}

const VIEW_DEF_VERSION: u8 = 1;

impl ViewDef {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(VIEW_DEF_VERSION);

        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        let sql_bytes = self.sql.as_bytes();
        buf.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(sql_bytes);

        buf.extend_from_slice(&(self.column_aliases.len() as u16).to_le_bytes());
        for alias in &self.column_aliases {
            let alias_bytes = alias.as_bytes();
            buf.extend_from_slice(&(alias_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(alias_bytes);
        }

        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != VIEW_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid view definition version".into(),
            ));
        }
        let mut pos = 1;

        let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let name = String::from_utf8_lossy(&data[pos..pos + name_len]).into_owned();
        pos += name_len;

        let sql_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            as usize;
        pos += 4;
        let sql = String::from_utf8_lossy(&data[pos..pos + sql_len]).into_owned();
        pos += sql_len;

        let alias_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let mut column_aliases = Vec::with_capacity(alias_count);
        for _ in 0..alias_count {
            let alias_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            let alias = String::from_utf8_lossy(&data[pos..pos + alias_len]).into_owned();
            pos += alias_len;
            column_aliases.push(alias);
        }

        Ok(Self {
            name,
            sql,
            column_aliases,
        })
    }
}

/// Table-level CHECK constraint stored in schema.
#[derive(Debug, Clone)]
pub struct TableCheckDef {
    pub name: Option<String>,
    pub expr: Expr,
    pub sql: String,
}

/// Foreign key definition stored in schema.
#[derive(Debug, Clone)]
pub struct ForeignKeySchemaEntry {
    pub name: Option<String>,
    pub columns: Vec<u16>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
}

/// Table schema stored in the _schema table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key_columns: Vec<u16>,
    pub indices: Vec<IndexDef>,
    pub check_constraints: Vec<TableCheckDef>,
    pub foreign_keys: Vec<ForeignKeySchemaEntry>,
    pk_idx_cache: Vec<usize>,
    non_pk_idx_cache: Vec<usize>,
    /// Physical encoding slots that have been dropped (O(1) DROP COLUMN).
    /// Sorted. Old rows still have data at these positions (skipped on decode);
    /// new rows encode NULL there to maintain position consistency.
    dropped_non_pk_slots: Vec<u16>,
    /// Physical encoding position -> logical column index.
    /// `usize::MAX` for dropped slots.
    decode_mapping_cache: Vec<usize>,
    /// Logical non-PK order -> physical encoding position.
    /// `encoding_positions_cache[i]` is the physical slot for `non_pk_idx_cache[i]`.
    encoding_positions_cache: Vec<u16>,
}

impl TableSchema {
    pub fn new(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_columns: Vec<u16>,
        indices: Vec<IndexDef>,
        check_constraints: Vec<TableCheckDef>,
        foreign_keys: Vec<ForeignKeySchemaEntry>,
    ) -> Self {
        Self::with_drops(
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            vec![],
        )
    }

    pub fn with_drops(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_columns: Vec<u16>,
        indices: Vec<IndexDef>,
        check_constraints: Vec<TableCheckDef>,
        foreign_keys: Vec<ForeignKeySchemaEntry>,
        dropped_non_pk_slots: Vec<u16>,
    ) -> Self {
        let pk_idx_cache: Vec<usize> = primary_key_columns.iter().map(|&i| i as usize).collect();
        let non_pk_idx_cache: Vec<usize> = (0..columns.len())
            .filter(|i| !primary_key_columns.contains(&(*i as u16)))
            .collect();

        let physical_count = non_pk_idx_cache.len() + dropped_non_pk_slots.len();
        let mut decode_mapping_cache = vec![usize::MAX; physical_count];
        let mut encoding_positions_cache = Vec::with_capacity(non_pk_idx_cache.len());

        let mut drop_idx = 0;
        let mut live_idx = 0;
        for (phys_pos, slot) in decode_mapping_cache.iter_mut().enumerate() {
            if drop_idx < dropped_non_pk_slots.len()
                && dropped_non_pk_slots[drop_idx] as usize == phys_pos
            {
                drop_idx += 1;
            } else {
                *slot = non_pk_idx_cache[live_idx];
                encoding_positions_cache.push(phys_pos as u16);
                live_idx += 1;
            }
        }

        Self {
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            pk_idx_cache,
            non_pk_idx_cache,
            dropped_non_pk_slots,
            decode_mapping_cache,
            encoding_positions_cache,
        }
    }

    /// Rebuild caches (preserving dropped slots). Use after mutating fields in place.
    pub fn rebuild(self) -> Self {
        let drops = self.dropped_non_pk_slots;
        Self::with_drops(
            self.name,
            self.columns,
            self.primary_key_columns,
            self.indices,
            self.check_constraints,
            self.foreign_keys,
            drops,
        )
    }

    /// Returns true if any column-level or table-level CHECK constraints exist.
    pub fn has_checks(&self) -> bool {
        !self.check_constraints.is_empty() || self.columns.iter().any(|c| c.check_expr.is_some())
    }

    /// Physical encoding position -> logical column index mapping.
    /// Length = physical_non_pk_count. `usize::MAX` for dropped slots.
    pub fn decode_col_mapping(&self) -> &[usize] {
        &self.decode_mapping_cache
    }

    /// Logical non-PK order -> physical encoding position.
    /// `encoding_positions()[i]` is the physical slot for `non_pk_indices()[i]`.
    pub fn encoding_positions(&self) -> &[u16] {
        &self.encoding_positions_cache
    }

    /// Total physical non-PK column count (live + dropped slots).
    pub fn physical_non_pk_count(&self) -> usize {
        self.non_pk_idx_cache.len() + self.dropped_non_pk_slots.len()
    }

    /// Physical encoding slots that have been dropped via O(1) DROP COLUMN.
    pub fn dropped_non_pk_slots(&self) -> &[u16] {
        &self.dropped_non_pk_slots
    }

    /// Create a new schema with the column at `drop_pos` removed.
    /// O(1): marks the physical encoding slot as dropped instead of rewriting rows.
    /// Decrements all logical position references > drop_pos. Filters out
    /// table-level CHECK constraints referencing the dropped column.
    pub fn without_column(&self, drop_pos: usize) -> Self {
        // Find physical encoding slot for the dropped column
        let non_pk_order = self
            .non_pk_idx_cache
            .iter()
            .position(|&i| i == drop_pos)
            .expect("cannot drop PK column via without_column");
        let physical_slot = self.encoding_positions_cache[non_pk_order];

        let mut new_dropped = self.dropped_non_pk_slots.clone();
        new_dropped.push(physical_slot);
        new_dropped.sort();

        let dropped_name = &self.columns[drop_pos].name;
        let drop_pos_u16 = drop_pos as u16;

        let mut columns: Vec<ColumnDef> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != drop_pos)
            .map(|(_, c)| {
                let mut col = c.clone();
                if col.position > drop_pos_u16 {
                    col.position -= 1;
                }
                col
            })
            .collect();
        for (i, col) in columns.iter_mut().enumerate() {
            col.position = i as u16;
        }

        let primary_key_columns: Vec<u16> = self
            .primary_key_columns
            .iter()
            .map(|&p| if p > drop_pos_u16 { p - 1 } else { p })
            .collect();

        let indices: Vec<IndexDef> = self
            .indices
            .iter()
            .map(|idx| IndexDef {
                name: idx.name.clone(),
                columns: idx
                    .columns
                    .iter()
                    .map(|&c| if c > drop_pos_u16 { c - 1 } else { c })
                    .collect(),
                unique: idx.unique,
            })
            .collect();

        let foreign_keys: Vec<ForeignKeySchemaEntry> = self
            .foreign_keys
            .iter()
            .map(|fk| ForeignKeySchemaEntry {
                name: fk.name.clone(),
                columns: fk
                    .columns
                    .iter()
                    .map(|&c| if c > drop_pos_u16 { c - 1 } else { c })
                    .collect(),
                foreign_table: fk.foreign_table.clone(),
                referred_columns: fk.referred_columns.clone(),
            })
            .collect();

        // Filter out table-level CHECKs that reference the dropped column
        let dropped_lower = dropped_name.to_ascii_lowercase();
        let check_constraints: Vec<TableCheckDef> = self
            .check_constraints
            .iter()
            .filter(|c| !c.sql.to_ascii_lowercase().contains(&dropped_lower))
            .cloned()
            .collect();

        Self::with_drops(
            self.name.clone(),
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            new_dropped,
        )
    }
}

const SCHEMA_VERSION: u8 = 4;

fn write_opt_string(buf: &mut Vec<u8>, s: &Option<String>) {
    match s {
        Some(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        None => buf.extend_from_slice(&0u16.to_le_bytes()),
    }
}

fn read_opt_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if len == 0 {
        None
    } else {
        let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
        *pos += len;
        Some(s)
    }
}

fn read_string(data: &[u8], pos: &mut usize) -> String {
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
    *pos += len;
    s
}

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

        // Columns (v1/v2 core fields)
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

        // ── v3: per-column defaults and checks ──
        for col in &self.columns {
            let mut flags: u8 = 0;
            if col.default_sql.is_some() {
                flags |= 1;
            }
            if col.check_sql.is_some() {
                flags |= 2;
            }
            buf.push(flags);
            if let Some(ref sql) = col.default_sql {
                let bytes = sql.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            if let Some(ref sql) = col.check_sql {
                let bytes = sql.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
                write_opt_string(&mut buf, &col.check_name);
            }
        }

        // ── v3: table-level check constraints ──
        buf.extend_from_slice(&(self.check_constraints.len() as u16).to_le_bytes());
        for chk in &self.check_constraints {
            write_opt_string(&mut buf, &chk.name);
            let sql_bytes = chk.sql.as_bytes();
            buf.extend_from_slice(&(sql_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(sql_bytes);
        }

        // ── v3: foreign keys ──
        buf.extend_from_slice(&(self.foreign_keys.len() as u16).to_le_bytes());
        for fk in &self.foreign_keys {
            write_opt_string(&mut buf, &fk.name);
            buf.extend_from_slice(&(fk.columns.len() as u16).to_le_bytes());
            for &col_idx in &fk.columns {
                buf.extend_from_slice(&col_idx.to_le_bytes());
            }
            let ft_bytes = fk.foreign_table.as_bytes();
            buf.extend_from_slice(&(ft_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(ft_bytes);
            buf.extend_from_slice(&(fk.referred_columns.len() as u16).to_le_bytes());
            for rc in &fk.referred_columns {
                let rc_bytes = rc.as_bytes();
                buf.extend_from_slice(&(rc_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(rc_bytes);
            }
        }

        // v4: dropped non-PK encoding slots
        buf.extend_from_slice(&(self.dropped_non_pk_slots.len() as u16).to_le_bytes());
        for &slot in &self.dropped_non_pk_slots {
            buf.extend_from_slice(&slot.to_le_bytes());
        }

        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        let mut pos = 0;

        if data.is_empty() || !matches!(data[0], 1 | 2 | 3 | SCHEMA_VERSION) {
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
                default_expr: None,
                default_sql: None,
                check_expr: None,
                check_sql: None,
                check_name: None,
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

        // Indices (v2+)
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
                idxs.push(IndexDef {
                    name: idx_name,
                    columns: cols,
                    unique,
                });
            }
            idxs
        } else {
            vec![]
        };

        // v3: per-column defaults and checks
        let mut check_constraints = Vec::new();
        let mut foreign_keys = Vec::new();

        if version >= 3 && pos < data.len() {
            for col in &mut columns {
                let flags = data[pos];
                pos += 1;
                if flags & 1 != 0 {
                    let sql = read_string(data, &mut pos);
                    col.default_expr = Some(crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse DEFAULT expression: {sql}"
                        ))
                    })?);
                    col.default_sql = Some(sql);
                }
                if flags & 2 != 0 {
                    let sql = read_string(data, &mut pos);
                    col.check_expr = Some(crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse CHECK expression: {sql}"
                        ))
                    })?);
                    col.check_sql = Some(sql);
                    col.check_name = read_opt_string(data, &mut pos);
                }
            }

            // Table-level check constraints
            let chk_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            for _ in 0..chk_count {
                let name = read_opt_string(data, &mut pos);
                let sql = read_string(data, &mut pos);
                let expr = crate::parser::parse_sql_expr(&sql).map_err(|_| {
                    crate::error::SqlError::InvalidValue(format!(
                        "cannot parse CHECK expression: {sql}"
                    ))
                })?;
                check_constraints.push(TableCheckDef { name, expr, sql });
            }

            // Foreign keys
            let fk_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            for _ in 0..fk_count {
                let name = read_opt_string(data, &mut pos);
                let col_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                let mut cols = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let col_idx = u16::from_le_bytes([data[pos], data[pos + 1]]);
                    pos += 2;
                    cols.push(col_idx);
                }
                let foreign_table = read_string(data, &mut pos);
                let ref_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                let mut referred_columns = Vec::with_capacity(ref_count);
                for _ in 0..ref_count {
                    referred_columns.push(read_string(data, &mut pos));
                }
                foreign_keys.push(ForeignKeySchemaEntry {
                    name,
                    columns: cols,
                    foreign_table,
                    referred_columns,
                });
            }
        }
        // v4: dropped non-PK encoding slots
        let mut dropped_non_pk_slots = Vec::new();
        if version >= 4 && pos + 2 <= data.len() {
            let slot_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            for _ in 0..slot_count {
                let slot = u16::from_le_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                dropped_non_pk_slots.push(slot);
            }
        }
        let _ = pos;

        Ok(Self::with_drops(
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            dropped_non_pk_slots,
        ))
    }

    /// Get column index by name (case-insensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get indices of non-PK columns (columns stored in the B+ tree value).
    pub fn non_pk_indices(&self) -> &[usize] {
        &self.non_pk_idx_cache
    }

    /// Get the PK column indices as usize.
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_idx_cache
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
#[derive(Debug, Clone)]
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
        assert_eq!(format!("{}", Value::Real(3.15)), "3.15");
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
        assert_eq!(Value::Null.coerce_to(DataType::Integer), Some(Value::Null));
        assert_eq!(Value::Text("x".into()).coerce_to(DataType::Integer), None);
    }

    fn col(name: &str, dt: DataType, nullable: bool, pos: u16) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: dt,
            nullable,
            position: pos,
            default_expr: None,
            default_sql: None,
            check_expr: None,
            check_sql: None,
            check_name: None,
        }
    }

    #[test]
    fn schema_roundtrip() {
        let schema = TableSchema::new(
            "users".into(),
            vec![
                col("id", DataType::Integer, false, 0),
                col("name", DataType::Text, true, 1),
                col("active", DataType::Boolean, false, 2),
            ],
            vec![0],
            vec![],
            vec![],
            vec![],
        );

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
        let schema = TableSchema::new(
            "orders".into(),
            vec![
                col("id", DataType::Integer, false, 0),
                col("customer", DataType::Text, false, 1),
                col("amount", DataType::Real, true, 2),
            ],
            vec![0],
            vec![
                IndexDef {
                    name: "idx_customer".into(),
                    columns: vec![1],
                    unique: false,
                },
                IndexDef {
                    name: "idx_amount_uniq".into(),
                    columns: vec![2],
                    unique: true,
                },
            ],
            vec![],
            vec![],
        );

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
        let old_schema = TableSchema::new(
            "test".into(),
            vec![col("id", DataType::Integer, false, 0)],
            vec![0],
            vec![],
            vec![],
            vec![],
        );
        let mut data = old_schema.serialize();
        // Patch to v1 format: replace version byte and truncate everything after PK
        data[0] = 1;
        // v1 has no indices or v3 data - truncate after PK columns
        // Header(1) + name_len(2) + "test"(4) + col_count(2) + col("id": name_len(2)+"id"(2)+type(1)+nullable(1)+position(2)) + pk_count(2) + pk(2)
        let v1_len = 1 + 2 + 4 + 2 + (2 + 2 + 1 + 1 + 2) + 2 + 2;
        data.truncate(v1_len);

        let restored = TableSchema::deserialize(&data).unwrap();
        assert_eq!(restored.name, "test");
        assert!(restored.indices.is_empty());
        assert!(restored.check_constraints.is_empty());
        assert!(restored.foreign_keys.is_empty());
    }

    #[test]
    fn schema_v2_backward_compat() {
        let schema = TableSchema::new(
            "test".into(),
            vec![col("id", DataType::Integer, false, 0)],
            vec![0],
            vec![],
            vec![],
            vec![],
        );
        let mut data = schema.serialize();
        // Patch version to 2 and truncate v3 data
        data[0] = 2;
        // v2 ends after indices section: find the v3 start and truncate
        // Header(1) + name_len(2) + "test"(4) + col_count(2) + col(8) + pk_count(2) + pk(2) + idx_count(2)
        let v2_len = 1 + 2 + 4 + 2 + 8 + 2 + 2 + 2;
        data.truncate(v2_len);

        let restored = TableSchema::deserialize(&data).unwrap();
        assert_eq!(restored.name, "test");
        assert!(restored.check_constraints.is_empty());
        assert!(restored.foreign_keys.is_empty());
        assert!(restored.columns[0].default_expr.is_none());
        assert!(restored.columns[0].check_expr.is_none());
    }

    #[test]
    fn schema_roundtrip_with_defaults_and_checks() {
        use crate::parser::parse_sql_expr;

        let mut columns = vec![
            col("id", DataType::Integer, false, 0),
            col("val", DataType::Integer, true, 1),
            col("name", DataType::Text, true, 2),
        ];
        columns[1].default_sql = Some("42".into());
        columns[1].default_expr = Some(parse_sql_expr("42").unwrap());
        columns[2].check_sql = Some("LENGTH(name) > 0".into());
        columns[2].check_expr = Some(parse_sql_expr("LENGTH(name) > 0").unwrap());
        columns[2].check_name = Some("chk_name_len".into());

        let schema = TableSchema::new(
            "t".into(),
            columns,
            vec![0],
            vec![],
            vec![TableCheckDef {
                name: Some("chk_val_pos".into()),
                expr: parse_sql_expr("val > 0").unwrap(),
                sql: "val > 0".into(),
            }],
            vec![],
        );

        let data = schema.serialize();
        let restored = TableSchema::deserialize(&data).unwrap();

        assert_eq!(restored.columns[1].default_sql.as_deref(), Some("42"));
        assert!(restored.columns[1].default_expr.is_some());
        assert_eq!(
            restored.columns[2].check_sql.as_deref(),
            Some("LENGTH(name) > 0")
        );
        assert!(restored.columns[2].check_expr.is_some());
        assert_eq!(
            restored.columns[2].check_name.as_deref(),
            Some("chk_name_len")
        );
        assert_eq!(restored.check_constraints.len(), 1);
        assert_eq!(
            restored.check_constraints[0].name.as_deref(),
            Some("chk_val_pos")
        );
        assert_eq!(restored.check_constraints[0].sql, "val > 0");
    }

    #[test]
    fn schema_roundtrip_with_foreign_keys() {
        let schema = TableSchema::new(
            "orders".into(),
            vec![
                col("id", DataType::Integer, false, 0),
                col("user_id", DataType::Integer, false, 1),
            ],
            vec![0],
            vec![],
            vec![],
            vec![ForeignKeySchemaEntry {
                name: Some("fk_user".into()),
                columns: vec![1],
                foreign_table: "users".into(),
                referred_columns: vec!["id".into()],
            }],
        );

        let data = schema.serialize();
        let restored = TableSchema::deserialize(&data).unwrap();

        assert_eq!(restored.foreign_keys.len(), 1);
        assert_eq!(restored.foreign_keys[0].name.as_deref(), Some("fk_user"));
        assert_eq!(restored.foreign_keys[0].columns, vec![1]);
        assert_eq!(restored.foreign_keys[0].foreign_table, "users");
        assert_eq!(restored.foreign_keys[0].referred_columns, vec!["id"]);
    }

    #[test]
    fn data_type_display() {
        assert_eq!(format!("{}", DataType::Integer), "INTEGER");
        assert_eq!(format!("{}", DataType::Text), "TEXT");
        assert_eq!(format!("{}", DataType::Boolean), "BOOLEAN");
    }
}
