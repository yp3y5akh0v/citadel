use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub use compact_str::CompactString;

use crate::parser::Expr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
    Boolean,
    Time,
    Date,
    Timestamp,
    Interval,
    Json,
    Jsonb,
    TsVector,
    TsQuery,
    Array,
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
            DataType::Time => 6,
            DataType::Date => 7,
            DataType::Timestamp => 8,
            DataType::Interval => 9,
            DataType::Json => 10,
            DataType::Jsonb => 11,
            DataType::TsVector => 12,
            DataType::TsQuery => 13,
            DataType::Array => 14,
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
            6 => Some(DataType::Time),
            7 => Some(DataType::Date),
            8 => Some(DataType::Timestamp),
            9 => Some(DataType::Interval),
            10 => Some(DataType::Json),
            11 => Some(DataType::Jsonb),
            12 => Some(DataType::TsVector),
            13 => Some(DataType::TsQuery),
            14 => Some(DataType::Array),
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
            DataType::Time => write!(f, "TIME"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Json => write!(f, "JSON"),
            DataType::Jsonb => write!(f, "JSONB"),
            DataType::TsVector => write!(f, "TSVECTOR"),
            DataType::TsQuery => write!(f, "TSQUERY"),
            DataType::Array => write!(f, "ARRAY"),
        }
    }
}

/// SQL value. Temporal epochs: days/µs since 1970-01-01 UTC.
/// `Date`/`Timestamp` reserve `i{32,64}::{MAX,MIN}` as `±infinity` sentinels.
#[derive(Debug, Clone, Default)]
pub enum Value {
    #[default]
    Null,
    Integer(i64),
    Real(f64),
    Text(CompactString),
    Blob(Vec<u8>),
    Boolean(bool),
    Time(i64),
    Date(i32),
    Timestamp(i64),
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    Json(CompactString),
    Jsonb(Arc<[u8]>),
    TsVector(Arc<[u8]>),
    TsQuery(Arc<[u8]>),
    Array(Arc<Vec<Value>>),
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
            Value::Time(_) => DataType::Time,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Interval { .. } => DataType::Interval,
            Value::Json(_) => DataType::Json,
            Value::Jsonb(_) => DataType::Jsonb,
            Value::TsVector(_) => DataType::TsVector,
            Value::TsQuery(_) => DataType::TsQuery,
            Value::Array(_) => DataType::Array,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_finite_temporal(&self) -> bool {
        match self {
            Value::Date(d) => *d != i32::MAX && *d != i32::MIN,
            Value::Timestamp(t) => *t != i64::MAX && *t != i64::MIN,
            _ => true,
        }
    }

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
            (Value::Time(t), DataType::Time) => Some(Value::Time(*t)),
            (Value::Date(d), DataType::Date) => Some(Value::Date(*d)),
            (Value::Timestamp(t), DataType::Timestamp) => Some(Value::Timestamp(*t)),
            (Value::TsVector(b), DataType::TsVector) => Some(Value::TsVector(b.clone())),
            (Value::TsQuery(b), DataType::TsQuery) => Some(Value::TsQuery(b.clone())),
            (Value::Array(a), DataType::Array) => Some(Value::Array(a.clone())),
            (
                Value::Interval {
                    months,
                    days,
                    micros,
                },
                DataType::Interval,
            ) => Some(Value::Interval {
                months: *months,
                days: *days,
                micros: *micros,
            }),
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
            (Value::Text(s), DataType::Date) => {
                crate::datetime::parse_date(&s).ok().map(Value::Date)
            }
            (Value::Text(s), DataType::Time) => {
                crate::datetime::parse_time(&s).ok().map(Value::Time)
            }
            (Value::Text(s), DataType::Timestamp) => crate::datetime::parse_timestamp(&s)
                .ok()
                .map(Value::Timestamp),
            (Value::Text(s), DataType::Interval) => {
                crate::datetime::parse_interval(&s)
                    .ok()
                    .map(|(m, d, u)| Value::Interval {
                        months: m,
                        days: d,
                        micros: u,
                    })
            }
            // INTEGER → TIMESTAMP: Unix epoch seconds.
            (Value::Integer(n), DataType::Timestamp) => {
                n.checked_mul(1_000_000).map(Value::Timestamp)
            }
            (Value::Integer(n), DataType::Date) => {
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    Some(Value::Date(n as i32))
                } else {
                    None
                }
            }
            (Value::Integer(n), DataType::Time) => {
                if (0..=86_400_000_000).contains(&n) {
                    Some(Value::Time(n))
                } else {
                    None
                }
            }
            (Value::Integer(n), DataType::Interval) => {
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    Some(Value::Interval {
                        months: 0,
                        days: n as i32,
                        micros: 0,
                    })
                } else {
                    None
                }
            }
            (Value::Timestamp(t), DataType::Integer) => Some(Value::Integer(t / 1_000_000)),
            (Value::Date(d), DataType::Integer) => Some(Value::Integer(d as i64)),
            (Value::Time(t), DataType::Integer) => Some(Value::Integer(t)),
            (Value::Date(d), DataType::Timestamp) => {
                (d as i64).checked_mul(86_400_000_000).map(Value::Timestamp)
            }
            (Value::Timestamp(t), DataType::Date) => {
                // div_euclid floors correctly for negative µs (pre-1970).
                let days = t.div_euclid(86_400_000_000);
                if days >= i32::MIN as i64 && days <= i32::MAX as i64 {
                    Some(Value::Date(days as i32))
                } else {
                    None
                }
            }
            (v, DataType::Text)
                if matches!(
                    v.data_type(),
                    DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval
                ) =>
            {
                Some(Value::Text(v.to_string().into()))
            }
            (Value::Text(s), DataType::Json) => {
                crate::json::validate_text(&s).ok()?;
                Some(Value::Json(s))
            }
            (Value::Text(s), DataType::Jsonb) => crate::json::text_to_jsonb(&s).ok(),
            (Value::Json(s), DataType::Text) => Some(Value::Text(s)),
            (Value::Json(s), DataType::Jsonb) => crate::json::text_to_jsonb(&s).ok(),
            (Value::Jsonb(b), DataType::Text) => crate::json::decode_to_text(&b)
                .ok()
                .map(|t| Value::Text(t.into())),
            (Value::Jsonb(b), DataType::Json) => crate::json::decode_to_text(&b)
                .ok()
                .map(|t| Value::Json(t.into())),
            _ => None,
        }
    }

    pub fn strict_coerce(&self, target: DataType) -> Option<Value> {
        if matches!(self, Value::Null) {
            return Some(Value::Null);
        }
        if self.data_type() == target {
            return Some(self.clone());
        }
        match (self, target) {
            (Value::Integer(i), DataType::Real) => {
                if i.unsigned_abs() <= (1u64 << 53) {
                    Some(Value::Real(*i as f64))
                } else {
                    None
                }
            }
            (Value::Real(r), DataType::Integer) => {
                if r.is_finite()
                    && r.fract() == 0.0
                    && (i64::MIN as f64..=i64::MAX as f64).contains(r)
                {
                    Some(Value::Integer(*r as i64))
                } else {
                    None
                }
            }
            (Value::Boolean(b), DataType::Integer) => Some(Value::Integer(if *b { 1 } else { 0 })),
            (Value::Integer(i), DataType::Boolean) => match i {
                0 => Some(Value::Boolean(false)),
                1 => Some(Value::Boolean(true)),
                _ => None,
            },
            (Value::Text(s), DataType::Integer) => {
                let trimmed = s.as_str();
                let parsed: i64 = trimmed.parse().ok()?;
                if parsed.to_string() == trimmed {
                    Some(Value::Integer(parsed))
                } else {
                    None
                }
            }
            (Value::Text(s), DataType::Real) => {
                let trimmed = s.as_str();
                let parsed: f64 = trimmed.parse().ok()?;
                if parsed.is_finite() {
                    Some(Value::Real(parsed))
                } else {
                    None
                }
            }
            (Value::Text(_), DataType::Date)
            | (Value::Text(_), DataType::Time)
            | (Value::Text(_), DataType::Timestamp)
            | (Value::Text(_), DataType::Interval)
            | (Value::Text(_), DataType::Json)
            | (Value::Text(_), DataType::Jsonb)
            | (Value::Json(_), DataType::Jsonb)
            | (Value::Json(_), DataType::Text)
            | (Value::Jsonb(_), DataType::Json)
            | (Value::Jsonb(_), DataType::Text) => self.clone().coerce_into(target),
            (Value::Date(d), DataType::Timestamp) => (*d as i64)
                .checked_mul(86_400_000_000)
                .map(Value::Timestamp),
            (Value::Timestamp(t), DataType::Date) => {
                if t % 86_400_000_000 == 0 {
                    let days = t.div_euclid(86_400_000_000);
                    if days >= i32::MIN as i64 && days <= i32::MAX as i64 {
                        Some(Value::Date(days as i32))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
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
    // Field-wise for Eq/Hash/Ord transitivity. SQL-level `=` on INTERVAL
    // normalizes separately (see eval.rs).
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
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (
                Value::Interval {
                    months: am,
                    days: ad,
                    micros: au,
                },
                Value::Interval {
                    months: bm,
                    days: bd,
                    micros: bu,
                },
            ) => am == bm && ad == bd && au == bu,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Jsonb(a), Value::Jsonb(b)) => a == b,
            (Value::TsVector(a), Value::TsVector(b)) => a == b,
            (Value::TsQuery(a), Value::TsQuery(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
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
            Value::Time(t) => {
                5u8.hash(state);
                t.hash(state);
            }
            Value::Date(d) => {
                6u8.hash(state);
                d.hash(state);
            }
            Value::Timestamp(t) => {
                7u8.hash(state);
                t.hash(state);
            }
            Value::Interval {
                months,
                days,
                micros,
            } => {
                8u8.hash(state);
                months.hash(state);
                days.hash(state);
                micros.hash(state);
            }
            Value::Json(s) => {
                9u8.hash(state);
                s.hash(state);
            }
            Value::Jsonb(b) => {
                10u8.hash(state);
                b.hash(state);
            }
            Value::TsVector(b) => {
                11u8.hash(state);
                b.hash(state);
            }
            Value::TsQuery(b) => {
                12u8.hash(state);
                b.hash(state);
            }
            Value::Array(a) => {
                13u8.hash(state);
                a.hash(state);
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
    // Order: NULL < BOOLEAN < numeric < TIME < DATE < TIMESTAMP < INTERVAL < TEXT < BLOB.
    // INTERVAL compares field-wise for trait-invariant safety; SQL-level ops normalize.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Boolean(_), _) => Ordering::Less,
            (_, Value::Boolean(_)) => Ordering::Greater,

            (Value::Integer(_) | Value::Real(_), Value::Integer(_) | Value::Real(_)) => {
                self.numeric_cmp(other).unwrap_or(Ordering::Equal)
            }
            (Value::Integer(_) | Value::Real(_), _) => Ordering::Less,
            (_, Value::Integer(_) | Value::Real(_)) => Ordering::Greater,

            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Time(_), _) => Ordering::Less,
            (_, Value::Time(_)) => Ordering::Greater,

            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Date(_), _) => Ordering::Less,
            (_, Value::Date(_)) => Ordering::Greater,

            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Timestamp(_), _) => Ordering::Less,
            (_, Value::Timestamp(_)) => Ordering::Greater,

            (
                Value::Interval {
                    months: am,
                    days: ad,
                    micros: au,
                },
                Value::Interval {
                    months: bm,
                    days: bd,
                    micros: bu,
                },
            ) => am.cmp(bm).then(ad.cmp(bd)).then(au.cmp(bu)),
            (Value::Interval { .. }, _) => Ordering::Less,
            (_, Value::Interval { .. }) => Ordering::Greater,

            (Value::Json(a), Value::Json(b)) => a.cmp(b),
            (Value::Json(_), _) => Ordering::Less,
            (_, Value::Json(_)) => Ordering::Greater,

            (Value::Jsonb(a), Value::Jsonb(b)) => a.as_ref().cmp(b.as_ref()),
            (Value::Jsonb(_), _) => Ordering::Less,
            (_, Value::Jsonb(_)) => Ordering::Greater,

            (Value::TsVector(a), Value::TsVector(b)) => a.as_ref().cmp(b.as_ref()),
            (Value::TsVector(_), _) => Ordering::Less,
            (_, Value::TsVector(_)) => Ordering::Greater,

            (Value::TsQuery(a), Value::TsQuery(b)) => a.as_ref().cmp(b.as_ref()),
            (Value::TsQuery(_), _) => Ordering::Less,
            (_, Value::TsQuery(_)) => Ordering::Greater,

            (Value::Array(a), Value::Array(b)) => a.as_ref().cmp(b.as_ref()),
            (Value::Array(_), _) => Ordering::Less,
            (_, Value::Array(_)) => Ordering::Greater,

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
            Value::Time(t) => write!(f, "{}", crate::datetime::format_time(*t)),
            Value::Date(d) => write!(f, "{}", crate::datetime::format_date(*d)),
            Value::Timestamp(t) => write!(f, "{}", crate::datetime::format_timestamp(*t)),
            Value::Interval {
                months,
                days,
                micros,
            } => {
                write!(
                    f,
                    "{}",
                    crate::datetime::format_interval(*months, *days, *micros)
                )
            }
            Value::Json(s) => write!(f, "{s}"),
            Value::Jsonb(b) => match crate::json::decode_to_text(b) {
                Ok(s) => write!(f, "{s}"),
                Err(_) => write!(f, "<invalid jsonb>"),
            },
            Value::TsVector(b) => write!(f, "{}", crate::fts::tsvector_display(b)),
            Value::TsQuery(b) => write!(f, "{}", crate::fts::tsquery_display(b)),
            Value::Array(a) => {
                write!(f, "{{")?;
                for (i, elem) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    match elem {
                        Value::Null => write!(f, "NULL")?,
                        Value::Text(s) => {
                            write!(f, "\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))?
                        }
                        other => write!(f, "{other}")?,
                    }
                }
                write!(f, "}}")
            }
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Collation {
    #[default]
    Binary = 0,
    NoCase = 1,
    Rtrim = 2,
}

impl Collation {
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Binary),
            1 => Some(Self::NoCase),
            2 => Some(Self::Rtrim),
            _ => None,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BINARY" => Some(Self::Binary),
            "NOCASE" => Some(Self::NoCase),
            "RTRIM" => Some(Self::Rtrim),
            _ => None,
        }
    }

    pub fn cmp_text(self, a: &str, b: &str) -> std::cmp::Ordering {
        match self {
            Collation::Binary => a.cmp(b),
            Collation::NoCase => Iterator::cmp(
                a.chars().map(|c| c.to_ascii_lowercase()),
                b.chars().map(|c| c.to_ascii_lowercase()),
            ),
            Collation::Rtrim => {
                let la = a.trim_end_matches(' ');
                let lb = b.trim_end_matches(' ');
                la.cmp(lb)
            }
        }
    }

    pub fn eq_text(self, a: &str, b: &str) -> bool {
        match self {
            Collation::Binary => a == b,
            Collation::NoCase => a.eq_ignore_ascii_case(b),
            Collation::Rtrim => a.trim_end_matches(' ') == b.trim_end_matches(' '),
        }
    }
}

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
    /// Display-only flag for `TIMESTAMPTZ` / `TIMETZ`; storage is i64 µs UTC.
    pub is_with_timezone: bool,
    pub generated_expr: Option<Expr>,
    pub generated_sql: Option<String>,
    pub generated_kind: Option<crate::parser::GeneratedKind>,
    pub collation: Collation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GinOpsClass {
    /// One entry per (key, value) pair; supports `@>` `?` `?|` `?&`.
    JsonbOps,
    /// One entry per hash(path‖value); supports `@>` only, ~3x smaller index.
    JsonbPathOps,
}

impl GinOpsClass {
    pub fn as_tag(self) -> u8 {
        match self {
            Self::JsonbOps => 0,
            Self::JsonbPathOps => 1,
        }
    }

    pub fn from_tag(t: u8) -> Option<Self> {
        match t {
            0 => Some(Self::JsonbOps),
            1 => Some(Self::JsonbPathOps),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvertedKind {
    Gin(GinOpsClass),
    Fts { config_id: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexKind {
    #[default]
    BTree,
    Inverted(InvertedKind),
}

/// `IndexKey::Column` for `CREATE INDEX ON t (email)`, `IndexKey::Expr` for `LOWER(email)`.
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub keys: Vec<IndexKey>,
    pub unique: bool,
    pub predicate_sql: Option<String>,
    pub predicate_expr: Option<crate::parser::Expr>,
    pub kind: IndexKind,
}

#[derive(Debug, Clone)]
pub enum IndexKey {
    Column {
        idx: u16,
        collate: Collation,
    },
    Expr {
        expr: crate::parser::Expr,
        original_sql: String,
    },
}

impl IndexDef {
    /// Used by FK/UNIQUE auto-indexes; expression-key indexes go through a different path.
    pub fn from_column_lists(
        name: String,
        columns: Vec<u16>,
        collations: Vec<Collation>,
        unique: bool,
        predicate_sql: Option<String>,
        predicate_expr: Option<crate::parser::Expr>,
        kind: IndexKind,
    ) -> Self {
        let keys = if collations.is_empty() {
            columns
                .into_iter()
                .map(|idx| IndexKey::Column {
                    idx,
                    collate: Collation::Binary,
                })
                .collect()
        } else {
            columns
                .into_iter()
                .zip(collations)
                .map(|(idx, collate)| IndexKey::Column { idx, collate })
                .collect()
        };
        Self {
            name,
            keys,
            unique,
            predicate_sql,
            predicate_expr,
            kind,
        }
    }

    /// Expression keys are skipped (positions only come from `IndexKey::Column`).
    pub fn columns_vec(&self) -> Vec<u16> {
        self.keys
            .iter()
            .filter_map(|k| match k {
                IndexKey::Column { idx, .. } => Some(*idx),
                IndexKey::Expr { .. } => None,
            })
            .collect()
    }

    /// Expression keys default to Binary.
    pub fn collations_vec(&self) -> Vec<Collation> {
        self.keys
            .iter()
            .map(|k| match k {
                IndexKey::Column { collate, .. } => *collate,
                IndexKey::Expr { .. } => Collation::Binary,
            })
            .collect()
    }

    pub fn column_positions_iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.keys.iter().filter_map(|k| match k {
            IndexKey::Column { idx, .. } => Some(*idx),
            IndexKey::Expr { .. } => None,
        })
    }

    pub fn collation_at(&self, i: usize) -> Collation {
        match self.keys.get(i) {
            Some(IndexKey::Column { collate, .. }) => *collate,
            _ => Collation::Binary,
        }
    }

    pub fn is_pure_column_index(&self) -> bool {
        self.keys
            .iter()
            .all(|k| matches!(k, IndexKey::Column { .. }))
    }
}

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

        let sql_len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
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

/// Backing table shares the matview's name and is repopulated on REFRESH.
#[derive(Debug, Clone)]
pub struct MatviewDef {
    pub name: String,
    pub select_sql: String,
    pub backing_table: String,
    pub with_data: bool,
    pub created_at_micros: i64,
}

const MATVIEW_DEF_VERSION: u8 = 1;

impl MatviewDef {
    pub fn backing_table_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(MATVIEW_DEF_VERSION);
        write_short_str(&mut buf, &self.name);
        write_long_str(&mut buf, &self.select_sql);
        write_short_str(&mut buf, &self.backing_table);
        buf.push(if self.with_data { 1 } else { 0 });
        buf.extend_from_slice(&self.created_at_micros.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != MATVIEW_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid matview definition version".into(),
            ));
        }
        let mut pos = 1usize;
        let name = read_short_str(data, &mut pos);
        let select_sql = read_long_str(data, &mut pos);
        let backing_table = read_short_str(data, &mut pos);
        let with_data = data[pos] != 0;
        pos += 1;
        let created_at_micros = i64::from_le_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        Ok(Self {
            name,
            select_sql,
            backing_table,
            with_data,
            created_at_micros,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub name: String,
    pub timing: crate::parser::TriggerTiming,
    pub events: Vec<crate::parser::TriggerEvent>,
    pub target: String,
    pub granularity: crate::parser::TriggerGranularity,
    pub referencing: Option<crate::parser::TransitionTables>,
    pub when_sql: Option<String>,
    pub body_sql: String,
    pub enabled: bool,
    pub created_at_micros: i64,
}

const TRIGGER_DEF_VERSION: u8 = 1;

impl TriggerDef {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(TRIGGER_DEF_VERSION);

        write_short_str(&mut buf, &self.name);
        buf.push(match self.timing {
            crate::parser::TriggerTiming::Before => 0,
            crate::parser::TriggerTiming::After => 1,
            crate::parser::TriggerTiming::InsteadOf => 2,
        });

        buf.extend_from_slice(&(self.events.len() as u16).to_le_bytes());
        for ev in &self.events {
            match ev {
                crate::parser::TriggerEvent::Insert => buf.push(0),
                crate::parser::TriggerEvent::Delete => buf.push(1),
                crate::parser::TriggerEvent::Update(cols) => {
                    buf.push(2);
                    buf.extend_from_slice(&(cols.len() as u16).to_le_bytes());
                    for c in cols {
                        write_short_str(&mut buf, c);
                    }
                }
            }
        }

        write_short_str(&mut buf, &self.target);
        buf.push(match self.granularity {
            crate::parser::TriggerGranularity::ForEachRow => 0,
            crate::parser::TriggerGranularity::ForEachStatement => 1,
        });

        match &self.referencing {
            None => buf.push(0),
            Some(r) => {
                buf.push(1);
                write_opt_string(&mut buf, &r.new_table_alias);
                write_opt_string(&mut buf, &r.old_table_alias);
            }
        }

        match &self.when_sql {
            None => buf.push(0),
            Some(s) => {
                buf.push(1);
                write_long_str(&mut buf, s);
            }
        }

        write_long_str(&mut buf, &self.body_sql);
        buf.push(if self.enabled { 1 } else { 0 });
        buf.extend_from_slice(&self.created_at_micros.to_le_bytes());

        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != TRIGGER_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid trigger definition version".into(),
            ));
        }
        let mut pos = 1;
        let name = read_short_str(data, &mut pos);
        let timing = match data[pos] {
            0 => crate::parser::TriggerTiming::Before,
            1 => crate::parser::TriggerTiming::After,
            2 => crate::parser::TriggerTiming::InsteadOf,
            _ => {
                return Err(crate::error::SqlError::InvalidValue(
                    "invalid trigger timing tag".into(),
                ))
            }
        };
        pos += 1;

        let event_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let mut events = Vec::with_capacity(event_count);
        for _ in 0..event_count {
            let tag = data[pos];
            pos += 1;
            let ev = match tag {
                0 => crate::parser::TriggerEvent::Insert,
                1 => crate::parser::TriggerEvent::Delete,
                2 => {
                    let cnt = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    let mut cols = Vec::with_capacity(cnt);
                    for _ in 0..cnt {
                        cols.push(read_short_str(data, &mut pos));
                    }
                    crate::parser::TriggerEvent::Update(cols)
                }
                _ => {
                    return Err(crate::error::SqlError::InvalidValue(
                        "invalid trigger event tag".into(),
                    ))
                }
            };
            events.push(ev);
        }

        let target = read_short_str(data, &mut pos);
        let granularity = match data[pos] {
            0 => crate::parser::TriggerGranularity::ForEachRow,
            1 => crate::parser::TriggerGranularity::ForEachStatement,
            _ => {
                return Err(crate::error::SqlError::InvalidValue(
                    "invalid trigger granularity tag".into(),
                ))
            }
        };
        pos += 1;

        let referencing = if data[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            let new_table_alias = read_opt_string(data, &mut pos);
            let old_table_alias = read_opt_string(data, &mut pos);
            Some(crate::parser::TransitionTables {
                new_table_alias,
                old_table_alias,
            })
        };

        let when_sql = if data[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            Some(read_long_str(data, &mut pos))
        };

        let body_sql = read_long_str(data, &mut pos);
        let enabled = data[pos] != 0;
        pos += 1;
        let created_at_micros = i64::from_le_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);

        Ok(Self {
            name,
            timing,
            events,
            target,
            granularity,
            referencing,
            when_sql,
            body_sql,
            enabled,
            created_at_micros,
        })
    }
}

fn write_short_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_short_str(data: &[u8], pos: &mut usize) -> String {
    let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
    *pos += len;
    s
}

fn write_long_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_long_str(data: &[u8], pos: &mut usize) -> String {
    let len =
        u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
    *pos += 4;
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
    *pos += len;
    s
}

#[derive(Debug, Clone)]
pub struct TableCheckDef {
    pub name: Option<String>,
    pub expr: Expr,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct ForeignKeySchemaEntry {
    pub name: Option<String>,
    pub columns: Vec<u16>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
    pub on_delete: crate::parser::ReferentialAction,
    pub on_update: crate::parser::ReferentialAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key_columns: Vec<u16>,
    pub indices: Vec<IndexDef>,
    pub check_constraints: Vec<TableCheckDef>,
    pub foreign_keys: Vec<ForeignKeySchemaEntry>,
    pub flags: u8,
    pk_idx_cache: Vec<usize>,
    non_pk_idx_cache: Vec<usize>,
    /// Sorted physical slots dropped via DROP COLUMN.
    dropped_non_pk_slots: Vec<u16>,
    /// Physical position -> logical column index. `usize::MAX` for dropped slots.
    decode_mapping_cache: Vec<usize>,
    /// Logical non-PK order -> physical encoding position.
    encoding_positions_cache: Vec<u16>,
    has_virtual_columns_cache: bool,
    column_map_cache: std::sync::OnceLock<crate::eval::ColumnMap>,
}

impl Clone for TableSchema {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: self.columns.clone(),
            primary_key_columns: self.primary_key_columns.clone(),
            indices: self.indices.clone(),
            check_constraints: self.check_constraints.clone(),
            foreign_keys: self.foreign_keys.clone(),
            flags: self.flags,
            pk_idx_cache: self.pk_idx_cache.clone(),
            non_pk_idx_cache: self.non_pk_idx_cache.clone(),
            dropped_non_pk_slots: self.dropped_non_pk_slots.clone(),
            decode_mapping_cache: self.decode_mapping_cache.clone(),
            encoding_positions_cache: self.encoding_positions_cache.clone(),
            has_virtual_columns_cache: self.has_virtual_columns_cache,
            column_map_cache: std::sync::OnceLock::new(),
        }
    }
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

        let has_virtual_columns_cache = columns.iter().any(|c| {
            matches!(
                c.generated_kind,
                Some(crate::parser::GeneratedKind::Virtual)
            )
        });

        Self {
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            flags: 0,
            pk_idx_cache,
            non_pk_idx_cache,
            dropped_non_pk_slots,
            decode_mapping_cache,
            encoding_positions_cache,
            has_virtual_columns_cache,
            column_map_cache: std::sync::OnceLock::new(),
        }
    }

    #[inline]
    pub fn column_map(&self) -> &crate::eval::ColumnMap {
        self.column_map_cache
            .get_or_init(|| crate::eval::ColumnMap::new(&self.columns))
    }

    pub fn is_strict(&self) -> bool {
        self.flags & TABLE_FLAG_STRICT != 0
    }

    pub fn has_virtual_columns(&self) -> bool {
        self.has_virtual_columns_cache
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

    pub fn has_checks(&self) -> bool {
        !self.check_constraints.is_empty() || self.columns.iter().any(|c| c.check_expr.is_some())
    }

    /// Physical position -> logical column index. `usize::MAX` for dropped slots.
    pub fn decode_col_mapping(&self) -> &[usize] {
        &self.decode_mapping_cache
    }

    /// Logical non-PK order -> physical encoding position.
    pub fn encoding_positions(&self) -> &[u16] {
        &self.encoding_positions_cache
    }

    /// Total physical non-PK column count (live + dropped slots).
    pub fn physical_non_pk_count(&self) -> usize {
        self.non_pk_idx_cache.len() + self.dropped_non_pk_slots.len()
    }

    pub fn dropped_non_pk_slots(&self) -> &[u16] {
        &self.dropped_non_pk_slots
    }

    pub fn without_column(&self, drop_pos: usize) -> Self {
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
                keys: idx
                    .keys
                    .iter()
                    .map(|k| match k {
                        IndexKey::Column { idx, collate } => IndexKey::Column {
                            idx: if *idx > drop_pos_u16 { *idx - 1 } else { *idx },
                            collate: *collate,
                        },
                        IndexKey::Expr { expr, original_sql } => IndexKey::Expr {
                            expr: expr.clone(),
                            original_sql: original_sql.clone(),
                        },
                    })
                    .collect(),
                unique: idx.unique,
                predicate_sql: idx.predicate_sql.clone(),
                predicate_expr: idx.predicate_expr.clone(),
                kind: idx.kind,
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
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                deferrable: fk.deferrable,
                initially_deferred: fk.initially_deferred,
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

const SCHEMA_VERSION: u8 = 12;
pub const TABLE_FLAG_STRICT: u8 = 0b0000_0001;

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

        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        for col in &self.columns {
            let col_name = col.name.as_bytes();
            buf.extend_from_slice(&(col_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(col_name);
            buf.push(col.data_type.type_tag());
            buf.push(if col.nullable { 1 } else { 0 });
            buf.extend_from_slice(&col.position.to_le_bytes());
        }

        buf.extend_from_slice(&(self.primary_key_columns.len() as u16).to_le_bytes());
        for &pk_idx in &self.primary_key_columns {
            buf.extend_from_slice(&pk_idx.to_le_bytes());
        }

        buf.extend_from_slice(&(self.indices.len() as u16).to_le_bytes());
        for idx in &self.indices {
            let idx_name = idx.name.as_bytes();
            buf.extend_from_slice(&(idx_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(idx_name);
            buf.extend_from_slice(&(idx.keys.len() as u16).to_le_bytes());
            for key in &idx.keys {
                let col_idx = match key {
                    IndexKey::Column { idx, .. } => *idx,
                    IndexKey::Expr { .. } => u16::MAX,
                };
                buf.extend_from_slice(&col_idx.to_le_bytes());
            }
            buf.push(if idx.unique { 1 } else { 0 });
        }

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

        buf.extend_from_slice(&(self.check_constraints.len() as u16).to_le_bytes());
        for chk in &self.check_constraints {
            write_opt_string(&mut buf, &chk.name);
            let sql_bytes = chk.sql.as_bytes();
            buf.extend_from_slice(&(sql_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(sql_bytes);
        }

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

        buf.extend_from_slice(&(self.dropped_non_pk_slots.len() as u16).to_le_bytes());
        for &slot in &self.dropped_non_pk_slots {
            buf.extend_from_slice(&slot.to_le_bytes());
        }

        for col in &self.columns {
            let kind_tag: u8 = match col.generated_kind {
                None => 0,
                Some(crate::parser::GeneratedKind::Stored) => 1,
                Some(crate::parser::GeneratedKind::Virtual) => 2,
            };
            buf.push(kind_tag);
            if kind_tag != 0 {
                let sql = col.generated_sql.as_deref().unwrap_or("");
                let bytes = sql.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }

        for idx in &self.indices {
            match &idx.predicate_sql {
                Some(sql) => {
                    buf.push(1);
                    let bytes = sql.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                None => buf.push(0),
            }
        }

        for fk in &self.foreign_keys {
            buf.push(fk.on_delete as u8);
            buf.push(fk.on_update as u8);
        }

        for fk in &self.foreign_keys {
            let mut flags: u8 = 0;
            if fk.deferrable {
                flags |= 0b01;
            }
            if fk.initially_deferred {
                flags |= 0b10;
            }
            buf.push(flags);
        }

        for col in &self.columns {
            buf.push(col.collation as u8);
        }
        for idx in &self.indices {
            let n = idx.keys.len() as u16;
            buf.extend_from_slice(&n.to_le_bytes());
            for key in &idx.keys {
                let c = match key {
                    IndexKey::Column { collate, .. } => *collate,
                    IndexKey::Expr { .. } => Collation::Binary,
                };
                buf.push(c as u8);
            }
        }
        for idx in &self.indices {
            match idx.kind {
                IndexKind::BTree => buf.push(0),
                IndexKind::Inverted(InvertedKind::Gin(ops)) => {
                    buf.push(1);
                    buf.push(ops.as_tag());
                }
                IndexKind::Inverted(InvertedKind::Fts { config_id }) => {
                    buf.push(2);
                    buf.push(config_id);
                }
            }
        }
        buf.push(self.flags);

        // v12: per-index expression-key extension. Emit (position, SQL) for each Expr key.
        // v11 readers stop before this section; v12 readers consume it.
        for idx in &self.indices {
            let expr_count = idx
                .keys
                .iter()
                .filter(|k| matches!(k, IndexKey::Expr { .. }))
                .count() as u16;
            buf.extend_from_slice(&expr_count.to_le_bytes());
            for (pos, key) in idx.keys.iter().enumerate() {
                if let IndexKey::Expr { original_sql, .. } = key {
                    buf.extend_from_slice(&(pos as u16).to_le_bytes());
                    let bytes = original_sql.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
        }

        buf
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        let mut pos = 0;

        if data.is_empty()
            || !matches!(
                data[0],
                1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | SCHEMA_VERSION
            )
        {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid schema version".into(),
            ));
        }
        let version = data[0];
        pos += 1;

        let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let name = String::from_utf8_lossy(&data[pos..pos + name_len]).into_owned();
        pos += name_len;

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
                is_with_timezone: false,
                generated_expr: None,
                generated_sql: None,
                generated_kind: None,
                collation: Collation::Binary,
            });
        }

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
                let mut keys: Vec<IndexKey> = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    let col_idx = u16::from_le_bytes([data[pos], data[pos + 1]]);
                    pos += 2;
                    // u16::MAX marks an expression key that the v12 section will fill in below.
                    // For v11 indexes (no expression section), this stays as a column placeholder.
                    keys.push(IndexKey::Column {
                        idx: col_idx,
                        collate: Collation::Binary,
                    });
                }
                let unique = data[pos] != 0;
                pos += 1;
                idxs.push(IndexDef {
                    name: idx_name,
                    keys,
                    unique,
                    predicate_sql: None,
                    predicate_expr: None,
                    kind: IndexKind::default(),
                });
            }
            idxs
        } else {
            vec![]
        };

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
                    on_delete: crate::parser::ReferentialAction::NoAction,
                    on_update: crate::parser::ReferentialAction::NoAction,
                    deferrable: false,
                    initially_deferred: false,
                });
            }
        }
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
        if version >= 5 && pos < data.len() {
            for col in &mut columns {
                let kind_tag = data[pos];
                pos += 1;
                if kind_tag != 0 {
                    let len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    let sql = String::from_utf8_lossy(&data[pos..pos + len]).into_owned();
                    pos += len;
                    let expr = crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse GENERATED expression: {sql}"
                        ))
                    })?;
                    col.generated_sql = Some(sql);
                    col.generated_expr = Some(expr);
                    col.generated_kind = Some(match kind_tag {
                        1 => crate::parser::GeneratedKind::Stored,
                        2 => crate::parser::GeneratedKind::Virtual,
                        _ => {
                            return Err(crate::error::SqlError::InvalidValue(
                                "unknown GENERATED kind tag".into(),
                            ));
                        }
                    });
                }
            }
        }
        let mut indices = indices;
        if version >= 6 && pos < data.len() {
            for idx in &mut indices {
                let flag = data[pos];
                pos += 1;
                if flag == 1 {
                    let len = u32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    let sql = String::from_utf8_lossy(&data[pos..pos + len]).into_owned();
                    pos += len;
                    let expr = crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse partial-index predicate: {sql}"
                        ))
                    })?;
                    idx.predicate_sql = Some(sql);
                    idx.predicate_expr = Some(expr);
                }
            }
            for fk in &mut foreign_keys {
                fk.on_delete =
                    crate::parser::ReferentialAction::from_tag(data[pos]).ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown FK on_delete tag".into())
                    })?;
                pos += 1;
                fk.on_update =
                    crate::parser::ReferentialAction::from_tag(data[pos]).ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown FK on_update tag".into())
                    })?;
                pos += 1;
            }
            if version >= 11 {
                for fk in &mut foreign_keys {
                    if pos >= data.len() {
                        break;
                    }
                    let flags = data[pos];
                    pos += 1;
                    fk.deferrable = flags & 0b01 != 0;
                    fk.initially_deferred = flags & 0b10 != 0;
                }
            }
        }

        let mut columns = columns;
        let mut indices = indices;
        let mut flags: u8 = 0;
        if version >= 7 && pos < data.len() {
            for col in &mut columns {
                col.collation = Collation::from_tag(data[pos]).ok_or_else(|| {
                    crate::error::SqlError::InvalidValue("unknown collation tag".into())
                })?;
                pos += 1;
            }
            for idx in &mut indices {
                let n = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;
                for i in 0..n {
                    let collate = Collation::from_tag(data[pos]).ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown collation tag".into())
                    })?;
                    pos += 1;
                    if let Some(IndexKey::Column { collate: c, .. }) = idx.keys.get_mut(i) {
                        *c = collate;
                    }
                }
            }
            if version >= 9 {
                for idx in &mut indices {
                    if pos >= data.len() {
                        break;
                    }
                    let tag = data[pos];
                    pos += 1;
                    idx.kind = match tag {
                        0 => IndexKind::BTree,
                        1 => {
                            if pos >= data.len() {
                                return Err(crate::error::SqlError::InvalidValue(
                                    "GIN index missing opclass tag".into(),
                                ));
                            }
                            let ops = GinOpsClass::from_tag(data[pos]).ok_or_else(|| {
                                crate::error::SqlError::InvalidValue(
                                    "unknown GIN opclass tag".into(),
                                )
                            })?;
                            pos += 1;
                            IndexKind::Inverted(InvertedKind::Gin(ops))
                        }
                        2 => {
                            if pos >= data.len() {
                                return Err(crate::error::SqlError::InvalidValue(
                                    "FTS index missing config_id".into(),
                                ));
                            }
                            let config_id = data[pos];
                            pos += 1;
                            IndexKind::Inverted(InvertedKind::Fts { config_id })
                        }
                        _ => {
                            return Err(crate::error::SqlError::InvalidValue(
                                "unknown IndexKind tag".into(),
                            ));
                        }
                    };
                }
            }
            if pos < data.len() {
                flags = data[pos];
                pos += 1;
            }
            if version >= 12 {
                for idx in &mut indices {
                    if pos + 2 > data.len() {
                        break;
                    }
                    let expr_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    for _ in 0..expr_count {
                        if pos + 6 > data.len() {
                            return Err(crate::error::SqlError::InvalidValue(
                                "truncated index expression key".into(),
                            ));
                        }
                        let key_pos = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                        pos += 2;
                        let sql_len = u32::from_le_bytes([
                            data[pos],
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                        ]) as usize;
                        pos += 4;
                        if pos + sql_len > data.len() {
                            return Err(crate::error::SqlError::InvalidValue(
                                "truncated expression-key SQL".into(),
                            ));
                        }
                        let sql = String::from_utf8_lossy(&data[pos..pos + sql_len]).into_owned();
                        pos += sql_len;
                        let expr = crate::parser::parse_sql_expr(&sql).map_err(|_| {
                            crate::error::SqlError::InvalidValue(format!(
                                "cannot parse index expression: {sql}"
                            ))
                        })?;
                        if key_pos < idx.keys.len() {
                            idx.keys[key_pos] = IndexKey::Expr {
                                expr,
                                original_sql: sql,
                            };
                        }
                    }
                }
            }
        }
        let _ = pos;

        let mut schema = Self::with_drops(
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            dropped_non_pk_slots,
        );
        schema.flags = flags;
        Ok(schema)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn non_pk_indices(&self) -> &[usize] {
        &self.non_pk_idx_cache
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_idx_cache
    }

    pub fn index_by_name(&self, name: &str) -> Option<&IndexDef> {
        let lower = name.to_ascii_lowercase();
        self.indices.iter().find(|i| i.name == lower)
    }

    pub fn index_table_name(table_name: &str, index_name: &str) -> Vec<u8> {
        format!("__idx_{table_name}_{index_name}").into_bytes()
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    RowsAffected(u64),
    Query(QueryResult),
    Ok,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
