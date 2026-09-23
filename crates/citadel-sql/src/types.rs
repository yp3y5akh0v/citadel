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
    Vector { dim: u16 },
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
            DataType::Vector { .. } => 15,
        }
    }

    /// Vector returns a `dim: 0` sentinel; the real dim is read at the schema layer.
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
            15 => Some(DataType::Vector { dim: 0 }),
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
            DataType::Vector { dim } => write!(f, "VECTOR({dim})"),
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
    Vector(Arc<[f32]>),
}

impl Value {
    /// Exact value representation, separate from SQL numeric equality.
    /// Used when detecting row changes or matching cached parameters.
    pub(crate) fn bit_eq(&self, other: &Self) -> bool {
        if std::mem::discriminant(self) != std::mem::discriminant(other) {
            return false;
        }
        match (self, other) {
            (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
            (Value::Array(x), Value::Array(y)) => {
                x.len() == y.len() && x.iter().zip(y.iter()).all(|(v, w)| v.bit_eq(w))
            }
            (Value::Vector(x), Value::Vector(y)) => {
                x.len() == y.len()
                    && x.iter()
                        .zip(y.iter())
                        .all(|(v, w)| v.to_bits() == w.to_bits())
            }
            _ => self == other,
        }
    }

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
            Value::Vector(v) => DataType::Vector {
                dim: v.len() as u16,
            },
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
                let magnitude = i.unsigned_abs();
                let significant_bits = u64::BITS - magnitude.leading_zeros();
                if significant_bits.saturating_sub(f64::MANTISSA_DIGITS)
                    <= magnitude.trailing_zeros()
                {
                    Some(Value::Real(*i as f64))
                } else {
                    None
                }
            }
            (Value::Real(r), DataType::Integer) => {
                let upper_exclusive = -(i64::MIN as f64);
                if r.is_finite()
                    && r.fract() == 0.0
                    && (i64::MIN as f64..upper_exclusive).contains(r)
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
            // total_cmp rather than ==, so NaN and signed zero agree with Ord and Hash.
            (Value::Vector(a), Value::Vector(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| x.total_cmp(y) == Ordering::Equal)
            }
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
                let bits = if *r == 0.0 { 0 } else { r.to_bits() };
                bits.hash(state);
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
            Value::Vector(v) => {
                14u8.hash(state);
                v.len().hash(state);
                for &x in v.iter() {
                    x.to_bits().hash(state);
                }
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

            (Value::Vector(a), Value::Vector(b)) => a.len().cmp(&b.len()).then_with(|| {
                for (x, y) in a.iter().zip(b.iter()) {
                    let ord = x.total_cmp(y);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            }),
            (Value::Vector(_), _) => Ordering::Less,
            (_, Value::Vector(_)) => Ordering::Greater,

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
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, &x) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{x}")?;
                }
                write!(f, "]")
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

    /// Compare two SQL values, applying this collation when both are text.
    /// Non-text values keep the engine's ordinary total ordering.
    pub(crate) fn cmp_value(self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Text(a), Value::Text(b)) => self.cmp_text(a, b),
            _ => a.cmp(b),
        }
    }

    /// Fold a value so that plain `Eq` and `Hash` agree with [`eq_text`]: two values this
    /// collation calls equal fold to one value.
    ///
    /// Hashing cannot consult a collation the way an operator does, so grouping,
    /// deduplicating and hash joins need a key that already carries it. Index keys fold the
    /// same way at write time (`encode_key_value_collated_into`), which lets a probe find them.
    pub fn fold(self, value: Value) -> Value {
        match (&value, self) {
            (Value::Text(s), Collation::NoCase) => Value::Text(s.to_ascii_lowercase()),
            (Value::Text(s), Collation::Rtrim) => Value::Text(s.trim_end_matches(' ').into()),
            _ => value,
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
    Ann { metric: AnnMetric },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnMetric {
    L2,
    Inner,
    Cosine,
}

impl AnnMetric {
    pub fn as_tag(self) -> u8 {
        match self {
            Self::L2 => 0,
            Self::Inner => 1,
            Self::Cosine => 2,
        }
    }

    pub fn from_tag(t: u8) -> Option<Self> {
        match t {
            0 => Some(Self::L2),
            1 => Some(Self::Inner),
            2 => Some(Self::Cosine),
            _ => None,
        }
    }
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
    /// ANN-only: schema column indices pushed into the PRISM cell filter.
    /// Empty for every other index kind.
    pub ann_filter_cols: Vec<u16>,
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
    /// Whether this index covers every row with exactly these column keys.
    ///
    /// FK lookups need the full ordered key, not just the column keys left after
    /// dropping expressions. Collations are allowed; probes must encode them and
    /// recheck the original row values when a collation folds distinct values.
    pub fn is_full_column_btree(&self, columns: &[u16]) -> bool {
        self.kind == IndexKind::BTree
            && self.predicate_sql.is_none()
            && self.predicate_expr.is_none()
            && self.keys.len() == columns.len()
            && self
                .keys
                .iter()
                .zip(columns)
                .all(|(key, column)| matches!(key, IndexKey::Column { idx, .. } if idx == column))
    }

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
            ann_filter_cols: Vec::new(),
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
    /// Serialize metadata. Panics if a field cannot fit its wire width.
    /// Use `try_serialize` to report invalid metadata as a SQL error.
    pub fn serialize(&self) -> Vec<u8> {
        self.try_serialize()
            .expect("unrepresentable schema metadata")
    }

    /// Serialize without truncating text lengths or collection counts.
    pub fn try_serialize(&self) -> crate::error::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(VIEW_DEF_VERSION);

        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&wire_u16_len(name_bytes.len())?.to_le_bytes());
        buf.extend_from_slice(name_bytes);

        let sql_bytes = self.sql.as_bytes();
        buf.extend_from_slice(&wire_u32_len(sql_bytes.len())?.to_le_bytes());
        buf.extend_from_slice(sql_bytes);

        buf.extend_from_slice(&wire_u16_len(self.column_aliases.len())?.to_le_bytes());
        for alias in &self.column_aliases {
            let alias_bytes = alias.as_bytes();
            buf.extend_from_slice(&wire_u16_len(alias_bytes.len())?.to_le_bytes());
            buf.extend_from_slice(alias_bytes);
        }

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != VIEW_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid view definition version".into(),
            ));
        }
        let mut reader = SchemaReader::new(&data[1..]);

        let name_len = reader.u16()? as usize;
        let name = reader.string(name_len)?;

        let sql_len = reader.u32()? as usize;
        let sql = reader.string(sql_len)?;

        let alias_count = reader.u16()? as usize;
        let mut column_aliases = Vec::with_capacity(alias_count.min(reader.remaining() / 2));
        for _ in 0..alias_count {
            let alias_len = reader.u16()? as usize;
            let alias = reader.string(alias_len)?;
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

    /// Serialize metadata. Panics if a field cannot fit its wire width.
    /// Use `try_serialize` to report invalid metadata as a SQL error.
    pub fn serialize(&self) -> Vec<u8> {
        self.try_serialize()
            .expect("unrepresentable schema metadata")
    }

    /// Serialize without truncating text lengths or collection counts.
    pub fn try_serialize(&self) -> crate::error::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(MATVIEW_DEF_VERSION);
        write_short_str(&mut buf, &self.name)?;
        write_long_str(&mut buf, &self.select_sql)?;
        write_short_str(&mut buf, &self.backing_table)?;
        buf.push(if self.with_data { 1 } else { 0 });
        buf.extend_from_slice(&self.created_at_micros.to_le_bytes());
        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != MATVIEW_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid matview definition version".into(),
            ));
        }
        let mut reader = SchemaReader::new(&data[1..]);
        let name = reader.short_string()?;
        let select_sql = reader.long_string()?;
        let backing_table = reader.short_string()?;
        let with_data = reader.u8()? != 0;
        let created_at_micros = reader.i64()?;
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
    /// Serialize metadata. Panics if a field cannot fit its wire width.
    /// Use `try_serialize` to report invalid metadata as a SQL error.
    pub fn serialize(&self) -> Vec<u8> {
        self.try_serialize()
            .expect("unrepresentable schema metadata")
    }

    /// Serialize without truncating text lengths or collection counts.
    pub fn try_serialize(&self) -> crate::error::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(TRIGGER_DEF_VERSION);

        write_short_str(&mut buf, &self.name)?;
        buf.push(match self.timing {
            crate::parser::TriggerTiming::Before => 0,
            crate::parser::TriggerTiming::After => 1,
            crate::parser::TriggerTiming::InsteadOf => 2,
        });

        buf.extend_from_slice(&wire_u16_len(self.events.len())?.to_le_bytes());
        for ev in &self.events {
            match ev {
                crate::parser::TriggerEvent::Insert => buf.push(0),
                crate::parser::TriggerEvent::Delete => buf.push(1),
                crate::parser::TriggerEvent::Update(cols) => {
                    buf.push(2);
                    buf.extend_from_slice(&wire_u16_len(cols.len())?.to_le_bytes());
                    for c in cols {
                        write_short_str(&mut buf, c)?;
                    }
                }
            }
        }

        write_short_str(&mut buf, &self.target)?;
        buf.push(match self.granularity {
            crate::parser::TriggerGranularity::ForEachRow => 0,
            crate::parser::TriggerGranularity::ForEachStatement => 1,
        });

        match &self.referencing {
            None => buf.push(0),
            Some(r) => {
                buf.push(1);
                write_opt_string(&mut buf, &r.new_table_alias)?;
                write_opt_string(&mut buf, &r.old_table_alias)?;
            }
        }

        match &self.when_sql {
            None => buf.push(0),
            Some(s) => {
                buf.push(1);
                write_long_str(&mut buf, s)?;
            }
        }

        write_long_str(&mut buf, &self.body_sql)?;
        buf.push(if self.enabled { 1 } else { 0 });
        buf.extend_from_slice(&self.created_at_micros.to_le_bytes());

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty() || data[0] != TRIGGER_DEF_VERSION {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid trigger definition version".into(),
            ));
        }
        let mut reader = SchemaReader::new(&data[1..]);
        let name = reader.short_string()?;
        let timing = match reader.u8()? {
            0 => crate::parser::TriggerTiming::Before,
            1 => crate::parser::TriggerTiming::After,
            2 => crate::parser::TriggerTiming::InsteadOf,
            _ => {
                return Err(crate::error::SqlError::InvalidValue(
                    "invalid trigger timing tag".into(),
                ))
            }
        };

        let event_count = reader.u16()? as usize;
        let mut events = Vec::with_capacity(event_count.min(reader.remaining()));
        for _ in 0..event_count {
            let tag = reader.u8()?;
            let ev = match tag {
                0 => crate::parser::TriggerEvent::Insert,
                1 => crate::parser::TriggerEvent::Delete,
                2 => {
                    let cnt = reader.u16()? as usize;
                    let mut cols = Vec::with_capacity(cnt.min(reader.remaining() / 2));
                    for _ in 0..cnt {
                        cols.push(reader.short_string()?);
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

        let target = reader.short_string()?;
        let granularity = match reader.u8()? {
            0 => crate::parser::TriggerGranularity::ForEachRow,
            1 => crate::parser::TriggerGranularity::ForEachStatement,
            _ => {
                return Err(crate::error::SqlError::InvalidValue(
                    "invalid trigger granularity tag".into(),
                ))
            }
        };

        let referencing = if reader.u8()? == 0 {
            None
        } else {
            let new_table_alias = reader.optional_string()?;
            let old_table_alias = reader.optional_string()?;
            Some(crate::parser::TransitionTables {
                new_table_alias,
                old_table_alias,
            })
        };

        let when_sql = if reader.u8()? == 0 {
            None
        } else {
            Some(reader.long_string()?)
        };

        let body_sql = reader.long_string()?;
        let enabled = reader.u8()? != 0;
        let created_at_micros = reader.i64()?;

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

fn wire_u16_len(len: usize) -> crate::error::Result<u16> {
    u16::try_from(len).map_err(|_| {
        crate::error::SqlError::InvalidValue(
            "schema metadata field exceeds 65535 bytes or entries".into(),
        )
    })
}

fn wire_u32_len(len: usize) -> crate::error::Result<u32> {
    u32::try_from(len).map_err(|_| {
        crate::error::SqlError::InvalidValue("schema metadata text exceeds 4294967295 bytes".into())
    })
}

fn write_short_str(buf: &mut Vec<u8>, value: &str) -> crate::error::Result<()> {
    buf.extend_from_slice(&wire_u16_len(value.len())?.to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_long_str(buf: &mut Vec<u8>, value: &str) -> crate::error::Result<()> {
    buf.extend_from_slice(&wire_u32_len(value.len())?.to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_opt_string(buf: &mut Vec<u8>, value: &Option<String>) -> crate::error::Result<()> {
    write_short_str(buf, value.as_deref().unwrap_or(""))
}

/// Bounds all reads before allocating or exposing a field. Record versions decide
/// which sections exist; this cursor does not silently supply missing metadata.
struct SchemaReader<'a> {
    remaining: &'a [u8],
}

impl<'a> SchemaReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn remaining(&self) -> usize {
        self.remaining.len()
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, len: usize) -> crate::error::Result<&'a [u8]> {
        if len > self.remaining.len() {
            return Err(crate::error::SqlError::InvalidValue(
                "truncated schema metadata".into(),
            ));
        }
        let (value, rest) = self.remaining.split_at(len);
        self.remaining = rest;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> crate::error::Result<[u8; N]> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("checked metadata field width"))
    }

    fn u8(&mut self) -> crate::error::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> crate::error::Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> crate::error::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> crate::error::Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn string(&mut self, len: usize) -> crate::error::Result<String> {
        Ok(String::from_utf8_lossy(self.take(len)?).into_owned())
    }

    fn short_string(&mut self) -> crate::error::Result<String> {
        let len = usize::from(self.u16()?);
        self.string(len)
    }

    fn long_string(&mut self) -> crate::error::Result<String> {
        let len = self.u32()? as usize;
        self.string(len)
    }

    fn optional_string(&mut self) -> crate::error::Result<Option<String>> {
        let len = usize::from(self.u16()?);
        if len == 0 {
            Ok(None)
        } else {
            self.string(len).map(Some)
        }
    }
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
    /// Schema evaluation can fall back to the statement's scoped parameters.
    /// Subqueries stay conservative because schema evaluation has no subquery
    /// executor and a nested query can hide additional dependencies.
    pub(crate) fn may_read_scoped_parameters(&self) -> bool {
        fn depends(expr: &Expr) -> bool {
            crate::parser::expr_uses_parameters(expr) || crate::parser::has_subquery(expr)
        }
        self.columns.iter().any(|column| {
            column
                .default_expr
                .iter()
                .chain(column.generated_expr.iter())
                .chain(column.check_expr.iter())
                .any(depends)
        }) || self
            .check_constraints
            .iter()
            .any(|check| depends(&check.expr))
            || self.indices.iter().any(|index| {
                index.predicate_expr.as_ref().is_some_and(depends)
                    || index.keys.iter().any(|key| match key {
                        IndexKey::Expr { expr, .. } => depends(expr),
                        IndexKey::Column { .. } => false,
                    })
            })
    }

    /// Describe the first persisted expression whose value can change with the
    /// session time zone or transaction date.
    ///
    /// Current DDL rejects these definitions. This scan exists for catalogs
    /// written by older builds so they can still be opened at the historical
    /// UTC default and explicitly remediated.
    pub(crate) fn session_dependent_persisted_expression(&self) -> Option<String> {
        for column in &self.columns {
            if column
                .generated_expr
                .as_ref()
                .is_some_and(crate::parser::expr_uses_session_dependent_jsonpath)
            {
                return Some(format!(
                    "generated column \"{}.{}\"",
                    self.name, column.name
                ));
            }
        }
        for index in &self.indices {
            if index.keys.iter().any(|key| match key {
                IndexKey::Expr { expr, .. } => {
                    crate::parser::expr_uses_session_dependent_jsonpath(expr)
                }
                IndexKey::Column { .. } => false,
            }) {
                return Some(format!(
                    "expression key of index \"{}\" on table \"{}\"",
                    index.name, self.name
                ));
            }
            if index
                .predicate_expr
                .as_ref()
                .is_some_and(crate::parser::expr_uses_session_dependent_jsonpath)
            {
                return Some(format!(
                    "predicate of partial index \"{}\" on table \"{}\"",
                    index.name, self.name
                ));
            }
        }
        None
    }

    /// Describe the first persisted expression that calls a volatile SQL
    /// function. Older catalogs could contain these even though current DDL
    /// rejects them. Connections use this to enter a DROP-only recovery mode,
    /// preventing the unsafe index/generated expression from being evaluated.
    pub(crate) fn volatile_persisted_expression(&self) -> Option<String> {
        for column in &self.columns {
            if let Some(function) = column
                .generated_expr
                .as_ref()
                .and_then(crate::parser::volatile_function_in_expr)
            {
                return Some(format!(
                    "generated column \"{}.{}\" calls volatile function {function}()",
                    self.name, column.name
                ));
            }
        }
        for index in &self.indices {
            for key in &index.keys {
                if let IndexKey::Expr { expr, .. } = key {
                    if let Some(function) = crate::parser::volatile_function_in_expr(expr) {
                        return Some(format!(
                            "expression key of index \"{}\" on table \"{}\" calls volatile function {function}()",
                            index.name, self.name
                        ));
                    }
                }
            }
            if let Some(function) = index
                .predicate_expr
                .as_ref()
                .and_then(crate::parser::volatile_function_in_expr)
            {
                return Some(format!(
                    "predicate of partial index \"{}\" on table \"{}\" calls volatile function {function}()",
                    index.name, self.name
                ));
            }
        }
        None
    }

    /// Build column mappings for a table or in-memory relation.
    ///
    /// Panics if the logical count or physical positions cannot be represented.
    /// Stored schemas additionally require at most 32767 non-PK physical slots.
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

    /// Build column mappings, preserving physical holes from DROP COLUMN.
    ///
    /// Panics if logical counts or physical positions cannot be represented, or
    /// dropped positions are not strictly increasing and within the layout.
    /// In-memory relation metadata may exceed the stored row's 32767-slot limit.
    pub fn with_drops(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_columns: Vec<u16>,
        indices: Vec<IndexDef>,
        check_constraints: Vec<TableCheckDef>,
        foreign_keys: Vec<ForeignKeySchemaEntry>,
        dropped_non_pk_slots: Vec<u16>,
    ) -> Self {
        Self::with_drops_checked(
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            dropped_non_pk_slots,
        )
        .expect("unrepresentable schema column layout")
    }

    fn with_drops_checked(
        name: String,
        columns: Vec<ColumnDef>,
        primary_key_columns: Vec<u16>,
        indices: Vec<IndexDef>,
        check_constraints: Vec<TableCheckDef>,
        foreign_keys: Vec<ForeignKeySchemaEntry>,
        dropped_non_pk_slots: Vec<u16>,
    ) -> crate::error::Result<Self> {
        Self::validate_column_count(columns.len())?;
        let pk_idx_cache: Vec<usize> = primary_key_columns.iter().map(|&i| i as usize).collect();
        let non_pk_idx_cache: Vec<usize> = (0..columns.len())
            .filter(|i| !primary_key_columns.contains(&(*i as u16)))
            .collect();

        let physical_count =
            Self::checked_physical_count(non_pk_idx_cache.len(), &dropped_non_pk_slots)?;
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

        Ok(Self {
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
        })
    }

    fn checked_physical_count(
        non_pk_count: usize,
        dropped_non_pk_slots: &[u16],
    ) -> crate::error::Result<usize> {
        let physical_count = non_pk_count
            .checked_add(dropped_non_pk_slots.len())
            .ok_or_else(|| {
                crate::error::SqlError::InvalidValue("schema physical column count overflow".into())
            })?;
        if physical_count > usize::from(u16::MAX) + 1 {
            return Err(crate::error::SqlError::InvalidValue(
                "schema physical column position exceeds u16".into(),
            ));
        }
        if dropped_non_pk_slots
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || dropped_non_pk_slots
                .last()
                .is_some_and(|&slot| usize::from(slot) >= physical_count)
        {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid dropped physical column positions".into(),
            ));
        }
        Ok(physical_count)
    }

    pub(crate) fn validate_column_count(count: usize) -> crate::error::Result<()> {
        if count > usize::from(u16::MAX) {
            return Err(crate::error::SqlError::InvalidValue(
                "schema exceeds 65535 logical columns".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_storage_layout(&self) -> crate::error::Result<()> {
        Self::validate_column_count(self.columns.len())?;
        // Public fields may have changed since the mapping caches were built.
        // Admission for serialization uses the columns that will be written.
        let non_pk_count = (0..self.columns.len())
            .filter(|i| !self.primary_key_columns.contains(&(*i as u16)))
            .count();
        let physical_count =
            Self::checked_physical_count(non_pk_count, &self.dropped_non_pk_slots)?;
        crate::encoding::validate_row_column_count(physical_count)
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
        let mut rebuilt = Self::with_drops(
            self.name,
            self.columns,
            self.primary_key_columns,
            self.indices,
            self.check_constraints,
            self.foreign_keys,
            drops,
        );
        rebuilt.flags = self.flags;
        rebuilt
    }

    pub fn has_checks(&self) -> bool {
        !self.check_constraints.is_empty() || self.columns.iter().any(|c| c.check_expr.is_some())
    }

    /// Only an ANN index owns a persisted segment; non-ANN tables skip the purge.
    pub fn has_ann_index(&self) -> bool {
        self.indices
            .iter()
            .any(|ix| matches!(ix.kind, IndexKind::Inverted(InvertedKind::Ann { .. })))
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
                ann_filter_cols: idx
                    .ann_filter_cols
                    .iter()
                    .filter(|&&p| p != drop_pos_u16)
                    .map(|&p| if p > drop_pos_u16 { p - 1 } else { p })
                    .collect(),
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

        let mut reduced = Self::with_drops(
            self.name.clone(),
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            new_dropped,
        );
        reduced.flags = self.flags;
        reduced
    }
}

const SCHEMA_VERSION: u8 = 14;
pub const TABLE_FLAG_STRICT: u8 = 0b0000_0001;

impl TableSchema {
    /// Serialize a stored schema. Panics if its physical layout exceeds
    /// 32767 slots or any metadata field cannot fit its wire width.
    /// Use `try_serialize` to report invalid metadata as a SQL error.
    pub fn serialize(&self) -> Vec<u8> {
        self.try_serialize()
            .expect("unrepresentable schema metadata")
    }

    /// Serialize without truncating text lengths or collection counts.
    pub fn try_serialize(&self) -> crate::error::Result<Vec<u8>> {
        let mut bound = false;
        let mut check = |expr: &Expr| {
            crate::parser::visit_expr(expr, &mut |node| {
                bound |= matches!(node, Expr::BoundColumn { .. });
            })
        };
        for column in &self.columns {
            for expr in column
                .default_expr
                .iter()
                .chain(column.generated_expr.iter())
                .chain(column.check_expr.iter())
            {
                check(expr);
            }
        }
        for constraint in &self.check_constraints {
            check(&constraint.expr);
        }
        for index in &self.indices {
            if let Some(expr) = &index.predicate_expr {
                check(expr);
            }
            for key in &index.keys {
                if let IndexKey::Expr { expr, .. } = key {
                    check(expr);
                }
            }
        }
        if bound {
            return Err(crate::error::SqlError::InvalidValue(
                "runtime-bound columns cannot be stored in schema definitions".into(),
            ));
        }
        self.validate_storage_layout()?;
        let mut buf = Vec::new();
        buf.push(SCHEMA_VERSION);

        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&wire_u16_len(name_bytes.len())?.to_le_bytes());
        buf.extend_from_slice(name_bytes);

        buf.extend_from_slice(&wire_u16_len(self.columns.len())?.to_le_bytes());

        for col in &self.columns {
            let col_name = col.name.as_bytes();
            buf.extend_from_slice(&wire_u16_len(col_name.len())?.to_le_bytes());
            buf.extend_from_slice(col_name);
            buf.push(col.data_type.type_tag());
            if let DataType::Vector { dim } = col.data_type {
                buf.extend_from_slice(&dim.to_le_bytes());
            }
            buf.push(if col.nullable { 1 } else { 0 });
            buf.extend_from_slice(&col.position.to_le_bytes());
        }

        buf.extend_from_slice(&wire_u16_len(self.primary_key_columns.len())?.to_le_bytes());
        for &pk_idx in &self.primary_key_columns {
            buf.extend_from_slice(&pk_idx.to_le_bytes());
        }

        buf.extend_from_slice(&wire_u16_len(self.indices.len())?.to_le_bytes());
        for idx in &self.indices {
            let idx_name = idx.name.as_bytes();
            buf.extend_from_slice(&wire_u16_len(idx_name.len())?.to_le_bytes());
            buf.extend_from_slice(idx_name);
            buf.extend_from_slice(&wire_u16_len(idx.keys.len())?.to_le_bytes());
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
                buf.extend_from_slice(&wire_u16_len(bytes.len())?.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            if let Some(ref sql) = col.check_sql {
                let bytes = sql.as_bytes();
                buf.extend_from_slice(&wire_u16_len(bytes.len())?.to_le_bytes());
                buf.extend_from_slice(bytes);
                write_opt_string(&mut buf, &col.check_name)?;
            }
        }

        buf.extend_from_slice(&wire_u16_len(self.check_constraints.len())?.to_le_bytes());
        for chk in &self.check_constraints {
            write_opt_string(&mut buf, &chk.name)?;
            let sql_bytes = chk.sql.as_bytes();
            buf.extend_from_slice(&wire_u16_len(sql_bytes.len())?.to_le_bytes());
            buf.extend_from_slice(sql_bytes);
        }

        buf.extend_from_slice(&wire_u16_len(self.foreign_keys.len())?.to_le_bytes());
        for fk in &self.foreign_keys {
            write_opt_string(&mut buf, &fk.name)?;
            buf.extend_from_slice(&wire_u16_len(fk.columns.len())?.to_le_bytes());
            for &col_idx in &fk.columns {
                buf.extend_from_slice(&col_idx.to_le_bytes());
            }
            let ft_bytes = fk.foreign_table.as_bytes();
            buf.extend_from_slice(&wire_u16_len(ft_bytes.len())?.to_le_bytes());
            buf.extend_from_slice(ft_bytes);
            buf.extend_from_slice(&wire_u16_len(fk.referred_columns.len())?.to_le_bytes());
            for rc in &fk.referred_columns {
                let rc_bytes = rc.as_bytes();
                buf.extend_from_slice(&wire_u16_len(rc_bytes.len())?.to_le_bytes());
                buf.extend_from_slice(rc_bytes);
            }
        }

        buf.extend_from_slice(&wire_u16_len(self.dropped_non_pk_slots.len())?.to_le_bytes());
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
                buf.extend_from_slice(&wire_u32_len(bytes.len())?.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }

        for idx in &self.indices {
            match &idx.predicate_sql {
                Some(sql) => {
                    buf.push(1);
                    let bytes = sql.as_bytes();
                    buf.extend_from_slice(&wire_u32_len(bytes.len())?.to_le_bytes());
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
            let n = wire_u16_len(idx.keys.len())?;
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
                IndexKind::Inverted(InvertedKind::Ann { metric }) => {
                    buf.push(3);
                    buf.push(metric.as_tag());
                }
            }
        }
        buf.push(self.flags);

        // v12: per-index expression-key extension. Emit (position, SQL) for each Expr key.
        // v11 readers stop before this section; v12 readers consume it.
        for idx in &self.indices {
            let expr_count = wire_u16_len(
                idx.keys
                    .iter()
                    .filter(|k| matches!(k, IndexKey::Expr { .. }))
                    .count(),
            )?;
            buf.extend_from_slice(&expr_count.to_le_bytes());
            for (pos, key) in idx.keys.iter().enumerate() {
                if let IndexKey::Expr { original_sql, .. } = key {
                    buf.extend_from_slice(&(pos as u16).to_le_bytes());
                    let bytes = original_sql.as_bytes();
                    buf.extend_from_slice(&wire_u32_len(bytes.len())?.to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
        }

        // v14: per-index ANN filter columns.
        for idx in &self.indices {
            buf.extend_from_slice(&wire_u16_len(idx.ann_filter_cols.len())?.to_le_bytes());
            for &col in &idx.ann_filter_cols {
                buf.extend_from_slice(&col.to_le_bytes());
            }
        }

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> crate::error::Result<Self> {
        if data.is_empty()
            || !matches!(
                data[0],
                1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | SCHEMA_VERSION
            )
        {
            return Err(crate::error::SqlError::InvalidValue(
                "invalid schema version".into(),
            ));
        }
        let version = data[0];
        let mut reader = SchemaReader::new(&data[1..]);

        let name_len = reader.u16()? as usize;
        let name = reader.string(name_len)?;

        let col_count = reader.u16()? as usize;

        let mut columns = Vec::with_capacity(col_count.min(reader.remaining() / 6));
        for _ in 0..col_count {
            let col_name_len = reader.u16()? as usize;
            let col_name = reader.string(col_name_len)?;
            let tag = reader.u8()?;
            let data_type = if tag == 15 {
                let dim = reader.u16()?;
                DataType::Vector { dim }
            } else {
                DataType::from_tag(tag).ok_or_else(|| {
                    crate::error::SqlError::InvalidValue("unknown data type tag".into())
                })?
            };
            let nullable = reader.u8()? != 0;
            let position = reader.u16()?;
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

        let pk_count = reader.u16()? as usize;
        let mut primary_key_columns = Vec::with_capacity(pk_count.min(reader.remaining() / 2));
        for _ in 0..pk_count {
            let pk_idx = reader.u16()?;
            primary_key_columns.push(pk_idx);
        }

        let indices = if version >= 2 {
            let idx_count = reader.u16()? as usize;
            let mut idxs = Vec::with_capacity(idx_count.min(reader.remaining() / 5));
            for _ in 0..idx_count {
                let idx_name_len = reader.u16()? as usize;
                let idx_name = reader.string(idx_name_len)?;
                let col_count = reader.u16()? as usize;
                let mut keys: Vec<IndexKey> =
                    Vec::with_capacity(col_count.min(reader.remaining() / 2));
                for _ in 0..col_count {
                    let col_idx = reader.u16()?;
                    // u16::MAX marks an expression key that the v12 section will fill in below.
                    // For v11 indexes (no expression section), this stays as a column placeholder.
                    keys.push(IndexKey::Column {
                        idx: col_idx,
                        collate: Collation::Binary,
                    });
                }
                let unique = reader.u8()? != 0;
                idxs.push(IndexDef {
                    name: idx_name,
                    keys,
                    unique,
                    predicate_sql: None,
                    predicate_expr: None,
                    kind: IndexKind::default(),
                    ann_filter_cols: Vec::new(),
                });
            }
            idxs
        } else {
            vec![]
        };

        let mut check_constraints = Vec::new();
        let mut foreign_keys = Vec::new();

        if version >= 3 {
            for col in &mut columns {
                let flags = reader.u8()?;
                if flags & 1 != 0 {
                    let sql = reader.short_string()?;
                    col.default_expr = Some(crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse DEFAULT expression: {sql}"
                        ))
                    })?);
                    col.default_sql = Some(sql);
                }
                if flags & 2 != 0 {
                    let sql = reader.short_string()?;
                    col.check_expr = Some(crate::parser::parse_sql_expr(&sql).map_err(|_| {
                        crate::error::SqlError::InvalidValue(format!(
                            "cannot parse CHECK expression: {sql}"
                        ))
                    })?);
                    col.check_sql = Some(sql);
                    col.check_name = reader.optional_string()?;
                }
            }

            let chk_count = reader.u16()? as usize;
            for _ in 0..chk_count {
                let name = reader.optional_string()?;
                let sql = reader.short_string()?;
                let expr = crate::parser::parse_sql_expr(&sql).map_err(|_| {
                    crate::error::SqlError::InvalidValue(format!(
                        "cannot parse CHECK expression: {sql}"
                    ))
                })?;
                check_constraints.push(TableCheckDef { name, expr, sql });
            }

            let fk_count = reader.u16()? as usize;
            for _ in 0..fk_count {
                let name = reader.optional_string()?;
                let col_count = reader.u16()? as usize;
                let mut cols = Vec::with_capacity(col_count.min(reader.remaining() / 2));
                for _ in 0..col_count {
                    let col_idx = reader.u16()?;
                    cols.push(col_idx);
                }
                let foreign_table = reader.short_string()?;
                let ref_count = reader.u16()? as usize;
                let mut referred_columns =
                    Vec::with_capacity(ref_count.min(reader.remaining() / 2));
                for _ in 0..ref_count {
                    referred_columns.push(reader.short_string()?);
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
        if version >= 4 {
            let slot_count = reader.u16()? as usize;
            for _ in 0..slot_count {
                let slot = reader.u16()?;
                dropped_non_pk_slots.push(slot);
            }
        }
        if version >= 5 {
            for col in &mut columns {
                let kind_tag = reader.u8()?;
                if kind_tag != 0 {
                    let len = reader.u32()? as usize;
                    let sql = reader.string(len)?;
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
        if version >= 6 {
            for idx in &mut indices {
                let flag = reader.u8()?;
                if flag == 1 {
                    let len = reader.u32()? as usize;
                    let sql = reader.string(len)?;
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
                fk.on_delete = crate::parser::ReferentialAction::from_tag(reader.u8()?)
                    .ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown FK on_delete tag".into())
                    })?;
                fk.on_update = crate::parser::ReferentialAction::from_tag(reader.u8()?)
                    .ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown FK on_update tag".into())
                    })?;
            }
            if version >= 11 {
                for fk in &mut foreign_keys {
                    let flags = reader.u8()?;
                    fk.deferrable = flags & 0b01 != 0;
                    fk.initially_deferred = flags & 0b10 != 0;
                }
            }
        }

        let mut columns = columns;
        let mut indices = indices;
        let mut flags: u8 = 0;
        if version >= 7 {
            for col in &mut columns {
                col.collation = Collation::from_tag(reader.u8()?).ok_or_else(|| {
                    crate::error::SqlError::InvalidValue("unknown collation tag".into())
                })?;
            }
            for idx in &mut indices {
                let n = reader.u16()? as usize;
                for i in 0..n {
                    let collate = Collation::from_tag(reader.u8()?).ok_or_else(|| {
                        crate::error::SqlError::InvalidValue("unknown collation tag".into())
                    })?;
                    if let Some(IndexKey::Column { collate: c, .. }) = idx.keys.get_mut(i) {
                        *c = collate;
                    }
                }
            }
            if version >= 9 {
                for idx in &mut indices {
                    let tag = reader.u8()?;
                    idx.kind = match tag {
                        0 => IndexKind::BTree,
                        1 => {
                            if reader.is_empty() {
                                return Err(crate::error::SqlError::InvalidValue(
                                    "GIN index missing opclass tag".into(),
                                ));
                            }
                            let ops = GinOpsClass::from_tag(reader.u8()?).ok_or_else(|| {
                                crate::error::SqlError::InvalidValue(
                                    "unknown GIN opclass tag".into(),
                                )
                            })?;
                            IndexKind::Inverted(InvertedKind::Gin(ops))
                        }
                        2 => {
                            if reader.is_empty() {
                                return Err(crate::error::SqlError::InvalidValue(
                                    "FTS index missing config_id".into(),
                                ));
                            }
                            let config_id = reader.u8()?;
                            IndexKind::Inverted(InvertedKind::Fts { config_id })
                        }
                        3 => {
                            if reader.is_empty() {
                                return Err(crate::error::SqlError::InvalidValue(
                                    "ANN index missing metric tag".into(),
                                ));
                            }
                            let metric = AnnMetric::from_tag(reader.u8()?).ok_or_else(|| {
                                crate::error::SqlError::InvalidValue(
                                    "unknown ANN metric tag".into(),
                                )
                            })?;
                            IndexKind::Inverted(InvertedKind::Ann { metric })
                        }
                        _ => {
                            return Err(crate::error::SqlError::InvalidValue(
                                "unknown IndexKind tag".into(),
                            ));
                        }
                    };
                }
            }
            // Older records may omit the table flags tail. Current
            // version 14 writers always emit it, even without indexes.
            if version >= 14 || !reader.is_empty() {
                flags = reader.u8()?;
            }
            if version >= 12 {
                for idx in &mut indices {
                    let expr_count = reader.u16()? as usize;
                    for _ in 0..expr_count {
                        if reader.remaining() < 6 {
                            return Err(crate::error::SqlError::InvalidValue(
                                "truncated index expression key".into(),
                            ));
                        }
                        let key_pos = reader.u16()? as usize;
                        let sql_len = reader.u32()? as usize;
                        if sql_len > reader.remaining() {
                            return Err(crate::error::SqlError::InvalidValue(
                                "truncated expression-key SQL".into(),
                            ));
                        }
                        let sql = reader.string(sql_len)?;
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
            if version >= 14 {
                for idx in &mut indices {
                    let fcount = reader.u16()? as usize;
                    let mut fcols = Vec::with_capacity(fcount.min(reader.remaining() / 2));
                    for _ in 0..fcount {
                        if reader.remaining() < 2 {
                            return Err(crate::error::SqlError::InvalidValue(
                                "truncated ANN filter columns".into(),
                            ));
                        }
                        fcols.push(reader.u16()?);
                    }
                    idx.ann_filter_cols = fcols;
                }
            }
        }

        let mut schema = Self::with_drops_checked(
            name,
            columns,
            primary_key_columns,
            indices,
            check_constraints,
            foreign_keys,
            dropped_non_pk_slots,
        )?;
        schema.validate_storage_layout()?;
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
