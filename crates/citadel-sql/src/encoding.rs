//! Order-preserving key encoding (tuple layer)
//! and row encoding for non-PK column storage.

use crate::error::{Result, SqlError};
use crate::types::{CompactString, DataType, Value};

// ── Key encoding (order-preserving) ─────────────────────────────────

/// Type tag bytes for key encoding. Ordering: NULL < BLOB < TEXT < BOOLEAN < INTEGER < REAL
const TAG_NULL: u8 = 0x00;
const TAG_BLOB: u8 = 0x01;
const TAG_TEXT: u8 = 0x02;
const TAG_BOOLEAN: u8 = 0x03;
const TAG_INTEGER: u8 = 0x04;
const TAG_REAL: u8 = 0x05;

/// Encode a single value into an order-preserving byte sequence.
pub fn encode_key_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![TAG_NULL],
        Value::Boolean(b) => vec![TAG_BOOLEAN, if *b { 0x01 } else { 0x00 }],
        Value::Integer(i) => encode_integer(*i),
        Value::Real(r) => encode_real(*r),
        Value::Text(s) => encode_bytes(TAG_TEXT, s.as_bytes()),
        Value::Blob(b) => encode_bytes(TAG_BLOB, b),
    }
}

/// Encode a composite key (multiple values concatenated).
pub fn encode_composite_key(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in values {
        buf.extend_from_slice(&encode_key_value(v));
    }
    buf
}

pub fn encode_composite_key_into(values: &[Value], buf: &mut Vec<u8>) {
    buf.clear();
    for v in values {
        encode_key_value_into(v, buf);
    }
}

fn encode_key_value_into(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Boolean(b) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *b { 0x01 } else { 0x00 });
        }
        Value::Integer(i) => encode_integer_into(*i, buf),
        Value::Real(r) => encode_real_into(*r, buf),
        Value::Text(s) => encode_bytes_into(TAG_TEXT, s.as_bytes(), buf),
        Value::Blob(b) => encode_bytes_into(TAG_BLOB, b, buf),
    }
}

fn encode_integer_into(val: i64, buf: &mut Vec<u8>) {
    buf.push(TAG_INTEGER);
    if val == 0 {
        buf.push(0x80);
        return;
    }
    if val > 0 {
        let bytes = val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap();
        let byte_count = (8 - start) as u8;
        buf.push(0x80 + byte_count);
        buf.extend_from_slice(&bytes[start..]);
    } else {
        let abs_val = if val == i64::MIN {
            u64::MAX / 2 + 1
        } else {
            (-val) as u64
        };
        let bytes = abs_val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap();
        let byte_count = (8 - start) as u8;
        buf.push(0x80 - byte_count);
        for &b in &bytes[start..] {
            buf.push(!b);
        }
    }
}

fn encode_real_into(val: f64, buf: &mut Vec<u8>) {
    buf.push(TAG_REAL);
    let bits = val.to_bits();
    let encoded = if val.is_sign_negative() {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    buf.extend_from_slice(&encoded.to_be_bytes());
}

fn encode_bytes_into(tag: u8, data: &[u8], buf: &mut Vec<u8>) {
    buf.push(tag);
    for &b in data {
        if b == 0x00 {
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00);
}

/// Decode a single key value, returning the value and the number of bytes consumed.
pub fn decode_key_value(data: &[u8]) -> Result<(Value, usize)> {
    if data.is_empty() {
        return Err(SqlError::InvalidValue("empty key data".into()));
    }
    match data[0] {
        TAG_NULL => Ok((Value::Null, 1)),
        TAG_BOOLEAN => {
            if data.len() < 2 {
                return Err(SqlError::InvalidValue("truncated boolean".into()));
            }
            Ok((Value::Boolean(data[1] != 0), 2))
        }
        TAG_INTEGER => decode_integer(&data[1..]).map(|(v, n)| (v, n + 1)),
        TAG_REAL => decode_real(&data[1..]).map(|(v, n)| (v, n + 1)),
        TAG_TEXT => {
            let (bytes, n) = decode_null_escaped(&data[1..])?;
            let s = String::from_utf8(bytes)
                .map_err(|_| SqlError::InvalidValue("invalid UTF-8 in key".into()))?;
            Ok((Value::Text(CompactString::from(s)), n + 1))
        }
        TAG_BLOB => {
            let (bytes, n) = decode_null_escaped(&data[1..])?;
            Ok((Value::Blob(bytes), n + 1))
        }
        tag => Err(SqlError::InvalidValue(format!("unknown key tag: {tag:#x}"))),
    }
}

/// Decode a composite key into multiple values.
pub fn decode_composite_key(data: &[u8], count: usize) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(count);
    let mut pos = 0;
    for _ in 0..count {
        let (v, n) = decode_key_value(&data[pos..])?;
        values.push(v);
        pos += n;
    }
    Ok(values)
}

// ── Integer encoding (variable-width) ───────────────────────────────

fn encode_integer(val: i64) -> Vec<u8> {
    let mut buf = vec![TAG_INTEGER];
    if val == 0 {
        buf.push(0x80);
        return buf;
    }
    if val > 0 {
        let bytes = val.to_be_bytes();
        // Find first non-zero byte
        let start = bytes.iter().position(|&b| b != 0).unwrap();
        let byte_count = (8 - start) as u8;
        buf.push(0x80 + byte_count);
        buf.extend_from_slice(&bytes[start..]);
    } else {
        // Negative: one's complement of absolute value
        let abs_val = if val == i64::MIN {
            // Special case: |i64::MIN| doesn't fit in i64
            u64::MAX / 2 + 1
        } else {
            (-val) as u64
        };
        let bytes = abs_val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap();
        let byte_count = (8 - start) as u8;
        buf.push(0x80 - byte_count);
        // One's complement: invert all bits
        for &b in &bytes[start..] {
            buf.push(!b);
        }
    }
    buf
}

fn decode_integer(data: &[u8]) -> Result<(Value, usize)> {
    if data.is_empty() {
        return Err(SqlError::InvalidValue("truncated integer".into()));
    }
    let marker = data[0];
    if marker == 0x80 {
        return Ok((Value::Integer(0), 1));
    }
    if marker > 0x80 {
        // Positive
        let byte_count = (marker - 0x80) as usize;
        if data.len() < 1 + byte_count {
            return Err(SqlError::InvalidValue("truncated positive integer".into()));
        }
        let mut bytes = [0u8; 8];
        bytes[8 - byte_count..].copy_from_slice(&data[1..1 + byte_count]);
        let val = i64::from_be_bytes(bytes);
        Ok((Value::Integer(val), 1 + byte_count))
    } else {
        // Negative
        let byte_count = (0x80 - marker) as usize;
        if data.len() < 1 + byte_count {
            return Err(SqlError::InvalidValue("truncated negative integer".into()));
        }
        let mut bytes = [0u8; 8];
        for i in 0..byte_count {
            bytes[8 - byte_count + i] = !data[1 + i];
        }
        let abs_val = u64::from_be_bytes(bytes);
        // Use wrapping negation to handle i64::MIN correctly
        let val = (-(abs_val as i128)) as i64;
        Ok((Value::Integer(val), 1 + byte_count))
    }
}

// ── Real encoding (IEEE 754 sign-bit manipulation) ──────────────────

fn encode_real(val: f64) -> Vec<u8> {
    let mut buf = vec![TAG_REAL];
    let bits = val.to_bits();
    let encoded = if val.is_sign_negative() {
        // Negative (including -0.0): flip ALL bits
        !bits
    } else {
        // Positive (including +0.0): flip sign bit only
        bits ^ (1u64 << 63)
    };
    buf.extend_from_slice(&encoded.to_be_bytes());
    buf
}

fn decode_real(data: &[u8]) -> Result<(Value, usize)> {
    if data.len() < 8 {
        return Err(SqlError::InvalidValue("truncated real".into()));
    }
    let encoded = u64::from_be_bytes(data[..8].try_into().unwrap());
    let bits = if encoded & (1u64 << 63) != 0 {
        // Was positive: undo sign bit flip
        encoded ^ (1u64 << 63)
    } else {
        // Was negative: undo full inversion
        !encoded
    };
    let val = f64::from_bits(bits);
    Ok((Value::Real(val), 8))
}

// ── Null-escaped byte encoding ──────────────────────────────────────

/// Encode bytes with null-escape: 0x00 → 0x00 0xFF, terminated by bare 0x00.
fn encode_bytes(tag: u8, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data.len() + 2);
    buf.push(tag);
    for &b in data {
        if b == 0x00 {
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00); // terminator
    buf
}

/// Decode null-escaped bytes. Returns (decoded bytes, bytes consumed including terminator).
fn decode_null_escaped(data: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            if i + 1 < data.len() && data[i + 1] == 0xFF {
                result.push(0x00);
                i += 2;
            } else {
                return Ok((result, i + 1)); // terminator consumed
            }
        } else {
            result.push(data[i]);
            i += 1;
        }
    }
    Err(SqlError::InvalidValue(
        "unterminated null-escaped string".into(),
    ))
}

// ── Row encoding (for B+ tree values — non-PK columns) ─────────────

/// Encode non-PK column values into a row.
/// Format: [col_count: u16][null_bitmap][per-column: data_type(u8) + data_len(u32) + data]
pub fn encode_row(values: &[Value]) -> Vec<u8> {
    let col_count = values.len();
    let bitmap_bytes = col_count.div_ceil(8);
    let mut buf = Vec::new();

    // Column count
    buf.extend_from_slice(&(col_count as u16).to_le_bytes());

    // Null bitmap
    let mut bitmap = vec![0u8; bitmap_bytes];
    for (i, v) in values.iter().enumerate() {
        if v.is_null() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend_from_slice(&bitmap);

    // Column data
    for v in values {
        if v.is_null() {
            continue;
        }
        match v {
            Value::Integer(i) => {
                buf.push(DataType::Integer.type_tag());
                buf.extend_from_slice(&8u32.to_le_bytes());
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Real(r) => {
                buf.push(DataType::Real.type_tag());
                buf.extend_from_slice(&8u32.to_le_bytes());
                buf.extend_from_slice(&r.to_le_bytes());
            }
            Value::Boolean(b) => {
                buf.push(DataType::Boolean.type_tag());
                buf.extend_from_slice(&1u32.to_le_bytes());
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Text(s) => {
                let bytes = s.as_bytes();
                buf.push(DataType::Text.type_tag());
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Blob(data) => {
                buf.push(DataType::Blob.type_tag());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            Value::Null => unreachable!(),
        }
    }

    buf
}

pub fn encode_row_into(values: &[Value], buf: &mut Vec<u8>) {
    buf.clear();
    let col_count = values.len();
    let bitmap_bytes = col_count.div_ceil(8);

    buf.extend_from_slice(&(col_count as u16).to_le_bytes());

    let bitmap_start = buf.len();
    buf.resize(buf.len() + bitmap_bytes, 0);

    for (i, v) in values.iter().enumerate() {
        if v.is_null() {
            buf[bitmap_start + i / 8] |= 1 << (i % 8);
            continue;
        }
        match v {
            Value::Integer(val) => {
                buf.push(DataType::Integer.type_tag());
                buf.extend_from_slice(&8u32.to_le_bytes());
                buf.extend_from_slice(&val.to_le_bytes());
            }
            Value::Real(r) => {
                buf.push(DataType::Real.type_tag());
                buf.extend_from_slice(&8u32.to_le_bytes());
                buf.extend_from_slice(&r.to_le_bytes());
            }
            Value::Boolean(b) => {
                buf.push(DataType::Boolean.type_tag());
                buf.extend_from_slice(&1u32.to_le_bytes());
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Text(s) => {
                let bytes = s.as_bytes();
                buf.push(DataType::Text.type_tag());
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Blob(data) => {
                buf.push(DataType::Blob.type_tag());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            Value::Null => unreachable!(),
        }
    }
}

fn decode_value(type_tag: u8, data: &[u8]) -> Result<Value> {
    match DataType::from_tag(type_tag) {
        Some(DataType::Integer) => Ok(Value::Integer(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Real) => Ok(Value::Real(f64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Boolean) => Ok(Value::Boolean(data[0] != 0)),
        Some(DataType::Text) => {
            let s = std::str::from_utf8(data)
                .map_err(|_| SqlError::InvalidValue("invalid UTF-8 in column".into()))?;
            Ok(Value::Text(CompactString::from(s)))
        }
        Some(DataType::Blob) => Ok(Value::Blob(data.to_vec())),
        _ => Err(SqlError::InvalidValue(format!(
            "unknown column type tag: {type_tag}"
        ))),
    }
}

fn parse_row_header(data: &[u8]) -> Result<(usize, &[u8], usize)> {
    if data.len() < 2 {
        return Err(SqlError::InvalidValue("row data too short".into()));
    }
    let col_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let bitmap_bytes = col_count.div_ceil(8);
    let pos = 2;
    if data.len() < pos + bitmap_bytes {
        return Err(SqlError::InvalidValue("truncated null bitmap".into()));
    }
    Ok((
        col_count,
        &data[pos..pos + bitmap_bytes],
        pos + bitmap_bytes,
    ))
}

pub fn decode_row(data: &[u8]) -> Result<Vec<Value>> {
    let (col_count, bitmap, mut pos) = parse_row_header(data)?;

    let mut values = Vec::with_capacity(col_count);
    for i in 0..col_count {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            values.push(Value::Null);
            continue;
        }

        if pos + 5 > data.len() {
            return Err(SqlError::InvalidValue("truncated column data".into()));
        }
        let type_tag = data[pos];
        pos += 1;
        let data_len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        if pos + data_len > data.len() {
            return Err(SqlError::InvalidValue("truncated column value".into()));
        }

        values.push(decode_value(type_tag, &data[pos..pos + data_len])?);
        pos += data_len;
    }

    Ok(values)
}

pub fn decode_row_into(data: &[u8], out: &mut [Value], col_mapping: &[usize]) -> Result<()> {
    let (col_count, bitmap, mut pos) = parse_row_header(data)?;

    for i in 0..col_count {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            continue;
        }

        if pos + 5 > data.len() {
            return Err(SqlError::InvalidValue("truncated column data".into()));
        }
        let type_tag = data[pos];
        pos += 1;
        let data_len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        if pos + data_len > data.len() {
            return Err(SqlError::InvalidValue("truncated column value".into()));
        }

        if i < col_mapping.len() {
            out[col_mapping[i]] = decode_value(type_tag, &data[pos..pos + data_len])?;
        }
        pos += data_len;
    }

    Ok(())
}

pub fn decode_pk_into(
    key: &[u8],
    count: usize,
    out: &mut [Value],
    pk_mapping: &[usize],
) -> Result<()> {
    let mut pos = 0;
    for i in 0..count {
        let (v, n) = decode_key_value(&key[pos..])?;
        if i < pk_mapping.len() {
            out[pk_mapping[i]] = v;
        }
        pos += n;
    }
    Ok(())
}

pub fn decode_columns(data: &[u8], targets: &[usize]) -> Result<Vec<Value>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let (col_count, bitmap, mut pos) = parse_row_header(data)?;
    if *targets.last().unwrap() >= col_count {
        return Err(SqlError::InvalidValue("column index out of bounds".into()));
    }

    let mut results = Vec::with_capacity(targets.len());
    let mut ti = 0;

    for col in 0..col_count {
        if ti >= targets.len() {
            break;
        }
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;

        if col == targets[ti] {
            if is_null {
                results.push(Value::Null);
            } else {
                if pos + 5 > data.len() {
                    return Err(SqlError::InvalidValue("truncated column data".into()));
                }
                let type_tag = data[pos];
                pos += 1;
                let data_len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + data_len > data.len() {
                    return Err(SqlError::InvalidValue("truncated column value".into()));
                }
                results.push(decode_value(type_tag, &data[pos..pos + data_len])?);
                pos += data_len;
            }
            ti += 1;
        } else if !is_null {
            if pos + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let data_len =
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;
            pos += 5 + data_len;
        }
    }

    Ok(results)
}

pub fn decode_columns_into(
    data: &[u8],
    targets: &[usize],
    schema_cols: &[usize],
    row: &mut [Value],
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }
    let (col_count, bitmap, mut pos) = parse_row_header(data)?;
    if *targets.last().unwrap() >= col_count {
        return Err(SqlError::InvalidValue("column index out of bounds".into()));
    }

    let mut ti = 0;
    for col in 0..col_count {
        if ti >= targets.len() {
            break;
        }
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;

        if col == targets[ti] {
            if is_null {
                row[schema_cols[ti]] = Value::Null;
            } else {
                if pos + 5 > data.len() {
                    return Err(SqlError::InvalidValue("truncated column data".into()));
                }
                let type_tag = data[pos];
                pos += 1;
                let data_len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + data_len > data.len() {
                    return Err(SqlError::InvalidValue("truncated column value".into()));
                }
                row[schema_cols[ti]] = decode_value(type_tag, &data[pos..pos + data_len])?;
                pos += data_len;
            }
            ti += 1;
        } else if !is_null {
            if pos + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let data_len =
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;
            pos += 5 + data_len;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum RawColumn<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> RawColumn<'a> {
    pub fn to_value(self) -> Value {
        match self {
            RawColumn::Null => Value::Null,
            RawColumn::Integer(i) => Value::Integer(i),
            RawColumn::Real(r) => Value::Real(r),
            RawColumn::Boolean(b) => Value::Boolean(b),
            RawColumn::Text(s) => Value::Text(CompactString::from(s)),
            RawColumn::Blob(b) => Value::Blob(b.to_vec()),
        }
    }

    pub fn cmp_value(&self, other: &Value) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (self, other) {
            (RawColumn::Null, Value::Null) => Some(Ordering::Equal),
            (RawColumn::Null, _) | (_, Value::Null) => None,
            (RawColumn::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (RawColumn::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b),
            (RawColumn::Real(a), Value::Real(b)) => a.partial_cmp(b),
            (RawColumn::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (RawColumn::Text(a), Value::Text(b)) => Some((*a).cmp(b.as_str())),
            (RawColumn::Blob(a), Value::Blob(b)) => Some((*a).cmp(b.as_slice())),
            (RawColumn::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub fn eq_value(&self, other: &Value) -> bool {
        match (self, other) {
            (RawColumn::Null, Value::Null) => true,
            (RawColumn::Integer(a), Value::Integer(b)) => a == b,
            (RawColumn::Integer(a), Value::Real(b)) => (*a as f64) == *b,
            (RawColumn::Real(a), Value::Real(b)) => a == b,
            (RawColumn::Real(a), Value::Integer(b)) => *a == (*b as f64),
            (RawColumn::Text(a), Value::Text(b)) => *a == b.as_str(),
            (RawColumn::Blob(a), Value::Blob(b)) => *a == b.as_slice(),
            (RawColumn::Boolean(a), Value::Boolean(b)) => a == b,
            _ => false,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RawColumn::Integer(i) => Some(*i as f64),
            RawColumn::Real(r) => Some(*r),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            RawColumn::Integer(i) => Some(*i),
            _ => None,
        }
    }
}

fn decode_value_raw(type_tag: u8, data: &[u8]) -> Result<RawColumn<'_>> {
    match DataType::from_tag(type_tag) {
        Some(DataType::Integer) => Ok(RawColumn::Integer(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Real) => Ok(RawColumn::Real(f64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Boolean) => Ok(RawColumn::Boolean(data[0] != 0)),
        Some(DataType::Text) => {
            let s = std::str::from_utf8(data)
                .map_err(|_| SqlError::InvalidValue("invalid UTF-8 in column".into()))?;
            Ok(RawColumn::Text(s))
        }
        Some(DataType::Blob) => Ok(RawColumn::Blob(data)),
        _ => Err(SqlError::InvalidValue(format!(
            "unknown column type tag: {type_tag}"
        ))),
    }
}

pub fn decode_column_raw(data: &[u8], target: usize) -> Result<RawColumn<'_>> {
    let (col_count, bitmap, mut pos) = parse_row_header(data)?;
    if target >= col_count {
        return Err(SqlError::InvalidValue("column index out of bounds".into()));
    }

    for col in 0..=target {
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;

        if col == target {
            if is_null {
                return Ok(RawColumn::Null);
            }
            if pos + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let type_tag = data[pos];
            pos += 1;
            let data_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;
            if pos + data_len > data.len() {
                return Err(SqlError::InvalidValue("truncated column value".into()));
            }
            return decode_value_raw(type_tag, &data[pos..pos + data_len]);
        } else if !is_null {
            if pos + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let data_len =
                u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;
            pos += 5 + data_len;
        }
    }

    unreachable!()
}

pub fn decode_pk_integer(key: &[u8]) -> Result<i64> {
    if key.is_empty() || key[0] != TAG_INTEGER {
        return Err(SqlError::InvalidValue("not an integer key".into()));
    }
    let (val, _) = decode_integer(&key[1..])?;
    match val {
        Value::Integer(i) => Ok(i),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Key encoding tests ──────────────────────────────────────────

    #[test]
    fn key_null() {
        let encoded = encode_key_value(&Value::Null);
        let (decoded, n) = decode_key_value(&encoded).unwrap();
        assert_eq!(n, 1);
        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn key_boolean() {
        let f_enc = encode_key_value(&Value::Boolean(false));
        let t_enc = encode_key_value(&Value::Boolean(true));
        assert!(f_enc < t_enc);

        let (f_dec, _) = decode_key_value(&f_enc).unwrap();
        let (t_dec, _) = decode_key_value(&t_enc).unwrap();
        assert_eq!(f_dec, Value::Boolean(false));
        assert_eq!(t_dec, Value::Boolean(true));
    }

    #[test]
    fn key_integer_roundtrip() {
        let test_values = [
            i64::MIN,
            -1_000_000,
            -256,
            -1,
            0,
            1,
            127,
            128,
            255,
            256,
            65535,
            1_000_000,
            i64::MAX,
        ];
        for &v in &test_values {
            let encoded = encode_key_value(&Value::Integer(v));
            let (decoded, _) = decode_key_value(&encoded).unwrap();
            assert_eq!(decoded, Value::Integer(v), "roundtrip failed for {v}");
        }
    }

    #[test]
    fn key_integer_sort_order() {
        let values: Vec<i64> = vec![i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| encode_key_value(&Value::Integer(v)))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order broken: {} vs {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn key_real_roundtrip() {
        let test_values = [
            f64::NEG_INFINITY,
            -1e100,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            0.5,
            1.0,
            1e100,
            f64::INFINITY,
        ];
        for &v in &test_values {
            let encoded = encode_key_value(&Value::Real(v));
            let (decoded, _) = decode_key_value(&encoded).unwrap();
            match decoded {
                Value::Real(r) => {
                    assert!(
                        v.to_bits() == r.to_bits(),
                        "roundtrip failed for {v}: got {r}"
                    );
                }
                _ => panic!("expected Real"),
            }
        }
    }

    #[test]
    fn key_real_sort_order() {
        let values = [
            f64::NEG_INFINITY,
            -100.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            100.0,
            f64::INFINITY,
        ];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| encode_key_value(&Value::Real(v)))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "sort order broken: {} vs {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn key_text_roundtrip() {
        let test_values = ["", "hello", "world", "hello\0world", "\0\0\0"];
        for &v in &test_values {
            let encoded = encode_key_value(&Value::Text(v.into()));
            let (decoded, _) = decode_key_value(&encoded).unwrap();
            assert_eq!(decoded, Value::Text(v.into()), "roundtrip failed for {v:?}");
        }
    }

    #[test]
    fn key_text_sort_order() {
        let values = ["", "a", "ab", "b", "ba", "z"];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| encode_key_value(&Value::Text(v.into())))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order broken: {:?} vs {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn key_blob_roundtrip() {
        let test_values: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00],
            vec![0x00, 0xFF],
            vec![0xFF, 0x00],
            vec![0x00, 0x00, 0x00],
        ];
        for v in &test_values {
            let encoded = encode_key_value(&Value::Blob(v.clone()));
            let (decoded, _) = decode_key_value(&encoded).unwrap();
            assert_eq!(decoded, Value::Blob(v.clone()));
        }
    }

    #[test]
    fn key_composite_roundtrip() {
        let values = vec![
            Value::Integer(42),
            Value::Text("hello".into()),
            Value::Boolean(true),
        ];
        let encoded = encode_composite_key(&values);
        let decoded = decode_composite_key(&encoded, 3).unwrap();
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Text("hello".into()));
        assert_eq!(decoded[2], Value::Boolean(true));
    }

    #[test]
    fn key_composite_sort_order() {
        // Composite keys: (1, "b") < (1, "c") < (2, "a")
        let k1 = encode_composite_key(&[Value::Integer(1), Value::Text("b".into())]);
        let k2 = encode_composite_key(&[Value::Integer(1), Value::Text("c".into())]);
        let k3 = encode_composite_key(&[Value::Integer(2), Value::Text("a".into())]);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn key_cross_type_ordering() {
        let null = encode_key_value(&Value::Null);
        let bool_val = encode_key_value(&Value::Boolean(false));
        let int = encode_key_value(&Value::Integer(0));
        let text = encode_key_value(&Value::Text("".into()));
        let blob = encode_key_value(&Value::Blob(vec![]));

        assert!(null < blob);
        assert!(blob < text);
        assert!(text < bool_val);
        assert!(bool_val < int);
    }

    // ── Row encoding tests ──────────────────────────────────────────

    #[test]
    fn row_roundtrip_simple() {
        let values = vec![
            Value::Integer(42),
            Value::Text("hello".into()),
            Value::Boolean(true),
        ];
        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Text("hello".into()));
        assert_eq!(decoded[2], Value::Boolean(true));
    }

    #[test]
    fn row_roundtrip_with_nulls() {
        let values = vec![
            Value::Integer(1),
            Value::Null,
            Value::Text("test".into()),
            Value::Null,
        ];
        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded).unwrap();
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0], Value::Integer(1));
        assert!(decoded[1].is_null());
        assert_eq!(decoded[2], Value::Text("test".into()));
        assert!(decoded[3].is_null());
    }

    #[test]
    fn row_roundtrip_empty() {
        let values: Vec<Value> = vec![];
        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn row_roundtrip_all_types() {
        let values = vec![
            Value::Integer(-100),
            Value::Real(3.15),
            Value::Text("hello world".into()),
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Value::Boolean(false),
            Value::Null,
        ];
        let encoded = encode_row(&values);
        let decoded = decode_row(&encoded).unwrap();
        assert_eq!(decoded.len(), 6);
        assert_eq!(decoded[0], Value::Integer(-100));
        assert_eq!(decoded[1], Value::Real(3.15));
        assert_eq!(decoded[2], Value::Text("hello world".into()));
        assert_eq!(decoded[3], Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(decoded[4], Value::Boolean(false));
        assert!(decoded[5].is_null());
    }

    #[test]
    fn null_escaped_with_embedded_nulls() {
        let text = "before\0after";
        let encoded = encode_key_value(&Value::Text(text.into()));
        let (decoded, _) = decode_key_value(&encoded).unwrap();
        assert_eq!(decoded, Value::Text(text.into()));
    }

    #[test]
    fn key_integer_edge_cases() {
        for v in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            let encoded = encode_key_value(&Value::Integer(v));
            let (decoded, n) = decode_key_value(&encoded).unwrap();
            assert_eq!(n, encoded.len());
            assert_eq!(decoded, Value::Integer(v), "edge case failed for {v}");
        }
    }

    #[test]
    fn decode_columns_single() {
        let values = vec![
            Value::Integer(42),
            Value::Text("hello".into()),
            Value::Boolean(true),
        ];
        let encoded = encode_row(&values);
        let cols = decode_columns(&encoded, &[1]).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0], Value::Text("hello".into()));
    }

    #[test]
    fn decode_columns_multiple() {
        let values = vec![
            Value::Integer(1),
            Value::Real(2.5),
            Value::Text("skip".into()),
            Value::Boolean(false),
            Value::Blob(vec![0xAB]),
        ];
        let encoded = encode_row(&values);
        let cols = decode_columns(&encoded, &[0, 3, 4]).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], Value::Integer(1));
        assert_eq!(cols[1], Value::Boolean(false));
        assert_eq!(cols[2], Value::Blob(vec![0xAB]));
    }

    #[test]
    fn decode_columns_with_nulls() {
        let values = vec![
            Value::Integer(10),
            Value::Null,
            Value::Text("after_null".into()),
            Value::Null,
            Value::Boolean(true),
        ];
        let encoded = encode_row(&values);
        let cols = decode_columns(&encoded, &[1, 2, 4]).unwrap();
        assert_eq!(cols.len(), 3);
        assert!(cols[0].is_null());
        assert_eq!(cols[1], Value::Text("after_null".into()));
        assert_eq!(cols[2], Value::Boolean(true));
    }

    #[test]
    fn decode_columns_first_and_last() {
        let values = vec![
            Value::Text("first".into()),
            Value::Integer(99),
            Value::Boolean(false),
            Value::Real(3.125),
        ];
        let encoded = encode_row(&values);
        let cols = decode_columns(&encoded, &[0, 3]).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], Value::Text("first".into()));
        assert_eq!(cols[1], Value::Real(3.125));
    }

    #[test]
    fn decode_columns_empty_targets() {
        let values = vec![Value::Integer(1)];
        let encoded = encode_row(&values);
        let cols = decode_columns(&encoded, &[]).unwrap();
        assert!(cols.is_empty());
    }

    #[test]
    fn decode_columns_all_matches_full_decode() {
        let values = vec![
            Value::Integer(-100),
            Value::Real(3.15),
            Value::Text("hello world".into()),
            Value::Blob(vec![0xDE, 0xAD]),
            Value::Boolean(false),
            Value::Null,
        ];
        let encoded = encode_row(&values);
        let full = decode_row(&encoded).unwrap();
        let selective = decode_columns(&encoded, &[0, 1, 2, 3, 4, 5]).unwrap();
        assert_eq!(full, selective);
    }

    #[test]
    fn raw_column_integer() {
        let values = vec![Value::Integer(42), Value::Text("hello".into())];
        let encoded = encode_row(&values);
        let raw = decode_column_raw(&encoded, 0).unwrap();
        assert!(matches!(raw, RawColumn::Integer(42)));
        assert_eq!(raw.to_value(), Value::Integer(42));
    }

    #[test]
    fn raw_column_text_borrows() {
        let values = vec![Value::Integer(1), Value::Text("hello".into())];
        let encoded = encode_row(&values);
        let raw = decode_column_raw(&encoded, 1).unwrap();
        match raw {
            RawColumn::Text(s) => assert_eq!(s, "hello"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn raw_column_null() {
        let values = vec![Value::Integer(1), Value::Null, Value::Boolean(true)];
        let encoded = encode_row(&values);
        let raw = decode_column_raw(&encoded, 1).unwrap();
        assert!(matches!(raw, RawColumn::Null));
    }

    #[test]
    fn raw_column_last() {
        let values = vec![
            Value::Integer(1),
            Value::Text("skip".into()),
            Value::Real(3.15),
        ];
        let encoded = encode_row(&values);
        let raw = decode_column_raw(&encoded, 2).unwrap();
        match raw {
            RawColumn::Real(r) => assert!((r - 3.15).abs() < 1e-10),
            other => panic!("expected Real, got {other:?}"),
        }
    }

    #[test]
    fn raw_column_out_of_bounds() {
        let values = vec![Value::Integer(1)];
        let encoded = encode_row(&values);
        assert!(decode_column_raw(&encoded, 1).is_err());
    }

    #[test]
    fn raw_column_eq_value() {
        let raw_int = RawColumn::Integer(42);
        assert!(raw_int.eq_value(&Value::Integer(42)));
        assert!(!raw_int.eq_value(&Value::Integer(43)));
        assert!(raw_int.eq_value(&Value::Real(42.0)));

        let raw_text = RawColumn::Text("hello");
        assert!(raw_text.eq_value(&Value::Text("hello".into())));
        assert!(!raw_text.eq_value(&Value::Text("world".into())));
    }

    #[test]
    fn raw_column_cmp_value() {
        use std::cmp::Ordering;
        let raw = RawColumn::Integer(42);
        assert_eq!(raw.cmp_value(&Value::Integer(42)), Some(Ordering::Equal));
        assert_eq!(raw.cmp_value(&Value::Integer(50)), Some(Ordering::Less));
        assert_eq!(raw.cmp_value(&Value::Integer(10)), Some(Ordering::Greater));
        assert_eq!(raw.cmp_value(&Value::Null), None);
    }

    #[test]
    fn raw_column_as_numeric() {
        assert_eq!(RawColumn::Integer(42).as_i64(), Some(42));
        assert_eq!(RawColumn::Integer(42).as_f64(), Some(42.0));
        assert_eq!(RawColumn::Real(3.15).as_f64(), Some(3.15));
        assert_eq!(RawColumn::Real(3.15).as_i64(), None);
        assert_eq!(RawColumn::Text("x").as_f64(), None);
        assert_eq!(RawColumn::Null.as_i64(), None);
    }

    #[test]
    fn decode_pk_integer_roundtrip() {
        for v in [0i64, 1, -1, 42, -1000, i64::MIN, i64::MAX] {
            let encoded = encode_key_value(&Value::Integer(v));
            let decoded = decode_pk_integer(&encoded).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn decode_pk_integer_rejects_non_integer() {
        let encoded = encode_key_value(&Value::Text("hello".into()));
        assert!(decode_pk_integer(&encoded).is_err());
    }

    #[test]
    fn raw_column_blob() {
        let values = vec![Value::Blob(vec![0xDE, 0xAD])];
        let encoded = encode_row(&values);
        let raw = decode_column_raw(&encoded, 0).unwrap();
        match raw {
            RawColumn::Blob(b) => assert_eq!(b, &[0xDE, 0xAD]),
            other => panic!("expected Blob, got {other:?}"),
        }
    }

    #[test]
    fn raw_column_matches_full_decode() {
        let values = vec![
            Value::Integer(-100),
            Value::Real(3.15),
            Value::Text("hello world".into()),
            Value::Blob(vec![0xDE, 0xAD]),
            Value::Boolean(false),
            Value::Null,
        ];
        let encoded = encode_row(&values);
        let full = decode_row(&encoded).unwrap();
        for (i, expected) in full.iter().enumerate() {
            let raw = decode_column_raw(&encoded, i).unwrap();
            assert_eq!(raw.to_value(), *expected, "mismatch at column {i}");
        }
    }
}
