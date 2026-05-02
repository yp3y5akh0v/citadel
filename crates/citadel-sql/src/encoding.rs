//! Order-preserving key encoding and row encoding for non-PK column storage.

use crate::error::{Result, SqlError};
use crate::types::{CompactString, DataType, Value};

/// Type tags for order-preserving key encoding.
const TAG_NULL: u8 = 0x00;
const TAG_BLOB: u8 = 0x01;
const TAG_TEXT: u8 = 0x02;
const TAG_BOOLEAN: u8 = 0x03;
const TAG_INTEGER: u8 = 0x04;
const TAG_REAL: u8 = 0x05;
const TAG_TIME: u8 = 0x06;
const TAG_DATE: u8 = 0x07;
const TAG_TIMESTAMP: u8 = 0x08;
const TAG_INTERVAL: u8 = 0x09;

/// Encode a single value into an order-preserving byte sequence.
pub fn encode_key_value(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    encode_key_value_into(value, &mut buf);
    buf
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

pub fn encode_composite_key_from_indices(indices: &[u16], row: &[Value], buf: &mut Vec<u8>) {
    buf.clear();
    for &i in indices {
        encode_key_value_into(&row[i as usize], buf);
    }
}

#[inline]
pub fn encode_int_key_into(val: i64, buf: &mut Vec<u8>) {
    buf.clear();
    encode_signed_varint(TAG_INTEGER, val, buf);
}

pub(crate) fn encode_key_value_into(value: &Value, buf: &mut Vec<u8>) {
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
        Value::Time(t) => encode_signed_varint(TAG_TIME, *t, buf),
        Value::Date(d) => encode_signed_varint(TAG_DATE, i64::from(*d), buf),
        Value::Timestamp(t) => encode_signed_varint(TAG_TIMESTAMP, *t, buf),
        Value::Interval {
            months,
            days,
            micros,
        } => {
            // 17 bytes: tag + (i32,i32,i64) BE with sign-flipped high byte per field.
            buf.push(TAG_INTERVAL);
            let mut mb = months.to_be_bytes();
            mb[0] ^= 0x80;
            buf.extend_from_slice(&mb);
            let mut db = days.to_be_bytes();
            db[0] ^= 0x80;
            buf.extend_from_slice(&db);
            let mut ub = micros.to_be_bytes();
            ub[0] ^= 0x80;
            buf.extend_from_slice(&ub);
        }
    }
}

fn encode_integer_into(val: i64, buf: &mut Vec<u8>) {
    encode_signed_varint(TAG_INTEGER, val, buf);
}

/// Order-preserving variable-width codec for signed i64 with a caller-supplied tag byte.
/// Layout: [tag] [marker] [data bytes].
/// marker = 0x80 for zero; 0x80+n for positive (n bytes follow);
/// 0x80-n for negative (n one's-complemented bytes follow).
/// Byte-wise lex compare matches signed integer order.
pub(crate) fn encode_signed_varint(tag: u8, val: i64, buf: &mut Vec<u8>) {
    buf.push(tag);
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
        TAG_TIME => decode_signed_varint(&data[1..]).map(|(v, n)| (Value::Time(v), n + 1)),
        TAG_DATE => decode_signed_varint(&data[1..]).map(|(v, n)| {
            let d = v.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
            (Value::Date(d), n + 1)
        }),
        TAG_TIMESTAMP => {
            decode_signed_varint(&data[1..]).map(|(v, n)| (Value::Timestamp(v), n + 1))
        }
        TAG_INTERVAL => {
            if data.len() < 1 + 16 {
                return Err(SqlError::InvalidValue("truncated interval".into()));
            }
            let mut mb: [u8; 4] = data[1..5].try_into().unwrap();
            mb[0] ^= 0x80;
            let mut db: [u8; 4] = data[5..9].try_into().unwrap();
            db[0] ^= 0x80;
            let mut ub: [u8; 8] = data[9..17].try_into().unwrap();
            ub[0] ^= 0x80;
            Ok((
                Value::Interval {
                    months: i32::from_be_bytes(mb),
                    days: i32::from_be_bytes(db),
                    micros: i64::from_be_bytes(ub),
                },
                17,
            ))
        }
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

fn decode_integer(data: &[u8]) -> Result<(Value, usize)> {
    let (v, n) = decode_signed_varint(data)?;
    Ok((Value::Integer(v), n))
}

/// Decode the variable-width codec emitted by `encode_signed_varint` (tag byte already consumed).
pub(crate) fn decode_signed_varint(data: &[u8]) -> Result<(i64, usize)> {
    if data.is_empty() {
        return Err(SqlError::InvalidValue("truncated integer".into()));
    }
    let marker = data[0];
    if marker == 0x80 {
        return Ok((0, 1));
    }
    if marker > 0x80 {
        let byte_count = (marker - 0x80) as usize;
        if data.len() < 1 + byte_count {
            return Err(SqlError::InvalidValue("truncated positive integer".into()));
        }
        let mut bytes = [0u8; 8];
        bytes[8 - byte_count..].copy_from_slice(&data[1..1 + byte_count]);
        let val = i64::from_be_bytes(bytes);
        Ok((val, 1 + byte_count))
    } else {
        let byte_count = (0x80 - marker) as usize;
        if data.len() < 1 + byte_count {
            return Err(SqlError::InvalidValue("truncated negative integer".into()));
        }
        let mut bytes = [0u8; 8];
        for i in 0..byte_count {
            bytes[8 - byte_count + i] = !data[1 + i];
        }
        let abs_val = u64::from_be_bytes(bytes);
        let val = (-(abs_val as i128)) as i64;
        Ok((val, 1 + byte_count))
    }
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

fn encode_cell_v2(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Integer(val) => {
            buf.push(DataType::Integer.type_tag());
            buf.extend_from_slice(&val.to_le_bytes());
        }
        Value::Real(r) => {
            buf.push(DataType::Real.type_tag());
            buf.extend_from_slice(&r.to_le_bytes());
        }
        Value::Boolean(b) => {
            buf.push(DataType::Boolean.type_tag());
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
        Value::Time(t) => {
            buf.push(DataType::Time.type_tag());
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Date(d) => {
            buf.push(DataType::Date.type_tag());
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(t) => {
            buf.push(DataType::Timestamp.type_tag());
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            buf.push(DataType::Interval.type_tag());
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&micros.to_le_bytes());
        }
        Value::Null => unreachable!(),
    }
}

pub fn encode_row(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_row_into(values, &mut buf);
    buf
}

pub fn encode_row_into(values: &[Value], buf: &mut Vec<u8>) {
    buf.clear();
    let col_count = values.len();
    let bitmap_bytes = col_count.div_ceil(8);

    let header = (col_count as u16) | V2_FLAG;
    buf.extend_from_slice(&header.to_le_bytes());

    let bitmap_start = buf.len();
    buf.resize(buf.len() + bitmap_bytes, 0);

    for (i, v) in values.iter().enumerate() {
        if v.is_null() {
            buf[bitmap_start + i / 8] |= 1 << (i % 8);
            continue;
        }
        encode_cell_v2(v, buf);
    }
}

pub struct IntRowTemplate {
    pub template: Vec<u8>,
    pub slot_offsets: Vec<(usize, usize)>,
}

pub fn build_int_row_template(phys_count: usize, null_slots: &[usize]) -> IntRowTemplate {
    let bitmap_bytes = phys_count.div_ceil(8);
    let mut template = Vec::with_capacity(2 + bitmap_bytes + phys_count * 9);
    let header = (phys_count as u16) | V2_FLAG;
    template.extend_from_slice(&header.to_le_bytes());
    let bitmap_start = template.len();
    template.resize(bitmap_start + bitmap_bytes, 0);
    for &i in null_slots {
        template[bitmap_start + i / 8] |= 1 << (i % 8);
    }
    let mut slot_offsets = Vec::with_capacity(phys_count.saturating_sub(null_slots.len()));
    for slot in 0..phys_count {
        if null_slots.contains(&slot) {
            continue;
        }
        template.push(DataType::Integer.type_tag());
        let value_offset = template.len();
        template.extend_from_slice(&[0u8; 8]);
        slot_offsets.push((slot, value_offset));
    }
    IntRowTemplate {
        template,
        slot_offsets,
    }
}

/// Caller must guarantee every non-NULL `values[slot]` is `Value::Integer`.
#[inline]
pub fn encode_int_row_with_template(
    tmpl: &IntRowTemplate,
    values: &[Value],
    buf: &mut Vec<u8>,
) -> Result<()> {
    buf.clear();
    buf.extend_from_slice(&tmpl.template);
    for &(slot, off) in &tmpl.slot_offsets {
        match &values[slot] {
            Value::Integer(v) => buf[off..off + 8].copy_from_slice(&v.to_le_bytes()),
            other => {
                return Err(SqlError::TypeMismatch {
                    expected: "Integer".into(),
                    got: other.data_type().to_string(),
                });
            }
        }
    }
    Ok(())
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
        Some(DataType::Time) => Ok(Value::Time(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Date) => Ok(Value::Date(i32::from_le_bytes(
            data[..4].try_into().unwrap(),
        ))),
        Some(DataType::Timestamp) => Ok(Value::Timestamp(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Interval) => {
            if data.len() < 16 {
                return Err(SqlError::InvalidValue("truncated interval".into()));
            }
            let months = i32::from_le_bytes(data[0..4].try_into().unwrap());
            let days = i32::from_le_bytes(data[4..8].try_into().unwrap());
            let micros = i64::from_le_bytes(data[8..16].try_into().unwrap());
            Ok(Value::Interval {
                months,
                days,
                micros,
            })
        }
        _ => Err(SqlError::InvalidValue(format!(
            "unknown column type tag: {type_tag}"
        ))),
    }
}

/// V1 cells: `[tag:u8][len:u32][data]`. V2 cells drop `len` for fixed-width types.
/// High bit of `col_count:u16` flags V2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RowVersion {
    V1,
    V2,
}

pub(crate) const V2_FLAG: u16 = 0x8000;
pub(crate) const COL_COUNT_MASK: u16 = 0x7FFF;

#[inline]
pub(crate) fn fixed_width_size(type_tag: u8) -> Option<usize> {
    match DataType::from_tag(type_tag)? {
        DataType::Integer | DataType::Real | DataType::Time | DataType::Timestamp => Some(8),
        DataType::Date => Some(4),
        DataType::Boolean => Some(1),
        DataType::Interval => Some(16),
        DataType::Text | DataType::Blob | DataType::Null => None,
    }
}

#[inline]
fn read_cell(data: &[u8], pos: usize, version: RowVersion) -> Result<(u8, &[u8], usize)> {
    if pos >= data.len() {
        return Err(SqlError::InvalidValue("truncated column data".into()));
    }
    let type_tag = data[pos];
    let after_tag = pos + 1;
    let (data_len, body_pos) = match version {
        RowVersion::V2 => match fixed_width_size(type_tag) {
            Some(n) => (n, after_tag),
            None => {
                if after_tag + 4 > data.len() {
                    return Err(SqlError::InvalidValue("truncated column data".into()));
                }
                let len = u32::from_le_bytes([
                    data[after_tag],
                    data[after_tag + 1],
                    data[after_tag + 2],
                    data[after_tag + 3],
                ]) as usize;
                (len, after_tag + 4)
            }
        },
        RowVersion::V1 => {
            if after_tag + 4 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let len = u32::from_le_bytes([
                data[after_tag],
                data[after_tag + 1],
                data[after_tag + 2],
                data[after_tag + 3],
            ]) as usize;
            (len, after_tag + 4)
        }
    };
    if body_pos + data_len > data.len() {
        return Err(SqlError::InvalidValue("truncated column value".into()));
    }
    Ok((
        type_tag,
        &data[body_pos..body_pos + data_len],
        body_pos + data_len,
    ))
}

#[inline]
fn skip_cell(data: &[u8], pos: usize, version: RowVersion) -> Result<usize> {
    let (_, _, next) = read_cell(data, pos, version)?;
    Ok(next)
}

fn copy_cell_to_v2(
    data: &[u8],
    pos: usize,
    version: RowVersion,
    out: &mut Vec<u8>,
) -> Result<usize> {
    let (tag, body, next) = read_cell(data, pos, version)?;
    out.push(tag);
    if fixed_width_size(tag).is_none() {
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    }
    out.extend_from_slice(body);
    Ok(next)
}

fn parse_row_header(data: &[u8]) -> Result<(RowVersion, usize, &[u8], usize)> {
    if data.len() < 2 {
        return Err(SqlError::InvalidValue("row data too short".into()));
    }
    let raw = u16::from_le_bytes([data[0], data[1]]);
    let version = if raw & V2_FLAG != 0 {
        RowVersion::V2
    } else {
        RowVersion::V1
    };
    let col_count = (raw & COL_COUNT_MASK) as usize;
    let bitmap_bytes = col_count.div_ceil(8);
    let pos = 2;
    if data.len() < pos + bitmap_bytes {
        return Err(SqlError::InvalidValue("truncated null bitmap".into()));
    }
    Ok((
        version,
        col_count,
        &data[pos..pos + bitmap_bytes],
        pos + bitmap_bytes,
    ))
}

pub fn decode_row(data: &[u8]) -> Result<Vec<Value>> {
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;

    let mut values = Vec::with_capacity(col_count);
    for i in 0..col_count {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            values.push(Value::Null);
            continue;
        }
        let (type_tag, body, next) = read_cell(data, pos, version)?;
        values.push(decode_value(type_tag, body)?);
        pos = next;
    }

    Ok(values)
}

/// Returns the number of non-PK columns stored in a row value blob.
#[inline]
pub fn row_non_pk_count(data: &[u8]) -> usize {
    (u16::from_le_bytes([data[0], data[1]]) & COL_COUNT_MASK) as usize
}

pub fn decode_row_into(data: &[u8], out: &mut [Value], col_mapping: &[usize]) -> Result<()> {
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;

    for i in 0..col_count {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            continue;
        }
        let (type_tag, body, next) = read_cell(data, pos, version)?;
        if i < col_mapping.len() && col_mapping[i] != usize::MAX {
            out[col_mapping[i]] = decode_value(type_tag, body)?;
        }
        pos = next;
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
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;

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
                let (type_tag, body, next) = read_cell(data, pos, version)?;
                results.push(decode_value(type_tag, body)?);
                pos = next;
            }
            ti += 1;
        } else if !is_null {
            pos = skip_cell(data, pos, version)?;
        }
    }

    while ti < targets.len() {
        results.push(Value::Null);
        ti += 1;
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
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;

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
                let (type_tag, body, next) = read_cell(data, pos, version)?;
                row[schema_cols[ti]] = decode_value(type_tag, body)?;
                pos = next;
            }
            ti += 1;
        } else if !is_null {
            pos = skip_cell(data, pos, version)?;
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
    Time(i64),
    Date(i32),
    Timestamp(i64),
    Interval { months: i32, days: i32, micros: i64 },
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
            RawColumn::Time(t) => Value::Time(t),
            RawColumn::Date(d) => Value::Date(d),
            RawColumn::Timestamp(t) => Value::Timestamp(t),
            RawColumn::Interval {
                months,
                days,
                micros,
            } => Value::Interval {
                months,
                days,
                micros,
            },
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
            (RawColumn::Time(a), Value::Time(b)) => Some(a.cmp(b)),
            (RawColumn::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (RawColumn::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (
                RawColumn::Interval {
                    months: am,
                    days: ad,
                    micros: au,
                },
                Value::Interval {
                    months: bm,
                    days: bd,
                    micros: bu,
                },
            ) => Some(am.cmp(bm).then(ad.cmp(bd)).then(au.cmp(bu))),
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
            (RawColumn::Time(a), Value::Time(b)) => a == b,
            (RawColumn::Date(a), Value::Date(b)) => a == b,
            (RawColumn::Timestamp(a), Value::Timestamp(b)) => a == b,
            (
                RawColumn::Interval {
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
            RawColumn::Time(t) => Some(*t),
            RawColumn::Date(d) => Some(*d as i64),
            RawColumn::Timestamp(t) => Some(*t),
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
        Some(DataType::Time) => Ok(RawColumn::Time(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Date) => Ok(RawColumn::Date(i32::from_le_bytes(
            data[..4].try_into().unwrap(),
        ))),
        Some(DataType::Timestamp) => Ok(RawColumn::Timestamp(i64::from_le_bytes(
            data[..8].try_into().unwrap(),
        ))),
        Some(DataType::Interval) => {
            if data.len() < 16 {
                return Err(SqlError::InvalidValue("truncated interval".into()));
            }
            let months = i32::from_le_bytes(data[0..4].try_into().unwrap());
            let days = i32::from_le_bytes(data[4..8].try_into().unwrap());
            let micros = i64::from_le_bytes(data[8..16].try_into().unwrap());
            Ok(RawColumn::Interval {
                months,
                days,
                micros,
            })
        }
        _ => Err(SqlError::InvalidValue(format!(
            "unknown column type tag: {type_tag}"
        ))),
    }
}

/// Patch column in-place if value size unchanged. Ok(false) = size mismatch, use `patch_row_column`.
pub fn patch_column_in_place(data: &mut [u8], target: usize, new_val: &Value) -> Result<bool> {
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;
    if target >= col_count || new_val.is_null() {
        return Ok(false);
    }
    let was_null = bitmap[target / 8] & (1 << (target % 8)) != 0;
    if was_null {
        return Ok(false);
    }
    for col in 0..target {
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;
        if !is_null {
            pos = skip_cell(data, pos, version)?;
        }
    }
    let type_tag = data[pos];
    let (old_data_len, val_start) = match version {
        RowVersion::V2 => match fixed_width_size(type_tag) {
            Some(n) => (n, pos + 1),
            None => {
                if pos + 5 > data.len() {
                    return Err(SqlError::InvalidValue("truncated column data".into()));
                }
                let len = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
                (len, pos + 5)
            }
        },
        RowVersion::V1 => {
            if pos + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let len = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
            (len, pos + 5)
        }
    };
    let new_data_len = match new_val {
        Value::Integer(_) | Value::Real(_) | Value::Time(_) | Value::Timestamp(_) => 8,
        Value::Date(_) => 4,
        Value::Interval { .. } => 16,
        Value::Boolean(_) => 1,
        Value::Text(s) => s.len(),
        Value::Blob(b) => b.len(),
        Value::Null => return Ok(false),
    };
    if new_data_len != old_data_len {
        return Ok(false);
    }
    data[pos] = new_val.data_type().type_tag();
    match new_val {
        Value::Integer(v) => data[val_start..val_start + 8].copy_from_slice(&v.to_le_bytes()),
        Value::Real(r) => data[val_start..val_start + 8].copy_from_slice(&r.to_le_bytes()),
        Value::Boolean(b) => data[val_start] = if *b { 1 } else { 0 },
        Value::Text(s) => data[val_start..val_start + s.len()].copy_from_slice(s.as_bytes()),
        Value::Blob(d) => data[val_start..val_start + d.len()].copy_from_slice(d),
        Value::Time(t) => data[val_start..val_start + 8].copy_from_slice(&t.to_le_bytes()),
        Value::Date(d) => data[val_start..val_start + 4].copy_from_slice(&d.to_le_bytes()),
        Value::Timestamp(t) => data[val_start..val_start + 8].copy_from_slice(&t.to_le_bytes()),
        Value::Interval {
            months,
            days,
            micros,
        } => {
            data[val_start..val_start + 4].copy_from_slice(&months.to_le_bytes());
            data[val_start + 4..val_start + 8].copy_from_slice(&days.to_le_bytes());
            data[val_start + 8..val_start + 16].copy_from_slice(&micros.to_le_bytes());
        }
        Value::Null => unreachable!(),
    }
    Ok(true)
}

/// Patch a single column in encoded row, writing result into `out`. Copies others unchanged.
pub fn patch_row_column(
    data: &[u8],
    target: usize,
    new_val: &Value,
    out: &mut Vec<u8>,
) -> Result<()> {
    let (version, col_count, bitmap, header_end) = parse_row_header(data)?;

    let new_col_count = if target >= col_count {
        target + 1
    } else {
        col_count
    };
    let new_bitmap_bytes = new_col_count.div_ceil(8);
    let bitmap_bytes = col_count.div_ceil(8);
    out.clear();

    let header = (new_col_count as u16) | V2_FLAG;
    out.extend_from_slice(&header.to_le_bytes());
    let bitmap_start = out.len();
    out.extend_from_slice(&data[2..2 + bitmap_bytes]);
    for _ in bitmap_bytes..new_bitmap_bytes {
        out.push(0xFF);
    }
    if new_val.is_null() {
        out[bitmap_start + target / 8] |= 1 << (target % 8);
    } else {
        out[bitmap_start + target / 8] &= !(1 << (target % 8));
    }

    let mut pos = header_end;
    for col in 0..new_col_count {
        let was_null = if col < col_count {
            bitmap[col / 8] & (1 << (col % 8)) != 0
        } else {
            true
        };

        if col == target {
            if !was_null {
                pos = skip_cell(data, pos, version)?;
            }
            if !new_val.is_null() {
                encode_cell_v2(new_val, out);
            }
        } else if !was_null {
            pos = copy_cell_to_v2(data, pos, version, out)?;
        }
    }
    Ok(())
}

pub fn decode_column_raw(data: &[u8], target: usize) -> Result<RawColumn<'_>> {
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;
    if target >= col_count {
        return Ok(RawColumn::Null);
    }

    for col in 0..=target {
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;

        if col == target {
            if is_null {
                return Ok(RawColumn::Null);
            }
            let (type_tag, body, _) = read_cell(data, pos, version)?;
            return decode_value_raw(type_tag, body);
        } else if !is_null {
            pos = skip_cell(data, pos, version)?;
        }
    }

    unreachable!()
}

/// Like `decode_column_raw` but also returns the byte offset (usize::MAX if NULL).
pub fn decode_column_with_offset(data: &[u8], target: usize) -> Result<(RawColumn<'_>, usize)> {
    let (version, col_count, bitmap, mut pos) = parse_row_header(data)?;
    if target >= col_count {
        return Ok((RawColumn::Null, usize::MAX));
    }

    for col in 0..=target {
        let is_null = bitmap[col / 8] & (1 << (col % 8)) != 0;

        if col == target {
            if is_null {
                return Ok((RawColumn::Null, usize::MAX));
            }
            let tag_offset = pos;
            let (type_tag, body, _) = read_cell(data, pos, version)?;
            let raw = decode_value_raw(type_tag, body)?;
            return Ok((raw, tag_offset));
        } else if !is_null {
            pos = skip_cell(data, pos, version)?;
        }
    }

    unreachable!()
}

/// Patch at a known byte offset. Ok(false) if size mismatch or NULL offset.
pub fn patch_at_offset(data: &mut [u8], offset: usize, new_val: &Value) -> Result<bool> {
    if offset == usize::MAX || new_val.is_null() {
        return Ok(false);
    }
    if data.len() < 2 || offset >= data.len() {
        return Err(SqlError::InvalidValue("truncated column data".into()));
    }
    let version = if u16::from_le_bytes([data[0], data[1]]) & V2_FLAG != 0 {
        RowVersion::V2
    } else {
        RowVersion::V1
    };
    let type_tag = data[offset];
    let (old_data_len, val_start) = match version {
        RowVersion::V2 => match fixed_width_size(type_tag) {
            Some(n) => (n, offset + 1),
            None => {
                if offset + 5 > data.len() {
                    return Err(SqlError::InvalidValue("truncated column data".into()));
                }
                let len =
                    u32::from_le_bytes(data[offset + 1..offset + 5].try_into().unwrap()) as usize;
                (len, offset + 5)
            }
        },
        RowVersion::V1 => {
            if offset + 5 > data.len() {
                return Err(SqlError::InvalidValue("truncated column data".into()));
            }
            let len = u32::from_le_bytes(data[offset + 1..offset + 5].try_into().unwrap()) as usize;
            (len, offset + 5)
        }
    };
    let new_data_len = match new_val {
        Value::Integer(_) | Value::Real(_) | Value::Time(_) | Value::Timestamp(_) => 8,
        Value::Date(_) => 4,
        Value::Interval { .. } => 16,
        Value::Boolean(_) => 1,
        Value::Text(s) => s.len(),
        Value::Blob(b) => b.len(),
        Value::Null => return Ok(false),
    };
    if new_data_len != old_data_len {
        return Ok(false);
    }
    data[offset] = new_val.data_type().type_tag();
    match new_val {
        Value::Integer(v) => data[val_start..val_start + 8].copy_from_slice(&v.to_le_bytes()),
        Value::Real(r) => data[val_start..val_start + 8].copy_from_slice(&r.to_le_bytes()),
        Value::Boolean(b) => data[val_start] = if *b { 1 } else { 0 },
        Value::Text(s) => data[val_start..val_start + s.len()].copy_from_slice(s.as_bytes()),
        Value::Blob(d) => data[val_start..val_start + d.len()].copy_from_slice(d),
        Value::Time(t) => data[val_start..val_start + 8].copy_from_slice(&t.to_le_bytes()),
        Value::Date(d) => data[val_start..val_start + 4].copy_from_slice(&d.to_le_bytes()),
        Value::Timestamp(t) => data[val_start..val_start + 8].copy_from_slice(&t.to_le_bytes()),
        Value::Interval {
            months,
            days,
            micros,
        } => {
            data[val_start..val_start + 4].copy_from_slice(&months.to_le_bytes());
            data[val_start + 4..val_start + 8].copy_from_slice(&days.to_le_bytes());
            data[val_start + 8..val_start + 16].copy_from_slice(&micros.to_le_bytes());
        }
        Value::Null => unreachable!(),
    }
    Ok(true)
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
#[path = "encoding_tests.rs"]
mod tests;
