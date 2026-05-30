use std::sync::Arc;

use crate::error::{Result, SqlError};
use crate::types::Value;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonbType {
    Null = 0,
    True = 1,
    False = 2,
    Integer = 3,
    Real = 4,
    String = 5,
    Array = 6,
    Object = 7,
}

impl JsonbType {
    fn from_nibble(n: u8) -> Option<Self> {
        match n {
            0 => Some(Self::Null),
            1 => Some(Self::True),
            2 => Some(Self::False),
            3 => Some(Self::Integer),
            4 => Some(Self::Real),
            5 => Some(Self::String),
            6 => Some(Self::Array),
            7 => Some(Self::Object),
            _ => None,
        }
    }
}

const SIZE_CLASS_U8: u8 = 12;
const SIZE_CLASS_U16: u8 = 13;
const SIZE_CLASS_U32: u8 = 14;
const SIZE_CLASS_U64: u8 = 15;

pub fn validate_text(s: &str) -> Result<()> {
    serde_json::from_str::<serde_json::Value>(s)
        .map(|_| ())
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}")))
}

pub fn text_to_jsonb(s: &str) -> Result<Value> {
    let v: serde_json::Value = serde_json::from_str(s)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}")))?;
    reject_null_bytes(&v)?;
    let mut buf = Vec::with_capacity(s.len());
    encode_canonical(&v, &mut buf)?;
    Ok(Value::Jsonb(Arc::from(buf)))
}

fn reject_null_bytes(v: &serde_json::Value) -> Result<()> {
    match v {
        serde_json::Value::String(s) if s.contains('\0') => Err(SqlError::InvalidValue(
            "unsupported Unicode escape sequence \\u0000".into(),
        )),
        serde_json::Value::Array(items) => items.iter().try_for_each(reject_null_bytes),
        serde_json::Value::Object(map) => map.iter().try_for_each(|(k, v)| {
            if k.contains('\0') {
                return Err(SqlError::InvalidValue(
                    "unsupported Unicode escape sequence \\u0000".into(),
                ));
            }
            reject_null_bytes(v)
        }),
        _ => Ok(()),
    }
}

pub fn decode_to_text(bytes: &[u8]) -> Result<String> {
    let v = decode_to_serde(bytes)?;
    serde_json::to_string(&v).map_err(|e| SqlError::InvalidValue(format!("JSONB render: {e}")))
}

pub fn decode_to_serde(bytes: &[u8]) -> Result<serde_json::Value> {
    let mut pos = 0;
    let v = decode_value(bytes, &mut pos)?;
    if pos != bytes.len() {
        return Err(SqlError::InvalidValue("trailing bytes in JSONB".into()));
    }
    Ok(v)
}

pub fn encode_canonical(v: &serde_json::Value, out: &mut Vec<u8>) -> Result<()> {
    match v {
        serde_json::Value::Null => out.push(header_byte(JsonbType::Null, 0)),
        serde_json::Value::Bool(true) => out.push(header_byte(JsonbType::True, 0)),
        serde_json::Value::Bool(false) => out.push(header_byte(JsonbType::False, 0)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push(header_byte(JsonbType::Integer, 0));
                out.extend_from_slice(&i.to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() {
                    out.push(header_byte(JsonbType::Real, 0));
                    out.extend_from_slice(&f.to_le_bytes());
                } else {
                    return Err(SqlError::InvalidValue("non-finite number in JSON".into()));
                }
            } else {
                return Err(SqlError::InvalidValue(format!("unsupported number: {n}")));
            }
        }
        serde_json::Value::String(s) => encode_string(s, out),
        serde_json::Value::Array(items) => {
            let mut payload = Vec::new();
            for item in items {
                encode_canonical(item, &mut payload)?;
            }
            write_header_with_len(JsonbType::Array, payload.len(), out);
            out.extend_from_slice(&payload);
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut payload = Vec::new();
            for k in keys {
                encode_string(k, &mut payload);
                encode_canonical(&map[k], &mut payload)?;
            }
            write_header_with_len(JsonbType::Object, payload.len(), out);
            out.extend_from_slice(&payload);
        }
    }
    Ok(())
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    write_header_with_len(JsonbType::String, bytes.len(), out);
    out.extend_from_slice(bytes);
}

fn header_byte(ty: JsonbType, size_class: u8) -> u8 {
    debug_assert!(size_class <= 15);
    (ty as u8) << 4 | (size_class & 0x0F)
}

fn write_header_with_len(ty: JsonbType, len: usize, out: &mut Vec<u8>) {
    if len <= 11 {
        out.push(header_byte(ty, len as u8));
    } else if len <= u8::MAX as usize {
        out.push(header_byte(ty, SIZE_CLASS_U8));
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(header_byte(ty, SIZE_CLASS_U16));
        out.extend_from_slice(&(len as u16).to_le_bytes());
    } else if len <= u32::MAX as usize {
        out.push(header_byte(ty, SIZE_CLASS_U32));
        out.extend_from_slice(&(len as u32).to_le_bytes());
    } else {
        out.push(header_byte(ty, SIZE_CLASS_U64));
        out.extend_from_slice(&(len as u64).to_le_bytes());
    }
}

pub fn read_header(bytes: &[u8]) -> Result<(JsonbType, usize, usize)> {
    if bytes.is_empty() {
        return Err(SqlError::InvalidValue("empty JSONB".into()));
    }
    let h = bytes[0];
    let ty = JsonbType::from_nibble(h >> 4)
        .ok_or_else(|| SqlError::InvalidValue("invalid JSONB type tag".into()))?;
    let size_class = h & 0x0F;
    let (payload_start, payload_len) = match size_class {
        0..=11 => (1, size_class as usize),
        SIZE_CLASS_U8 => {
            if bytes.len() < 2 {
                return Err(SqlError::InvalidValue("truncated JSONB header".into()));
            }
            (2, bytes[1] as usize)
        }
        SIZE_CLASS_U16 => {
            if bytes.len() < 3 {
                return Err(SqlError::InvalidValue("truncated JSONB header".into()));
            }
            (3, u16::from_le_bytes([bytes[1], bytes[2]]) as usize)
        }
        SIZE_CLASS_U32 => {
            if bytes.len() < 5 {
                return Err(SqlError::InvalidValue("truncated JSONB header".into()));
            }
            (
                5,
                u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize,
            )
        }
        SIZE_CLASS_U64 => {
            if bytes.len() < 9 {
                return Err(SqlError::InvalidValue("truncated JSONB header".into()));
            }
            let arr: [u8; 8] = bytes[1..9].try_into().unwrap();
            (9, u64::from_le_bytes(arr) as usize)
        }
        _ => unreachable!(),
    };
    let fixed_payload = match ty {
        JsonbType::Null | JsonbType::True | JsonbType::False => Some(0),
        JsonbType::Integer | JsonbType::Real => Some(8),
        _ => None,
    };
    let payload_len = fixed_payload.unwrap_or(payload_len);
    if payload_start + payload_len > bytes.len() {
        return Err(SqlError::InvalidValue("JSONB payload truncated".into()));
    }
    Ok((ty, payload_start, payload_len))
}

pub fn skip_value(bytes: &[u8]) -> Result<usize> {
    let (_ty, payload_start, payload_len) = read_header(bytes)?;
    Ok(payload_start + payload_len)
}

pub fn find_object_key<'a>(bytes: &'a [u8], key: &str) -> Result<Option<&'a [u8]>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Object {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let key_bytes = key.as_bytes();
    let mut pos = 0usize;
    while pos < payload.len() {
        let (kty, kp_start, kp_len) = read_header(&payload[pos..])?;
        if kty != JsonbType::String {
            return Err(SqlError::InvalidValue("JSONB object key not string".into()));
        }
        let k_total = kp_start + kp_len;
        let k_slice = &payload[pos + kp_start..pos + k_total];
        let value_start = pos + k_total;
        let value_total = skip_value(&payload[value_start..])?;
        if k_slice == key_bytes {
            return Ok(Some(&payload[value_start..value_start + value_total]));
        }
        pos = value_start + value_total;
    }
    Ok(None)
}

pub fn array_get(bytes: &[u8], idx: i64) -> Result<Option<&[u8]>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Array {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    if idx < 0 {
        let mut elems = Vec::new();
        let mut pos = 0usize;
        while pos < payload.len() {
            let total = skip_value(&payload[pos..])?;
            elems.push((pos, total));
            pos += total;
        }
        let len = elems.len() as i64;
        let real = len + idx;
        if real < 0 {
            return Ok(None);
        }
        let (start, total) = elems[real as usize];
        return Ok(Some(&payload[start..start + total]));
    }
    let mut pos = 0usize;
    let mut remaining = idx;
    while pos < payload.len() {
        let total = skip_value(&payload[pos..])?;
        if remaining == 0 {
            return Ok(Some(&payload[pos..pos + total]));
        }
        remaining -= 1;
        pos += total;
    }
    Ok(None)
}

pub fn array_len_bytes(bytes: &[u8]) -> Result<Option<usize>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Array {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos < payload.len() {
        pos += skip_value(&payload[pos..])?;
        count += 1;
    }
    Ok(Some(count))
}

pub fn object_len_bytes(bytes: &[u8]) -> Result<Option<usize>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Object {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos < payload.len() {
        pos += skip_value(&payload[pos..])?;
        pos += skip_value(&payload[pos..])?;
        count += 1;
    }
    Ok(Some(count))
}

pub fn read_scalar_text(bytes: &[u8]) -> Result<Option<String>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    let payload = &bytes[payload_start..payload_start + payload_len];
    match ty {
        JsonbType::Null => Ok(None),
        JsonbType::True => Ok(Some("true".into())),
        JsonbType::False => Ok(Some("false".into())),
        JsonbType::Integer => {
            let arr: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB integer payload size".into()))?;
            Ok(Some(i64::from_le_bytes(arr).to_string()))
        }
        JsonbType::Real => {
            let arr: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB real payload size".into()))?;
            let f = f64::from_le_bytes(arr);
            let n = serde_json::Number::from_f64(f)
                .ok_or_else(|| SqlError::InvalidValue("non-finite JSONB number".into()))?;
            Ok(Some(n.to_string()))
        }
        JsonbType::String => {
            let s = std::str::from_utf8(payload)
                .map_err(|_| SqlError::InvalidValue("JSONB string not UTF-8".into()))?;
            Ok(Some(s.to_string()))
        }
        JsonbType::Array | JsonbType::Object => {
            let v = decode_to_serde(bytes)?;
            Ok(Some(serde_json::to_string(&v).map_err(|e| {
                SqlError::InvalidValue(format!("JSON render: {e}"))
            })?))
        }
    }
}

pub fn jsonb_contains_bytes(lhs: &[u8], rhs: &[u8]) -> Result<bool> {
    let (lty, lps, lpl) = read_header(lhs)?;
    let (rty, rps, rpl) = read_header(rhs)?;
    let lpay = &lhs[lps..lps + lpl];
    let rpay = &rhs[rps..rps + rpl];
    match (lty, rty) {
        (JsonbType::Object, JsonbType::Object) => {
            let mut rp = 0usize;
            while rp < rpay.len() {
                let (_rkty, rkps, rkpl) = read_header(&rpay[rp..])?;
                let rk_total = rkps + rkpl;
                let rk = &rpay[rp + rkps..rp + rk_total];
                let rv_start = rp + rk_total;
                let rv_total = skip_value(&rpay[rv_start..])?;
                let rv = &rpay[rv_start..rv_start + rv_total];
                let mut lp = 0usize;
                let mut found = false;
                while lp < lpay.len() {
                    let (_lkty, lkps, lkpl) = read_header(&lpay[lp..])?;
                    let lk_total = lkps + lkpl;
                    let lk = &lpay[lp + lkps..lp + lk_total];
                    let lv_start = lp + lk_total;
                    let lv_total = skip_value(&lpay[lv_start..])?;
                    if lk == rk {
                        let lv = &lpay[lv_start..lv_start + lv_total];
                        if !jsonb_contains_bytes(lv, rv)? {
                            return Ok(false);
                        }
                        found = true;
                        break;
                    }
                    lp = lv_start + lv_total;
                }
                if !found {
                    return Ok(false);
                }
                rp = rv_start + rv_total;
            }
            Ok(true)
        }
        (JsonbType::Array, JsonbType::Array) => {
            let mut rp = 0usize;
            while rp < rpay.len() {
                let rv_total = skip_value(&rpay[rp..])?;
                let rv = &rpay[rp..rp + rv_total];
                let mut lp = 0usize;
                let mut found = false;
                while lp < lpay.len() {
                    let lv_total = skip_value(&lpay[lp..])?;
                    let lv = &lpay[lp..lp + lv_total];
                    if jsonb_contains_bytes(lv, rv)? {
                        found = true;
                        break;
                    }
                    lp += lv_total;
                }
                if !found {
                    return Ok(false);
                }
                rp += rv_total;
            }
            Ok(true)
        }
        (JsonbType::Array, _) => {
            let r_total = rps + rpl;
            let r_full = &rhs[..r_total];
            let mut lp = 0usize;
            while lp < lpay.len() {
                let lv_total = skip_value(&lpay[lp..])?;
                if &lpay[lp..lp + lv_total] == r_full {
                    return Ok(true);
                }
                lp += lv_total;
            }
            Ok(false)
        }
        _ => {
            let l_total = lps + lpl;
            let r_total = rps + rpl;
            Ok(lhs[..l_total] == rhs[..r_total])
        }
    }
}

pub fn has_top_key_bytes(bytes: &[u8], key: &str) -> Result<bool> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    let payload = &bytes[payload_start..payload_start + payload_len];
    let key_bytes = key.as_bytes();
    match ty {
        JsonbType::Object => {
            let mut pos = 0usize;
            while pos < payload.len() {
                let (_kty, kps, kpl) = read_header(&payload[pos..])?;
                let k_total = kps + kpl;
                if &payload[pos + kps..pos + k_total] == key_bytes {
                    return Ok(true);
                }
                pos += k_total;
                pos += skip_value(&payload[pos..])?;
            }
            Ok(false)
        }
        JsonbType::Array => {
            let mut pos = 0usize;
            while pos < payload.len() {
                let (ety, eps, epl) = read_header(&payload[pos..])?;
                if ety == JsonbType::String && &payload[pos + eps..pos + eps + epl] == key_bytes {
                    return Ok(true);
                }
                pos += eps + epl;
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn decode_value(bytes: &[u8], pos: &mut usize) -> Result<serde_json::Value> {
    let (ty, payload_start, payload_len) = read_header(&bytes[*pos..])?;
    let payload = &bytes[*pos + payload_start..*pos + payload_start + payload_len];
    let total = payload_start + payload_len;
    let v = match ty {
        JsonbType::Null => serde_json::Value::Null,
        JsonbType::True => serde_json::Value::Bool(true),
        JsonbType::False => serde_json::Value::Bool(false),
        JsonbType::Integer => {
            let arr: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB integer payload size".into()))?;
            serde_json::Value::Number(i64::from_le_bytes(arr).into())
        }
        JsonbType::Real => {
            let arr: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB real payload size".into()))?;
            let f = f64::from_le_bytes(arr);
            serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .ok_or_else(|| SqlError::InvalidValue("non-finite JSONB number".into()))?
        }
        JsonbType::String => {
            let s = std::str::from_utf8(payload)
                .map_err(|_| SqlError::InvalidValue("JSONB string not UTF-8".into()))?;
            serde_json::Value::String(s.to_string())
        }
        JsonbType::Array => {
            let mut items = Vec::new();
            let mut child_pos = 0usize;
            while child_pos < payload.len() {
                let mut local = child_pos;
                let item = decode_value(payload, &mut local)?;
                items.push(item);
                child_pos = local;
            }
            serde_json::Value::Array(items)
        }
        JsonbType::Object => {
            let mut map = serde_json::Map::new();
            let mut child_pos = 0usize;
            while child_pos < payload.len() {
                let mut local = child_pos;
                let key = match decode_value(payload, &mut local)? {
                    serde_json::Value::String(s) => s,
                    _ => return Err(SqlError::InvalidValue("JSONB object key not string".into())),
                };
                let value = decode_value(payload, &mut local)?;
                map.insert(key, value);
                child_pos = local;
            }
            serde_json::Value::Object(map)
        }
    };
    *pos += total;
    Ok(v)
}

pub(crate) fn value_to_serde(v: &Value) -> Result<serde_json::Value> {
    match v {
        Value::Json(s) => serde_json::from_str(s)
            .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}"))),
        Value::Jsonb(b) => decode_to_serde(b),
        _ => Err(SqlError::TypeMismatch {
            expected: "JSON or JSONB".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn serde_to_value(j: serde_json::Value, target: crate::types::DataType) -> Result<Value> {
    use crate::types::DataType;
    match target {
        DataType::Json => Ok(Value::Json(
            serde_json::to_string(&j)
                .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                .into(),
        )),
        DataType::Jsonb => {
            let mut buf = Vec::new();
            encode_canonical(&j, &mut buf)?;
            Ok(Value::Jsonb(Arc::from(buf)))
        }
        _ => Err(SqlError::InvalidValue(format!(
            "cannot serialize JSON to {target}"
        ))),
    }
}

fn serde_to_scalar_value(j: serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Real(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Text(s.into()),
        other => {
            let text = serde_json::to_string(&other).unwrap_or_default();
            Value::Text(text.into())
        }
    }
}

pub fn op_get(lhs: &Value, key: &Value) -> Result<Value> {
    if let Value::Jsonb(b) = lhs {
        let slice = match key {
            Value::Text(k) => find_object_key(b, k.as_str())?,
            Value::Integer(i) => array_get(b, *i)?,
            _ => None,
        };
        return match slice {
            Some(bytes) => Ok(Value::Jsonb(Arc::from(bytes))),
            None => Ok(Value::Null),
        };
    }
    let target = match lhs {
        Value::Json(_) => crate::types::DataType::Json,
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "JSON or JSONB".into(),
                got: lhs.data_type().to_string(),
            })
        }
    };
    let j = value_to_serde(lhs)?;
    let extracted = navigate_one(&j, key);
    match extracted {
        Some(v) => serde_to_value(v, target),
        None => Ok(Value::Null),
    }
}

pub fn op_get_text(lhs: &Value, key: &Value) -> Result<Value> {
    if let Value::Jsonb(b) = lhs {
        let slice = match key {
            Value::Text(k) => find_object_key(b, k.as_str())?,
            Value::Integer(i) => array_get(b, *i)?,
            _ => None,
        };
        return match slice {
            Some(bytes) => match read_scalar_text(bytes)? {
                Some(s) => Ok(Value::Text(s.into())),
                None => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        };
    }
    let j = value_to_serde(lhs)?;
    match navigate_one(&j, key) {
        Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::String(s)) => Ok(Value::Text(s.into())),
        Some(v) => Ok(Value::Text(
            serde_json::to_string(&v)
                .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                .into(),
        )),
        None => Ok(Value::Null),
    }
}

pub fn op_path(lhs: &Value, path: &Value) -> Result<Value> {
    let target = match lhs {
        Value::Json(_) => crate::types::DataType::Json,
        Value::Jsonb(_) => crate::types::DataType::Jsonb,
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "JSON or JSONB".into(),
                got: lhs.data_type().to_string(),
            })
        }
    };
    let j = value_to_serde(lhs)?;
    let segments = path_to_segments(path)?;
    match navigate_path(&j, &segments) {
        Some(v) => serde_to_value(v, target),
        None => Ok(Value::Null),
    }
}

pub fn op_path_text(lhs: &Value, path: &Value) -> Result<Value> {
    let j = value_to_serde(lhs)?;
    let segments = path_to_segments(path)?;
    match navigate_path(&j, &segments) {
        Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::String(s)) => Ok(Value::Text(s.into())),
        Some(v) => Ok(Value::Text(
            serde_json::to_string(&v)
                .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                .into(),
        )),
        None => Ok(Value::Null),
    }
}

pub fn op_contains(lhs: &Value, rhs: &Value) -> Result<Value> {
    if let (Value::Jsonb(l), Value::Jsonb(r)) = (lhs, rhs) {
        return Ok(Value::Boolean(jsonb_contains_bytes(l, r)?));
    }
    let left = value_to_serde(lhs)?;
    let right = value_to_serde(rhs)?;
    Ok(Value::Boolean(json_contains(&left, &right)))
}

pub fn op_contained_by(lhs: &Value, rhs: &Value) -> Result<Value> {
    if let (Value::Jsonb(l), Value::Jsonb(r)) = (lhs, rhs) {
        return Ok(Value::Boolean(jsonb_contains_bytes(r, l)?));
    }
    let left = value_to_serde(lhs)?;
    let right = value_to_serde(rhs)?;
    Ok(Value::Boolean(json_contains(&right, &left)))
}

pub fn op_has_key(lhs: &Value, rhs: &Value) -> Result<Value> {
    let key = match rhs {
        Value::Text(s) => s.as_str(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT key".into(),
                got: rhs.data_type().to_string(),
            })
        }
    };
    if let Value::Jsonb(b) = lhs {
        return Ok(Value::Boolean(has_top_key_bytes(b, key)?));
    }
    let left = value_to_serde(lhs)?;
    let exists = match &left {
        serde_json::Value::Object(m) => m.contains_key(key),
        serde_json::Value::Array(arr) => arr
            .iter()
            .any(|e| matches!(e, serde_json::Value::String(s) if s == key)),
        _ => false,
    };
    Ok(Value::Boolean(exists))
}

pub fn op_has_any_key(lhs: &Value, rhs: &Value) -> Result<Value> {
    let left = value_to_serde(lhs)?;
    let keys = text_array(rhs)?;
    let m = match &left {
        serde_json::Value::Object(m) => m,
        _ => return Ok(Value::Boolean(false)),
    };
    Ok(Value::Boolean(
        keys.iter().any(|k| m.contains_key(k.as_str())),
    ))
}

pub fn op_has_all_keys(lhs: &Value, rhs: &Value) -> Result<Value> {
    let left = value_to_serde(lhs)?;
    let keys = text_array(rhs)?;
    let m = match &left {
        serde_json::Value::Object(m) => m,
        _ => return Ok(Value::Boolean(keys.is_empty())),
    };
    Ok(Value::Boolean(
        keys.iter().all(|k| m.contains_key(k.as_str())),
    ))
}

pub fn op_delete_path(lhs: &Value, path: &Value) -> Result<Value> {
    let target = match lhs {
        Value::Json(_) => crate::types::DataType::Json,
        Value::Jsonb(_) => crate::types::DataType::Jsonb,
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "JSON or JSONB".into(),
                got: lhs.data_type().to_string(),
            })
        }
    };
    let mut j = value_to_serde(lhs)?;
    let segments = path_to_segments(path)?;
    delete_at_path(&mut j, &segments);
    serde_to_value(j, target)
}

pub fn op_delete_one(lhs: &Value, rhs: &Value) -> Result<Value> {
    let target = match lhs {
        Value::Json(_) => crate::types::DataType::Json,
        Value::Jsonb(_) => crate::types::DataType::Jsonb,
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "JSON or JSONB".into(),
                got: lhs.data_type().to_string(),
            })
        }
    };
    let mut j = value_to_serde(lhs)?;
    match (&mut j, rhs) {
        (serde_json::Value::Object(m), Value::Text(k)) => {
            m.remove(k.as_str());
        }
        (serde_json::Value::Array(arr), Value::Integer(i)) => {
            let len = arr.len() as i64;
            let idx = if *i < 0 { len + i } else { *i };
            if (0..len).contains(&idx) {
                arr.remove(idx as usize);
            }
        }
        (serde_json::Value::Array(arr), Value::Text(k)) => {
            arr.retain(|e| !matches!(e, serde_json::Value::String(s) if s == k.as_str()));
        }
        _ => {}
    }
    serde_to_value(j, target)
}

pub fn op_concat(lhs: &Value, rhs: &Value) -> Result<Value> {
    let target = match (lhs, rhs) {
        (Value::Jsonb(_), _) | (_, Value::Jsonb(_)) => crate::types::DataType::Jsonb,
        _ => crate::types::DataType::Json,
    };
    let mut left = value_to_serde(lhs)?;
    let right = value_to_serde(rhs)?;
    match (&mut left, right) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            a.extend(b);
        }
        (serde_json::Value::Array(a), other) => {
            a.push(other);
        }
        (a, serde_json::Value::Array(mut b)) => {
            let owned = std::mem::take(a);
            let mut combined = vec![owned];
            combined.append(&mut b);
            *a = serde_json::Value::Array(combined);
        }
        (a, b) => {
            let av = std::mem::take(a);
            *a = serde_json::Value::Array(vec![av, b]);
        }
    }
    serde_to_value(left, target)
}

fn coerce_path_arg(v: &Value) -> Result<String> {
    match v {
        Value::Text(s) => Ok(s.to_string()),
        Value::Json(s) => Ok(s.to_string()),
        _ => Err(SqlError::TypeMismatch {
            expected: "TEXT path".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn coerce_vars_arg(v: &Value) -> Result<Option<serde_json::Value>> {
    if v.is_null() {
        return Ok(None);
    }
    let j = value_to_serde(v)?;
    if !j.is_object() {
        return Err(SqlError::InvalidValue(
            "jsonpath vars argument must be a JSONB object".into(),
        ));
    }
    Ok(Some(j))
}

fn coerce_silent_arg(v: &Value) -> Result<bool> {
    if v.is_null() {
        return Ok(false);
    }
    match v {
        Value::Boolean(b) => Ok(*b),
        _ => Err(SqlError::TypeMismatch {
            expected: "BOOLEAN".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn jp_query(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Vec<serde_json::Value>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.query_with_vars(j, v),
        None => jp.query(j),
    };
    match result {
        Ok(nodes) => Ok(nodes.into_iter().map(|c| c.into_owned()).collect()),
        Err(e) if silent && e.can_silent() => Ok(vec![]),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

fn jp_query_first(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Option<serde_json::Value>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.query_first_with_vars(j, v),
        None => jp.query_first(j),
    };
    match result {
        Ok(opt) => Ok(opt.map(|c| c.into_owned())),
        Err(e) if silent && e.can_silent() => Ok(None),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

fn jp_exists(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Option<bool>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.exists_with_vars(j, v),
        None => jp.exists(j),
    };
    match result {
        Ok(b) => Ok(Some(b)),
        Err(e) if silent && e.can_silent() => Ok(None),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

pub fn op_path_exists(lhs: &Value, path: &Value) -> Result<Value> {
    let j = value_to_serde(lhs)?;
    let path_str = coerce_path_arg(path)?;
    let exists = jp_exists(&j, &path_str, None, false)?.unwrap_or(false);
    Ok(Value::Boolean(exists))
}

pub fn op_path_match(lhs: &Value, path: &Value) -> Result<Value> {
    let j = value_to_serde(lhs)?;
    let path_str = coerce_path_arg(path)?;
    let nodes = jp_query(&j, &path_str, None, false)?;
    let truthy = nodes
        .iter()
        .any(|node| matches!(node, serde_json::Value::Bool(true)));
    Ok(Value::Boolean(truthy))
}

pub fn fn_json_exists(j_val: &Value, path: &Value) -> Result<Value> {
    op_path_exists(j_val, path)
}

pub fn fn_json_value(j_val: &Value, path: &Value) -> Result<Value> {
    let j = value_to_serde(j_val)?;
    let path_str = coerce_path_arg(path)?;
    match jp_query_first(&j, &path_str, None, false)? {
        Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::String(s)) => Ok(Value::Text(s.into())),
        Some(other) => Ok(Value::Text(
            serde_json::to_string(&other)
                .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                .into(),
        )),
        None => Ok(Value::Null),
    }
}

pub fn fn_json_query(j_val: &Value, path: &Value, target: crate::types::DataType) -> Result<Value> {
    let j = value_to_serde(j_val)?;
    let path_str = coerce_path_arg(path)?;
    let nodes = jp_query(&j, &path_str, None, false)?;
    if nodes.is_empty() {
        return Ok(Value::Null);
    }
    let result_json = if nodes.len() == 1 {
        nodes[0].clone()
    } else {
        serde_json::Value::Array(nodes)
    };
    serde_to_value(result_json, target)
}

pub fn fn_jsonb_path_exists(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_exists: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    match jp_exists(&j, &path_str, vars.as_ref(), silent)? {
        Some(b) => Ok(Value::Boolean(b)),
        None => Ok(Value::Null),
    }
}

pub fn fn_jsonb_path_match(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_match: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    let nodes = jp_query(&j, &path_str, vars.as_ref(), silent)?;
    if nodes.len() != 1 {
        if silent {
            return Ok(Value::Null);
        }
        return Err(SqlError::InvalidValue(
            "jsonb_path_match: expected exactly one boolean result".into(),
        ));
    }
    match &nodes[0] {
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        _ if silent => Ok(Value::Null),
        _ => Err(SqlError::InvalidValue(
            "jsonb_path_match: result is not a boolean".into(),
        )),
    }
}

pub fn fn_jsonb_path_query_first(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_query_first: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    match jp_query_first(&j, &path_str, vars.as_ref(), silent)? {
        Some(v) => serde_to_value(v, crate::types::DataType::Jsonb),
        None => Ok(Value::Null),
    }
}

pub fn fn_jsonb_path_query_array(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_query_array: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    let nodes = jp_query(&j, &path_str, vars.as_ref(), silent)?;
    serde_to_value(
        serde_json::Value::Array(nodes),
        crate::types::DataType::Jsonb,
    )
}

fn jp_query_tz(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Vec<serde_json::Value>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.query_with_vars_tz(j, v),
        None => jp.query_tz(j),
    };
    match result {
        Ok(nodes) => Ok(nodes.into_iter().map(|c| c.into_owned()).collect()),
        Err(e) if silent && e.can_silent() => Ok(vec![]),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

fn jp_query_first_tz(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Option<serde_json::Value>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.query_first_with_vars_tz(j, v),
        None => jp.query_first_tz(j),
    };
    match result {
        Ok(opt) => Ok(opt.map(|c| c.into_owned())),
        Err(e) if silent && e.can_silent() => Ok(None),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

fn jp_exists_tz(
    j: &serde_json::Value,
    path_str: &str,
    vars: Option<&serde_json::Value>,
    silent: bool,
) -> Result<Option<bool>> {
    let jp = sql_json_path::JsonPath::new(path_str)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON path: {e}")))?;
    let result = match vars {
        Some(v) => jp.exists_with_vars_tz(j, v),
        None => jp.exists_tz(j),
    };
    match result {
        Ok(b) => Ok(Some(b)),
        Err(e) if silent && e.can_silent() => Ok(None),
        Err(e) => Err(SqlError::InvalidValue(format!("JSON path eval: {e}"))),
    }
}

pub fn fn_jsonb_path_exists_tz(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_exists_tz: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    match jp_exists_tz(&j, &path_str, vars.as_ref(), silent)? {
        Some(b) => Ok(Value::Boolean(b)),
        None => Ok(Value::Null),
    }
}

pub fn fn_jsonb_path_match_tz(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_match_tz: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    let nodes = jp_query_tz(&j, &path_str, vars.as_ref(), silent)?;
    if nodes.len() != 1 {
        if silent {
            return Ok(Value::Null);
        }
        return Err(SqlError::InvalidValue(
            "jsonb_path_match_tz: expected exactly one boolean result".into(),
        ));
    }
    match &nodes[0] {
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        _ if silent => Ok(Value::Null),
        _ => Err(SqlError::InvalidValue(
            "jsonb_path_match_tz: result is not a boolean".into(),
        )),
    }
}

pub fn fn_jsonb_path_query_first_tz(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_query_first_tz: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    match jp_query_first_tz(&j, &path_str, vars.as_ref(), silent)? {
        Some(v) => serde_to_value(v, crate::types::DataType::Jsonb),
        None => Ok(Value::Null),
    }
}

pub fn fn_jsonb_path_query_array_tz(args: &[Value]) -> Result<Value> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::InvalidValue(
            "jsonb_path_query_array_tz: expected 2..=4 arguments".into(),
        ));
    }
    let j = value_to_serde(&args[0])?;
    let path_str = coerce_path_arg(&args[1])?;
    let vars = args.get(2).map(coerce_vars_arg).transpose()?.flatten();
    let silent = args
        .get(3)
        .map(coerce_silent_arg)
        .transpose()?
        .unwrap_or(false);
    let nodes = jp_query_tz(&j, &path_str, vars.as_ref(), silent)?;
    serde_to_value(
        serde_json::Value::Array(nodes),
        crate::types::DataType::Jsonb,
    )
}

pub fn fn_jsonb_path_query_tz(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_first_tz(args)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSeg {
    Key(String),
    Index(i64),
    Wildcard,
}

pub fn parse_dollar_path(s: &str) -> Result<Vec<PathSeg>> {
    let s = s.trim();
    let s = s.strip_prefix('$').unwrap_or(s);
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                    i += 1;
                }
                if i > start {
                    let key = std::str::from_utf8(&bytes[start..i])
                        .map_err(|_| SqlError::InvalidValue("invalid path segment".into()))?;
                    out.push(PathSeg::Key(key.to_string()));
                }
            }
            b'[' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                if i > bytes.len() {
                    return Err(SqlError::InvalidValue("unterminated index".into()));
                }
                let inner = std::str::from_utf8(&bytes[start..i])
                    .map_err(|_| SqlError::InvalidValue("invalid path index".into()))?;
                if inner.trim() == "*" {
                    out.push(PathSeg::Wildcard);
                } else if let Ok(idx) = inner.parse::<i64>() {
                    out.push(PathSeg::Index(idx));
                } else {
                    let key = inner.trim_matches('"').trim_matches('\'');
                    out.push(PathSeg::Key(key.to_string()));
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    Ok(out)
}

fn path_to_segments(v: &Value) -> Result<Vec<PathSeg>> {
    match v {
        Value::Text(s) => {
            if s.starts_with('$') {
                parse_dollar_path(s)
            } else if s.starts_with('{') && s.ends_with('}') {
                parse_pg_array_path(s)
            } else {
                Ok(vec![PathSeg::Key(s.to_string())])
            }
        }
        Value::Integer(i) => Ok(vec![PathSeg::Index(*i)]),
        Value::Json(_) | Value::Jsonb(_) => {
            let parsed = value_to_serde(v)?;
            json_to_path(&parsed)
        }
        _ => Err(SqlError::TypeMismatch {
            expected: "TEXT or path array".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn parse_pg_array_path(s: &str) -> Result<Vec<PathSeg>> {
    let inner = &s[1..s.len() - 1];
    if inner.is_empty() {
        return Ok(vec![]);
    }
    inner
        .split(',')
        .map(|raw| {
            let trimmed = raw.trim().trim_matches('"');
            if let Ok(idx) = trimmed.parse::<i64>() {
                PathSeg::Index(idx)
            } else {
                PathSeg::Key(trimmed.to_string())
            }
        })
        .map(Ok)
        .collect()
}

fn json_to_path(j: &serde_json::Value) -> Result<Vec<PathSeg>> {
    let arr = j
        .as_array()
        .ok_or_else(|| SqlError::InvalidValue("path must be a JSON array".into()))?;
    arr.iter()
        .map(|item| match item {
            serde_json::Value::String(s) => Ok(PathSeg::Key(s.clone())),
            serde_json::Value::Number(n) => n
                .as_i64()
                .map(PathSeg::Index)
                .ok_or_else(|| SqlError::InvalidValue("path index out of range".into())),
            _ => Err(SqlError::InvalidValue(
                "path segments must be strings or integers".into(),
            )),
        })
        .collect()
}

fn navigate_one(j: &serde_json::Value, key: &Value) -> Option<serde_json::Value> {
    match (j, key) {
        (serde_json::Value::Object(m), Value::Text(k)) => m.get(k.as_str()).cloned(),
        (serde_json::Value::Array(arr), Value::Integer(i)) => {
            let len = arr.len() as i64;
            let idx = if *i < 0 { len + i } else { *i };
            if (0..len).contains(&idx) {
                Some(arr[idx as usize].clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn navigate_path(j: &serde_json::Value, segments: &[PathSeg]) -> Option<serde_json::Value> {
    let mut cur = j.clone();
    for seg in segments {
        cur = match (&cur, seg) {
            (serde_json::Value::Object(m), PathSeg::Key(k)) => m.get(k.as_str())?.clone(),
            (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
                let len = arr.len() as i64;
                let idx = if *i < 0 { len + i } else { *i };
                if (0..len).contains(&idx) {
                    arr[idx as usize].clone()
                } else {
                    return None;
                }
            }
            (serde_json::Value::Array(arr), PathSeg::Key(k)) => {
                let idx: i64 = k.parse().ok()?;
                let len = arr.len() as i64;
                let idx = if idx < 0 { len + idx } else { idx };
                if (0..len).contains(&idx) {
                    arr[idx as usize].clone()
                } else {
                    return None;
                }
            }
            _ => return None,
        };
    }
    Some(cur)
}

fn delete_at_path(j: &mut serde_json::Value, segments: &[PathSeg]) {
    if segments.is_empty() {
        return;
    }
    let (last, prefix) = segments.split_last().unwrap();
    let target = navigate_mut(j, prefix);
    if let Some(t) = target {
        match (t, last) {
            (serde_json::Value::Object(m), PathSeg::Key(k)) => {
                m.remove(k.as_str());
            }
            (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
                let len = arr.len() as i64;
                let idx = if *i < 0 { len + i } else { *i };
                if (0..len).contains(&idx) {
                    arr.remove(idx as usize);
                }
            }
            _ => {}
        }
    }
}

fn navigate_mut<'a>(
    j: &'a mut serde_json::Value,
    segments: &[PathSeg],
) -> Option<&'a mut serde_json::Value> {
    let mut cur = j;
    for seg in segments {
        cur = match (cur, seg) {
            (serde_json::Value::Object(m), PathSeg::Key(k)) => m.get_mut(k.as_str())?,
            (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
                let len = arr.len() as i64;
                let idx = if *i < 0 { len + i } else { *i };
                if (0..len).contains(&idx) {
                    arr.get_mut(idx as usize)?
                } else {
                    return None;
                }
            }
            _ => return None,
        };
    }
    Some(cur)
}

fn json_contains(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => b
            .iter()
            .all(|(k, v)| a.get(k).is_some_and(|av| json_contains(av, v))),
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            b.iter().all(|bv| a.iter().any(|av| json_contains(av, bv)))
        }
        (serde_json::Value::Array(a), other) => a.iter().any(|av| json_contains(av, other)),
        (a, b) => a == b,
    }
}

fn text_array(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Text(s) => Ok(vec![s.to_string()]),
        Value::Json(_) | Value::Jsonb(_) => {
            let j = value_to_serde(v)?;
            j.as_array()
                .ok_or_else(|| SqlError::InvalidValue("expected JSON text array".into()))?
                .iter()
                .map(|e| match e {
                    serde_json::Value::String(s) => Ok(s.clone()),
                    _ => Err(SqlError::InvalidValue(
                        "array elements must be strings".into(),
                    )),
                })
                .collect()
        }
        _ => Err(SqlError::TypeMismatch {
            expected: "TEXT array or JSON array".into(),
            got: v.data_type().to_string(),
        }),
    }
}

pub fn agg_array(values: &[Value], target: crate::types::DataType) -> Result<Value> {
    let items: Result<Vec<serde_json::Value>> = values.iter().map(value_to_serde_lossy).collect();
    serde_to_value(serde_json::Value::Array(items?), target)
}

pub fn materialize_json_table(
    source: &Value,
    spec: &crate::parser::JsonTableSpec,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if source.is_null() {
        let names = json_table_column_names(&spec.columns);
        return Ok((names, vec![]));
    }
    let root = value_to_serde(source)?;
    let root_segs = parse_dollar_path(&spec.root_path)?;
    let matches = json_table_walk(&root, &root_segs);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut ordinality_counter = 0i64;
    for m in matches {
        ordinality_counter += 1;
        emit_json_table_rows(
            &m,
            &spec.columns,
            ordinality_counter,
            &mut Vec::new(),
            &mut rows,
        )?;
    }
    let names = json_table_column_names(&spec.columns);
    Ok((names, rows))
}

fn json_table_column_names(columns: &[crate::parser::JsonTableCol]) -> Vec<String> {
    use crate::parser::JsonTableCol as C;
    let mut out = Vec::new();
    for c in columns {
        match c {
            C::Named { name, .. } | C::Ordinality { name } => out.push(name.clone()),
            C::Nested { columns, .. } => out.extend(json_table_column_names(columns)),
        }
    }
    out
}

fn json_table_walk(j: &serde_json::Value, segs: &[PathSeg]) -> Vec<serde_json::Value> {
    let mut frontier = vec![j.clone()];
    for seg in segs {
        let mut next = Vec::new();
        for cur in frontier {
            match (cur, seg) {
                (serde_json::Value::Object(m), PathSeg::Key(k)) => {
                    if let Some(v) = m.get(k.as_str()) {
                        next.push(v.clone());
                    }
                }
                (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
                    let len = arr.len() as i64;
                    let idx = if *i < 0 { len + i } else { *i };
                    if (0..len).contains(&idx) {
                        next.push(arr[idx as usize].clone());
                    }
                }
                (serde_json::Value::Array(arr), PathSeg::Wildcard) => {
                    next.extend(arr);
                }
                _ => {}
            }
        }
        frontier = next;
    }
    frontier
        .into_iter()
        .flat_map(|v| match v {
            serde_json::Value::Array(arr) if segs.last() == Some(&PathSeg::Wildcard) => arr,
            other => vec![other],
        })
        .collect()
}

fn emit_json_table_rows(
    row_doc: &serde_json::Value,
    columns: &[crate::parser::JsonTableCol],
    parent_ordinality: i64,
    _prefix: &mut Vec<Value>,
    out: &mut Vec<Vec<Value>>,
) -> Result<()> {
    use crate::parser::JsonTableCol as C;

    let mut scalars: Vec<(usize, Value)> = Vec::new();
    let mut nesteds: Vec<(usize, Vec<Vec<Value>>)> = Vec::new();
    let mut widths: Vec<usize> = Vec::with_capacity(columns.len());

    for (idx, c) in columns.iter().enumerate() {
        match c {
            C::Named {
                ty, path, exists, ..
            } => {
                let segs = parse_dollar_path(path)?;
                let matches = json_table_walk(row_doc, &segs);
                let v = if *exists {
                    Value::Boolean(!matches.is_empty())
                } else if matches.is_empty() {
                    Value::Null
                } else {
                    json_table_coerce(&matches[0], *ty)?
                };
                scalars.push((idx, v));
                widths.push(1);
            }
            C::Ordinality { .. } => {
                scalars.push((idx, Value::Integer(parent_ordinality)));
                widths.push(1);
            }
            C::Nested { path, columns } => {
                let segs = parse_dollar_path(path)?;
                let matches = json_table_walk(row_doc, &segs);
                let inner_width = json_table_column_names(columns).len();
                let mut inner: Vec<Vec<Value>> = Vec::new();
                let mut ord = 0i64;
                for m in matches {
                    ord += 1;
                    let mut empty_prefix: Vec<Value> = Vec::new();
                    emit_json_table_rows(&m, columns, ord, &mut empty_prefix, &mut inner)?;
                }
                if inner.is_empty() {
                    inner.push(vec![Value::Null; inner_width]);
                }
                nesteds.push((idx, inner));
                widths.push(inner_width);
            }
        }
    }

    let offsets: Vec<usize> = widths
        .iter()
        .scan(0usize, |acc, w| {
            let cur = *acc;
            *acc += w;
            Some(cur)
        })
        .collect();
    let total: usize = widths.iter().sum();

    if nesteds.is_empty() {
        let mut row = vec![Value::Null; total];
        for (idx, v) in scalars {
            row[offsets[idx]] = v;
        }
        out.push(row);
        return Ok(());
    }

    let mut indices = vec![0usize; nesteds.len()];
    loop {
        let mut row = vec![Value::Null; total];
        for (idx, v) in &scalars {
            row[offsets[*idx]] = v.clone();
        }
        for (ni, (col_idx, group)) in nesteds.iter().enumerate() {
            let off = offsets[*col_idx];
            let inner_row = &group[indices[ni]];
            for (k, v) in inner_row.iter().enumerate() {
                row[off + k] = v.clone();
            }
        }
        out.push(row);

        let mut k = indices.len();
        let done = loop {
            if k == 0 {
                break true;
            }
            k -= 1;
            indices[k] += 1;
            if indices[k] < nesteds[k].1.len() {
                break false;
            }
            indices[k] = 0;
        };
        if done {
            return Ok(());
        }
    }
}

fn json_table_coerce(v: &serde_json::Value, target: crate::types::DataType) -> Result<Value> {
    use crate::types::DataType;
    if matches!(v, serde_json::Value::Null) {
        return Ok(Value::Null);
    }
    match (v, target) {
        (_, DataType::Json) => serde_to_value(v.clone(), DataType::Json),
        (_, DataType::Jsonb) => serde_to_value(v.clone(), DataType::Jsonb),
        (serde_json::Value::Number(n), DataType::Integer) => n
            .as_i64()
            .map(Value::Integer)
            .ok_or_else(|| SqlError::InvalidValue("JSON_TABLE: number not i64".into())),
        (serde_json::Value::Number(n), DataType::Real) => n
            .as_f64()
            .map(Value::Real)
            .ok_or_else(|| SqlError::InvalidValue("JSON_TABLE: number not f64".into())),
        (serde_json::Value::Bool(b), DataType::Boolean) => Ok(Value::Boolean(*b)),
        (serde_json::Value::String(s), DataType::Text) => Ok(Value::Text(s.clone().into())),
        _ => {
            let text_form = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => serde_json::to_string(v)
                    .map_err(|e| SqlError::InvalidValue(format!("JSON_TABLE render: {e}")))?,
            };
            let text_val = Value::Text(text_form.into());
            text_val.coerce_into(target).ok_or_else(|| {
                SqlError::InvalidValue(format!("JSON_TABLE: cannot coerce value to {target}"))
            })
        }
    }
}

/// GIN entry layout (jsonb_ops): `0x01‖key` (key-exists, `?`),
/// `0x02‖key‖0x00‖value` (pair, `@>`), `0x03‖value` (array element).
pub fn extract_gin_entries(value: &Value, ops: crate::types::GinOpsClass) -> Result<Vec<Vec<u8>>> {
    use crate::types::GinOpsClass;
    if value.is_null() {
        return Ok(vec![]);
    }
    let j = value_to_serde(value)?;
    let mut out: Vec<Vec<u8>> = Vec::new();
    match ops {
        GinOpsClass::JsonbOps => extract_jsonb_ops_walk(&j, &mut out),
        GinOpsClass::JsonbPathOps => extract_path_ops_walk(&j, 0, &mut out),
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn extract_jsonb_ops_walk(j: &serde_json::Value, out: &mut Vec<Vec<u8>>) {
    match j {
        serde_json::Value::Object(m) => {
            for (k, v) in m {
                let mut key_entry = Vec::with_capacity(k.len() + 1);
                key_entry.push(0x01);
                key_entry.extend_from_slice(k.as_bytes());
                out.push(key_entry);
                if let Some(s) = scalar_repr(v) {
                    let mut pair = Vec::with_capacity(k.len() + s.len() + 2);
                    pair.push(0x02);
                    pair.extend_from_slice(k.as_bytes());
                    pair.push(0x00);
                    pair.extend_from_slice(s.as_bytes());
                    out.push(pair);
                }
                extract_jsonb_ops_walk(v, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                if let Some(s) = scalar_repr(v) {
                    let mut entry = Vec::with_capacity(s.len() + 1);
                    entry.push(0x03);
                    entry.extend_from_slice(s.as_bytes());
                    out.push(entry);
                }
                extract_jsonb_ops_walk(v, out);
            }
        }
        _ => {}
    }
}

fn extract_path_ops_walk(j: &serde_json::Value, path: u32, out: &mut Vec<Vec<u8>>) {
    match j {
        serde_json::Value::Object(m) => {
            for (k, v) in m {
                let next = path.rotate_left(1) ^ fx_hash_u32(k.as_bytes());
                extract_path_ops_walk(v, next, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                extract_path_ops_walk(v, path, out);
            }
        }
        _ => {
            let leaf = path.rotate_left(1) ^ hash_scalar_for_path(j);
            out.push(leaf.to_le_bytes().to_vec());
        }
    }
}

fn fx_hash_u32(bytes: &[u8]) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = rustc_hash::FxHasher::default();
    bytes.hash(&mut h);
    h.finish() as u32
}

fn hash_scalar_for_path(v: &serde_json::Value) -> u32 {
    match v {
        serde_json::Value::Null => 0x0000_0001,
        serde_json::Value::Bool(true) => 0x0000_0002,
        serde_json::Value::Bool(false) => 0x0000_0004,
        serde_json::Value::String(s) => fx_hash_u32(s.as_bytes()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return fx_hash_u32(&i.to_le_bytes());
            }
            if let Some(u) = n.as_u64() {
                return fx_hash_u32(&u.to_le_bytes());
            }
            let f = n.as_f64().unwrap_or(0.0);
            if f.is_finite() && f.fract() == 0.0 && f.abs() < i64::MAX as f64 {
                fx_hash_u32(&(f as i64).to_le_bytes())
            } else {
                fx_hash_u32(&f.to_bits().to_le_bytes())
            }
        }
        _ => 0,
    }
}

fn scalar_repr(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => Some("null".into()),
        serde_json::Value::Bool(true) => Some("true".into()),
        serde_json::Value::Bool(false) => Some("false".into()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub fn agg_object(pairs: &[(Value, Value)], target: crate::types::DataType) -> Result<Value> {
    let mut map = serde_json::Map::new();
    for (k, v) in pairs {
        if k.is_null() {
            continue;
        }
        let key_str = match k {
            Value::Text(s) => s.to_string(),
            Value::Json(s) => s.to_string(),
            _ => format!("{k}"),
        };
        let val = value_to_serde_lossy(v)?;
        map.insert(key_str, val);
    }
    serde_to_value(serde_json::Value::Object(map), target)
}

pub fn populate_record_row(
    obj: &serde_json::Map<String, serde_json::Value>,
    columns: &[crate::types::ColumnDef],
) -> Result<Vec<Value>> {
    columns
        .iter()
        .map(|col| match obj.get(&col.name) {
            None | Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(v) => coerce_json_field(v, col.data_type),
        })
        .collect()
}

fn coerce_json_field(j: &serde_json::Value, target: crate::types::DataType) -> Result<Value> {
    use crate::types::DataType;
    match target {
        DataType::Json | DataType::Jsonb => serde_to_value(j.clone(), target),
        _ => {
            let v = serde_to_scalar_value(j.clone());
            crate::eval::eval_cast(&v, target)
        }
    }
}

pub fn dispatch_srf(name: &str, args: &[Value]) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let upper = name.to_ascii_uppercase();
    match upper.as_str() {
        "JSONB_ARRAY_ELEMENTS" | "JSON_ARRAY_ELEMENTS" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                return Ok((vec!["value".into()], vec![]));
            }
            let j = value_to_serde(&args[0])?;
            let arr = j
                .as_array()
                .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON array")))?;
            let target = if upper.starts_with("JSONB") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            let rows: Result<Vec<Vec<Value>>> = arr
                .iter()
                .map(|v| serde_to_value(v.clone(), target).map(|val| vec![val]))
                .collect();
            Ok((vec!["value".into()], rows?))
        }
        "JSONB_ARRAY_ELEMENTS_TEXT" | "JSON_ARRAY_ELEMENTS_TEXT" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                return Ok((vec!["value".into()], vec![]));
            }
            let j = value_to_serde(&args[0])?;
            let arr = j
                .as_array()
                .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON array")))?;
            let rows: Vec<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    let text = match v {
                        serde_json::Value::String(s) => s.clone(),
                        _ => serde_json::to_string(v).unwrap_or_default(),
                    };
                    vec![Value::Text(text.into())]
                })
                .collect();
            Ok((vec!["value".into()], rows))
        }
        "JSONB_EACH" | "JSON_EACH" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                return Ok((vec!["key".into(), "value".into()], vec![]));
            }
            let j = value_to_serde(&args[0])?;
            let obj = j
                .as_object()
                .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON object")))?;
            let target = if upper.starts_with("JSONB") {
                crate::types::DataType::Jsonb
            } else {
                crate::types::DataType::Json
            };
            let rows: Result<Vec<Vec<Value>>> = obj
                .iter()
                .map(|(k, v)| {
                    Ok(vec![
                        Value::Text(k.clone().into()),
                        serde_to_value(v.clone(), target)?,
                    ])
                })
                .collect();
            Ok((vec!["key".into(), "value".into()], rows?))
        }
        "JSONB_EACH_TEXT" | "JSON_EACH_TEXT" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                return Ok((vec!["key".into(), "value".into()], vec![]));
            }
            let j = value_to_serde(&args[0])?;
            let obj = j
                .as_object()
                .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON object")))?;
            let rows: Vec<Vec<Value>> = obj
                .iter()
                .map(|(k, v)| {
                    let text = match v {
                        serde_json::Value::String(s) => s.clone(),
                        _ => serde_json::to_string(v).unwrap_or_default(),
                    };
                    vec![Value::Text(k.clone().into()), Value::Text(text.into())]
                })
                .collect();
            Ok((vec!["key".into(), "value".into()], rows))
        }
        "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                return Ok((vec!["key".into()], vec![]));
            }
            let j = value_to_serde(&args[0])?;
            let obj = j
                .as_object()
                .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON object")))?;
            let rows: Vec<Vec<Value>> = obj
                .keys()
                .map(|k| vec![Value::Text(k.clone().into())])
                .collect();
            Ok((vec!["key".into()], rows))
        }
        _ => Err(SqlError::Unsupported(format!(
            "set-returning function: {name}"
        ))),
    }
}

pub fn is_srf_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "JSONB_ARRAY_ELEMENTS"
            | "JSON_ARRAY_ELEMENTS"
            | "JSONB_ARRAY_ELEMENTS_TEXT"
            | "JSON_ARRAY_ELEMENTS_TEXT"
            | "JSONB_EACH"
            | "JSON_EACH"
            | "JSONB_EACH_TEXT"
            | "JSON_EACH_TEXT"
            | "JSONB_OBJECT_KEYS"
            | "JSON_OBJECT_KEYS"
            | "JSONB_POPULATE_RECORD"
            | "JSONB_POPULATE_RECORDSET"
    )
}

pub fn extract_to_value(target: crate::types::DataType, j: serde_json::Value) -> Result<Value> {
    serde_to_value(j, target)
}

pub fn to_scalar(j: serde_json::Value) -> Value {
    serde_to_scalar_value(j)
}

pub fn fn_typeof(v: &Value) -> Result<Value> {
    if let Value::Jsonb(b) = v {
        let (ty, _, _) = read_header(b)?;
        let s = match ty {
            JsonbType::Null => "null",
            JsonbType::True | JsonbType::False => "boolean",
            JsonbType::Integer | JsonbType::Real => "number",
            JsonbType::String => "string",
            JsonbType::Array => "array",
            JsonbType::Object => "object",
        };
        return Ok(Value::Text(s.into()));
    }
    let j = value_to_serde(v)?;
    let s = match j {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    };
    Ok(Value::Text(s.into()))
}

pub fn fn_array_length(v: &Value) -> Result<Value> {
    if let Value::Jsonb(b) = v {
        return match array_len_bytes(b)? {
            Some(n) => Ok(Value::Integer(n as i64)),
            None => Err(SqlError::InvalidValue(
                "jsonb_array_length called on non-array".into(),
            )),
        };
    }
    let j = value_to_serde(v)?;
    match j {
        serde_json::Value::Array(arr) => Ok(Value::Integer(arr.len() as i64)),
        _ => Err(SqlError::InvalidValue(
            "jsonb_array_length called on non-array".into(),
        )),
    }
}

pub fn fn_object_length(v: &Value) -> Result<Value> {
    if let Value::Jsonb(b) = v {
        return match object_len_bytes(b)? {
            Some(n) => Ok(Value::Integer(n as i64)),
            None => Err(SqlError::InvalidValue(
                "jsonb_object_length called on non-object".into(),
            )),
        };
    }
    let j = value_to_serde(v)?;
    match j {
        serde_json::Value::Object(m) => Ok(Value::Integer(m.len() as i64)),
        _ => Err(SqlError::InvalidValue(
            "jsonb_object_length called on non-object".into(),
        )),
    }
}

pub fn fn_extract_path(
    args: &[Value],
    target: crate::types::DataType,
    as_text: bool,
) -> Result<Value> {
    let mut j = value_to_serde(&args[0])?;
    for key_val in &args[1..] {
        if key_val.is_null() {
            return Ok(Value::Null);
        }
        let key = match key_val {
            Value::Text(s) => s.to_string(),
            other => other.to_string(),
        };
        match &mut j {
            serde_json::Value::Object(m) => {
                if let Some(next) = m.remove(&key) {
                    j = next;
                } else {
                    return Ok(Value::Null);
                }
            }
            serde_json::Value::Array(arr) => {
                let idx: i64 = key.parse().map_err(|_| {
                    SqlError::InvalidValue(format!("array path key not integer: {key}"))
                })?;
                let len = arr.len() as i64;
                let idx = if idx < 0 { len + idx } else { idx };
                if (0..len).contains(&idx) {
                    j = arr.remove(idx as usize);
                } else {
                    return Ok(Value::Null);
                }
            }
            _ => return Ok(Value::Null),
        }
    }
    if as_text {
        match j {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::String(s) => Ok(Value::Text(s.into())),
            other => Ok(Value::Text(
                serde_json::to_string(&other)
                    .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                    .into(),
            )),
        }
    } else {
        serde_to_value(j, target)
    }
}

pub fn fn_sqlite_extract(j_val: &Value, path: &Value) -> Result<Value> {
    let path_str = match path {
        Value::Text(s) => s.to_string(),
        _ => {
            return Err(SqlError::TypeMismatch {
                expected: "TEXT path".into(),
                got: path.data_type().to_string(),
            })
        }
    };
    let j = value_to_serde(j_val)?;
    let segments = parse_dollar_path(&path_str)?;
    match navigate_path(&j, &segments) {
        Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::String(s)) => Ok(Value::Text(s.into())),
        Some(other) => Ok(Value::Text(
            serde_json::to_string(&other)
                .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?
                .into(),
        )),
        None => Ok(Value::Null),
    }
}

pub fn fn_valid(v: &Value) -> Result<Value> {
    let s = match v {
        Value::Text(s) => s.as_str(),
        Value::Json(s) => s.as_str(),
        _ => return Ok(Value::Boolean(false)),
    };
    Ok(Value::Boolean(
        serde_json::from_str::<serde_json::Value>(s).is_ok(),
    ))
}

pub fn fn_strip_nulls(v: &Value, target: crate::types::DataType) -> Result<Value> {
    let mut j = value_to_serde(v)?;
    strip_nulls_inplace(&mut j);
    serde_to_value(j, target)
}

fn strip_nulls_inplace(j: &mut serde_json::Value) {
    match j {
        serde_json::Value::Object(m) => {
            m.retain(|_, v| !matches!(v, serde_json::Value::Null));
            for v in m.values_mut() {
                strip_nulls_inplace(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                strip_nulls_inplace(v);
            }
        }
        _ => {}
    }
}

pub fn fn_pretty(v: &Value) -> Result<Value> {
    let j = value_to_serde(v)?;
    let s = serde_json::to_string_pretty(&j)
        .map_err(|e| SqlError::InvalidValue(format!("JSON pretty render: {e}")))?;
    Ok(Value::Text(s.into()))
}

pub fn fn_build_object(args: &[Value], target: crate::types::DataType) -> Result<Value> {
    if args.len() % 2 != 0 {
        return Err(SqlError::InvalidValue(
            "jsonb_build_object requires an even number of arguments".into(),
        ));
    }
    let mut map = serde_json::Map::new();
    for pair in args.chunks(2) {
        let key = match &pair[0] {
            Value::Null => continue,
            Value::Text(s) => s.to_string(),
            other => other.to_string(),
        };
        let val = value_to_serde_lossy(&pair[1])?;
        map.insert(key, val);
    }
    serde_to_value(serde_json::Value::Object(map), target)
}

pub fn fn_build_array(args: &[Value], target: crate::types::DataType) -> Result<Value> {
    let items: Result<Vec<serde_json::Value>> = args.iter().map(value_to_serde_lossy).collect();
    serde_to_value(serde_json::Value::Array(items?), target)
}

pub fn fn_set(
    j: &Value,
    path: &Value,
    new_value: &Value,
    create_missing: bool,
    target: crate::types::DataType,
) -> Result<Value> {
    let mut root = value_to_serde(j)?;
    let segments = path_to_segments(path)?;
    let new_serde = value_to_serde_lossy(new_value)?;
    if !set_at_path(&mut root, &segments, new_serde, create_missing, false) {
        return serde_to_value(root, target);
    }
    serde_to_value(root, target)
}

pub fn fn_insert(
    j: &Value,
    path: &Value,
    new_value: &Value,
    insert_after: bool,
    target: crate::types::DataType,
) -> Result<Value> {
    let mut root = value_to_serde(j)?;
    let segments = path_to_segments(path)?;
    let new_serde = value_to_serde_lossy(new_value)?;
    set_at_path(&mut root, &segments, new_serde, true, insert_after);
    serde_to_value(root, target)
}

fn set_at_path(
    root: &mut serde_json::Value,
    segments: &[PathSeg],
    new_value: serde_json::Value,
    create_missing: bool,
    insert_array: bool,
) -> bool {
    if segments.is_empty() {
        return false;
    }
    let (last, prefix) = segments.split_last().unwrap();
    let Some(target) = navigate_mut(root, prefix) else {
        return false;
    };
    match (target, last) {
        (serde_json::Value::Object(m), PathSeg::Key(k)) => {
            let exists = m.contains_key(k.as_str());
            if exists || create_missing {
                m.insert(k.clone(), new_value);
                true
            } else {
                false
            }
        }
        (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
            let len = arr.len() as i64;
            let idx = if *i < 0 { len + i } else { *i };
            if insert_array {
                let target_pos = if idx <= 0 {
                    0
                } else if idx >= len {
                    arr.len()
                } else {
                    idx as usize
                };
                arr.insert(target_pos, new_value);
                true
            } else if (0..len).contains(&idx) {
                arr[idx as usize] = new_value;
                true
            } else if create_missing {
                if idx < 0 {
                    arr.insert(0, new_value);
                } else {
                    arr.push(new_value);
                }
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

pub fn fn_to_json(v: &Value, target: crate::types::DataType) -> Result<Value> {
    let j = value_to_serde_lossy(v)?;
    serde_to_value(j, target)
}

pub fn fn_json_object(args: &[Value]) -> Result<Value> {
    match args.len() {
        1 => {
            let j = value_to_serde(&args[0])?;
            let arr = j
                .as_array()
                .ok_or_else(|| SqlError::InvalidValue("json_object expects text array".into()))?;
            let mut map = serde_json::Map::new();
            let mut i = 0;
            while i + 1 < arr.len() {
                let key = arr[i]
                    .as_str()
                    .ok_or_else(|| SqlError::InvalidValue("json_object key must be string".into()))?
                    .to_string();
                let val = arr[i + 1].clone();
                map.insert(key, val);
                i += 2;
            }
            serde_to_value(serde_json::Value::Object(map), crate::types::DataType::Json)
        }
        2 => {
            let keys = text_array(&args[0])?;
            let vals = text_array(&args[1])?;
            if keys.len() != vals.len() {
                return Err(SqlError::InvalidValue(
                    "json_object: keys and values must be same length".into(),
                ));
            }
            let mut map = serde_json::Map::new();
            for (k, v) in keys.into_iter().zip(vals) {
                map.insert(k, serde_json::Value::String(v));
            }
            serde_to_value(serde_json::Value::Object(map), crate::types::DataType::Json)
        }
        _ => Err(SqlError::InvalidValue(
            "json_object requires 1 or 2 arguments".into(),
        )),
    }
}

fn value_to_serde_lossy(v: &Value) -> Result<serde_json::Value> {
    match v {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Real(r) => serde_json::Number::from_f64(*r)
            .map(serde_json::Value::Number)
            .ok_or_else(|| SqlError::InvalidValue("non-finite number".into())),
        Value::Text(s) => Ok(serde_json::Value::String(s.to_string())),
        Value::Json(s) => serde_json::from_str(s)
            .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}"))),
        Value::Jsonb(b) => decode_to_serde(b),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            Ok(serde_json::Value::String(hex))
        }
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval { .. } => {
            Ok(serde_json::Value::String(format!("{v}")))
        }
        Value::TsVector(_) | Value::TsQuery(_) => Ok(serde_json::Value::String(format!("{v}"))),
        Value::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for elem in a.iter() {
                out.push(value_to_serde_lossy(elem)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        Value::Vector(v) => {
            let out: Vec<serde_json::Value> = v
                .iter()
                .map(|&x| {
                    serde_json::Number::from_f64(x as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();
            Ok(serde_json::Value::Array(out))
        }
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;
