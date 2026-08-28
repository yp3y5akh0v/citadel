use std::sync::Arc;

use citadel::CancelToken;

use crate::error::{Result, SqlError};
use crate::types::Value;

const CANCEL_CHECK_INTERVAL: usize = 256;
const JSON_READ_CHUNK: usize = 8 * 1024;

struct JsonWork<'a> {
    cancel: Option<&'a CancelToken>,
    completed: usize,
}

impl<'a> JsonWork<'a> {
    fn new(cancel: Option<&'a CancelToken>) -> Result<Self> {
        let work = Self {
            cancel,
            completed: 0,
        };
        work.check_now()?;
        Ok(work)
    }

    #[inline]
    fn check_now(&self) -> Result<()> {
        match self.cancel {
            Some(token) => token.check().map_err(SqlError::Storage),
            None => Ok(()),
        }
    }

    #[inline]
    fn tick(&mut self) -> Result<()> {
        if self.cancel.is_none() {
            return Ok(());
        }

        #[cfg(test)]
        let injected = tick_json_cancel_hook();
        #[cfg(not(test))]
        let injected = false;

        self.completed = self.completed.wrapping_add(1);
        if injected || self.completed.is_multiple_of(CANCEL_CHECK_INTERVAL) {
            self.check_now()?;
        }
        Ok(())
    }

    #[inline]
    fn checkpoint(&mut self) -> Result<()> {
        if self.cancel.is_none() {
            return Ok(());
        }
        #[cfg(test)]
        tick_json_cancel_hook();
        self.check_now()
    }

    fn finish(self) -> Result<()> {
        self.check_now()
    }
}

fn run_json_work<T>(
    cancel: Option<&CancelToken>,
    operation: impl FnOnce(&mut JsonWork<'_>) -> Result<T>,
) -> Result<T> {
    let mut work = JsonWork::new(cancel)?;
    let result = operation(&mut work);
    if result.is_ok() {
        work.finish()?;
    }
    result
}

fn checked_json_phase<T>(
    work: &mut JsonWork<'_>,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    work.checkpoint()?;
    let result = operation();
    work.checkpoint()?;
    result
}

struct CancellableJsonReader<'bytes, 'work, 'cancel> {
    bytes: &'bytes [u8],
    position: usize,
    work: &'work mut JsonWork<'cancel>,
    interrupted: bool,
}

impl std::io::Read for CancellableJsonReader<'_, '_, '_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position == self.bytes.len() {
            return Ok(0);
        }
        if self.work.checkpoint().is_err() {
            self.interrupted = true;
            return Err(std::io::Error::other("JSON parsing interrupted"));
        }
        let count = buf
            .len()
            .min(JSON_READ_CHUNK)
            .min(self.bytes.len() - self.position);
        buf[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

struct CancellableJsonWriter<'work, 'cancel> {
    bytes: Vec<u8>,
    work: &'work mut JsonWork<'cancel>,
    interrupted: bool,
}

impl std::io::Write for CancellableJsonWriter<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for chunk in buf.chunks(JSON_READ_CHUNK) {
            if self.work.checkpoint().is_err() {
                self.interrupted = true;
                return Err(std::io::Error::other("JSON rendering interrupted"));
            }
            self.bytes.extend_from_slice(chunk);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static JSON_CANCEL_HOOK: std::cell::RefCell<Option<(CancelToken, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct JsonCancelGuard {
    previous: Option<(CancelToken, usize)>,
}

#[cfg(test)]
impl Drop for JsonCancelGuard {
    fn drop(&mut self) {
        JSON_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = self.previous.take());
    }
}

#[cfg(test)]
pub(crate) fn cancel_json_after(token: CancelToken, work: usize) -> JsonCancelGuard {
    assert!(work > 0, "the hook must trip after JSON work starts");
    let previous = JSON_CANCEL_HOOK.with(|hook| hook.borrow_mut().replace((token, work)));
    JsonCancelGuard { previous }
}

#[cfg(test)]
fn tick_json_cancel_hook() -> bool {
    JSON_CANCEL_HOOK.with(|hook| {
        let fire = {
            let mut hook = hook.borrow_mut();
            match hook.as_mut() {
                Some((token, remaining)) if *remaining == 1 => {
                    let token = token.clone();
                    *hook = None;
                    Some(token)
                }
                Some((_, remaining)) => {
                    *remaining -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some(token) = fire {
            token.cancel();
            true
        } else {
            false
        }
    })
}

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

pub(crate) fn validate_text_with_cancel(s: &str, cancel: Option<&CancelToken>) -> Result<()> {
    let Some(cancel) = cancel else {
        return validate_text(s);
    };
    run_json_work(Some(cancel), |work| {
        parse_json_text_with_work(s, work).map(|_| ())
    })
}

pub fn text_to_jsonb(s: &str) -> Result<Value> {
    let v: serde_json::Value = serde_json::from_str(s)
        .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}")))?;
    reject_null_bytes(&v)?;
    let mut buf = Vec::with_capacity(s.len());
    encode_canonical(&v, &mut buf)?;
    Ok(Value::Jsonb(Arc::from(buf)))
}

pub(crate) fn text_to_jsonb_with_cancel(text: &str, cancel: Option<&CancelToken>) -> Result<Value> {
    let Some(cancel) = cancel else {
        return text_to_jsonb(text);
    };
    run_json_work(Some(cancel), |work| {
        let value = parse_json_text_with_work(text, work)?;
        reject_null_bytes_with_work(&value, work)?;
        let mut bytes = Vec::with_capacity(text.len());
        encode_canonical_with_work(&value, &mut bytes, work)?;
        Ok(Value::Jsonb(Arc::from(bytes)))
    })
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

fn reject_null_bytes_with_work(value: &serde_json::Value, work: &mut JsonWork<'_>) -> Result<()> {
    work.tick()?;
    match value {
        serde_json::Value::String(text) => {
            for chunk in text.as_bytes().chunks(JSON_READ_CHUNK) {
                work.checkpoint()?;
                if chunk.contains(&0) {
                    return Err(SqlError::InvalidValue(
                        "unsupported Unicode escape sequence \\u0000".into(),
                    ));
                }
            }
            Ok(())
        }
        serde_json::Value::Array(items) => {
            for item in items {
                reject_null_bytes_with_work(item, work)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                work.tick()?;
                for chunk in key.as_bytes().chunks(JSON_READ_CHUNK) {
                    work.checkpoint()?;
                    if chunk.contains(&0) {
                        return Err(SqlError::InvalidValue(
                            "unsupported Unicode escape sequence \\u0000".into(),
                        ));
                    }
                }
                reject_null_bytes_with_work(value, work)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn decode_to_text(bytes: &[u8]) -> Result<String> {
    let v = decode_to_serde(bytes)?;
    serde_json::to_string(&v).map_err(|e| SqlError::InvalidValue(format!("JSONB render: {e}")))
}

pub(crate) fn decode_to_text_with_cancel(
    bytes: &[u8],
    cancel: Option<&CancelToken>,
) -> Result<String> {
    let Some(cancel) = cancel else {
        return decode_to_text(bytes);
    };
    run_json_work(Some(cancel), |work| {
        let mut pos = 0usize;
        let value = decode_value(bytes, &mut pos, work)?;
        if pos != bytes.len() {
            return Err(SqlError::InvalidValue("trailing bytes in JSONB".into()));
        }
        json_to_string_with_work(&value, false, work)
    })
}

pub fn decode_to_serde(bytes: &[u8]) -> Result<serde_json::Value> {
    decode_to_serde_with_cancel(bytes, None)
}

pub(crate) fn decode_to_serde_with_cancel(
    bytes: &[u8],
    cancel: Option<&CancelToken>,
) -> Result<serde_json::Value> {
    let mut work = JsonWork::new(cancel)?;
    let mut pos = 0;
    let v = decode_value(bytes, &mut pos, &mut work)?;
    if pos != bytes.len() {
        return Err(SqlError::InvalidValue("trailing bytes in JSONB".into()));
    }
    work.finish()?;
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
    run_json_work(None, |work| array_len_bytes_with_work(bytes, work))
}

fn array_len_bytes_with_work(bytes: &[u8], work: &mut JsonWork<'_>) -> Result<Option<usize>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Array {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos < payload.len() {
        work.tick()?;
        pos += skip_value(&payload[pos..])?;
        count += 1;
    }
    Ok(Some(count))
}

pub fn object_len_bytes(bytes: &[u8]) -> Result<Option<usize>> {
    run_json_work(None, |work| object_len_bytes_with_work(bytes, work))
}

fn object_len_bytes_with_work(bytes: &[u8], work: &mut JsonWork<'_>) -> Result<Option<usize>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Object {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos < payload.len() {
        work.tick()?;
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

pub(crate) fn jsonb_contains_bytes_with_cancel(
    lhs: &[u8],
    rhs: &[u8],
    cancel: Option<&CancelToken>,
) -> Result<bool> {
    let Some(cancel) = cancel else {
        return jsonb_contains_bytes(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        jsonb_contains_bytes_with_work(lhs, rhs, work)
    })
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

fn decode_value(
    bytes: &[u8],
    pos: &mut usize,
    work: &mut JsonWork<'_>,
) -> Result<serde_json::Value> {
    work.tick()?;
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
            serde_json::Value::String(clone_string_with_work(s, work)?)
        }
        JsonbType::Array => {
            let mut items = Vec::new();
            let mut child_pos = 0usize;
            while child_pos < payload.len() {
                let mut local = child_pos;
                let item = decode_value(payload, &mut local, work)?;
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
                let key = match decode_value(payload, &mut local, work)? {
                    serde_json::Value::String(s) => s,
                    _ => return Err(SqlError::InvalidValue("JSONB object key not string".into())),
                };
                let value = decode_value(payload, &mut local, work)?;
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
    value_to_serde_with_cancel(v, None)
}

pub(crate) fn value_to_serde_with_cancel(
    v: &Value,
    cancel: Option<&CancelToken>,
) -> Result<serde_json::Value> {
    let mut work = JsonWork::new(cancel)?;
    let value = value_to_serde_with_work(v, &mut work)?;
    work.finish()?;
    Ok(value)
}

fn value_to_serde_with_work(v: &Value, work: &mut JsonWork<'_>) -> Result<serde_json::Value> {
    match v {
        Value::Json(s) => parse_json_text_with_work(s, work),
        Value::Jsonb(b) => {
            let mut pos = 0;
            let value = decode_value(b, &mut pos, work)?;
            if pos != b.len() {
                return Err(SqlError::InvalidValue("trailing bytes in JSONB".into()));
            }
            Ok(value)
        }
        _ => Err(SqlError::TypeMismatch {
            expected: "JSON or JSONB".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn bytes_equal_with_work(left: &[u8], right: &[u8], work: &mut JsonWork<'_>) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    if work.cancel.is_none() {
        return Ok(left == right);
    }
    for (left, right) in left
        .chunks(JSON_READ_CHUNK)
        .zip(right.chunks(JSON_READ_CHUNK))
    {
        work.checkpoint()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

fn find_object_key_with_work<'a>(
    bytes: &'a [u8],
    key: &str,
    work: &mut JsonWork<'_>,
) -> Result<Option<&'a [u8]>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Object {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    let key_bytes = key.as_bytes();
    let mut pos = 0usize;
    while pos < payload.len() {
        work.tick()?;
        let (key_type, key_payload_start, key_payload_len) = read_header(&payload[pos..])?;
        if key_type != JsonbType::String {
            return Err(SqlError::InvalidValue("JSONB object key not string".into()));
        }
        let key_total = key_payload_start + key_payload_len;
        let key_slice = &payload[pos + key_payload_start..pos + key_total];
        let value_start = pos + key_total;
        let value_total = skip_value(&payload[value_start..])?;
        if bytes_equal_with_work(key_slice, key_bytes, work)? {
            return Ok(Some(&payload[value_start..value_start + value_total]));
        }
        pos = value_start + value_total;
    }
    Ok(None)
}

fn array_get_with_work<'a>(
    bytes: &'a [u8],
    index: i64,
    work: &mut JsonWork<'_>,
) -> Result<Option<&'a [u8]>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    if ty != JsonbType::Array {
        return Ok(None);
    }
    let payload = &bytes[payload_start..payload_start + payload_len];
    if index < 0 {
        let mut elements = Vec::new();
        let mut pos = 0usize;
        while pos < payload.len() {
            work.tick()?;
            let total = skip_value(&payload[pos..])?;
            elements.push((pos, total));
            pos += total;
        }
        let real = elements.len() as i64 + index;
        if real < 0 {
            return Ok(None);
        }
        let (start, total) = elements[real as usize];
        return Ok(Some(&payload[start..start + total]));
    }
    let mut pos = 0usize;
    let mut remaining = index;
    while pos < payload.len() {
        work.tick()?;
        let total = skip_value(&payload[pos..])?;
        if remaining == 0 {
            return Ok(Some(&payload[pos..pos + total]));
        }
        remaining -= 1;
        pos += total;
    }
    Ok(None)
}

fn read_scalar_text_with_work(bytes: &[u8], work: &mut JsonWork<'_>) -> Result<Option<String>> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    let payload = &bytes[payload_start..payload_start + payload_len];
    match ty {
        JsonbType::Null => Ok(None),
        JsonbType::True => Ok(Some("true".into())),
        JsonbType::False => Ok(Some("false".into())),
        JsonbType::Integer => {
            let value: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB integer payload size".into()))?;
            Ok(Some(i64::from_le_bytes(value).to_string()))
        }
        JsonbType::Real => {
            let value: [u8; 8] = payload
                .try_into()
                .map_err(|_| SqlError::InvalidValue("JSONB real payload size".into()))?;
            let number = serde_json::Number::from_f64(f64::from_le_bytes(value))
                .ok_or_else(|| SqlError::InvalidValue("non-finite JSONB number".into()))?;
            Ok(Some(number.to_string()))
        }
        JsonbType::String => {
            let text = std::str::from_utf8(payload)
                .map_err(|_| SqlError::InvalidValue("JSONB string not UTF-8".into()))?;
            Ok(Some(clone_string_with_work(text, work)?))
        }
        JsonbType::Array | JsonbType::Object => {
            let mut pos = 0usize;
            let value = decode_value(bytes, &mut pos, work)?;
            if pos != bytes.len() {
                return Err(SqlError::InvalidValue("trailing bytes in JSONB".into()));
            }
            Ok(Some(json_to_string_with_work(&value, false, work)?))
        }
    }
}

fn jsonb_contains_bytes_with_work(
    left: &[u8],
    right: &[u8],
    work: &mut JsonWork<'_>,
) -> Result<bool> {
    work.tick()?;
    let (left_type, left_payload_start, left_payload_len) = read_header(left)?;
    let (right_type, right_payload_start, right_payload_len) = read_header(right)?;
    let left_payload = &left[left_payload_start..left_payload_start + left_payload_len];
    let right_payload = &right[right_payload_start..right_payload_start + right_payload_len];
    match (left_type, right_type) {
        (JsonbType::Object, JsonbType::Object) => {
            let mut right_pos = 0usize;
            while right_pos < right_payload.len() {
                work.tick()?;
                let (_, key_payload_start, key_payload_len) =
                    read_header(&right_payload[right_pos..])?;
                let key_total = key_payload_start + key_payload_len;
                let key = &right_payload[right_pos + key_payload_start..right_pos + key_total];
                let right_value_start = right_pos + key_total;
                let right_value_total = skip_value(&right_payload[right_value_start..])?;
                let right_value =
                    &right_payload[right_value_start..right_value_start + right_value_total];
                let mut left_pos = 0usize;
                let mut found = false;
                while left_pos < left_payload.len() {
                    work.tick()?;
                    let (_, left_key_payload_start, left_key_payload_len) =
                        read_header(&left_payload[left_pos..])?;
                    let left_key_total = left_key_payload_start + left_key_payload_len;
                    let left_key =
                        &left_payload[left_pos + left_key_payload_start..left_pos + left_key_total];
                    let left_value_start = left_pos + left_key_total;
                    let left_value_total = skip_value(&left_payload[left_value_start..])?;
                    if bytes_equal_with_work(left_key, key, work)? {
                        let left_value =
                            &left_payload[left_value_start..left_value_start + left_value_total];
                        if !jsonb_contains_bytes_with_work(left_value, right_value, work)? {
                            return Ok(false);
                        }
                        found = true;
                        break;
                    }
                    left_pos = left_value_start + left_value_total;
                }
                if !found {
                    return Ok(false);
                }
                right_pos = right_value_start + right_value_total;
            }
            Ok(true)
        }
        (JsonbType::Array, JsonbType::Array) => {
            let mut right_pos = 0usize;
            while right_pos < right_payload.len() {
                work.tick()?;
                let right_value_total = skip_value(&right_payload[right_pos..])?;
                let right_value = &right_payload[right_pos..right_pos + right_value_total];
                let mut left_pos = 0usize;
                let mut found = false;
                while left_pos < left_payload.len() {
                    work.tick()?;
                    let left_value_total = skip_value(&left_payload[left_pos..])?;
                    let left_value = &left_payload[left_pos..left_pos + left_value_total];
                    if jsonb_contains_bytes_with_work(left_value, right_value, work)? {
                        found = true;
                        break;
                    }
                    left_pos += left_value_total;
                }
                if !found {
                    return Ok(false);
                }
                right_pos += right_value_total;
            }
            Ok(true)
        }
        (JsonbType::Array, _) => {
            let right_total = right_payload_start + right_payload_len;
            let right_full = &right[..right_total];
            let mut left_pos = 0usize;
            while left_pos < left_payload.len() {
                work.tick()?;
                let left_value_total = skip_value(&left_payload[left_pos..])?;
                if bytes_equal_with_work(
                    &left_payload[left_pos..left_pos + left_value_total],
                    right_full,
                    work,
                )? {
                    return Ok(true);
                }
                left_pos += left_value_total;
            }
            Ok(false)
        }
        _ => {
            let left_total = left_payload_start + left_payload_len;
            let right_total = right_payload_start + right_payload_len;
            bytes_equal_with_work(&left[..left_total], &right[..right_total], work)
        }
    }
}

fn has_top_key_bytes_with_work(bytes: &[u8], key: &str, work: &mut JsonWork<'_>) -> Result<bool> {
    let (ty, payload_start, payload_len) = read_header(bytes)?;
    let payload = &bytes[payload_start..payload_start + payload_len];
    let key_bytes = key.as_bytes();
    match ty {
        JsonbType::Object => {
            let mut pos = 0usize;
            while pos < payload.len() {
                work.tick()?;
                let (_, key_payload_start, key_payload_len) = read_header(&payload[pos..])?;
                let key_total = key_payload_start + key_payload_len;
                if bytes_equal_with_work(
                    &payload[pos + key_payload_start..pos + key_total],
                    key_bytes,
                    work,
                )? {
                    return Ok(true);
                }
                pos += key_total;
                pos += skip_value(&payload[pos..])?;
            }
            Ok(false)
        }
        JsonbType::Array => {
            let mut pos = 0usize;
            while pos < payload.len() {
                work.tick()?;
                let (element_type, element_payload_start, element_payload_len) =
                    read_header(&payload[pos..])?;
                if element_type == JsonbType::String
                    && bytes_equal_with_work(
                        &payload[pos + element_payload_start
                            ..pos + element_payload_start + element_payload_len],
                        key_bytes,
                        work,
                    )?
                {
                    return Ok(true);
                }
                pos += element_payload_start + element_payload_len;
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn parse_json_text_with_work(text: &str, work: &mut JsonWork<'_>) -> Result<serde_json::Value> {
    if work.cancel.is_none() {
        return serde_json::from_str(text)
            .map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}")));
    }

    let reader = CancellableJsonReader {
        bytes: text.as_bytes(),
        position: 0,
        work,
        interrupted: false,
    };
    let mut reader = std::io::BufReader::with_capacity(JSON_READ_CHUNK, reader);
    let value = serde_json::from_reader(&mut reader);
    let interrupted = reader.get_ref().interrupted;
    drop(reader);
    if interrupted {
        Err(SqlError::Storage(citadel_core::Error::Interrupted))
    } else {
        value.map_err(|e| SqlError::InvalidValue(format!("invalid JSON: {e}")))
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

fn serde_to_value_with_work(
    j: serde_json::Value,
    target: crate::types::DataType,
    work: &mut JsonWork<'_>,
) -> Result<Value> {
    use crate::types::DataType;
    if work.cancel.is_none() {
        return serde_to_value(j, target);
    }
    match target {
        DataType::Json => Ok(Value::Json(
            json_to_string_with_work(&j, false, work)?.into(),
        )),
        DataType::Jsonb => {
            let mut bytes = Vec::new();
            encode_canonical_with_work(&j, &mut bytes, work)?;
            Ok(Value::Jsonb(Arc::from(bytes)))
        }
        _ => Err(SqlError::InvalidValue(format!(
            "cannot serialize JSON to {target}"
        ))),
    }
}

fn serde_ref_to_value_with_work(
    value: &serde_json::Value,
    target: crate::types::DataType,
    work: &mut JsonWork<'_>,
) -> Result<Value> {
    use crate::types::DataType;
    match target {
        DataType::Json => Ok(Value::Json(
            json_to_string_with_work(value, false, work)?.into(),
        )),
        DataType::Jsonb => {
            let mut bytes = Vec::new();
            encode_canonical_with_work(value, &mut bytes, work)?;
            Ok(Value::Jsonb(Arc::from(bytes)))
        }
        _ => Err(SqlError::InvalidValue(format!(
            "cannot serialize JSON to {target}"
        ))),
    }
}

fn json_to_string_with_work(
    value: &serde_json::Value,
    pretty: bool,
    work: &mut JsonWork<'_>,
) -> Result<String> {
    if work.cancel.is_none() {
        return if pretty {
            serde_json::to_string_pretty(value)
        } else {
            serde_json::to_string(value)
        }
        .map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")));
    }

    let mut writer = CancellableJsonWriter {
        bytes: Vec::new(),
        work,
        interrupted: false,
    };
    let result = if pretty {
        serde_json::to_writer_pretty(&mut writer, value)
    } else {
        serde_json::to_writer(&mut writer, value)
    };
    if writer.interrupted {
        return Err(SqlError::Storage(citadel_core::Error::Interrupted));
    }
    result.map_err(|e| SqlError::InvalidValue(format!("JSON render: {e}")))?;
    String::from_utf8(writer.bytes)
        .map_err(|e| SqlError::InvalidValue(format!("JSON render produced invalid UTF-8: {e}")))
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

pub(crate) fn op_get_with_cancel(
    lhs: &Value,
    key: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_get(lhs, key);
    };
    run_json_work(Some(cancel), |work| {
        if let Value::Jsonb(bytes) = lhs {
            let slice = match key {
                Value::Text(key) => find_object_key_with_work(bytes, key, work)?,
                Value::Integer(index) => array_get_with_work(bytes, *index, work)?,
                _ => None,
            };
            return match slice {
                Some(slice) => {
                    let mut copied = Vec::with_capacity(slice.len());
                    extend_bytes_with_work(&mut copied, slice, work)?;
                    Ok(Value::Jsonb(Arc::from(copied)))
                }
                None => Ok(Value::Null),
            };
        }
        let target = match lhs {
            Value::Json(_) => crate::types::DataType::Json,
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "JSON or JSONB".into(),
                    got: lhs.data_type().to_string(),
                });
            }
        };
        let value = value_to_serde_with_work(lhs, work)?;
        match navigate_one_ref_with_work(&value, key, work)? {
            Some(value) => serde_ref_to_value_with_work(value, target, work),
            None => Ok(Value::Null),
        }
    })
}

pub(crate) fn op_get_text_with_cancel(
    lhs: &Value,
    key: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_get_text(lhs, key);
    };
    run_json_work(Some(cancel), |work| {
        if let Value::Jsonb(bytes) = lhs {
            let slice = match key {
                Value::Text(key) => find_object_key_with_work(bytes, key, work)?,
                Value::Integer(index) => array_get_with_work(bytes, *index, work)?,
                _ => None,
            };
            return match slice {
                Some(slice) => match read_scalar_text_with_work(slice, work)? {
                    Some(text) => Ok(Value::Text(text.into())),
                    None => Ok(Value::Null),
                },
                None => Ok(Value::Null),
            };
        }
        let value = value_to_serde_with_work(lhs, work)?;
        match navigate_one_ref_with_work(&value, key, work)? {
            Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(serde_json::Value::String(text)) => {
                Ok(Value::Text(clone_string_with_work(text, work)?.into()))
            }
            Some(value) => Ok(Value::Text(
                json_to_string_with_work(value, false, work)?.into(),
            )),
            None => Ok(Value::Null),
        }
    })
}

pub(crate) fn op_path_with_cancel(
    lhs: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_path(lhs, path);
    };
    run_json_work(Some(cancel), |work| {
        let target = match lhs {
            Value::Json(_) => crate::types::DataType::Json,
            Value::Jsonb(_) => crate::types::DataType::Jsonb,
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "JSON or JSONB".into(),
                    got: lhs.data_type().to_string(),
                });
            }
        };
        let value = value_to_serde_with_work(lhs, work)?;
        let segments = path_to_segments_with_work(path, work)?;
        match navigate_path_ref_with_work(&value, &segments, work)? {
            Some(value) => serde_ref_to_value_with_work(value, target, work),
            None => Ok(Value::Null),
        }
    })
}

pub(crate) fn op_path_text_with_cancel(
    lhs: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_path_text(lhs, path);
    };
    run_json_work(Some(cancel), |work| {
        let value = value_to_serde_with_work(lhs, work)?;
        let segments = path_to_segments_with_work(path, work)?;
        match navigate_path_ref_with_work(&value, &segments, work)? {
            Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(serde_json::Value::String(text)) => {
                Ok(Value::Text(clone_string_with_work(text, work)?.into()))
            }
            Some(value) => Ok(Value::Text(
                json_to_string_with_work(value, false, work)?.into(),
            )),
            None => Ok(Value::Null),
        }
    })
}

pub(crate) fn op_contains_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_contains(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        if let (Value::Jsonb(left), Value::Jsonb(right)) = (lhs, rhs) {
            return Ok(Value::Boolean(jsonb_contains_bytes_with_work(
                left, right, work,
            )?));
        }
        let left = value_to_serde_with_work(lhs, work)?;
        let right = value_to_serde_with_work(rhs, work)?;
        Ok(Value::Boolean(json_contains_with_work(
            &left, &right, work,
        )?))
    })
}

pub(crate) fn op_contained_by_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_contained_by(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        if let (Value::Jsonb(left), Value::Jsonb(right)) = (lhs, rhs) {
            return Ok(Value::Boolean(jsonb_contains_bytes_with_work(
                right, left, work,
            )?));
        }
        let left = value_to_serde_with_work(lhs, work)?;
        let right = value_to_serde_with_work(rhs, work)?;
        Ok(Value::Boolean(json_contains_with_work(
            &right, &left, work,
        )?))
    })
}

pub(crate) fn op_has_key_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_has_key(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        let key = match rhs {
            Value::Text(key) => key.as_str(),
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "TEXT key".into(),
                    got: rhs.data_type().to_string(),
                });
            }
        };
        if let Value::Jsonb(bytes) = lhs {
            return Ok(Value::Boolean(has_top_key_bytes_with_work(
                bytes, key, work,
            )?));
        }
        let left = value_to_serde_with_work(lhs, work)?;
        let exists = match &left {
            serde_json::Value::Object(map) => {
                work.checkpoint()?;
                let exists = map.contains_key(key);
                work.checkpoint()?;
                exists
            }
            serde_json::Value::Array(array) => {
                let mut exists = false;
                for value in array {
                    work.tick()?;
                    if let serde_json::Value::String(text) = value {
                        if bytes_equal_with_work(text.as_bytes(), key.as_bytes(), work)? {
                            exists = true;
                            break;
                        }
                    }
                }
                exists
            }
            _ => false,
        };
        Ok(Value::Boolean(exists))
    })
}

pub(crate) fn op_has_any_key_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_has_any_key(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        let left = value_to_serde_with_work(lhs, work)?;
        let keys = text_array_with_work(rhs, work)?;
        let serde_json::Value::Object(map) = &left else {
            return Ok(Value::Boolean(false));
        };
        for key in keys {
            work.tick()?;
            if map.contains_key(key.as_str()) {
                return Ok(Value::Boolean(true));
            }
        }
        Ok(Value::Boolean(false))
    })
}

pub(crate) fn op_has_all_keys_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_has_all_keys(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        let left = value_to_serde_with_work(lhs, work)?;
        let keys = text_array_with_work(rhs, work)?;
        let serde_json::Value::Object(map) = &left else {
            return Ok(Value::Boolean(keys.is_empty()));
        };
        for key in keys {
            work.tick()?;
            if !map.contains_key(key.as_str()) {
                return Ok(Value::Boolean(false));
            }
        }
        Ok(Value::Boolean(true))
    })
}

pub(crate) fn op_delete_path_with_cancel(
    lhs: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_delete_path(lhs, path);
    };
    run_json_work(Some(cancel), |work| {
        let target = match lhs {
            Value::Json(_) => crate::types::DataType::Json,
            Value::Jsonb(_) => crate::types::DataType::Jsonb,
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "JSON or JSONB".into(),
                    got: lhs.data_type().to_string(),
                });
            }
        };
        let mut value = value_to_serde_with_work(lhs, work)?;
        let segments = path_to_segments_with_work(path, work)?;
        delete_at_path_with_work(&mut value, &segments, work)?;
        serde_to_value_with_work(value, target, work)
    })
}

pub(crate) fn op_delete_one_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_delete_one(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        let target = match lhs {
            Value::Json(_) => crate::types::DataType::Json,
            Value::Jsonb(_) => crate::types::DataType::Jsonb,
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "JSON or JSONB".into(),
                    got: lhs.data_type().to_string(),
                });
            }
        };
        let mut value = value_to_serde_with_work(lhs, work)?;
        match (&mut value, rhs) {
            (serde_json::Value::Object(map), Value::Text(key)) => {
                work.checkpoint()?;
                map.remove(key.as_str());
                work.checkpoint()?;
            }
            (serde_json::Value::Array(array), Value::Integer(index)) => {
                let len = array.len() as i64;
                let index = if *index < 0 { len + index } else { *index };
                if (0..len).contains(&index) {
                    work.checkpoint()?;
                    array.remove(index as usize);
                    work.checkpoint()?;
                }
            }
            (serde_json::Value::Array(array), Value::Text(key)) => {
                let source = std::mem::take(array);
                array.reserve(source.len());
                for item in source {
                    work.tick()?;
                    let remove = match &item {
                        serde_json::Value::String(text) => {
                            bytes_equal_with_work(text.as_bytes(), key.as_bytes(), work)?
                        }
                        _ => false,
                    };
                    if !remove {
                        array.push(item);
                    }
                }
            }
            _ => {}
        }
        serde_to_value_with_work(value, target, work)
    })
}

pub(crate) fn op_concat_with_cancel(
    lhs: &Value,
    rhs: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_concat(lhs, rhs);
    };
    run_json_work(Some(cancel), |work| {
        let target = match (lhs, rhs) {
            (Value::Jsonb(_), _) | (_, Value::Jsonb(_)) => crate::types::DataType::Jsonb,
            _ => crate::types::DataType::Json,
        };
        let mut left = value_to_serde_with_work(lhs, work)?;
        let right = value_to_serde_with_work(rhs, work)?;
        match (&mut left, right) {
            (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
                for (key, value) in right {
                    work.tick()?;
                    left.insert(key, value);
                }
            }
            (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
                left.reserve(right.len());
                for value in right {
                    work.tick()?;
                    left.push(value);
                }
            }
            (serde_json::Value::Array(left), other) => left.push(other),
            (left, serde_json::Value::Array(right)) => {
                let owned = std::mem::take(left);
                let mut combined = Vec::with_capacity(right.len() + 1);
                combined.push(owned);
                for value in right {
                    work.tick()?;
                    combined.push(value);
                }
                *left = serde_json::Value::Array(combined);
            }
            (left, right) => {
                let left_value = std::mem::take(left);
                *left = serde_json::Value::Array(vec![left_value, right]);
            }
        }
        serde_to_value_with_work(left, target, work)
    })
}

pub(crate) fn op_path_exists_with_cancel(
    lhs: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_path_exists(lhs, path);
    };
    run_json_work(Some(cancel), |work| {
        let value = value_to_serde_with_work(lhs, work)?;
        let path = coerce_path_arg_with_work(path, work)?;
        let exists =
            checked_json_phase(work, || jp_exists(&value, &path, None, false))?.unwrap_or(false);
        Ok(Value::Boolean(exists))
    })
}

pub(crate) fn op_path_match_with_cancel(
    lhs: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return op_path_match(lhs, path);
    };
    run_json_work(Some(cancel), |work| {
        let value = value_to_serde_with_work(lhs, work)?;
        let path = coerce_path_arg_with_work(path, work)?;
        let nodes = checked_json_phase(work, || jp_query(&value, &path, None, false))?;
        for node in nodes {
            work.tick()?;
            if matches!(node, serde_json::Value::Bool(true)) {
                return Ok(Value::Boolean(true));
            }
        }
        Ok(Value::Boolean(false))
    })
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

fn coerce_path_arg_with_work(v: &Value, work: &mut JsonWork<'_>) -> Result<String> {
    match v {
        Value::Text(path) | Value::Json(path) => clone_string_with_work(path, work),
        _ => Err(SqlError::TypeMismatch {
            expected: "TEXT path".into(),
            got: v.data_type().to_string(),
        }),
    }
}

fn coerce_vars_arg_with_work(
    v: &Value,
    work: &mut JsonWork<'_>,
) -> Result<Option<serde_json::Value>> {
    if v.is_null() {
        return Ok(None);
    }
    let j = value_to_serde_with_work(v, work)?;
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
    fn_json_exists_with_cancel(j_val, path, None)
}

pub(crate) fn fn_json_exists_with_cancel(
    j_val: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let j = value_to_serde_with_work(j_val, work)?;
        let path_str = coerce_path_arg_with_work(path, work)?;
        let exists =
            checked_json_phase(work, || jp_exists(&j, &path_str, None, false))?.unwrap_or(false);
        Ok(Value::Boolean(exists))
    })
}

pub fn fn_json_value(j_val: &Value, path: &Value) -> Result<Value> {
    fn_json_value_with_cancel(j_val, path, None)
}

pub(crate) fn fn_json_value_with_cancel(
    j_val: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let j = value_to_serde_with_work(j_val, work)?;
        let path_str = coerce_path_arg_with_work(path, work)?;
        match checked_json_phase(work, || jp_query_first(&j, &path_str, None, false))? {
            Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(serde_json::Value::String(s)) => Ok(Value::Text(s.into())),
            Some(other) => {
                let text = json_to_string_with_work(&other, false, work)?;
                Ok(Value::Text(text.into()))
            }
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_json_query(j_val: &Value, path: &Value, target: crate::types::DataType) -> Result<Value> {
    fn_json_query_with_cancel(j_val, path, target, None)
}

pub(crate) fn fn_json_query_with_cancel(
    j_val: &Value,
    path: &Value,
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let j = value_to_serde_with_work(j_val, work)?;
        let path_str = coerce_path_arg_with_work(path, work)?;
        let nodes = checked_json_phase(work, || jp_query(&j, &path_str, None, false))?;
        if nodes.is_empty() {
            return Ok(Value::Null);
        }
        let result_json = if nodes.len() == 1 {
            nodes[0].clone()
        } else {
            serde_json::Value::Array(nodes)
        };
        serde_to_value_with_work(result_json, target, work)
    })
}

pub fn fn_jsonb_path_exists(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_exists_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_exists_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_exists: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        match checked_json_phase(work, || jp_exists(&j, &path_str, vars.as_ref(), silent))? {
            Some(b) => Ok(Value::Boolean(b)),
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_jsonb_path_match(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_match_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_match_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_match: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        let nodes = checked_json_phase(work, || jp_query(&j, &path_str, vars.as_ref(), silent))?;
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
    })
}

pub fn fn_jsonb_path_query_first(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_first_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_query_first_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_query_first: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        match checked_json_phase(work, || {
            jp_query_first(&j, &path_str, vars.as_ref(), silent)
        })? {
            Some(v) => serde_to_value_with_work(v, crate::types::DataType::Jsonb, work),
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_jsonb_path_query_array(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_array_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_query_array_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_query_array: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        let nodes = checked_json_phase(work, || jp_query(&j, &path_str, vars.as_ref(), silent))?;
        serde_to_value_with_work(
            serde_json::Value::Array(nodes),
            crate::types::DataType::Jsonb,
            work,
        )
    })
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
    fn_jsonb_path_exists_tz_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_exists_tz_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_exists_tz: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        match checked_json_phase(work, || jp_exists_tz(&j, &path_str, vars.as_ref(), silent))? {
            Some(b) => Ok(Value::Boolean(b)),
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_jsonb_path_match_tz(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_match_tz_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_match_tz_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_match_tz: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        let nodes = checked_json_phase(work, || jp_query_tz(&j, &path_str, vars.as_ref(), silent))?;
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
    })
}

pub fn fn_jsonb_path_query_first_tz(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_first_tz_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_query_first_tz_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_query_first_tz: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        match checked_json_phase(work, || {
            jp_query_first_tz(&j, &path_str, vars.as_ref(), silent)
        })? {
            Some(v) => serde_to_value_with_work(v, crate::types::DataType::Jsonb, work),
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_jsonb_path_query_array_tz(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_array_tz_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_query_array_tz_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !(2..=4).contains(&args.len()) {
            return Err(SqlError::InvalidValue(
                "jsonb_path_query_array_tz: expected 2..=4 arguments".into(),
            ));
        }
        let j = value_to_serde_with_work(&args[0], work)?;
        let path_str = coerce_path_arg_with_work(&args[1], work)?;
        let vars = match args.get(2) {
            Some(value) => coerce_vars_arg_with_work(value, work)?,
            None => None,
        };
        let silent = args
            .get(3)
            .map(coerce_silent_arg)
            .transpose()?
            .unwrap_or(false);
        let nodes = checked_json_phase(work, || jp_query_tz(&j, &path_str, vars.as_ref(), silent))?;
        serde_to_value_with_work(
            serde_json::Value::Array(nodes),
            crate::types::DataType::Jsonb,
            work,
        )
    })
}

pub fn fn_jsonb_path_query_tz(args: &[Value]) -> Result<Value> {
    fn_jsonb_path_query_tz_with_cancel(args, None)
}

pub(crate) fn fn_jsonb_path_query_tz_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    fn_jsonb_path_query_first_tz_with_cancel(args, cancel)
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

fn parse_dollar_path_with_work(s: &str, work: &mut JsonWork<'_>) -> Result<Vec<PathSeg>> {
    if work.cancel.is_none() {
        return parse_dollar_path(s);
    }
    let s = s.trim();
    let s = s.strip_prefix('$').unwrap_or(s);
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        work.tick()?;
        match bytes[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                    work.tick()?;
                    i += 1;
                }
                if i > start {
                    let key = std::str::from_utf8(&bytes[start..i])
                        .map_err(|_| SqlError::InvalidValue("invalid path segment".into()))?;
                    out.push(PathSeg::Key(clone_string_with_work(key, work)?));
                }
            }
            b'[' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    work.tick()?;
                    i += 1;
                }
                if i > bytes.len() {
                    return Err(SqlError::InvalidValue("unterminated index".into()));
                }
                let inner = std::str::from_utf8(&bytes[start..i])
                    .map_err(|_| SqlError::InvalidValue("invalid path index".into()))?;
                if inner.trim() == "*" {
                    out.push(PathSeg::Wildcard);
                } else if let Some(index) = parse_i64_with_work(inner, work)? {
                    out.push(PathSeg::Index(index));
                } else {
                    let key = inner.trim_matches('"').trim_matches('\'');
                    out.push(PathSeg::Key(clone_string_with_work(key, work)?));
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    Ok(out)
}

fn path_to_segments(v: &Value) -> Result<Vec<PathSeg>> {
    run_json_work(None, |work| path_to_segments_with_work(v, work))
}

fn path_to_segments_with_work(v: &Value, work: &mut JsonWork<'_>) -> Result<Vec<PathSeg>> {
    match v {
        Value::Text(s) => {
            if s.starts_with('$') {
                parse_dollar_path_with_work(s, work)
            } else if s.starts_with('{') && s.ends_with('}') {
                parse_pg_array_path_with_work(s, work)
            } else {
                Ok(vec![PathSeg::Key(clone_string_with_work(s, work)?)])
            }
        }
        Value::Integer(i) => Ok(vec![PathSeg::Index(*i)]),
        Value::Json(_) | Value::Jsonb(_) => {
            let parsed = value_to_serde_with_work(v, work)?;
            json_to_path_with_work(&parsed, work)
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

fn parse_pg_array_path_with_work(s: &str, work: &mut JsonWork<'_>) -> Result<Vec<PathSeg>> {
    if work.cancel.is_none() {
        return parse_pg_array_path(s);
    }
    let inner = &s[1..s.len() - 1];
    if inner.is_empty() {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    for raw in inner.split(',') {
        work.tick()?;
        let trimmed = raw.trim().trim_matches('"');
        if let Some(index) = parse_i64_with_work(trimmed, work)? {
            out.push(PathSeg::Index(index));
            continue;
        }
        out.push(PathSeg::Key(clone_string_with_work(trimmed, work)?));
    }
    Ok(out)
}

fn parse_i64_with_work(text: &str, work: &mut JsonWork<'_>) -> Result<Option<i64>> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Ok(None);
    }
    let (negative, digits) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return Ok(None);
    }
    let mut magnitude = 0u64;
    for digit in digits {
        work.tick()?;
        if !digit.is_ascii_digit() {
            return Ok(None);
        }
        let Some(next) = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(*digit - b'0')))
        else {
            return Ok(None);
        };
        magnitude = next;
    }
    if negative {
        if magnitude == (i64::MAX as u64) + 1 {
            Ok(Some(i64::MIN))
        } else {
            Ok(i64::try_from(magnitude).ok().map(|value| -value))
        }
    } else {
        Ok(i64::try_from(magnitude).ok())
    }
}

fn json_to_path_with_work(j: &serde_json::Value, work: &mut JsonWork<'_>) -> Result<Vec<PathSeg>> {
    let arr = j
        .as_array()
        .ok_or_else(|| SqlError::InvalidValue("path must be a JSON array".into()))?;
    arr.iter()
        .map(|item| {
            work.tick()?;
            match item {
                serde_json::Value::String(s) => clone_string_with_work(s, work).map(PathSeg::Key),
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .map(PathSeg::Index)
                    .ok_or_else(|| SqlError::InvalidValue("path index out of range".into())),
                _ => Err(SqlError::InvalidValue(
                    "path segments must be strings or integers".into(),
                )),
            }
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

fn navigate_path_ref_with_work<'a>(
    j: &'a serde_json::Value,
    segments: &[PathSeg],
    work: &mut JsonWork<'_>,
) -> Result<Option<&'a serde_json::Value>> {
    let mut current = j;
    for segment in segments {
        work.tick()?;
        current = match (current, segment) {
            (serde_json::Value::Object(map), PathSeg::Key(key)) => {
                let Some(value) = map.get(key.as_str()) else {
                    return Ok(None);
                };
                value
            }
            (serde_json::Value::Array(array), PathSeg::Index(index)) => {
                let len = array.len() as i64;
                let index = if *index < 0 { len + index } else { *index };
                let Some(value) = usize::try_from(index)
                    .ok()
                    .filter(|index| *index < array.len())
                    .map(|index| &array[index])
                else {
                    return Ok(None);
                };
                value
            }
            (serde_json::Value::Array(array), PathSeg::Key(key)) => {
                let Ok(index) = key.parse::<i64>() else {
                    return Ok(None);
                };
                let len = array.len() as i64;
                let index = if index < 0 { len + index } else { index };
                let Some(value) = usize::try_from(index)
                    .ok()
                    .filter(|index| *index < array.len())
                    .map(|index| &array[index])
                else {
                    return Ok(None);
                };
                value
            }
            _ => return Ok(None),
        };
    }
    Ok(Some(current))
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

fn navigate_mut_with_work<'a>(
    value: &'a mut serde_json::Value,
    segments: &[PathSeg],
    work: &mut JsonWork<'_>,
) -> Result<Option<&'a mut serde_json::Value>> {
    let mut current = value;
    for segment in segments {
        work.tick()?;
        current = match (current, segment) {
            (serde_json::Value::Object(map), PathSeg::Key(key)) => {
                let Some(value) = map.get_mut(key.as_str()) else {
                    return Ok(None);
                };
                value
            }
            (serde_json::Value::Array(array), PathSeg::Index(index)) => {
                let len = array.len() as i64;
                let index = if *index < 0 { len + index } else { *index };
                let Some(value) = usize::try_from(index)
                    .ok()
                    .filter(|index| *index < array.len())
                    .and_then(|index| array.get_mut(index))
                else {
                    return Ok(None);
                };
                value
            }
            _ => return Ok(None),
        };
    }
    Ok(Some(current))
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
    run_json_work(None, |work| text_array_with_work(v, work))
}

fn text_array_with_work(v: &Value, work: &mut JsonWork<'_>) -> Result<Vec<String>> {
    match v {
        Value::Text(s) => Ok(vec![s.to_string()]),
        Value::Json(_) | Value::Jsonb(_) => {
            let j = value_to_serde_with_work(v, work)?;
            j.as_array()
                .ok_or_else(|| SqlError::InvalidValue("expected JSON text array".into()))?
                .iter()
                .map(|e| {
                    work.tick()?;
                    match e {
                        serde_json::Value::String(s) => clone_string_with_work(s, work),
                        _ => Err(SqlError::InvalidValue(
                            "array elements must be strings".into(),
                        )),
                    }
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

pub(crate) fn agg_array_with_cancel(
    values: &[Value],
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return agg_array(values, target);
    };
    run_json_work(Some(cancel), |work| {
        let mut items = Vec::with_capacity(values.len());
        for value in values {
            work.tick()?;
            items.push(value_to_serde_lossy_with_work(value, work)?);
        }
        serde_to_value_with_work(serde_json::Value::Array(items), target, work)
    })
}

pub fn materialize_json_table(
    source: &Value,
    spec: &crate::parser::JsonTableSpec,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    materialize_json_table_with_cancel(source, spec, None)
}

pub(crate) fn materialize_json_table_with_cancel(
    source: &Value,
    spec: &crate::parser::JsonTableSpec,
    cancel: Option<&CancelToken>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut work = JsonWork::new(cancel)?;
    if source.is_null() {
        let names = json_table_column_names(&spec.columns);
        work.finish()?;
        return Ok((names, vec![]));
    }
    let root = value_to_serde_with_work(source, &mut work)?;
    let root_segs = parse_dollar_path_with_work(&spec.root_path, &mut work)?;
    let matches = json_table_walk(&root, &root_segs, &mut work)?;
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut ordinality_counter = 0i64;
    for m in matches {
        work.tick()?;
        ordinality_counter += 1;
        emit_json_table_rows(m, &spec.columns, ordinality_counter, &mut rows, &mut work)?;
    }
    let names = json_table_column_names(&spec.columns);
    work.finish()?;
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

fn json_table_walk<'a>(
    j: &'a serde_json::Value,
    segs: &[PathSeg],
    work: &mut JsonWork<'_>,
) -> Result<Vec<&'a serde_json::Value>> {
    let mut frontier = vec![j];
    for seg in segs {
        let mut next = Vec::new();
        for cur in frontier {
            work.tick()?;
            match (cur, seg) {
                (serde_json::Value::Object(m), PathSeg::Key(k)) => {
                    if let Some(v) = m.get(k.as_str()) {
                        next.push(v);
                    }
                }
                (serde_json::Value::Array(arr), PathSeg::Index(i)) => {
                    let len = arr.len() as i64;
                    let idx = if *i < 0 { len + i } else { *i };
                    if (0..len).contains(&idx) {
                        next.push(&arr[idx as usize]);
                    }
                }
                (serde_json::Value::Array(arr), PathSeg::Wildcard) => {
                    next.extend(arr.iter());
                }
                _ => {}
            }
        }
        frontier = next;
    }

    if segs.last() != Some(&PathSeg::Wildcard) {
        return Ok(frontier);
    }

    let mut flattened = Vec::new();
    for value in frontier {
        work.tick()?;
        match value {
            serde_json::Value::Array(arr) => flattened.extend(arr.iter()),
            other => flattened.push(other),
        }
    }
    Ok(flattened)
}

fn emit_json_table_rows(
    row_doc: &serde_json::Value,
    columns: &[crate::parser::JsonTableCol],
    parent_ordinality: i64,
    out: &mut Vec<Vec<Value>>,
    work: &mut JsonWork<'_>,
) -> Result<()> {
    use crate::parser::JsonTableCol as C;

    let mut scalars: Vec<(usize, Value)> = Vec::new();
    let mut nesteds: Vec<(usize, Vec<Vec<Value>>)> = Vec::new();
    let mut widths: Vec<usize> = Vec::with_capacity(columns.len());

    for (idx, c) in columns.iter().enumerate() {
        work.tick()?;
        match c {
            C::Named {
                ty, path, exists, ..
            } => {
                let segs = parse_dollar_path_with_work(path, work)?;
                let matches = json_table_walk(row_doc, &segs, work)?;
                let v = if *exists {
                    Value::Boolean(!matches.is_empty())
                } else if matches.is_empty() {
                    Value::Null
                } else {
                    json_table_coerce(matches[0], *ty, work)?
                };
                scalars.push((idx, v));
                widths.push(1);
            }
            C::Ordinality { .. } => {
                scalars.push((idx, Value::Integer(parent_ordinality)));
                widths.push(1);
            }
            C::Nested { path, columns } => {
                let segs = parse_dollar_path_with_work(path, work)?;
                let matches = json_table_walk(row_doc, &segs, work)?;
                let inner_width = json_table_column_names(columns).len();
                let mut inner: Vec<Vec<Value>> = Vec::new();
                let mut ord = 0i64;
                for m in matches {
                    work.tick()?;
                    ord += 1;
                    emit_json_table_rows(m, columns, ord, &mut inner, work)?;
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
        work.tick()?;
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

fn json_table_coerce(
    v: &serde_json::Value,
    target: crate::types::DataType,
    work: &mut JsonWork<'_>,
) -> Result<Value> {
    use crate::types::DataType;
    if matches!(v, serde_json::Value::Null) {
        return Ok(Value::Null);
    }
    match (v, target) {
        (_, DataType::Json) => serde_to_value_with_work(v.clone(), DataType::Json, work),
        (_, DataType::Jsonb) => serde_to_value_with_work(v.clone(), DataType::Jsonb, work),
        (serde_json::Value::Number(n), DataType::Integer) => n
            .as_i64()
            .map(Value::Integer)
            .ok_or_else(|| SqlError::InvalidValue("JSON_TABLE: number not i64".into())),
        (serde_json::Value::Number(n), DataType::Real) => n
            .as_f64()
            .map(Value::Real)
            .ok_or_else(|| SqlError::InvalidValue("JSON_TABLE: number not f64".into())),
        (serde_json::Value::Bool(b), DataType::Boolean) => Ok(Value::Boolean(*b)),
        (serde_json::Value::String(s), DataType::Text) => {
            Ok(Value::Text(clone_string_with_work(s, work)?.into()))
        }
        _ => {
            let text_form = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => json_to_string_with_work(v, false, work)?,
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
    extract_gin_entries_with_cancel(value, ops, None)
}

pub(crate) fn extract_gin_entries_with_cancel(
    value: &Value,
    ops: crate::types::GinOpsClass,
    cancel: Option<&CancelToken>,
) -> Result<Vec<Vec<u8>>> {
    use crate::types::GinOpsClass;
    let mut work = JsonWork::new(cancel)?;
    if value.is_null() {
        work.finish()?;
        return Ok(vec![]);
    }
    let j = value_to_serde_with_work(value, &mut work)?;
    let mut out: Vec<Vec<u8>> = Vec::new();
    match ops {
        GinOpsClass::JsonbOps => extract_jsonb_ops_walk(&j, &mut out, &mut work)?,
        GinOpsClass::JsonbPathOps => extract_path_ops_walk(&j, 0, &mut out, &mut work)?,
    }
    out.sort();
    out.dedup();
    work.finish()?;
    Ok(out)
}

fn extract_jsonb_ops_walk(
    j: &serde_json::Value,
    out: &mut Vec<Vec<u8>>,
    work: &mut JsonWork<'_>,
) -> Result<()> {
    match j {
        serde_json::Value::Object(m) => {
            for (k, v) in m {
                work.tick()?;
                let large_copy = k.len() >= JSON_READ_CHUNK || is_large_json_string(v);
                if large_copy {
                    work.checkpoint()?;
                }
                let mut key_entry = Vec::with_capacity(k.len() + 1);
                key_entry.push(0x01);
                extend_bytes_with_work(&mut key_entry, k.as_bytes(), work)?;
                out.push(key_entry);
                if let Some(s) = scalar_repr_with_work(v, work)? {
                    let mut pair = Vec::with_capacity(k.len() + s.len() + 2);
                    pair.push(0x02);
                    extend_bytes_with_work(&mut pair, k.as_bytes(), work)?;
                    pair.push(0x00);
                    extend_bytes_with_work(&mut pair, s.as_bytes(), work)?;
                    out.push(pair);
                }
                extract_jsonb_ops_walk(v, out, work)?;
                if large_copy {
                    work.checkpoint()?;
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                work.tick()?;
                let large_copy = is_large_json_string(v);
                if large_copy {
                    work.checkpoint()?;
                }
                if let Some(s) = scalar_repr_with_work(v, work)? {
                    let mut entry = Vec::with_capacity(s.len() + 1);
                    entry.push(0x03);
                    extend_bytes_with_work(&mut entry, s.as_bytes(), work)?;
                    out.push(entry);
                }
                extract_jsonb_ops_walk(v, out, work)?;
                if large_copy {
                    work.checkpoint()?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn encode_canonical_with_work(
    v: &serde_json::Value,
    out: &mut Vec<u8>,
    work: &mut JsonWork<'_>,
) -> Result<()> {
    work.tick()?;
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
        serde_json::Value::String(s) => {
            write_header_with_len(JsonbType::String, s.len(), out);
            extend_bytes_with_work(out, s.as_bytes(), work)?;
        }
        serde_json::Value::Array(items) => {
            let mut payload = Vec::new();
            for item in items {
                encode_canonical_with_work(item, &mut payload, work)?;
            }
            write_header_with_len(JsonbType::Array, payload.len(), out);
            extend_bytes_with_work(out, &payload, work)?;
        }
        serde_json::Value::Object(map) => {
            work.checkpoint()?;
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            work.checkpoint()?;
            let mut payload = Vec::new();
            for key in keys {
                work.tick()?;
                write_header_with_len(JsonbType::String, key.len(), &mut payload);
                extend_bytes_with_work(&mut payload, key.as_bytes(), work)?;
                encode_canonical_with_work(&map[key], &mut payload, work)?;
            }
            write_header_with_len(JsonbType::Object, payload.len(), out);
            extend_bytes_with_work(out, &payload, work)?;
        }
    }
    Ok(())
}

fn extend_bytes_with_work(out: &mut Vec<u8>, bytes: &[u8], work: &mut JsonWork<'_>) -> Result<()> {
    if work.cancel.is_none() {
        out.extend_from_slice(bytes);
        return Ok(());
    }
    for chunk in bytes.chunks(JSON_READ_CHUNK) {
        work.checkpoint()?;
        out.extend_from_slice(chunk);
    }
    Ok(())
}

fn clone_string_with_work(text: &str, work: &mut JsonWork<'_>) -> Result<String> {
    if work.cancel.is_none() {
        return Ok(text.to_owned());
    }
    let mut bytes = Vec::with_capacity(text.len());
    extend_bytes_with_work(&mut bytes, text.as_bytes(), work)?;
    String::from_utf8(bytes)
        .map_err(|e| SqlError::InvalidValue(format!("JSON string was not UTF-8: {e}")))
}

pub(crate) fn clone_text_with_cancel(text: &str, cancel: Option<&CancelToken>) -> Result<String> {
    let Some(cancel) = cancel else {
        return Ok(text.to_owned());
    };
    run_json_work(Some(cancel), |work| clone_string_with_work(text, work))
}

fn extract_path_ops_walk(
    j: &serde_json::Value,
    path: u32,
    out: &mut Vec<Vec<u8>>,
    work: &mut JsonWork<'_>,
) -> Result<()> {
    match j {
        serde_json::Value::Object(m) => {
            for (k, v) in m {
                work.tick()?;
                if k.len() >= JSON_READ_CHUNK {
                    work.checkpoint()?;
                }
                let next = path.rotate_left(1) ^ fx_hash_u32(k.as_bytes());
                if k.len() >= JSON_READ_CHUNK {
                    work.checkpoint()?;
                }
                extract_path_ops_walk(v, next, out, work)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                work.tick()?;
                extract_path_ops_walk(v, path, out, work)?;
            }
        }
        _ => {
            work.checkpoint()?;
            let leaf = path.rotate_left(1) ^ hash_scalar_for_path(j);
            work.checkpoint()?;
            out.push(leaf.to_le_bytes().to_vec());
        }
    }
    Ok(())
}

fn is_large_json_string(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::String(text) if text.len() >= JSON_READ_CHUNK)
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

fn delete_at_path_with_work(
    value: &mut serde_json::Value,
    segments: &[PathSeg],
    work: &mut JsonWork<'_>,
) -> Result<()> {
    let Some((last, prefix)) = segments.split_last() else {
        return Ok(());
    };
    let Some(target) = navigate_mut_with_work(value, prefix, work)? else {
        return Ok(());
    };
    work.checkpoint()?;
    match (target, last) {
        (serde_json::Value::Object(map), PathSeg::Key(key)) => {
            map.remove(key.as_str());
        }
        (serde_json::Value::Array(array), PathSeg::Index(index)) => {
            let len = array.len() as i64;
            let index = if *index < 0 { len + index } else { *index };
            if (0..len).contains(&index) {
                array.remove(index as usize);
            }
        }
        _ => {}
    }
    work.checkpoint()
}

fn json_contains_with_work(
    left: &serde_json::Value,
    right: &serde_json::Value,
    work: &mut JsonWork<'_>,
) -> Result<bool> {
    work.tick()?;
    match (left, right) {
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            for (key, right_value) in right {
                work.tick()?;
                let Some(left_value) = left.get(key) else {
                    return Ok(false);
                };
                if !json_contains_with_work(left_value, right_value, work)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            for right_value in right {
                work.tick()?;
                let mut found = false;
                for left_value in left {
                    work.tick()?;
                    if json_contains_with_work(left_value, right_value, work)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (serde_json::Value::Array(left), right) => {
            for left_value in left {
                work.tick()?;
                if json_contains_with_work(left_value, right, work)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        (serde_json::Value::String(left), serde_json::Value::String(right)) => {
            bytes_equal_with_work(left.as_bytes(), right.as_bytes(), work)
        }
        (left, right) => Ok(left == right),
    }
}

fn navigate_one_ref_with_work<'a>(
    value: &'a serde_json::Value,
    key: &Value,
    work: &mut JsonWork<'_>,
) -> Result<Option<&'a serde_json::Value>> {
    work.tick()?;
    Ok(match (value, key) {
        (serde_json::Value::Object(map), Value::Text(key)) => map.get(key.as_str()),
        (serde_json::Value::Array(array), Value::Integer(index)) => {
            let len = array.len() as i64;
            let index = if *index < 0 { len + index } else { *index };
            usize::try_from(index)
                .ok()
                .filter(|index| *index < array.len())
                .map(|index| &array[index])
        }
        _ => None,
    })
}

fn scalar_repr_with_work(
    value: &serde_json::Value,
    work: &mut JsonWork<'_>,
) -> Result<Option<String>> {
    match value {
        serde_json::Value::String(text) => clone_string_with_work(text, work).map(Some),
        _ => Ok(scalar_repr(value)),
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

pub(crate) fn agg_object_with_cancel(
    pairs: &[(Value, Value)],
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    let Some(cancel) = cancel else {
        return agg_object(pairs, target);
    };
    run_json_work(Some(cancel), |work| {
        let mut map = serde_json::Map::new();
        for (key, value) in pairs {
            work.tick()?;
            if key.is_null() {
                continue;
            }
            let key = match key {
                Value::Text(text) | Value::Json(text) => clone_string_with_work(text, work)?,
                _ => {
                    work.checkpoint()?;
                    let text = format!("{key}");
                    work.checkpoint()?;
                    text
                }
            };
            let value = value_to_serde_lossy_with_work(value, work)?;
            map.insert(key, value);
        }
        serde_to_value_with_work(serde_json::Value::Object(map), target, work)
    })
}

pub fn populate_record_row(
    obj: &serde_json::Map<String, serde_json::Value>,
    columns: &[crate::types::ColumnDef],
) -> Result<Vec<Value>> {
    populate_record_row_with_cancel(obj, columns, None)
}

pub(crate) fn populate_record_row_with_cancel(
    obj: &serde_json::Map<String, serde_json::Value>,
    columns: &[crate::types::ColumnDef],
    cancel: Option<&CancelToken>,
) -> Result<Vec<Value>> {
    let mut work = JsonWork::new(cancel)?;
    let mut row = Vec::with_capacity(columns.len());
    for col in columns {
        work.tick()?;
        row.push(match obj.get(&col.name) {
            None | Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(v) => coerce_json_field(v, col.data_type, &mut work),
        }?);
    }
    work.finish()?;
    Ok(row)
}

fn coerce_json_field(
    j: &serde_json::Value,
    target: crate::types::DataType,
    work: &mut JsonWork<'_>,
) -> Result<Value> {
    use crate::types::DataType;
    if target == DataType::Text {
        if let serde_json::Value::String(text) = j {
            return Ok(Value::Text(clone_string_with_work(text, work)?.into()));
        }
    }
    match target {
        DataType::Json | DataType::Jsonb => serde_to_value_with_work(j.clone(), target, work),
        _ => {
            work.checkpoint()?;
            let v = serde_to_scalar_value(j.clone());
            let value = crate::eval::eval_cast(&v, target)?;
            work.checkpoint()?;
            Ok(value)
        }
    }
}

pub fn dispatch_srf(name: &str, args: &[Value]) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    dispatch_srf_with_cancel(name, args, None)
}

pub(crate) fn dispatch_srf_with_cancel(
    name: &str,
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut work = JsonWork::new(cancel)?;
    let upper = name.to_ascii_uppercase();
    let result = match upper.as_str() {
        "JSONB_ARRAY_ELEMENTS" | "JSON_ARRAY_ELEMENTS" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                Ok((vec!["value".into()], vec![]))
            } else {
                let j = value_to_serde_with_work(&args[0], &mut work)?;
                let arr = j
                    .as_array()
                    .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON array")))?;
                let target = if upper.starts_with("JSONB") {
                    crate::types::DataType::Jsonb
                } else {
                    crate::types::DataType::Json
                };
                let mut rows = Vec::with_capacity(arr.len());
                for v in arr {
                    work.tick()?;
                    rows.push(vec![serde_to_value_with_work(
                        v.clone(),
                        target,
                        &mut work,
                    )?]);
                }
                Ok((vec!["value".into()], rows))
            }
        }
        "JSONB_ARRAY_ELEMENTS_TEXT" | "JSON_ARRAY_ELEMENTS_TEXT" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                Ok((vec!["value".into()], vec![]))
            } else {
                let j = value_to_serde_with_work(&args[0], &mut work)?;
                let arr = j
                    .as_array()
                    .ok_or_else(|| SqlError::InvalidValue(format!("{name} requires JSON array")))?;
                let mut rows = Vec::with_capacity(arr.len());
                for v in arr {
                    work.tick()?;
                    let text = match v {
                        serde_json::Value::String(s) => clone_string_with_work(s, &mut work)?,
                        _ => json_to_string_with_work(v, false, &mut work)?,
                    };
                    rows.push(vec![Value::Text(text.into())]);
                }
                Ok((vec!["value".into()], rows))
            }
        }
        "JSONB_EACH" | "JSON_EACH" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                Ok((vec!["key".into(), "value".into()], vec![]))
            } else {
                let j = value_to_serde_with_work(&args[0], &mut work)?;
                let obj = j.as_object().ok_or_else(|| {
                    SqlError::InvalidValue(format!("{name} requires JSON object"))
                })?;
                let target = if upper.starts_with("JSONB") {
                    crate::types::DataType::Jsonb
                } else {
                    crate::types::DataType::Json
                };
                let mut rows = Vec::with_capacity(obj.len());
                for (k, v) in obj {
                    work.tick()?;
                    rows.push(vec![
                        Value::Text(k.clone().into()),
                        serde_to_value_with_work(v.clone(), target, &mut work)?,
                    ]);
                }
                Ok((vec!["key".into(), "value".into()], rows))
            }
        }
        "JSONB_EACH_TEXT" | "JSON_EACH_TEXT" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                Ok((vec!["key".into(), "value".into()], vec![]))
            } else {
                let j = value_to_serde_with_work(&args[0], &mut work)?;
                let obj = j.as_object().ok_or_else(|| {
                    SqlError::InvalidValue(format!("{name} requires JSON object"))
                })?;
                let mut rows = Vec::with_capacity(obj.len());
                for (k, v) in obj {
                    work.tick()?;
                    let text = match v {
                        serde_json::Value::String(s) => clone_string_with_work(s, &mut work)?,
                        _ => json_to_string_with_work(v, false, &mut work)?,
                    };
                    rows.push(vec![
                        Value::Text(k.clone().into()),
                        Value::Text(text.into()),
                    ]);
                }
                Ok((vec!["key".into(), "value".into()], rows))
            }
        }
        "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
            if args.len() != 1 {
                return Err(SqlError::InvalidValue(format!(
                    "{name} requires 1 argument"
                )));
            }
            if args[0].is_null() {
                Ok((vec!["key".into()], vec![]))
            } else {
                let j = value_to_serde_with_work(&args[0], &mut work)?;
                let obj = j.as_object().ok_or_else(|| {
                    SqlError::InvalidValue(format!("{name} requires JSON object"))
                })?;
                let mut rows = Vec::with_capacity(obj.len());
                for k in obj.keys() {
                    work.tick()?;
                    rows.push(vec![Value::Text(k.clone().into())]);
                }
                Ok((vec!["key".into()], rows))
            }
        }
        _ => Err(SqlError::Unsupported(format!(
            "set-returning function: {name}"
        ))),
    };
    if result.is_ok() {
        work.finish()?;
    }
    result
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
    fn_typeof_with_cancel(v, None)
}

pub(crate) fn fn_typeof_with_cancel(v: &Value, cancel: Option<&CancelToken>) -> Result<Value> {
    run_json_work(cancel, |work| {
        if let Value::Jsonb(b) = v {
            work.checkpoint()?;
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
        let j = value_to_serde_with_work(v, work)?;
        let s = match j {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        Ok(Value::Text(s.into()))
    })
}

pub fn fn_array_length(v: &Value) -> Result<Value> {
    fn_array_length_with_cancel(v, None)
}

pub(crate) fn fn_array_length_with_cancel(
    v: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if let Value::Jsonb(b) = v {
            return match array_len_bytes_with_work(b, work)? {
                Some(n) => Ok(Value::Integer(n as i64)),
                None => Err(SqlError::InvalidValue(
                    "jsonb_array_length called on non-array".into(),
                )),
            };
        }
        let j = value_to_serde_with_work(v, work)?;
        match j {
            serde_json::Value::Array(arr) => Ok(Value::Integer(arr.len() as i64)),
            _ => Err(SqlError::InvalidValue(
                "jsonb_array_length called on non-array".into(),
            )),
        }
    })
}

pub fn fn_object_length(v: &Value) -> Result<Value> {
    fn_object_length_with_cancel(v, None)
}

pub(crate) fn fn_object_length_with_cancel(
    v: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if let Value::Jsonb(b) = v {
            return match object_len_bytes_with_work(b, work)? {
                Some(n) => Ok(Value::Integer(n as i64)),
                None => Err(SqlError::InvalidValue(
                    "jsonb_object_length called on non-object".into(),
                )),
            };
        }
        let j = value_to_serde_with_work(v, work)?;
        match j {
            serde_json::Value::Object(m) => Ok(Value::Integer(m.len() as i64)),
            _ => Err(SqlError::InvalidValue(
                "jsonb_object_length called on non-object".into(),
            )),
        }
    })
}

pub fn fn_extract_path(
    args: &[Value],
    target: crate::types::DataType,
    as_text: bool,
) -> Result<Value> {
    fn_extract_path_with_cancel(args, target, as_text, None)
}

pub(crate) fn fn_extract_path_with_cancel(
    args: &[Value],
    target: crate::types::DataType,
    as_text: bool,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let mut j = value_to_serde_with_work(&args[0], work)?;
        for key_val in &args[1..] {
            work.tick()?;
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
                other => {
                    let text = json_to_string_with_work(&other, false, work)?;
                    Ok(Value::Text(text.into()))
                }
            }
        } else {
            serde_to_value_with_work(j, target, work)
        }
    })
}

pub fn fn_sqlite_extract(j_val: &Value, path: &Value) -> Result<Value> {
    fn_sqlite_extract_with_cancel(j_val, path, None)
}

pub(crate) fn fn_sqlite_extract_with_cancel(
    j_val: &Value,
    path: &Value,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let path_str = match path {
            Value::Text(s) => s.to_string(),
            _ => {
                return Err(SqlError::TypeMismatch {
                    expected: "TEXT path".into(),
                    got: path.data_type().to_string(),
                })
            }
        };
        let j = value_to_serde_with_work(j_val, work)?;
        let segments = parse_dollar_path_with_work(&path_str, work)?;
        match navigate_path_ref_with_work(&j, &segments, work)? {
            Some(serde_json::Value::Null) => Ok(Value::Null),
            Some(serde_json::Value::String(s)) => {
                Ok(Value::Text(clone_string_with_work(s, work)?.into()))
            }
            Some(other) => {
                let text = json_to_string_with_work(other, false, work)?;
                Ok(Value::Text(text.into()))
            }
            None => Ok(Value::Null),
        }
    })
}

pub fn fn_valid(v: &Value) -> Result<Value> {
    fn_valid_with_cancel(v, None)
}

pub(crate) fn fn_valid_with_cancel(v: &Value, cancel: Option<&CancelToken>) -> Result<Value> {
    run_json_work(cancel, |work| {
        let text = match v {
            Value::Text(text) | Value::Json(text) => text.as_str(),
            _ => return Ok(Value::Boolean(false)),
        };
        match parse_json_text_with_work(text, work) {
            Ok(_) => Ok(Value::Boolean(true)),
            Err(SqlError::InvalidValue(_)) => Ok(Value::Boolean(false)),
            Err(error) => Err(error),
        }
    })
}

pub fn fn_strip_nulls(v: &Value, target: crate::types::DataType) -> Result<Value> {
    fn_strip_nulls_with_cancel(v, target, None)
}

pub(crate) fn fn_strip_nulls_with_cancel(
    v: &Value,
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let mut j = value_to_serde_with_work(v, work)?;
        strip_nulls_inplace(&mut j, work)?;
        serde_to_value_with_work(j, target, work)
    })
}

fn strip_nulls_inplace(j: &mut serde_json::Value, work: &mut JsonWork<'_>) -> Result<()> {
    work.tick()?;
    match j {
        serde_json::Value::Object(m) => {
            let entries = std::mem::take(m);
            for (key, mut value) in entries {
                work.tick()?;
                if matches!(value, serde_json::Value::Null) {
                    continue;
                }
                strip_nulls_inplace(&mut value, work)?;
                m.insert(key, value);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                strip_nulls_inplace(v, work)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn fn_pretty(v: &Value) -> Result<Value> {
    fn_pretty_with_cancel(v, None)
}

pub(crate) fn fn_pretty_with_cancel(v: &Value, cancel: Option<&CancelToken>) -> Result<Value> {
    run_json_work(cancel, |work| {
        let j = value_to_serde_with_work(v, work)?;
        let text = json_to_string_with_work(&j, true, work)?;
        Ok(Value::Text(text.into()))
    })
}

pub fn fn_build_object(args: &[Value], target: crate::types::DataType) -> Result<Value> {
    fn_build_object_with_cancel(args, target, None)
}

pub(crate) fn fn_build_object_with_cancel(
    args: &[Value],
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        if !args.len().is_multiple_of(2) {
            return Err(SqlError::InvalidValue(
                "jsonb_build_object requires an even number of arguments".into(),
            ));
        }
        let mut map = serde_json::Map::new();
        for pair in args.chunks(2) {
            work.tick()?;
            let key = match &pair[0] {
                Value::Null => continue,
                Value::Text(s) => s.to_string(),
                other => other.to_string(),
            };
            let value = value_to_serde_lossy_with_work(&pair[1], work)?;
            map.insert(key, value);
        }
        serde_to_value_with_work(serde_json::Value::Object(map), target, work)
    })
}

pub fn fn_build_array(args: &[Value], target: crate::types::DataType) -> Result<Value> {
    fn_build_array_with_cancel(args, target, None)
}

pub(crate) fn fn_build_array_with_cancel(
    args: &[Value],
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let mut items = Vec::with_capacity(args.len());
        for value in args {
            items.push(value_to_serde_lossy_with_work(value, work)?);
        }
        serde_to_value_with_work(serde_json::Value::Array(items), target, work)
    })
}

pub fn fn_set(
    j: &Value,
    path: &Value,
    new_value: &Value,
    create_missing: bool,
    target: crate::types::DataType,
) -> Result<Value> {
    fn_set_with_cancel(j, path, new_value, create_missing, target, None)
}

pub(crate) fn fn_set_with_cancel(
    j: &Value,
    path: &Value,
    new_value: &Value,
    create_missing: bool,
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let mut root = value_to_serde_with_work(j, work)?;
        let segments = path_to_segments_with_work(path, work)?;
        let new_serde = value_to_serde_lossy_with_work(new_value, work)?;
        work.checkpoint()?;
        set_at_path(&mut root, &segments, new_serde, create_missing, false);
        work.checkpoint()?;
        serde_to_value_with_work(root, target, work)
    })
}

pub fn fn_insert(
    j: &Value,
    path: &Value,
    new_value: &Value,
    insert_after: bool,
    target: crate::types::DataType,
) -> Result<Value> {
    fn_insert_with_cancel(j, path, new_value, insert_after, target, None)
}

pub(crate) fn fn_insert_with_cancel(
    j: &Value,
    path: &Value,
    new_value: &Value,
    insert_after: bool,
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let mut root = value_to_serde_with_work(j, work)?;
        let segments = path_to_segments_with_work(path, work)?;
        let new_serde = value_to_serde_lossy_with_work(new_value, work)?;
        work.checkpoint()?;
        set_at_path(&mut root, &segments, new_serde, true, insert_after);
        work.checkpoint()?;
        serde_to_value_with_work(root, target, work)
    })
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
    fn_to_json_with_cancel(v, target, None)
}

pub(crate) fn fn_to_json_with_cancel(
    v: &Value,
    target: crate::types::DataType,
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| {
        let j = value_to_serde_lossy_with_work(v, work)?;
        serde_to_value_with_work(j, target, work)
    })
}

pub fn fn_json_object(args: &[Value]) -> Result<Value> {
    fn_json_object_with_cancel(args, None)
}

pub(crate) fn fn_json_object_with_cancel(
    args: &[Value],
    cancel: Option<&CancelToken>,
) -> Result<Value> {
    run_json_work(cancel, |work| match args.len() {
        1 => {
            let j = value_to_serde_with_work(&args[0], work)?;
            let arr = j
                .as_array()
                .ok_or_else(|| SqlError::InvalidValue("json_object expects text array".into()))?;
            let mut map = serde_json::Map::new();
            let mut i = 0;
            while i + 1 < arr.len() {
                work.tick()?;
                let key = arr[i]
                    .as_str()
                    .ok_or_else(|| SqlError::InvalidValue("json_object key must be string".into()))?
                    .to_string();
                let val = arr[i + 1].clone();
                map.insert(key, val);
                i += 2;
            }
            serde_to_value_with_work(
                serde_json::Value::Object(map),
                crate::types::DataType::Json,
                work,
            )
        }
        2 => {
            let keys = text_array_with_work(&args[0], work)?;
            let vals = text_array_with_work(&args[1], work)?;
            if keys.len() != vals.len() {
                return Err(SqlError::InvalidValue(
                    "json_object: keys and values must be same length".into(),
                ));
            }
            let mut map = serde_json::Map::new();
            for (k, v) in keys.into_iter().zip(vals) {
                work.tick()?;
                map.insert(k, serde_json::Value::String(v));
            }
            serde_to_value_with_work(
                serde_json::Value::Object(map),
                crate::types::DataType::Json,
                work,
            )
        }
        _ => Err(SqlError::InvalidValue(
            "json_object requires 1 or 2 arguments".into(),
        )),
    })
}

fn value_to_serde_lossy(v: &Value) -> Result<serde_json::Value> {
    run_json_work(None, |work| value_to_serde_lossy_with_work(v, work))
}

fn value_to_serde_lossy_with_work(v: &Value, work: &mut JsonWork<'_>) -> Result<serde_json::Value> {
    work.tick()?;
    match v {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Real(r) => serde_json::Number::from_f64(*r)
            .map(serde_json::Value::Number)
            .ok_or_else(|| SqlError::InvalidValue("non-finite number".into())),
        Value::Text(s) => Ok(serde_json::Value::String(clone_string_with_work(s, work)?)),
        Value::Json(_) | Value::Jsonb(_) => value_to_serde_with_work(v, work),
        Value::Blob(b) => {
            work.checkpoint()?;
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            work.checkpoint()?;
            Ok(serde_json::Value::String(hex))
        }
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval { .. } => {
            Ok(serde_json::Value::String(format!("{v}")))
        }
        Value::TsVector(_) | Value::TsQuery(_) => Ok(serde_json::Value::String(format!("{v}"))),
        Value::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for elem in a.iter() {
                out.push(value_to_serde_lossy_with_work(elem, work)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        Value::Vector(v) => {
            let mut out = Vec::with_capacity(v.len());
            for &x in v.iter() {
                work.tick()?;
                out.push(
                    serde_json::Number::from_f64(x as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
            }
            Ok(serde_json::Value::Array(out))
        }
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;
