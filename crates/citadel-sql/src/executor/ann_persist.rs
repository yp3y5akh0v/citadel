//! Persisted ANN segments: the storage envelope around
//! [`citadel_vector::segment`]. Each indexed table owns one hidden storage
//! tree `__annseg_{table}` (never registered in the schema manager, invisible
//! to SQL) holding a header row plus body chunks, encrypted like every tree.
//!
//! Three independent layers refuse a stale segment (any failure falls through
//! to a rebuild):
//! - transactional: every DML/DDL site that marks a table dirty drops its
//!   segment in the same write txn (shadow paging keeps "table changed but
//!   segment survived" unrepresentable for those paths);
//! - content fingerprint: BLAKE3 over the scan-order row content
//!   (domain-separated, length-framed) at persist time, recomputed by the
//!   load-time rehydration scan;
//! - header checks: format/config/shape pins compared before the scan.

use citadel_vector::segment;
use citadel_vector::PrismConfig;
use rustc_hash::FxHashMap;

use crate::error::{Result, SqlError};

/// Bump on ANY layout change of the header or the segment body.
pub const ANNSEG_FORMAT_VERSION: u16 = 2;

const MAGIC: &[u8; 7] = b"ANNSEG\0";

/// Body chunk size. Chunking bounds the peak memory of a single value
/// read/write; storage chains overflow pages above ~2 KB anyway, so smaller
/// chunks cost only a few hundred point-gets per attach while keeping buffers
/// modest.
pub const CHUNK_BYTES: usize = 1024 * 1024;

/// The hidden storage tree for a table's segment.
pub fn segment_table_name(table: &str) -> Vec<u8> {
    format!("__annseg_{table}").into_bytes()
}

/// Key 0 is the header; chunks are 1..=chunk_count (big-endian for scan order).
pub fn segment_key(chunk_no: u32) -> [u8; 4] {
    chunk_no.to_be_bytes()
}

/// Everything the loader must verify BEFORE paying for the rehydration scan,
/// plus the two content hashes it verifies during/after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format_version: u16,
    /// BLAKE3 of the canonical encoding of the builder's `PrismConfig`.
    pub prism_config_hash: [u8; 32],
    pub dim: u16,
    pub metric_tag: u8,
    /// Indexed (non-null) row count - compared as `n <= live rows` pre-scan
    /// (NULL vectors are unindexed), exactly via the fingerprint scan.
    pub n: u64,
    pub snapshot_max: u64,
    /// The table's catalog root at persist - a differing live root means stale (CoW gate).
    pub table_root: u64,
    /// The indexed column and the filter columns, IN ATTRIBUTE ORDER - an
    /// index re-created over different columns must be refused explicitly,
    /// never discovered via fingerprint luck.
    pub col_idx: u32,
    pub filter_cols: Vec<u32>,
    /// Per attribute dim: encoded filter value -> PRISM code, in scan order.
    pub dicts: Vec<Vec<(Vec<u8>, u32)>>,
    pub content_fingerprint: [u8; 32],
    /// BLAKE3 of the concatenated body chunks (the segment.rs payload).
    pub segment_b3: [u8; 32],
    pub chunk_count: u32,
    /// Forensics only - never compared.
    pub writer: String,
}

impl SegmentHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&self.format_version.to_le_bytes());
        b.extend_from_slice(&self.prism_config_hash);
        b.extend_from_slice(&self.dim.to_le_bytes());
        b.push(self.metric_tag);
        b.extend_from_slice(&self.n.to_le_bytes());
        b.extend_from_slice(&self.snapshot_max.to_le_bytes());
        b.extend_from_slice(&self.table_root.to_le_bytes());
        b.extend_from_slice(&self.col_idx.to_le_bytes());
        b.extend_from_slice(&(self.filter_cols.len() as u32).to_le_bytes());
        for &c in &self.filter_cols {
            b.extend_from_slice(&c.to_le_bytes());
        }
        b.extend_from_slice(&(self.dicts.len() as u32).to_le_bytes());
        for dict in &self.dicts {
            b.extend_from_slice(&(dict.len() as u64).to_le_bytes());
            for (k, v) in dict {
                b.extend_from_slice(&(k.len() as u64).to_le_bytes());
                b.extend_from_slice(k);
                b.extend_from_slice(&v.to_le_bytes());
            }
        }
        b.extend_from_slice(&self.content_fingerprint);
        b.extend_from_slice(&self.segment_b3);
        b.extend_from_slice(&self.chunk_count.to_le_bytes());
        b.extend_from_slice(&(self.writer.len() as u32).to_le_bytes());
        b.extend_from_slice(self.writer.as_bytes());
        // Self-hash binds header fields beyond page-level HMAC (cheap
        // hardening: a header is never accepted with internal bit-rot).
        let self_hash = blake3::hash(&b);
        b.extend_from_slice(self_hash.as_bytes());
        b
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let fail = |what: &str| SqlError::InvalidValue(format!("ANN segment header: {what}"));
        if bytes.len() < 32 {
            return Err(fail("truncated"));
        }
        let (body, hash) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(body).as_bytes() != hash {
            return Err(fail("self-hash mismatch (corrupt)"));
        }
        let mut at = 0usize;
        let mut take = |n: usize| -> Result<&[u8]> {
            let end = at.checked_add(n).filter(|&e| e <= body.len());
            let end = end.ok_or_else(|| fail("truncated"))?;
            let s = &body[at..end];
            at = end;
            Ok(s)
        };
        if take(7)? != MAGIC {
            return Err(fail("bad magic"));
        }
        let format_version = u16::from_le_bytes(take(2)?.try_into().unwrap());
        let prism_config_hash: [u8; 32] = take(32)?.try_into().unwrap();
        let dim = u16::from_le_bytes(take(2)?.try_into().unwrap());
        let metric_tag = take(1)?[0];
        let n = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let snapshot_max = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let table_root = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let col_idx = u32::from_le_bytes(take(4)?.try_into().unwrap());
        let fc_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut filter_cols = Vec::with_capacity(fc_len);
        for _ in 0..fc_len {
            filter_cols.push(u32::from_le_bytes(take(4)?.try_into().unwrap()));
        }
        let dicts_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut dicts = Vec::with_capacity(dicts_len);
        for _ in 0..dicts_len {
            let entries = u64::from_le_bytes(take(8)?.try_into().unwrap()) as usize;
            let mut dict = Vec::with_capacity(entries);
            for _ in 0..entries {
                let klen = u64::from_le_bytes(take(8)?.try_into().unwrap()) as usize;
                let k = take(klen)?.to_vec();
                let v = u32::from_le_bytes(take(4)?.try_into().unwrap());
                dict.push((k, v));
            }
            dicts.push(dict);
        }
        let content_fingerprint: [u8; 32] = take(32)?.try_into().unwrap();
        let segment_b3: [u8; 32] = take(32)?.try_into().unwrap();
        let chunk_count = u32::from_le_bytes(take(4)?.try_into().unwrap());
        let wlen = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let writer = String::from_utf8_lossy(take(wlen)?).into_owned();
        if at != body.len() {
            return Err(fail("trailing bytes"));
        }
        Ok(Self {
            format_version,
            prism_config_hash,
            dim,
            metric_tag,
            n,
            snapshot_max,
            table_root,
            col_idx,
            filter_cols,
            dicts,
            content_fingerprint,
            segment_b3,
            chunk_count,
            writer,
        })
    }

    /// The dicts as the runtime maps the filter pushdown uses.
    pub fn dict_maps(&self) -> Vec<FxHashMap<Vec<u8>, u32>> {
        self.dicts
            .iter()
            .map(|d| d.iter().cloned().collect())
            .collect()
    }
}

/// The INJECTIVE content fingerprint: domain-separated, every component
/// length-framed (unframed concatenation admits boundary-shift collisions),
/// bound to the table/column/filter identity, fed rows IN SCAN ORDER. Persist
/// and load MUST construct it identically - both go through this one type.
pub struct FingerprintHasher {
    h: blake3::Hasher,
}

impl FingerprintHasher {
    pub fn new(table: &str, col_idx: u32, filter_cols: &[u32], dim: u16, metric_tag: u8) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(b"citadel-annseg-fp-v1");
        h.update(&(table.len() as u64).to_le_bytes());
        h.update(table.as_bytes());
        h.update(&col_idx.to_le_bytes());
        h.update(&(filter_cols.len() as u32).to_le_bytes());
        for &c in filter_cols {
            h.update(&c.to_le_bytes());
        }
        h.update(&dim.to_le_bytes());
        h.update(&[metric_tag]);
        Self { h }
    }

    /// One scanned row: its key, the RAW encoded vector-column bytes (null =
    /// empty, still framed - unindexed rows are part of the content), and each
    /// filter column's encoded bytes.
    pub fn row(&mut self, key: &[u8], vector_raw: &[u8], filter_encoded: &[&[u8]]) {
        self.h.update(&(key.len() as u64).to_le_bytes());
        self.h.update(key);
        self.h.update(&(vector_raw.len() as u64).to_le_bytes());
        self.h.update(vector_raw);
        for f in filter_encoded {
            self.h.update(&(f.len() as u64).to_le_bytes());
            self.h.update(f);
        }
    }

    pub fn finish(self) -> [u8; 32] {
        *self.h.finalize().as_bytes()
    }
}

/// The active config's hash for `metric` - what persist writes and the loader
/// requires (a binary with a different geometry must rebuild, not load).
pub fn active_config_hash(metric: citadel_vector::Metric) -> [u8; 32] {
    let cfg: PrismConfig = citadel_vector::AnnIndex::active_config(metric);
    segment::prism_config_hash(&cfg)
}

/// What `persist_ann_index` returns for the caller's manifest: the hashes a
/// later attach verifies against, and the shape for the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnSegmentInfo {
    pub segment_b3: [u8; 32],
    pub content_fingerprint: [u8; 32],
    pub n: u64,
    pub dim: u16,
    pub metric_tag: u8,
    pub chunk_count: u32,
}

/// Drop a table's persisted segment INSIDE the caller's write txn (the
/// transactional staleness layer). Absent segment = nothing to do; savepoint
/// rollback restores a dropped one automatically.
pub(crate) fn purge_segment(
    wtx: &mut citadel_txn::write_txn::WriteTxn<'_>,
    table_name: &str,
) -> Result<()> {
    match wtx.drop_table(&segment_table_name(table_name)) {
        Ok(()) => Ok(()),
        Err(citadel_core::Error::TableNotFound(_)) => Ok(()),
        Err(e) => Err(SqlError::Storage(e)),
    }
}

/// Split a segment body into storage chunks (chunk 0 is the header's key).
pub fn chunks(body: &[u8]) -> impl Iterator<Item = (u32, &[u8])> {
    body.chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(i, c)| ((i + 1) as u32, c))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_fixture() -> SegmentHeader {
        SegmentHeader {
            format_version: ANNSEG_FORMAT_VERSION,
            prism_config_hash: [7; 32],
            dim: 768,
            metric_tag: 2,
            n: 311_592,
            snapshot_max: 99,
            table_root: 1234,
            col_idx: 3,
            filter_cols: vec![1, 2],
            dicts: vec![
                vec![(b"region".to_vec(), 0), (b"other".to_vec(), 1)],
                vec![(b"kind".to_vec(), 0)],
            ],
            content_fingerprint: [9; 32],
            segment_b3: [4; 32],
            chunk_count: 41,
            writer: "citadel-test".into(),
        }
    }

    #[test]
    fn header_roundtrips_exactly() {
        let h = header_fixture();
        assert_eq!(SegmentHeader::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn header_corruption_is_refused() {
        let bytes = header_fixture().encode();
        for spot in [0, 9, 45, bytes.len() / 2, bytes.len() - 40] {
            let mut corrupt = bytes.clone();
            corrupt[spot] ^= 0xFF;
            assert!(
                SegmentHeader::decode(&corrupt).is_err(),
                "corruption at {spot} must refuse"
            );
        }
    }

    #[test]
    fn fingerprint_is_framed_against_boundary_shifts() {
        // Same concatenated bytes, different row framing -> different hashes.
        let mut a = FingerprintHasher::new("t", 0, &[], 4, 2);
        a.row(b"ab", b"cd", &[]);
        let mut b = FingerprintHasher::new("t", 0, &[], 4, 2);
        b.row(b"abc", b"d", &[]);
        assert_ne!(a.finish(), b.finish());

        // Identity changes perturb it too.
        let mut c = FingerprintHasher::new("t", 1, &[], 4, 2);
        c.row(b"ab", b"cd", &[]);
        let mut d = FingerprintHasher::new("t", 0, &[2], 4, 2);
        d.row(b"ab", b"cd", &[]);
        let mut base = FingerprintHasher::new("t", 0, &[], 4, 2);
        base.row(b"ab", b"cd", &[]);
        let base = base.finish();
        assert_ne!(c.finish(), base);
        assert_ne!(d.finish(), base);
    }

    #[test]
    fn chunking_covers_the_body_in_order() {
        let body = vec![0xABu8; CHUNK_BYTES + 17];
        let parts: Vec<(u32, usize)> = chunks(&body).map(|(no, c)| (no, c.len())).collect();
        assert_eq!(parts, vec![(1, CHUNK_BYTES), (2, 17)]);
    }
}
