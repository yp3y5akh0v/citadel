//! Persisted ANN segments: the storage envelope around
//! [`citadel_vector::segment`]. Each indexed table owns one hidden storage
//! tree `__annseg_{table}` (never registered in the schema manager, invisible
//! to SQL) holding a header row plus body chunks, encrypted like every tree.
//!
//! Three independent layers refuse a stale segment, each falling through to a
//! rebuild: a transactional drop in the same write txn that dirties the table,
//! a non-ABA root stamp (root page id plus that page's txn id, so allocator
//! reuse cannot look current), and header format/config/shape pins.

use citadel_vector::segment;
use citadel_vector::PrismConfig;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::error::{Result, SqlError};

/// Bump on ANY layout change of the header or the segment body.
pub const ANNSEG_FORMAT_VERSION: u16 = 4;

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

/// Everything the loader verifies before decoding the segment body, plus the
/// artifact hashes retained for integrity and forensics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format_version: u16,
    /// BLAKE3 of the canonical encoding of the builder's `PrismConfig`.
    pub prism_config_hash: [u8; 32],
    pub dim: u16,
    pub metric_tag: u8,
    /// Indexed (non-null) row count recorded by the builder.
    pub n: u64,
    pub snapshot_max: u64,
    /// The table's catalog root at persist.
    pub table_root: u64,
    /// Transaction id stored in `table_root`. Together the two fields form a
    /// non-ABA CoW stamp even when the allocator recycles a physical page id.
    pub table_root_txn: u64,
    /// The indexed column and the filter columns, IN ATTRIBUTE ORDER - an
    /// index re-created over different columns must be refused explicitly,
    /// never discovered via fingerprint luck.
    pub col_idx: u32,
    pub filter_cols: Vec<u32>,
    /// Per attribute dim: encoded filter value -> PRISM code, in scan order.
    pub dicts: Vec<Vec<(Vec<u8>, u32)>>,
    /// Persist-time source identity for manifests and forensics. Fast load does
    /// not recompute this hash.
    pub content_fingerprint: [u8; 32],
    /// BLAKE3 of the concatenated body chunks (the segment.rs payload).
    pub segment_b3: [u8; 32],
    pub chunk_count: u32,
    /// Forensics only - never compared.
    pub writer: String,
}

fn header_error(what: &str) -> SqlError {
    SqlError::InvalidValue(format!("ANN segment header: {what}"))
}

struct HeaderReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> HeaderReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| header_error("truncated"))?;
        let value = &self.bytes[self.at..end];
        self.at = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("reader returned the requested array width"))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn finish(self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(header_error("trailing bytes"))
        }
    }
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
        b.extend_from_slice(&self.table_root_txn.to_le_bytes());
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
        if bytes.len() < 32 {
            return Err(header_error("truncated"));
        }
        let (body, hash) = bytes.split_at(bytes.len() - 32);
        if blake3::hash(body).as_bytes() != hash {
            return Err(header_error("self-hash mismatch (corrupt)"));
        }
        let mut reader = HeaderReader::new(body);
        if reader.take(7)? != MAGIC {
            return Err(header_error("bad magic"));
        }
        let format_version = reader.u16()?;
        let prism_config_hash = reader.array()?;
        let dim = reader.u16()?;
        let metric_tag = reader.u8()?;
        let n = reader.u64()?;
        let snapshot_max = reader.u64()?;
        let table_root = reader.u64()?;
        let table_root_txn = reader.u64()?;
        let col_idx = reader.u32()?;
        let fc_len = reader.u32()? as usize;
        if fc_len > reader.remaining() / 4 {
            return Err(header_error("filter column count exceeds the header"));
        }
        let mut filter_cols = Vec::new();
        filter_cols
            .try_reserve_exact(fc_len)
            .map_err(|_| header_error("filter column count is too large"))?;
        for _ in 0..fc_len {
            filter_cols.push(reader.u32()?);
        }
        let dicts_len = reader.u32()? as usize;
        if dicts_len != fc_len {
            return Err(header_error(
                "dictionary count does not match filter columns",
            ));
        }
        if dicts_len > reader.remaining() / 8 {
            return Err(header_error("dictionary count exceeds the header"));
        }
        let mut dicts = Vec::new();
        dicts
            .try_reserve_exact(dicts_len)
            .map_err(|_| header_error("dictionary count is too large"))?;
        for _ in 0..dicts_len {
            let entries = usize::try_from(reader.u64()?)
                .map_err(|_| header_error("dictionary entry count is not addressable"))?;
            // Every entry needs at least its key length and code. Bound the
            // allocation by authenticated bytes before reserving from it.
            if entries > reader.remaining() / 12 {
                return Err(header_error("dictionary entry count exceeds the header"));
            }
            let mut dict = Vec::new();
            dict.try_reserve_exact(entries)
                .map_err(|_| header_error("dictionary entry count is too large"))?;
            for expected_code in 0..entries {
                let klen = usize::try_from(reader.u64()?)
                    .map_err(|_| header_error("dictionary key length is not addressable"))?;
                if klen > reader.remaining().saturating_sub(4) {
                    return Err(header_error("truncated"));
                }
                let k = reader.take(klen)?.to_vec();
                let v = reader.u32()?;
                let expected_code = u32::try_from(expected_code)
                    .map_err(|_| header_error("dictionary has too many codes"))?;
                if v != expected_code {
                    return Err(header_error("dictionary codes are not canonical"));
                }
                dict.push((k, v));
            }
            let mut unique_keys = FxHashSet::default();
            unique_keys
                .try_reserve(dict.len())
                .map_err(|_| header_error("dictionary key set is too large"))?;
            if dict
                .iter()
                .any(|(key, _)| !unique_keys.insert(key.as_slice()))
            {
                return Err(header_error("dictionary contains duplicate keys"));
            }
            dicts.push(dict);
        }
        let content_fingerprint = reader.array()?;
        let segment_b3 = reader.array()?;
        let chunk_count = reader.u32()?;
        let wlen = reader.u32()? as usize;
        let writer = String::from_utf8_lossy(reader.take(wlen)?).into_owned();
        reader.finish()?;
        Ok(Self {
            format_version,
            prism_config_hash,
            dim,
            metric_tag,
            n,
            snapshot_max,
            table_root,
            table_root_txn,
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

/// The content fingerprint is domain-separated, every component is
/// length-framed (unframed concatenation admits boundary-shift collisions),
/// and it is bound to table/column/filter identity in scan order. It identifies
/// the persisted source artifact for diagnostics; fast load does not recompute
/// it because that would require an O(N) source-table scan.
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

/// What `persist_ann_index` returns for the caller's manifest: the verified
/// segment-body hash, forensic source fingerprint, and shape metadata.
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
            table_root_txn: 5678,
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

    fn rehash_header(mut bytes: Vec<u8>, edit: impl FnOnce(&mut [u8])) -> Vec<u8> {
        let body_len = bytes.len() - 32;
        edit(&mut bytes[..body_len]);
        let hash = blake3::hash(&bytes[..body_len]);
        bytes[body_len..].copy_from_slice(hash.as_bytes());
        bytes
    }

    #[test]
    fn header_magic_bytes_are_frozen() {
        assert_eq!(&header_fixture().encode()[..MAGIC.len()], b"ANNSEG\0");
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
    fn authenticated_oversized_counts_are_refused_before_allocation() {
        const FILTER_COUNT_AT: usize = 80;
        const DICT_COUNT_AT: usize = FILTER_COUNT_AT + 4 + 2 * 4;
        const FIRST_DICT_ENTRIES_AT: usize = DICT_COUNT_AT + 4;
        const FIRST_KEY_LEN_AT: usize = FIRST_DICT_ENTRIES_AT + 8;

        let encoded = header_fixture().encode();
        for (at, bytes) in [
            (FILTER_COUNT_AT, u64::from(u32::MAX).to_le_bytes()),
            (DICT_COUNT_AT, u64::from(u32::MAX).to_le_bytes()),
            (FIRST_DICT_ENTRIES_AT, u64::MAX.to_le_bytes()),
            (FIRST_KEY_LEN_AT, u64::MAX.to_le_bytes()),
        ] {
            let corrupt = rehash_header(encoded.clone(), |body| {
                let width = if at <= DICT_COUNT_AT { 4 } else { 8 };
                body[at..at + width].copy_from_slice(&bytes[..width]);
            });
            assert!(SegmentHeader::decode(&corrupt).is_err(), "count at {at}");
        }
    }

    #[test]
    fn malformed_filter_dictionaries_are_refused() {
        let mut mismatched = header_fixture();
        mismatched.dicts.pop();
        assert!(SegmentHeader::decode(&mismatched.encode()).is_err());

        let mut noncanonical = header_fixture();
        noncanonical.dicts[0][1].1 = 2;
        assert!(SegmentHeader::decode(&noncanonical.encode()).is_err());

        let mut duplicate = header_fixture();
        duplicate.dicts[0][1].0 = duplicate.dicts[0][0].0.clone();
        assert!(SegmentHeader::decode(&duplicate.encode()).is_err());
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
