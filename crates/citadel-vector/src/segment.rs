//! The ANNSEG body format: a storage-agnostic byte encoding of everything a
//! built [`AnnIndex`] holds EXCEPT the f32 vectors. The vectors are rehydrated
//! at load time from the table rows themselves - the rows are the source of
//! truth, and the rehydration scan doubles as the staleness proof (it computes
//! the content fingerprint the storage layer compares against its header).
//!
//! Layout: a fixed sequence of REQUIRED sections, each
//! `[tag u8][len u64 LE][payload][blake3(payload) 32B]`. Per-section hashes
//! refuse corruption at the section that broke; the storage layer additionally
//! hashes the whole body. All integers little-endian. Any layout change bumps
//! the storage header's `format_version` - this module never reads old
//! formats silently.
//!
//! `PointStore.vectors` order is PRISM-INTERNAL (cell-reordered): loaders must
//! place each scanned row's vector at `inverse(id_map)[row_id]`, never in scan
//! order - a scan-order fill silently corrupts every f32 rerank.

use rustc_hash::FxHashMap;

use crate::ann::AnnIndex;
use crate::prism::{
    BinaryStore, Cell, Graph, Metric, PartitionTree, PointStore, PrismConfig, PrismIndex, SQ8Store,
};

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("segment truncated in {0}")]
    Truncated(&'static str),
    #[error("segment section tag mismatch: expected {expected}, got {got}")]
    BadTag { expected: u8, got: u8 },
    #[error("segment section {0} failed its BLAKE3 check (corrupt)")]
    SectionHash(&'static str),
    #[error("segment metric tag {0} unknown")]
    BadMetric(u8),
    #[error("rehydrated vectors length {got} != n*dim {expected}")]
    VectorLen { expected: usize, got: usize },
    #[error("rehydration filled {got} of {expected} vector slots")]
    RehydrationIncomplete { expected: usize, got: usize },
    #[error("segment internal inconsistency: {0}")]
    Inconsistent(&'static str),
}

const TAG_GRAPH: u8 = 1;
const TAG_LOCAL_GRAPH: u8 = 2;
const TAG_SQ8: u8 = 3;
const TAG_BINARY: u8 = 4;
const TAG_TREE: u8 = 5;
const TAG_IDS: u8 = 6;
const TAG_ATTRS: u8 = 7;

/// BLAKE3 of the canonical little-endian encoding of EVERY [`PrismConfig`]
/// field, domain-separated. The storage header pins this; a binary whose
/// active config differs must refuse the segment (the graph was built for a
/// different search geometry).
pub fn prism_config_hash(cfg: &PrismConfig) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"citadel-annseg-config-v1");
    for v in [
        cfg.m_local as u64,
        cfg.m_greedy as u64,
        cfg.m_random as u64,
        cfg.t as u64,
        cfg.beam_width as u64,
        cfg.binary_rerank as u64,
    ] {
        h.update(&v.to_le_bytes());
    }
    for v in [
        cfg.alpha,
        cfg.vamana_alpha,
        cfg.sigma_high,
        cfg.sigma_low,
        cfg.beta,
        cfg.epsilon,
    ] {
        h.update(&v.to_le_bytes());
    }
    h.update(&[metric_tag(cfg.metric)]);
    *h.finalize().as_bytes()
}

pub fn metric_tag(m: Metric) -> u8 {
    match m {
        Metric::L2 => 0,
        Metric::InnerProduct => 1,
        Metric::Cosine => 2,
    }
}

fn metric_from_tag(t: u8) -> Result<Metric, SegmentError> {
    Ok(match t {
        0 => Metric::L2,
        1 => Metric::InnerProduct,
        2 => Metric::Cosine,
        other => return Err(SegmentError::BadMetric(other)),
    })
}

/// Encode everything but the vectors. The output is the segment BODY; the
/// storage layer wraps it in its header (fingerprint, config hash, counts).
pub fn encode(index: &AnnIndex) -> Vec<u8> {
    let p = index.prism();
    let mut out = Vec::new();

    section(&mut out, TAG_GRAPH, |b| encode_graph(b, &p.graph));
    section(&mut out, TAG_LOCAL_GRAPH, |b| {
        encode_graph(b, &p.local_graph)
    });
    section(&mut out, TAG_SQ8, |b| {
        push_u64(b, p.sq8.dim() as u64);
        push_slice_u8(b, p.sq8.codes());
        push_slice_f32(b, p.sq8.mins());
        push_slice_f32(b, p.sq8.scales());
    });
    section(&mut out, TAG_BINARY, |b| {
        push_u64(b, p.binary.code_words() as u64);
        push_u64(b, p.binary.block_size() as u64);
        push_slice_u64(b, p.binary.codes());
        push_slice_f32(b, p.binary.signs());
    });
    section(&mut out, TAG_TREE, |b| {
        push_u64(b, p.tree.k as u64);
        push_u64(b, p.tree.split_order.len() as u64);
        for &s in &p.tree.split_order {
            push_u64(b, s as u64);
        }
        push_u64(b, p.tree.cells.len() as u64);
        for cell in &p.tree.cells {
            push_slice_u32(b, &cell.values);
            push_slice_u32(b, &cell.point_ids);
        }
    });
    section(&mut out, TAG_IDS, |b| {
        push_u64(b, index.snapshot_max);
        b.push(metric_tag(index.metric));
        b.extend_from_slice(&index.dim.to_le_bytes());
        push_u64(b, u64::from(p.global_medoid));
        push_slice_u32(b, &p.medoids);
        push_slice_u32(b, &p.point_cell);
        push_slice_u32(b, &p.original_ids);
        push_slice_u64(b, index.id_map());
    });
    section(&mut out, TAG_ATTRS, |b| {
        push_u64(b, p.store.attrs.len() as u64);
        push_u64(b, p.store.len as u64);
        for col in &p.store.attrs {
            push_slice_u32(b, col);
        }
    });
    out
}

/// Everything a segment carries; vectors arrive separately via
/// [`SegmentParts::into_index`].
pub struct SegmentParts {
    graph: Graph,
    local_graph: Graph,
    sq8: SQ8Store,
    binary: BinaryStore,
    tree: PartitionTree,
    snapshot_max: u64,
    metric: Metric,
    dim: u16,
    global_medoid: u32,
    medoids: Vec<u32>,
    point_cell: Vec<u32>,
    original_ids: Vec<u32>,
    id_map: Vec<u64>,
    attrs: Vec<Vec<u32>>,
    n: usize,
}

impl SegmentParts {
    pub fn n(&self) -> usize {
        self.n
    }

    pub fn dim(&self) -> u16 {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn id_map(&self) -> &[u64] {
        &self.id_map
    }

    /// `row_id -> PRISM-internal slot`: the PERMUTATION rehydration must use.
    pub fn internal_of_row(&self) -> FxHashMap<u64, u32> {
        self.id_map
            .iter()
            .enumerate()
            .map(|(internal, &row)| (row, internal as u32))
            .collect()
    }

    /// Finish the index with vectors ALREADY PLACED in PRISM-internal order
    /// (`filled` = how many slots the loader filled; must be exactly `n`).
    pub fn into_index(self, vectors: Vec<f32>, filled: usize) -> Result<AnnIndex, SegmentError> {
        if filled != self.n {
            return Err(SegmentError::RehydrationIncomplete {
                expected: self.n,
                got: filled,
            });
        }
        if vectors.len() != self.n * self.dim as usize {
            return Err(SegmentError::VectorLen {
                expected: self.n * self.dim as usize,
                got: vectors.len(),
            });
        }
        let store = PointStore::from_parts(vectors, self.dim as usize, self.attrs);
        let prism = PrismIndex {
            store,
            tree: self.tree,
            graph: self.graph,
            local_graph: self.local_graph,
            medoids: self.medoids,
            global_medoid: self.global_medoid,
            point_cell: self.point_cell,
            original_ids: self.original_ids,
            sq8: self.sq8,
            binary: self.binary,
            config: AnnIndex::active_config(self.metric),
        };
        Ok(AnnIndex::from_parts(
            prism,
            self.id_map,
            self.snapshot_max,
            self.metric,
            self.dim,
        ))
    }
}

/// Decode a segment body. Every section's BLAKE3 must verify; any mismatch is
/// a corruption refusal, never a partial result.
pub fn decode(bytes: &[u8]) -> Result<SegmentParts, SegmentError> {
    let mut r = Reader { buf: bytes, at: 0 };

    let g = r.section(TAG_GRAPH, "graph")?;
    let graph = decode_graph(&mut Reader { buf: g, at: 0 }, "graph")?;
    let lg = r.section(TAG_LOCAL_GRAPH, "local_graph")?;
    let local_graph = decode_graph(&mut Reader { buf: lg, at: 0 }, "local_graph")?;

    let s = r.section(TAG_SQ8, "sq8")?;
    let mut sr = Reader { buf: s, at: 0 };
    let sq8_dim = sr.u64("sq8")? as usize;
    let codes = sr.slice_u8("sq8")?.to_vec();
    let mins = sr.slice_f32("sq8")?;
    let scales = sr.slice_f32("sq8")?;
    let sq8 = SQ8Store::from_parts(codes, mins, scales, sq8_dim);

    let b = r.section(TAG_BINARY, "binary")?;
    let mut br = Reader { buf: b, at: 0 };
    let code_words = br.u64("binary")? as usize;
    let block_size = br.u64("binary")? as usize;
    let bcodes = br.slice_u64("binary")?;
    let signs = br.slice_f32("binary")?;
    let binary = BinaryStore::from_parts(bcodes, code_words, signs, block_size);

    let t = r.section(TAG_TREE, "tree")?;
    let mut tr = Reader { buf: t, at: 0 };
    let k = tr.u64("tree")? as usize;
    let so_len = tr.u64("tree")? as usize;
    let mut split_order = Vec::with_capacity(so_len);
    for _ in 0..so_len {
        split_order.push(tr.u64("tree")? as usize);
    }
    let cells_len = tr.u64("tree")? as usize;
    let mut cells = Vec::with_capacity(cells_len);
    for _ in 0..cells_len {
        let values = tr.slice_u32("tree")?;
        let point_ids = tr.slice_u32("tree")?;
        cells.push(Cell { values, point_ids });
    }
    let tree = PartitionTree {
        cells,
        split_order,
        k,
    };

    let i = r.section(TAG_IDS, "ids")?;
    let mut ir = Reader { buf: i, at: 0 };
    let snapshot_max = ir.u64("ids")?;
    let metric = metric_from_tag(ir.u8("ids")?)?;
    let dim = ir.u16("ids")?;
    let global_medoid = ir.u64("ids")? as u32;
    let medoids = ir.slice_u32("ids")?;
    let point_cell = ir.slice_u32("ids")?;
    let original_ids = ir.slice_u32("ids")?;
    let id_map = ir.slice_u64("ids")?;

    let a = r.section(TAG_ATTRS, "attrs")?;
    let mut ar = Reader { buf: a, at: 0 };
    let attr_k = ar.u64("attrs")? as usize;
    let n = ar.u64("attrs")? as usize;
    let mut attrs = Vec::with_capacity(attr_k);
    for _ in 0..attr_k {
        let col = ar.slice_u32("attrs")?;
        if col.len() != n {
            return Err(SegmentError::Inconsistent("attr column length != n"));
        }
        attrs.push(col);
    }

    if id_map.len() != n || original_ids.len() != n || point_cell.len() != n {
        return Err(SegmentError::Inconsistent("id arrays disagree on n"));
    }
    Ok(SegmentParts {
        graph,
        local_graph,
        sq8,
        binary,
        tree,
        snapshot_max,
        metric,
        dim,
        global_medoid,
        medoids,
        point_cell,
        original_ids,
        id_map,
        attrs,
        n,
    })
}

fn encode_graph(b: &mut Vec<u8>, g: &Graph) {
    push_u64(b, g.n as u64);
    push_slice_u32(b, &g.offsets);
    push_slice_u32(b, &g.neighbors);
}

fn decode_graph(r: &mut Reader<'_>, what: &'static str) -> Result<Graph, SegmentError> {
    let n = r.u64(what)? as usize;
    let offsets = r.slice_u32(what)?;
    let neighbors = r.slice_u32(what)?;
    if offsets.len() != n + 1 {
        return Err(SegmentError::Inconsistent("graph offsets length != n+1"));
    }
    Ok(Graph {
        offsets,
        neighbors,
        n,
    })
}

fn section(out: &mut Vec<u8>, tag: u8, fill: impl FnOnce(&mut Vec<u8>)) {
    let mut payload = Vec::new();
    fill(&mut payload);
    out.push(tag);
    push_u64(out, payload.len() as u64);
    let hash = blake3::hash(&payload);
    out.extend_from_slice(&payload);
    out.extend_from_slice(hash.as_bytes());
}

fn push_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn push_slice_u8(b: &mut Vec<u8>, s: &[u8]) {
    push_u64(b, s.len() as u64);
    b.extend_from_slice(s);
}

fn push_slice_u32(b: &mut Vec<u8>, s: &[u32]) {
    push_u64(b, s.len() as u64);
    for &v in s {
        b.extend_from_slice(&v.to_le_bytes());
    }
}

fn push_slice_u64(b: &mut Vec<u8>, s: &[u64]) {
    push_u64(b, s.len() as u64);
    for &v in s {
        b.extend_from_slice(&v.to_le_bytes());
    }
}

fn push_slice_f32(b: &mut Vec<u8>, s: &[f32]) {
    push_u64(b, s.len() as u64);
    for &v in s {
        b.extend_from_slice(&v.to_le_bytes());
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize, what: &'static str) -> Result<&'a [u8], SegmentError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or(SegmentError::Truncated(what))?;
        let s = &self.buf[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn u8(&mut self, what: &'static str) -> Result<u8, SegmentError> {
        Ok(self.take(1, what)?[0])
    }

    fn u16(&mut self, what: &'static str) -> Result<u16, SegmentError> {
        Ok(u16::from_le_bytes(self.take(2, what)?.try_into().unwrap()))
    }

    fn u64(&mut self, what: &'static str) -> Result<u64, SegmentError> {
        Ok(u64::from_le_bytes(self.take(8, what)?.try_into().unwrap()))
    }

    /// One framed section: tag + length + payload + verified BLAKE3.
    fn section(&mut self, tag: u8, what: &'static str) -> Result<&'a [u8], SegmentError> {
        let got = self.u8(what)?;
        if got != tag {
            return Err(SegmentError::BadTag { expected: tag, got });
        }
        let len = self.u64(what)? as usize;
        let payload = self.take(len, what)?;
        let hash: [u8; 32] = self.take(32, what)?.try_into().unwrap();
        if *blake3::hash(payload).as_bytes() != hash {
            return Err(SegmentError::SectionHash(what));
        }
        Ok(payload)
    }

    fn slice_u8(&mut self, what: &'static str) -> Result<&'a [u8], SegmentError> {
        let len = self.u64(what)? as usize;
        self.take(len, what)
    }

    fn slice_u32(&mut self, what: &'static str) -> Result<Vec<u32>, SegmentError> {
        let len = self.u64(what)? as usize;
        let raw = self.take(
            len.checked_mul(4).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        Ok(raw
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }

    fn slice_u64(&mut self, what: &'static str) -> Result<Vec<u64>, SegmentError> {
        let len = self.u64(what)? as usize;
        let raw = self.take(
            len.checked_mul(8).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        Ok(raw
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }

    fn slice_f32(&mut self, what: &'static str) -> Result<Vec<f32>, SegmentError> {
        let len = self.u64(what)? as usize;
        let raw = self.take(
            len.checked_mul(4).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        Ok(raw
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small index with two attribute cells, NON-monotonic row ids (so
    /// id_map order != insertion order), and deterministic vectors.
    fn build_fixture() -> AnnIndex {
        let mut rows: Vec<(u64, Vec<f32>, Vec<u32>)> = Vec::new();
        for i in 0..200u64 {
            // Reverse-ish ids: external order differs from internal.
            let id = 1000 - i * 3;
            let v: Vec<f32> = (0..8).map(|d| ((i * 7 + d) % 23) as f32 * 0.5).collect();
            rows.push((id, v, vec![(i % 2) as u32]));
        }
        AnnIndex::build_with_attrs(rows, 1, Metric::Cosine, 8).expect("build fixture")
    }

    /// Rehydrate exactly as the storage loader will: by the id_map PERMUTATION.
    fn rehydrate(index: &AnnIndex, parts: &SegmentParts) -> (Vec<f32>, usize) {
        let inv = parts.internal_of_row();
        let dim = parts.dim() as usize;
        let mut vectors = vec![0.0f32; parts.n() * dim];
        let mut filled = 0;
        // Source the vectors from the ORIGINAL index's store, keyed by row id,
        // simulating the table scan (arbitrary order: ascending row id).
        let p = index.prism();
        for internal in 0..parts.n() {
            let row = index.id_map()[internal];
            let slot = inv[&row] as usize;
            let src = &p.store.vectors[internal * dim..(internal + 1) * dim];
            vectors[slot * dim..(slot + 1) * dim].copy_from_slice(src);
            filled += 1;
        }
        (vectors, filled)
    }

    #[test]
    fn roundtrip_preserves_filtered_search_results_exactly() {
        // Attribute-filtered search exercises the persisted tree + attrs +
        // dicts machinery, not just the graph.
        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        let (vectors, filled) = rehydrate(&index, &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");
        let query: Vec<f32> = (0..8).map(|d| d as f32 * 0.7).collect();
        for code in [0u32, 1] {
            let filter = crate::prism::Filter::new(vec![(0, vec![code])]);
            let a = index.search_filtered(&query, 8, 64, &filter);
            let b = loaded.search_filtered(&query, 8, 64, &filter);
            assert_eq!(a, b, "filtered (attr0={code}) results identical");
            assert!(!a.is_empty(), "filter {code} matches half the fixture");
        }
    }

    #[test]
    fn roundtrip_holds_for_every_metric() {
        for metric in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
            let rows: Vec<(u64, Vec<f32>, Vec<u32>)> = (0..60u64)
                .map(|i| {
                    let v: Vec<f32> = (0..4).map(|d| ((i + d) % 13) as f32 - 6.0).collect();
                    (i * 2 + 1, v, vec![0])
                })
                .collect();
            let index = AnnIndex::build_with_attrs(rows, 1, metric, 4).expect("build");
            let parts = decode(&encode(&index)).expect("decode");
            assert_eq!(parts.metric(), metric, "metric tag survives");
            let (vectors, filled) = rehydrate(&index, &parts);
            let loaded = parts.into_index(vectors, filled).expect("into_index");
            let q = [1.0f32, -2.0, 3.0, 0.5];
            assert_eq!(index.search(&q, 5), loaded.search(&q, 5), "{metric:?}");
        }
    }

    #[test]
    fn single_row_index_roundtrips() {
        let index =
            AnnIndex::build_with_attrs(vec![(42, vec![1.0, 2.0], vec![0])], 1, Metric::L2, 2)
                .expect("build single");
        let parts = decode(&encode(&index)).expect("decode");
        assert_eq!(parts.n(), 1);
        let (vectors, filled) = rehydrate(&index, &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");
        assert_eq!(loaded.search(&[1.0, 2.0], 1), vec![(42, 0.0)]);
    }

    #[test]
    fn truncation_at_every_byte_boundary_is_refused() {
        // Cutting the segment ANYWHERE must produce an error, never a panic or
        // a silently partial decode.
        let index = AnnIndex::build_with_attrs(
            (0..12u64)
                .map(|i| (i, vec![i as f32, 1.0], vec![0]))
                .collect(),
            1,
            Metric::L2,
            2,
        )
        .expect("build");
        let bytes = encode(&index);
        for cut in 0..bytes.len() {
            assert!(
                decode(&bytes[..cut]).is_err(),
                "truncation at {cut}/{} must refuse",
                bytes.len()
            );
        }
    }

    #[test]
    fn internal_of_row_is_a_complete_bijection() {
        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        let map = parts.internal_of_row();
        assert_eq!(map.len(), parts.n(), "every row maps");
        let mut slots: Vec<u32> = map.values().copied().collect();
        slots.sort_unstable();
        let expected: Vec<u32> = (0..parts.n() as u32).collect();
        assert_eq!(slots, expected, "slots form a permutation of 0..n");
    }

    #[test]
    fn wrong_vector_length_is_refused() {
        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        let n = parts.n();
        let too_short = vec![0.0f32; (n - 1) * 8];
        assert!(matches!(
            parts.into_index(too_short, n),
            Err(SegmentError::VectorLen { .. })
        ));
    }

    #[test]
    fn roundtrip_preserves_search_results_exactly() {
        let index = build_fixture();
        let bytes = encode(&index);
        let parts = decode(&bytes).expect("decode");
        let (vectors, filled) = rehydrate(&index, &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");

        let query: Vec<f32> = (0..8).map(|d| d as f32 * 0.3).collect();
        let a = index.search(&query, 10);
        let b = loaded.search(&query, 10);
        assert_eq!(a, b, "loaded index must answer EXACTLY like the original");
        assert_eq!(index.snapshot_max, loaded.snapshot_max);
        assert_eq!(index.id_map(), loaded.id_map());
    }

    #[test]
    fn every_section_corruption_is_refused() {
        let index = build_fixture();
        let bytes = encode(&index);
        // Flip one byte inside each section's payload region and expect a
        // refusal each time (walk the framing to find payload offsets).
        let mut at = 0usize;
        let mut payload_spots = Vec::new();
        while at < bytes.len() {
            let len = u64::from_le_bytes(bytes[at + 1..at + 9].try_into().unwrap()) as usize;
            payload_spots.push(at + 9 + len / 2);
            at += 1 + 8 + len + 32;
        }
        assert_eq!(payload_spots.len(), 7, "all seven sections present");
        for spot in payload_spots {
            let mut corrupt = bytes.clone();
            corrupt[spot] ^= 0xFF;
            assert!(
                matches!(decode(&corrupt), Err(SegmentError::SectionHash(_))),
                "corruption at {spot} must be refused"
            );
        }
    }

    #[test]
    fn incomplete_rehydration_is_refused() {
        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        let dim = parts.dim() as usize;
        let n = parts.n();
        let vectors = vec![0.0f32; n * dim];
        assert!(matches!(
            parts.into_index(vectors, n - 1),
            Err(SegmentError::RehydrationIncomplete { .. })
        ));
    }

    #[test]
    fn config_hash_is_sensitive_to_every_field() {
        let base = AnnIndex::active_config(Metric::Cosine);
        let h0 = prism_config_hash(&base);
        let variants: Vec<PrismConfig> = vec![
            PrismConfig {
                m_local: base.m_local + 1,
                ..base.clone()
            },
            PrismConfig {
                m_greedy: base.m_greedy + 1,
                ..base.clone()
            },
            PrismConfig {
                m_random: base.m_random + 2,
                ..base.clone()
            },
            PrismConfig {
                t: base.t + 1,
                ..base.clone()
            },
            PrismConfig {
                alpha: base.alpha + 0.5,
                ..base.clone()
            },
            PrismConfig {
                vamana_alpha: base.vamana_alpha + 0.5,
                ..base.clone()
            },
            PrismConfig {
                beam_width: base.beam_width + 1,
                ..base.clone()
            },
            PrismConfig {
                metric: Metric::L2,
                ..base.clone()
            },
            PrismConfig {
                sigma_high: base.sigma_high + 0.25,
                ..base.clone()
            },
            PrismConfig {
                sigma_low: base.sigma_low + 0.25,
                ..base.clone()
            },
            PrismConfig {
                beta: base.beta + 0.5,
                ..base.clone()
            },
            PrismConfig {
                epsilon: base.epsilon + 0.5,
                ..base.clone()
            },
            PrismConfig {
                binary_rerank: base.binary_rerank + 1,
                ..base.clone()
            },
        ];
        for (i, v) in variants.iter().enumerate() {
            assert_ne!(
                prism_config_hash(v),
                h0,
                "config field {i} must perturb the hash"
            );
        }
    }
}
