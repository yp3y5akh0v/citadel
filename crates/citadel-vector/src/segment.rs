//! The ANNSEG body format: a storage-agnostic byte encoding of everything a
//! built [`AnnIndex`] holds, including build-form f32 vectors. Storage loaders
//! may instead rehydrate vectors from table rows; that scan doubles as the
//! staleness proof.
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

use citadel_core::CancelToken;
use rustc_hash::FxHashMap;
use zeroize::Zeroizing;

use crate::ann::AnnIndex;
use crate::prism::{
    BinaryStore, Cell, Graph, Metric, PartitionTree, PointStore, PrismConfig, PrismError,
    PrismIndex, SQ8Store,
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
    #[error("segment count in {0} exceeds the available payload")]
    CountOutOfBounds(&'static str),
    #[error("segment value in {0} exceeds its supported range")]
    ValueOutOfBounds(&'static str),
    #[error("segment allocation failed in {0}")]
    Allocation(&'static str),
    #[error("segment has trailing data in {0}")]
    TrailingData(&'static str),
    #[error("segment contains duplicate external row id {0}")]
    DuplicateRowId(u64),
    #[error("segment snapshot max {got} does not match maximum row id {expected}")]
    SnapshotMax { expected: u64, got: u64 },
    #[error("PRISM rejected the decoded segment: {0}")]
    Prism(#[from] PrismError),
}

/// Failure from a cancellable segment operation.
///
/// Interruption is separate from malformed segment data so a
/// storage loader never mistakes a cancelled read for corruption.
#[derive(Debug, thiserror::Error)]
pub enum SegmentOperationError {
    #[error("segment operation interrupted")]
    Interrupted,
    #[error("segment allocation failed in {0}")]
    Allocation(&'static str),
    #[error(transparent)]
    Segment(#[from] SegmentError),
}

const CANCEL_CHUNK_BYTES: usize = 64 * 1024;
const CANCEL_CHUNK_U32_ITEMS: usize = CANCEL_CHUNK_BYTES / size_of::<u32>();
const CANCEL_CHUNK_U64_ITEMS: usize = CANCEL_CHUNK_BYTES / size_of::<u64>();
const CANCEL_CHUNK_COLLECTIONS: usize = 1024;
const SECTION_FRAME_BYTES: usize = 1 + 8 + 32;

struct CancelContext<'a> {
    token: Option<&'a CancelToken>,
    work: usize,
    #[cfg(test)]
    test_hook: Option<&'a mut dyn FnMut(usize)>,
}

impl<'a> CancelContext<'a> {
    fn new(token: Option<&'a CancelToken>) -> Self {
        Self {
            token,
            work: 0,
            #[cfg(test)]
            test_hook: None,
        }
    }

    #[cfg(test)]
    fn with_hook(token: Option<&'a CancelToken>, test_hook: &'a mut dyn FnMut(usize)) -> Self {
        Self {
            token,
            work: 0,
            test_hook: Some(test_hook),
        }
    }

    fn checkpoint(&mut self) -> Result<(), SegmentOperationError> {
        #[cfg(test)]
        if let Some(hook) = self.test_hook.as_deref_mut() {
            hook(self.work);
        }
        if self.token.is_some_and(CancelToken::is_cancelled) {
            return Err(SegmentOperationError::Interrupted);
        }
        Ok(())
    }

    fn advance(&mut self, amount: usize) {
        self.work = self.work.saturating_add(amount);
    }
}

const TAG_GRAPH: u8 = 1;
const TAG_SQ8: u8 = 3;
const TAG_BINARY: u8 = 4;
const TAG_TREE: u8 = 5;
const TAG_IDS: u8 = 6;
const TAG_ATTRS: u8 = 7;
const TAG_VECTORS: u8 = 8;

/// BLAKE3 of the canonical little-endian encoding of EVERY [`PrismConfig`]
/// field, domain-separated. The storage header pins this; a binary whose
/// active config differs must refuse the segment (the graph was built for a
/// different search geometry). The domain string carries the search-geometry
/// version: bump it whenever build or search semantics change shape.
pub fn prism_config_hash(cfg: &PrismConfig) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"citadel-annseg-config-v4");
    for v in [
        cfg.m_local as u64,
        cfg.m_greedy as u64,
        cfg.m_random as u64,
        cfg.t as u64,
    ] {
        h.update(&v.to_le_bytes());
    }
    for v in [cfg.alpha, cfg.vamana_alpha] {
        h.update(&v.to_le_bytes());
    }
    for v in [
        cfg.beam_width as u64,
        cfg.cross_cell_exact_ranking_limit as u64,
    ] {
        h.update(&v.to_le_bytes());
    }
    h.update(&[metric_tag(cfg.metric)]);
    for v in [cfg.sigma_high, cfg.sigma_low, cfg.beta, cfg.epsilon] {
        h.update(&v.to_le_bytes());
    }
    for v in [
        cfg.binary_rerank as u64,
        cfg.scan_threshold as u64,
        cfg.multi_cell_scan_threshold as u64,
        cfg.graph_expansion as u64,
        cfg.build_seed,
    ] {
        h.update(&v.to_le_bytes());
    }
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

/// Encode the complete build-form index. The output is the segment BODY; the
/// storage layer wraps it in its header (fingerprint, config hash, counts).
pub fn encode(index: &AnnIndex) -> Vec<u8> {
    match encode_with_cancel(index, None) {
        Ok(mut encoded) => std::mem::take(encoded.as_mut()),
        Err(SegmentOperationError::Interrupted) => {
            unreachable!("segment encoding without a cancellation token was interrupted")
        }
        Err(SegmentOperationError::Allocation(where_)) => {
            panic!("segment encoding allocation failed in {where_}")
        }
        Err(SegmentOperationError::Segment(error)) => {
            panic!("segment encoding failed: {error}")
        }
    }
}

/// Encode the complete build-form index while observing `cancel` during large
/// copies, numeric-array serialization, and section hashing.
///
/// The returned plaintext owns a zeroizing buffer. Sections are written
/// directly into that buffer and their lengths are backpatched, avoiding the
/// second unzeroized payload buffer that per-section assembly would require.
pub fn encode_with_cancel(
    index: &AnnIndex,
    cancel: Option<&CancelToken>,
) -> Result<Zeroizing<Vec<u8>>, SegmentOperationError> {
    let mut context = CancelContext::new(cancel);
    encode_with_context(index, &mut context)
}

fn encode_with_context(
    index: &AnnIndex,
    context: &mut CancelContext<'_>,
) -> Result<Zeroizing<Vec<u8>>, SegmentOperationError> {
    context.checkpoint()?;
    let p = index.prism();
    let expected_len = encoded_len(index, context)?;
    let mut out = Zeroizing::new(Vec::new());
    out.try_reserve_exact(expected_len)
        .map_err(|_| SegmentOperationError::Allocation("encode output"))?;

    write_section(out.as_mut(), context, TAG_GRAPH, |b, context| {
        encode_graph(b, p.graph(), context)
    })?;
    write_section(out.as_mut(), context, TAG_SQ8, |b, context| {
        put_u64(b, p.sq8().dim() as u64, context);
        put_slice_u8(b, p.sq8().codes(), context)?;
        put_slice_f32(b, p.sq8().mins(), context)?;
        put_slice_f32(b, p.sq8().scales(), context)
    })?;
    write_section(out.as_mut(), context, TAG_BINARY, |b, context| {
        put_u64(b, p.binary().code_words() as u64, context);
        put_u64(b, p.binary().block_size() as u64, context);
        put_slice_u64(b, p.binary().codes(), context)?;
        put_slice_f32(b, p.binary().signs(), context)
    })?;
    write_section(out.as_mut(), context, TAG_TREE, |b, context| {
        put_u64(b, p.tree().num_attributes() as u64, context);
        put_u64(b, p.tree().split_order().len() as u64, context);
        for values in p.tree().split_order().chunks(CANCEL_CHUNK_U64_ITEMS) {
            context.checkpoint()?;
            for &value in values {
                put_u64(b, value as u64, context);
            }
        }
        put_u64(b, p.tree().cells().len() as u64, context);
        for cell in p.tree().cells() {
            context.checkpoint()?;
            put_slice_u32(b, cell.values(), context)?;
            put_slice_u32(b, cell.point_ids(), context)?;
        }
        Ok(())
    })?;
    write_section(out.as_mut(), context, TAG_IDS, |b, context| {
        put_u64(b, index.snapshot_max, context);
        put_u8(b, metric_tag(index.metric), context);
        put_bytes_small(b, &index.dim.to_le_bytes(), context);
        put_u64(b, u64::from(p.global_medoid()), context);
        put_slice_u32(b, p.medoids(), context)?;
        put_slice_u32(b, p.point_cell(), context)?;
        put_slice_u32(b, p.original_ids(), context)?;
        put_slice_u64(b, index.id_map(), context)
    })?;
    write_section(out.as_mut(), context, TAG_ATTRS, |b, context| {
        put_u64(b, p.store().attributes().len() as u64, context);
        put_u64(b, p.store().len() as u64, context);
        for col in p.store().attributes() {
            context.checkpoint()?;
            put_slice_u32(b, col, context)?;
        }
        Ok(())
    })?;
    // The f32 vectors in PRISM slot order, so a cold load is a bulk read, not a rescan.
    write_section(out.as_mut(), context, TAG_VECTORS, |b, context| {
        put_u64(b, p.store().dim() as u64, context);
        put_slice_f32(b, p.store().vectors(), context)
    })?;
    context.checkpoint()?;
    debug_assert_eq!(
        out.len(),
        expected_len,
        "encoded length calculation drifted"
    );
    Ok(out)
}

/// Everything a decoded segment carries. Vectors reach the index from the
/// embedded copy ([`SegmentParts::into_index_embedded`]) or via row rehydration
/// ([`SegmentParts::into_index`]).
pub struct SegmentParts {
    graph: Graph,
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
    /// Zeroizing: sealed loaders may rehydrate rows, leaving this copy unconsumed.
    vectors: Zeroizing<Vec<f32>>,
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

    pub fn snapshot_max(&self) -> u64 {
        self.snapshot_max
    }

    pub fn id_map(&self) -> &[u64] {
        &self.id_map
    }

    /// Whether every persisted attribute code belongs to the corresponding
    /// storage-envelope dictionary. The segment format owns the numeric
    /// columns; their external value dictionaries live in its SQL or memory
    /// envelope and must be cross-checked there. An unfiltered envelope has no
    /// dictionaries and matches PRISM's single synthetic all-zero column.
    pub fn attributes_fit_domains(&self, domain_sizes: &[usize]) -> bool {
        self.attributes_fit_domains_with_cancel(domain_sizes, None)
            .expect("attribute-domain validation without a cancellation token was interrupted")
    }

    /// Cancellable form of [`Self::attributes_fit_domains`].
    pub fn attributes_fit_domains_with_cancel(
        &self,
        domain_sizes: &[usize],
        cancel: Option<&CancelToken>,
    ) -> Result<bool, SegmentOperationError> {
        let mut context = CancelContext::new(cancel);
        self.attributes_fit_domains_with_context(domain_sizes, &mut context)
    }

    fn attributes_fit_domains_with_context(
        &self,
        domain_sizes: &[usize],
        context: &mut CancelContext<'_>,
    ) -> Result<bool, SegmentOperationError> {
        context.checkpoint()?;

        // PRISM represents an unfiltered index as one synthetic all-zero
        // attribute column, while its storage envelope has no dictionaries.
        if domain_sizes.is_empty() {
            if self.attrs.len() != 1 {
                return Ok(false);
            }
            for chunk in self.attrs[0].chunks(CANCEL_CHUNK_U32_ITEMS) {
                context.checkpoint()?;
                if chunk.iter().any(|&code| code != 0) {
                    context.checkpoint()?;
                    return Ok(false);
                }
                context.advance(size_of_val(chunk));
            }
            context.checkpoint()?;
            return Ok(true);
        }

        if self.attrs.len() != domain_sizes.len() {
            return Ok(false);
        }
        for (column, &size) in self.attrs.iter().zip(domain_sizes) {
            for chunk in column.chunks(CANCEL_CHUNK_U32_ITEMS) {
                context.checkpoint()?;
                if chunk.iter().any(|&code| (code as usize) >= size) {
                    context.checkpoint()?;
                    return Ok(false);
                }
                context.advance(size_of_val(chunk));
            }
        }
        context.checkpoint()?;
        Ok(true)
    }

    /// `row_id -> PRISM-internal slot`: the PERMUTATION the rehydration loader uses.
    pub fn internal_of_row(&self) -> FxHashMap<u64, u32> {
        self.id_map
            .iter()
            .enumerate()
            .map(|(internal, &row)| (row, internal as u32))
            .collect()
    }

    /// Assemble the index from vectors ALREADY in PRISM-internal slot order.
    fn build(self, vectors: Vec<f32>) -> Result<AnnIndex, SegmentError> {
        let store = PointStore::from_parts(vectors, self.dim as usize, self.attrs)?;
        let prism = PrismIndex::from_parts(
            store,
            self.tree,
            self.graph,
            self.medoids,
            self.global_medoid,
            self.point_cell,
            self.original_ids,
            self.sq8,
            self.binary,
            AnnIndex::active_config(self.metric),
        )?;
        Ok(AnnIndex::from_parts(
            prism,
            self.id_map,
            self.snapshot_max,
            self.metric,
            self.dim,
        ))
    }

    /// Build the index from externally-rehydrated vectors (id_map order); the sealed-load path.
    pub fn into_index(
        self,
        mut vectors: Vec<f32>,
        filled: usize,
    ) -> Result<AnnIndex, SegmentError> {
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
        if self.metric == Metric::Cosine {
            crate::prism::distance::normalize_rows(&mut vectors, self.dim as usize);
        }
        self.build(vectors)
    }

    /// Build the index from the segment's embedded build-form vectors - the fast cold-load path.
    pub fn into_index_embedded(mut self) -> Result<AnnIndex, SegmentError> {
        let vectors = std::mem::take(self.vectors.as_mut());
        self.build(vectors)
    }
}

/// Decode a segment body. Every section's BLAKE3 must verify; any mismatch is
/// a corruption refusal, never a partial result.
pub fn decode(bytes: &[u8]) -> Result<SegmentParts, SegmentError> {
    match decode_with_cancel(bytes, None) {
        Ok(parts) => Ok(parts),
        Err(SegmentOperationError::Segment(error)) => Err(error),
        Err(SegmentOperationError::Allocation(where_)) => Err(SegmentError::Allocation(where_)),
        Err(SegmentOperationError::Interrupted) => {
            unreachable!("decoding without a cancellation token cannot be interrupted")
        }
    }
}

/// Decode a segment body while observing `cancel` during section hashing and
/// numeric-array materialization. Interruption is not reported as corruption.
pub fn decode_with_cancel(
    bytes: &[u8],
    cancel: Option<&CancelToken>,
) -> Result<SegmentParts, SegmentOperationError> {
    let mut context = CancelContext::new(cancel);
    decode_with_context(bytes, &mut context)
}

fn decode_with_context(
    bytes: &[u8],
    context: &mut CancelContext<'_>,
) -> Result<SegmentParts, SegmentOperationError> {
    context.checkpoint()?;
    let mut r = Reader { buf: bytes, at: 0 };

    let g = r.section(TAG_GRAPH, "graph", context)?;
    let mut gr = Reader { buf: g, at: 0 };
    let graph_n = gr.count("graph")?;
    let mut graph_offsets = gr.slice_u32("graph", context)?;
    let mut graph_neighbors = gr.slice_u32("graph", context)?;
    gr.finish("graph")?;
    if graph_n.checked_add(1) != Some(graph_offsets.len()) {
        return Err(SegmentError::Inconsistent("graph offsets length != n+1").into());
    }

    let s = r.section(TAG_SQ8, "sq8", context)?;
    let mut sr = Reader { buf: s, at: 0 };
    let sq8_dim = sr.count("sq8")?;
    let mut codes = sr.slice_u8("sq8", context)?;
    let mut mins = sr.slice_f32("sq8", context)?;
    let mut scales = sr.slice_f32("sq8", context)?;
    sr.finish("sq8")?;

    let b = r.section(TAG_BINARY, "binary", context)?;
    let mut br = Reader { buf: b, at: 0 };
    let code_words = br.count("binary")?;
    let block_size = br.count("binary")?;
    let mut bcodes = br.slice_u64("binary", context)?;
    let mut signs = br.slice_f32("binary", context)?;
    br.finish("binary")?;

    let t = r.section(TAG_TREE, "tree", context)?;
    let mut tr = Reader { buf: t, at: 0 };
    let k = tr.count("tree")?;
    let so_len = tr.count("tree")?;
    if so_len > tr.remaining().saturating_sub(8) / 8 {
        return Err(SegmentError::CountOutOfBounds("tree split order").into());
    }
    let mut split_order = Zeroizing::new(Vec::new());
    split_order
        .try_reserve_exact(so_len)
        .map_err(|_| SegmentOperationError::Allocation("tree split order"))?;
    for chunk_start in (0..so_len).step_by(CANCEL_CHUNK_U64_ITEMS) {
        context.checkpoint()?;
        let chunk_end = (chunk_start + CANCEL_CHUNK_U64_ITEMS).min(so_len);
        for _ in chunk_start..chunk_end {
            split_order.push(tr.count("tree split order")?);
        }
        context.advance((chunk_end - chunk_start) * 8);
    }
    context.checkpoint()?;
    let cells_len = tr.count("tree")?;
    if cells_len > tr.remaining() / 16 {
        return Err(SegmentError::CountOutOfBounds("tree cells").into());
    }
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(cells_len)
        .map_err(|_| SegmentOperationError::Allocation("tree cells"))?;
    for _ in 0..cells_len {
        context.checkpoint()?;
        let mut values = tr.slice_u32("tree", context)?;
        let mut point_ids = tr.slice_u32("tree", context)?;
        cells.push(Cell::from_parts(
            std::mem::take(values.as_mut()),
            std::mem::take(point_ids.as_mut()),
        ));
    }
    tr.finish("tree")?;

    let i = r.section(TAG_IDS, "ids", context)?;
    let mut ir = Reader { buf: i, at: 0 };
    let snapshot_max = ir.u64("ids")?;
    let metric = metric_from_tag(ir.u8("ids")?)?;
    let dim = ir.u16("ids")?;
    let global_medoid = u32::try_from(ir.u64("ids")?)
        .map_err(|_| SegmentError::ValueOutOfBounds("global medoid"))?;
    let mut medoids = ir.slice_u32("ids", context)?;
    let mut point_cell = ir.slice_u32("ids", context)?;
    let mut original_ids = ir.slice_u32("ids", context)?;
    let mut id_map = ir.slice_u64("ids", context)?;
    ir.finish("ids")?;

    let a = r.section(TAG_ATTRS, "attrs", context)?;
    let mut ar = Reader { buf: a, at: 0 };
    let attr_k = ar.count("attrs")?;
    let n = ar.count("attrs")?;
    if attr_k > ar.remaining() / 8 {
        return Err(SegmentError::CountOutOfBounds("attribute columns").into());
    }
    let mut raw_attrs = Vec::new();
    raw_attrs
        .try_reserve_exact(attr_k)
        .map_err(|_| SegmentOperationError::Allocation("attribute columns"))?;
    for _ in 0..attr_k {
        context.checkpoint()?;
        let col = ar.slice_u32("attrs", context)?;
        if col.len() != n {
            return Err(SegmentError::Inconsistent("attr column length != n").into());
        }
        raw_attrs.push(col);
    }
    ar.finish("attrs")?;

    let v = r.section(TAG_VECTORS, "vectors", context)?;
    r.finish("body")?;
    let mut vr = Reader { buf: v, at: 0 };
    let vdim = vr.count("vectors")?;
    let vectors = vr.slice_f32("vectors", context)?;
    vr.finish("vectors")?;
    let expected_vectors = n
        .checked_mul(dim as usize)
        .ok_or(SegmentError::Inconsistent("n*dim overflows usize"))?;
    if vdim != dim as usize {
        return Err(SegmentError::Inconsistent(
            "vector section dimension disagrees with ids dimension",
        )
        .into());
    }
    if vectors.len() != expected_vectors {
        return Err(SegmentError::VectorLen {
            expected: expected_vectors,
            got: vectors.len(),
        }
        .into());
    }

    if id_map.len() != n || original_ids.len() != n || point_cell.len() != n {
        return Err(SegmentError::Inconsistent("id arrays disagree on n").into());
    }
    if n > u32::MAX as usize {
        return Err(SegmentError::Inconsistent("point count exceeds u32 ids").into());
    }
    let mut row_by_original = Vec::new();
    row_by_original
        .try_reserve_exact(n)
        .map_err(|_| SegmentOperationError::Allocation("external row-id order"))?;
    row_by_original.resize(n, 0u64);
    let mut seen_original = Vec::new();
    seen_original
        .try_reserve_exact(n)
        .map_err(|_| SegmentOperationError::Allocation("original-id permutation"))?;
    seen_original.resize(n, false);
    for (internal, (&original_id, &row_id)) in original_ids.iter().zip(id_map.iter()).enumerate() {
        if internal % CANCEL_CHUNK_COLLECTIONS == 0 {
            context.checkpoint()?;
        }
        let original = original_id as usize;
        if original >= n || seen_original[original] {
            return Err(
                SegmentError::Inconsistent("original ids are not a permutation of 0..n").into(),
            );
        }
        seen_original[original] = true;
        row_by_original[original] = row_id;
        context.advance(size_of::<u32>() + size_of::<u64>());
    }

    let mut previous = None;
    for (original, &row_id) in row_by_original.iter().enumerate() {
        if original % CANCEL_CHUNK_COLLECTIONS == 0 {
            context.checkpoint()?;
        }
        if let Some(prior) = previous {
            if row_id == prior {
                return Err(SegmentError::DuplicateRowId(row_id).into());
            }
            if row_id < prior {
                return Err(SegmentError::Inconsistent(
                    "external row ids do not follow original-id order",
                )
                .into());
            }
        }
        previous = Some(row_id);
        context.advance(size_of::<u64>());
    }
    if let Some(expected) = previous {
        if snapshot_max != expected {
            return Err(SegmentError::SnapshotMax {
                expected,
                got: snapshot_max,
            }
            .into());
        }
    }
    drop(row_by_original);
    drop(seen_original);
    context.checkpoint()?;

    let graph = Graph::from_parts(
        std::mem::take(graph_offsets.as_mut()),
        std::mem::take(graph_neighbors.as_mut()),
        graph_n,
    )
    .map_err(SegmentError::from)?;
    context.checkpoint()?;
    let sq8 = SQ8Store::from_parts(
        std::mem::take(codes.as_mut()),
        std::mem::take(mins.as_mut()),
        std::mem::take(scales.as_mut()),
        sq8_dim,
    )
    .map_err(SegmentError::from)?;
    context.checkpoint()?;
    let binary = BinaryStore::from_parts(
        std::mem::take(bcodes.as_mut()),
        code_words,
        std::mem::take(signs.as_mut()),
        block_size,
    )
    .map_err(SegmentError::from)?;
    context.checkpoint()?;
    let tree = PartitionTree::from_parts(cells, std::mem::take(split_order.as_mut()), k, n)
        .map_err(SegmentError::from)?;
    context.checkpoint()?;
    let mut attrs = Vec::new();
    attrs
        .try_reserve_exact(raw_attrs.len())
        .map_err(|_| SegmentOperationError::Allocation("decoded attributes"))?;
    for (index, mut col) in raw_attrs.into_iter().enumerate() {
        if index % CANCEL_CHUNK_COLLECTIONS == 0 {
            context.checkpoint()?;
        }
        attrs.push(std::mem::take(col.as_mut()));
    }
    context.checkpoint()?;
    let parts = SegmentParts {
        graph,
        sq8,
        binary,
        tree,
        snapshot_max,
        metric,
        dim,
        global_medoid,
        medoids: std::mem::take(medoids.as_mut()),
        point_cell: std::mem::take(point_cell.as_mut()),
        original_ids: std::mem::take(original_ids.as_mut()),
        id_map: std::mem::take(id_map.as_mut()),
        attrs,
        vectors,
        n,
    };
    context.checkpoint()?;
    Ok(parts)
}

fn encoded_len(
    index: &AnnIndex,
    context: &mut CancelContext<'_>,
) -> Result<usize, SegmentOperationError> {
    fn slice_len(items: usize, width: u8) -> u128 {
        8 + (items as u128) * u128::from(width)
    }

    let p = index.prism();
    let mut len = (7 * SECTION_FRAME_BYTES) as u128;

    len += 8 + slice_len(p.graph().offsets().len(), 4);
    len += slice_len(p.graph().neighbor_ids().len(), 4);

    len += 8 + slice_len(p.sq8().codes().len(), 1);
    len += slice_len(p.sq8().mins().len(), 4);
    len += slice_len(p.sq8().scales().len(), 4);

    len += 16 + slice_len(p.binary().codes().len(), 8);
    len += slice_len(p.binary().signs().len(), 4);

    len += 24 + (p.tree().split_order().len() as u128) * 8;
    for cell in p.tree().cells() {
        context.checkpoint()?;
        len += slice_len(cell.values().len(), 4);
        len += slice_len(cell.point_ids().len(), 4);
    }

    len += 19 + slice_len(p.medoids().len(), 4);
    len += slice_len(p.point_cell().len(), 4);
    len += slice_len(p.original_ids().len(), 4);
    len += slice_len(index.id_map().len(), 8);

    len += 16;
    for col in p.store().attributes() {
        context.checkpoint()?;
        len += slice_len(col.len(), 4);
    }

    len += 8 + slice_len(p.store().vectors().len(), 4);
    usize::try_from(len)
        .map_err(|_| SegmentError::Inconsistent("encoded segment length overflows usize").into())
}

fn write_section(
    out: &mut Vec<u8>,
    context: &mut CancelContext<'_>,
    tag: u8,
    fill: impl FnOnce(&mut Vec<u8>, &mut CancelContext<'_>) -> Result<(), SegmentOperationError>,
) -> Result<(), SegmentOperationError> {
    context.checkpoint()?;
    put_u8(out, tag, context);
    let length_offset = out.len();
    put_bytes_small(out, &[0; 8], context);
    let payload_offset = out.len();
    fill(out, context)?;

    let payload_len = out.len() - payload_offset;
    let payload_len = u64::try_from(payload_len)
        .map_err(|_| SegmentError::Inconsistent("section length overflows u64"))?;
    out[length_offset..length_offset + 8].copy_from_slice(&payload_len.to_le_bytes());
    let hash = digest_with_context(&out[payload_offset..], context)?;
    put_bytes_small(out, &hash, context);
    Ok(())
}

fn encode_graph(
    out: &mut Vec<u8>,
    graph: &Graph,
    context: &mut CancelContext<'_>,
) -> Result<(), SegmentOperationError> {
    put_u64(out, graph.len() as u64, context);
    put_slice_u32(out, graph.offsets(), context)?;
    put_slice_u32(out, graph.neighbor_ids(), context)
}

fn put_u8(out: &mut Vec<u8>, value: u8, context: &mut CancelContext<'_>) {
    assert_spare_capacity(out, 1);
    out.push(value);
    context.advance(1);
}

fn put_u64(out: &mut Vec<u8>, value: u64, context: &mut CancelContext<'_>) {
    put_bytes_small(out, &value.to_le_bytes(), context);
}

fn put_bytes_small(out: &mut Vec<u8>, bytes: &[u8], context: &mut CancelContext<'_>) {
    assert_spare_capacity(out, bytes.len());
    out.extend_from_slice(bytes);
    context.advance(bytes.len());
}

#[inline]
fn assert_spare_capacity(out: &Vec<u8>, additional: usize) {
    debug_assert!(
        out.len()
            .checked_add(additional)
            .is_some_and(|required| required <= out.capacity()),
        "encoded length calculation under-allocated"
    );
}

fn put_slice_u8(
    out: &mut Vec<u8>,
    values: &[u8],
    context: &mut CancelContext<'_>,
) -> Result<(), SegmentOperationError> {
    put_u64(out, values.len() as u64, context);
    for chunk in values.chunks(CANCEL_CHUNK_BYTES) {
        context.checkpoint()?;
        assert_spare_capacity(out, chunk.len());
        out.extend_from_slice(chunk);
        context.advance(chunk.len());
    }
    context.checkpoint()
}

fn put_slice_u32(
    out: &mut Vec<u8>,
    values: &[u32],
    context: &mut CancelContext<'_>,
) -> Result<(), SegmentOperationError> {
    put_u64(out, values.len() as u64, context);
    for chunk in values.chunks(CANCEL_CHUNK_U32_ITEMS) {
        context.checkpoint()?;
        assert_spare_capacity(out, chunk.len() * 4);
        for &value in chunk {
            out.extend_from_slice(&value.to_le_bytes());
        }
        context.advance(chunk.len() * 4);
    }
    context.checkpoint()
}

fn put_slice_u64(
    out: &mut Vec<u8>,
    values: &[u64],
    context: &mut CancelContext<'_>,
) -> Result<(), SegmentOperationError> {
    put_u64(out, values.len() as u64, context);
    for chunk in values.chunks(CANCEL_CHUNK_U64_ITEMS) {
        context.checkpoint()?;
        assert_spare_capacity(out, chunk.len() * 8);
        for &value in chunk {
            out.extend_from_slice(&value.to_le_bytes());
        }
        context.advance(chunk.len() * 8);
    }
    context.checkpoint()
}

fn put_slice_f32(
    out: &mut Vec<u8>,
    values: &[f32],
    context: &mut CancelContext<'_>,
) -> Result<(), SegmentOperationError> {
    put_u64(out, values.len() as u64, context);
    for chunk in values.chunks(CANCEL_CHUNK_U32_ITEMS) {
        context.checkpoint()?;
        assert_spare_capacity(out, chunk.len() * 4);
        for &value in chunk {
            out.extend_from_slice(&value.to_le_bytes());
        }
        context.advance(chunk.len() * 4);
    }
    context.checkpoint()
}

#[cfg(test)]
fn digest(bytes: &[u8]) -> [u8; 32] {
    digest_with_cancel(bytes, None).expect("a digest without a cancellation token cannot fail")
}

/// BLAKE3 digest that observes `cancel` between bounded input chunks.
pub fn digest_with_cancel(
    bytes: &[u8],
    cancel: Option<&CancelToken>,
) -> Result<[u8; 32], SegmentOperationError> {
    let mut context = CancelContext::new(cancel);
    digest_with_context(bytes, &mut context)
}

fn digest_with_context(
    bytes: &[u8],
    context: &mut CancelContext<'_>,
) -> Result<[u8; 32], SegmentOperationError> {
    context.checkpoint()?;
    let mut hasher = blake3::Hasher::new();
    for chunk in bytes.chunks(CANCEL_CHUNK_BYTES) {
        context.checkpoint()?;
        hasher.update(chunk);
        context.advance(chunk.len());
    }
    context.checkpoint()?;
    Ok(*hasher.finalize().as_bytes())
}

struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.at
    }

    fn finish(&self, what: &'static str) -> Result<(), SegmentError> {
        if self.at == self.buf.len() {
            Ok(())
        } else {
            Err(SegmentError::TrailingData(what))
        }
    }

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

    fn count(&mut self, what: &'static str) -> Result<usize, SegmentError> {
        usize::try_from(self.u64(what)?).map_err(|_| SegmentError::CountOutOfBounds(what))
    }

    /// One framed section: tag + length + payload + verified BLAKE3.
    fn section(
        &mut self,
        tag: u8,
        what: &'static str,
        context: &mut CancelContext<'_>,
    ) -> Result<&'a [u8], SegmentOperationError> {
        context.checkpoint()?;
        let got = self.u8(what)?;
        if got != tag {
            return Err(SegmentError::BadTag { expected: tag, got }.into());
        }
        let len = self.count(what)?;
        let payload = self.take(len, what)?;
        let hash: [u8; 32] = self.take(32, what)?.try_into().unwrap();
        if digest_with_context(payload, context)? != hash {
            return Err(SegmentError::SectionHash(what).into());
        }
        Ok(payload)
    }

    fn slice_u8(
        &mut self,
        what: &'static str,
        context: &mut CancelContext<'_>,
    ) -> Result<Zeroizing<Vec<u8>>, SegmentOperationError> {
        let len = self.count(what)?;
        let raw = self.take(len, what)?;
        let mut values = Zeroizing::new(Vec::new());
        values
            .try_reserve_exact(len)
            .map_err(|_| SegmentOperationError::Allocation(what))?;
        for chunk in raw.chunks(CANCEL_CHUNK_BYTES) {
            context.checkpoint()?;
            values.extend_from_slice(chunk);
            context.advance(chunk.len());
        }
        context.checkpoint()?;
        Ok(values)
    }

    fn slice_u32(
        &mut self,
        what: &'static str,
        context: &mut CancelContext<'_>,
    ) -> Result<Zeroizing<Vec<u32>>, SegmentOperationError> {
        let len = self.count(what)?;
        let raw = self.take(
            len.checked_mul(4).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        let mut values = Zeroizing::new(Vec::new());
        values
            .try_reserve_exact(len)
            .map_err(|_| SegmentOperationError::Allocation(what))?;
        for chunk in raw.chunks(CANCEL_CHUNK_BYTES) {
            context.checkpoint()?;
            for encoded in chunk.as_chunks::<4>().0 {
                values.push(u32::from_le_bytes(*encoded));
            }
            context.advance(chunk.len());
        }
        context.checkpoint()?;
        Ok(values)
    }

    fn slice_u64(
        &mut self,
        what: &'static str,
        context: &mut CancelContext<'_>,
    ) -> Result<Zeroizing<Vec<u64>>, SegmentOperationError> {
        let len = self.count(what)?;
        let raw = self.take(
            len.checked_mul(8).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        let mut values = Zeroizing::new(Vec::new());
        values
            .try_reserve_exact(len)
            .map_err(|_| SegmentOperationError::Allocation(what))?;
        for chunk in raw.chunks(CANCEL_CHUNK_BYTES) {
            context.checkpoint()?;
            for encoded in chunk.as_chunks::<8>().0 {
                values.push(u64::from_le_bytes(*encoded));
            }
            context.advance(chunk.len());
        }
        context.checkpoint()?;
        Ok(values)
    }

    fn slice_f32(
        &mut self,
        what: &'static str,
        context: &mut CancelContext<'_>,
    ) -> Result<Zeroizing<Vec<f32>>, SegmentOperationError> {
        let len = self.count(what)?;
        let raw = self.take(
            len.checked_mul(4).ok_or(SegmentError::Truncated(what))?,
            what,
        )?;
        let mut values = Zeroizing::new(Vec::new());
        values
            .try_reserve_exact(len)
            .map_err(|_| SegmentOperationError::Allocation(what))?;
        for chunk in raw.chunks(CANCEL_CHUNK_BYTES) {
            context.checkpoint()?;
            for encoded in chunk.as_chunks::<4>().0 {
                values.push(f32::from_le_bytes(*encoded));
            }
            context.advance(chunk.len());
        }
        context.checkpoint()?;
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic fixture rows: two attribute cells and NON-monotonic row
    /// ids (so id_map order != insertion order). RAW vectors, exactly what a
    /// table scan would yield.
    fn fixture_rows() -> Vec<(u64, Vec<f32>, Vec<u32>)> {
        (0..200u64)
            .map(|i| {
                // Reverse-ish ids: external order differs from internal.
                let id = 1000 - i * 3;
                let v: Vec<f32> = (0..8).map(|d| ((i * 7 + d) % 23) as f32 * 0.5).collect();
                (id, v, vec![(i % 2) as u32])
            })
            .collect()
    }

    fn build_fixture() -> AnnIndex {
        AnnIndex::build_with_attrs(fixture_rows(), 1, Metric::Cosine, 8).expect("build fixture")
    }

    fn edit_section_payload(bytes: &mut Vec<u8>, tag: u8, edit: impl FnOnce(&mut Vec<u8>)) {
        let mut at = 0usize;
        while at < bytes.len() {
            let len = u64::from_le_bytes(bytes[at + 1..at + 9].try_into().unwrap()) as usize;
            let payload_start = at + 9;
            let payload_end = payload_start + len;
            let frame_end = payload_end + 32;
            if bytes[at] == tag {
                let mut payload = bytes[payload_start..payload_end].to_vec();
                edit(&mut payload);
                let mut frame = Vec::with_capacity(9 + payload.len() + 32);
                frame.push(tag);
                frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
                frame.extend_from_slice(&payload);
                frame.extend_from_slice(blake3::hash(&payload).as_bytes());
                bytes.splice(at..frame_end, frame);
                return;
            }
            at = frame_end;
        }
        panic!("section tag {tag} not found");
    }

    fn read_u64_at(bytes: &[u8], at: usize) -> u64 {
        u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
    }

    fn ids_original_and_row_offsets(payload: &[u8]) -> ((usize, usize), (usize, usize)) {
        let mut at = 8 + 1 + 2 + 8;
        for _ in 0..2 {
            let len = read_u64_at(payload, at) as usize;
            at += 8 + len * size_of::<u32>();
        }
        let original_count = read_u64_at(payload, at) as usize;
        let original_at = at + 8;
        at = original_at + original_count * size_of::<u32>();
        let row_count = read_u64_at(payload, at) as usize;
        let row_at = at + 8;
        ((original_at, original_count), (row_at, row_count))
    }

    #[test]
    fn encoded_segment_has_a_stable_wire_digest() {
        fn require_zeroizing_bytes(_: &Zeroizing<Vec<u8>>) {}

        let index = build_fixture();
        let encoded = encode_with_cancel(&index, None).expect("encode fixture");
        require_zeroizing_bytes(&encoded);
        assert_eq!(encode(&index).as_slice(), encoded.as_slice());
        assert_eq!(
            digest(&encoded),
            [
                51, 215, 194, 155, 99, 119, 90, 139, 225, 232, 32, 234, 67, 166, 245, 249, 153, 35,
                53, 213, 82, 76, 120, 139, 27, 215, 226, 230, 115, 58, 126, 231,
            ]
        );
    }

    #[test]
    fn encode_cancellation_is_deterministic_and_mid_work() {
        use std::cell::Cell;

        let index = build_fixture();
        let completed_at = Cell::new(0);
        let mut completion_hook = |work| completed_at.set(work);
        let mut completion_context = CancelContext::with_hook(None, &mut completion_hook);
        encode_with_context(&index, &mut completion_context).expect("uncancelled encode");

        let token = CancelToken::new();
        let cancelled_at = Cell::new(None);
        let mut hook = |work| {
            if work >= 512 && !token.is_cancelled() {
                cancelled_at.set(Some(work));
                token.cancel();
            }
        };
        let mut context = CancelContext::with_hook(Some(&token), &mut hook);
        let result = encode_with_context(&index, &mut context);

        assert!(matches!(result, Err(SegmentOperationError::Interrupted)));
        assert!(cancelled_at.get().is_some_and(|work| {
            work >= 512 && work < completed_at.get() && work < 512 + CANCEL_CHUNK_BYTES + 1024
        }));
    }

    #[test]
    fn decode_cancellation_is_deterministic_and_not_corruption() {
        use std::cell::Cell;

        let bytes = encode(&build_fixture());
        let completed_at = Cell::new(0);
        let mut completion_hook = |work| completed_at.set(work);
        let mut completion_context = CancelContext::with_hook(None, &mut completion_hook);
        decode_with_context(&bytes, &mut completion_context).expect("uncancelled decode");

        let token = CancelToken::new();
        let cancelled_at = Cell::new(None);
        let mut hook = |work| {
            if work >= 512 && !token.is_cancelled() {
                cancelled_at.set(Some(work));
                token.cancel();
            }
        };
        let mut context = CancelContext::with_hook(Some(&token), &mut hook);
        let result = decode_with_context(&bytes, &mut context);

        assert!(matches!(result, Err(SegmentOperationError::Interrupted)));
        assert!(cancelled_at.get().is_some_and(|work| {
            work >= 512 && work < completed_at.get() && work < 512 + CANCEL_CHUNK_BYTES + 1024
        }));
    }

    #[test]
    fn digest_cancellation_is_deterministic_and_mid_work() {
        use std::cell::Cell;

        let bytes = vec![0xA5; CANCEL_CHUNK_BYTES * 3];
        let token = CancelToken::new();
        let cancelled_at = Cell::new(None);
        let mut hook = |work| {
            if work >= CANCEL_CHUNK_BYTES && !token.is_cancelled() {
                cancelled_at.set(Some(work));
                token.cancel();
            }
        };
        let mut context = CancelContext::with_hook(Some(&token), &mut hook);
        let result = digest_with_context(&bytes, &mut context);

        assert!(matches!(result, Err(SegmentOperationError::Interrupted)));
        assert_eq!(cancelled_at.get(), Some(CANCEL_CHUNK_BYTES));
        assert_eq!(digest(&bytes), *blake3::hash(&bytes).as_bytes());
    }

    #[test]
    fn public_cancellable_apis_refuse_a_pre_cancelled_token() {
        let index = build_fixture();
        let bytes = encode(&index);
        let parts = decode(&bytes).expect("decode fixture");
        let token = CancelToken::new();
        token.cancel();

        assert!(matches!(
            encode_with_cancel(&index, Some(&token)),
            Err(SegmentOperationError::Interrupted)
        ));
        assert!(matches!(
            decode_with_cancel(&bytes, Some(&token)),
            Err(SegmentOperationError::Interrupted)
        ));
        assert!(matches!(
            digest_with_cancel(&bytes, Some(&token)),
            Err(SegmentOperationError::Interrupted)
        ));
        assert!(matches!(
            parts.attributes_fit_domains_with_cancel(&[2], Some(&token)),
            Err(SegmentOperationError::Interrupted)
        ));
    }

    #[test]
    fn malicious_collection_counts_are_refused_without_allocating() {
        let encoded = encode(&build_fixture());

        let mut split_order = encoded.clone();
        edit_section_payload(&mut split_order, TAG_TREE, |payload| {
            payload[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        });
        assert!(matches!(
            decode(&split_order),
            Err(SegmentError::CountOutOfBounds("tree split order"))
        ));

        let mut cells = encoded.clone();
        edit_section_payload(&mut cells, TAG_TREE, |payload| {
            let split_order_len = read_u64_at(payload, 8) as usize;
            let cells_len_at = 16 + split_order_len * 8;
            payload[cells_len_at..cells_len_at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        });
        assert!(matches!(
            decode(&cells),
            Err(SegmentError::CountOutOfBounds("tree cells"))
        ));

        let mut attrs = encoded;
        edit_section_payload(&mut attrs, TAG_ATTRS, |payload| {
            payload[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        });
        assert!(matches!(
            decode(&attrs),
            Err(SegmentError::CountOutOfBounds("attribute columns"))
        ));
    }

    #[test]
    fn duplicate_row_ids_and_wrong_snapshot_max_are_refused() {
        let encoded = encode(&build_fixture());

        let mut duplicate = encoded.clone();
        edit_section_payload(&mut duplicate, TAG_IDS, |payload| {
            let ((original_at, original_count), (row_at, row_count)) =
                ids_original_and_row_offsets(payload);
            assert_eq!(original_count, row_count);
            let mut internal_zero = None;
            let mut internal_one = None;
            for internal in 0..original_count {
                let at = original_at + internal * size_of::<u32>();
                match u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) {
                    0 => internal_zero = Some(internal),
                    1 => internal_one = Some(internal),
                    _ => {}
                }
            }
            let zero_row = row_at + internal_zero.expect("original id 0") * size_of::<u64>();
            let one_row = row_at + internal_one.expect("original id 1") * size_of::<u64>();
            let duplicate_row = payload[zero_row..zero_row + 8].to_vec();
            payload[one_row..one_row + 8].copy_from_slice(&duplicate_row);
        });
        assert!(matches!(
            decode(&duplicate),
            Err(SegmentError::DuplicateRowId(_))
        ));

        let mut wrong_max = encoded;
        edit_section_payload(&mut wrong_max, TAG_IDS, |payload| {
            payload[..8].copy_from_slice(&0u64.to_le_bytes());
        });
        assert!(matches!(
            decode(&wrong_max),
            Err(SegmentError::SnapshotMax { .. })
        ));
    }

    #[test]
    fn row_ids_must_match_the_persisted_original_id_permutation() {
        let mut encoded = encode(&build_fixture());
        edit_section_payload(&mut encoded, TAG_IDS, |payload| {
            let ((original_at, original_count), (row_at, row_count)) =
                ids_original_and_row_offsets(payload);
            assert_eq!(original_count, row_count);
            let mut internal_zero = None;
            let mut internal_last = None;
            for internal in 0..original_count {
                let at = original_at + internal * size_of::<u32>();
                let original = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap());
                if original == 0 {
                    internal_zero = Some(internal);
                } else if original as usize == original_count - 1 {
                    internal_last = Some(internal);
                }
            }
            let first = row_at + internal_zero.expect("original id 0") * size_of::<u64>();
            let last = row_at + internal_last.expect("last original id") * size_of::<u64>();
            for offset in 0..size_of::<u64>() {
                payload.swap(first + offset, last + offset);
            }
        });

        assert!(matches!(
            decode(&encoded),
            Err(SegmentError::Inconsistent(
                "external row ids do not follow original-id order"
            ))
        ));
    }

    #[test]
    fn out_of_range_global_medoid_is_not_truncated() {
        let mut encoded = encode(&build_fixture());
        edit_section_payload(&mut encoded, TAG_IDS, |payload| {
            let global_medoid_at = 8 + 1 + 2;
            payload[global_medoid_at..global_medoid_at + 8]
                .copy_from_slice(&(u64::from(u32::MAX) + 1).to_le_bytes());
        });

        assert!(matches!(
            decode(&encoded),
            Err(SegmentError::ValueOutOfBounds("global medoid"))
        ));
    }

    #[test]
    fn trailing_body_or_section_data_is_refused() {
        let encoded = encode(&build_fixture());
        let mut trailing_body = encoded.clone();
        trailing_body.push(0);
        assert!(matches!(
            decode(&trailing_body),
            Err(SegmentError::TrailingData("body"))
        ));

        for (tag, name) in [
            (TAG_GRAPH, "graph"),
            (TAG_SQ8, "sq8"),
            (TAG_BINARY, "binary"),
            (TAG_TREE, "tree"),
            (TAG_IDS, "ids"),
            (TAG_ATTRS, "attrs"),
            (TAG_VECTORS, "vectors"),
        ] {
            let mut trailing_section = encoded.clone();
            edit_section_payload(&mut trailing_section, tag, |payload| payload.push(0));
            assert!(matches!(
                decode(&trailing_section),
                Err(SegmentError::TrailingData(found)) if found == name
            ));
        }
    }

    #[test]
    fn decoded_embedded_vectors_have_a_zeroizing_owner() {
        fn require_zeroizing_owner(_: &Zeroizing<Vec<f32>>) {}

        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        require_zeroizing_owner(&parts.vectors);
        assert_eq!(parts.vectors.len(), parts.n() * usize::from(parts.dim()));
    }

    #[test]
    fn attribute_codes_must_fit_the_envelope_domains() {
        let parts = decode(&encode(&build_fixture())).expect("decode");
        assert!(parts.attributes_fit_domains(&[2]));
        assert!(!parts.attributes_fit_domains(&[1]));
        assert!(!parts.attributes_fit_domains(&[]));
    }

    #[test]
    fn unfiltered_index_accepts_its_synthetic_attribute_column() {
        let index = AnnIndex::build(
            vec![(1, vec![0.0, 1.0]), (2, vec![1.0, 0.0])],
            Metric::L2,
            2,
        )
        .expect("build unfiltered index");
        let parts = decode(&encode(&index)).expect("decode unfiltered index");
        assert!(parts.attributes_fit_domains(&[]));
    }

    #[test]
    fn attribute_domain_validation_is_cancellable_mid_scan() {
        use std::cell::Cell;

        let mut parts = decode(&encode(&build_fixture())).expect("decode fixture");
        parts.attrs = vec![vec![0; CANCEL_CHUNK_U32_ITEMS * 3]];
        let token = CancelToken::new();
        let cancelled_at = Cell::new(None);
        let mut hook = |work| {
            if work >= CANCEL_CHUNK_BYTES && !token.is_cancelled() {
                cancelled_at.set(Some(work));
                token.cancel();
            }
        };
        let mut context = CancelContext::with_hook(Some(&token), &mut hook);
        let result = parts.attributes_fit_domains_with_context(&[1], &mut context);

        assert!(matches!(result, Err(SegmentOperationError::Interrupted)));
        assert_eq!(cancelled_at.get(), Some(CANCEL_CHUNK_BYTES));
    }

    /// Rehydrate exactly as the storage loader will: RAW row vectors placed by
    /// the id_map PERMUTATION (the index re-applies any build normalization).
    fn rehydrate(rows: &[(u64, Vec<f32>, Vec<u32>)], parts: &SegmentParts) -> (Vec<f32>, usize) {
        let inv = parts.internal_of_row();
        let dim = parts.dim() as usize;
        let mut vectors = vec![0.0f32; parts.n() * dim];
        let mut filled = 0;
        for (row, v, _) in rows {
            let slot = inv[row] as usize;
            vectors[slot * dim..(slot + 1) * dim].copy_from_slice(v);
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
        let (vectors, filled) = rehydrate(&fixture_rows(), &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");
        let query: Vec<f32> = (0..8).map(|d| d as f32 * 0.7).collect();
        for code in [0u32, 1] {
            let filter = crate::prism::Filter::new(vec![(0, vec![code])]);
            let a = index
                .search_filtered(&query, 8, 64, &filter)
                .expect("search");
            let b = loaded
                .search_filtered(&query, 8, 64, &filter)
                .expect("search");
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
            let index = AnnIndex::build_with_attrs(rows.clone(), 1, metric, 4).expect("build");
            let parts = decode(&encode(&index)).expect("decode");
            assert_eq!(parts.metric(), metric, "metric tag survives");
            let (vectors, filled) = rehydrate(&rows, &parts);
            let loaded = parts.into_index(vectors, filled).expect("into_index");
            let q = [1.0f32, -2.0, 3.0, 0.5];
            assert_eq!(index.search(&q, 5), loaded.search(&q, 5), "{metric:?}");
        }
    }

    #[test]
    fn single_row_index_roundtrips() {
        let rows = vec![(42u64, vec![1.0f32, 2.0], vec![0u32])];
        let index =
            AnnIndex::build_with_attrs(rows.clone(), 1, Metric::L2, 2).expect("build single");
        let parts = decode(&encode(&index)).expect("decode");
        assert_eq!(parts.n(), 1);
        let (vectors, filled) = rehydrate(&rows, &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");
        assert_eq!(
            loaded.search(&[1.0, 2.0], 1).expect("search"),
            vec![(42, 0.0)]
        );
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
        let (vectors, filled) = rehydrate(&fixture_rows(), &parts);
        let loaded = parts.into_index(vectors, filled).expect("into_index");

        let query: Vec<f32> = (0..8).map(|d| d as f32 * 0.3).collect();
        let a = index.search(&query, 10);
        let b = loaded.search(&query, 10);
        assert_eq!(a, b, "loaded index must answer EXACTLY like the original");
        assert_eq!(index.snapshot_max, loaded.snapshot_max);
        assert_eq!(index.id_map(), loaded.id_map());
    }

    #[test]
    fn embedded_load_answers_like_the_original() {
        // into_index_embedded (the fast path) must rebuild a search-identical index.
        let index = build_fixture();
        let parts = decode(&encode(&index)).expect("decode");
        let loaded = parts.into_index_embedded().expect("into_index_embedded");
        let query: Vec<f32> = (0..8).map(|d| d as f32 * 0.3).collect();
        assert_eq!(
            index.search(&query, 10).expect("search"),
            loaded.search(&query, 10).expect("search"),
            "embedded-vector load must answer EXACTLY like the original"
        );
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
                cross_cell_exact_ranking_limit: base.cross_cell_exact_ranking_limit + 1,
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
            PrismConfig {
                scan_threshold: base.scan_threshold + 1,
                ..base.clone()
            },
            PrismConfig {
                multi_cell_scan_threshold: base.multi_cell_scan_threshold + 1,
                ..base.clone()
            },
            PrismConfig {
                graph_expansion: base.graph_expansion + 1,
                ..base.clone()
            },
            PrismConfig {
                build_seed: base.build_seed.wrapping_add(1),
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

    #[test]
    fn config_hash_has_a_stable_known_answer() {
        let config = PrismConfig {
            m_local: 48,
            m_greedy: 12,
            m_random: 4,
            t: 2,
            alpha: 1.0,
            vamana_alpha: 1.25,
            beam_width: 128,
            cross_cell_exact_ranking_limit: 4_096,
            metric: Metric::Cosine,
            sigma_high: 0.2,
            sigma_low: 0.01,
            beta: 3.5,
            epsilon: 0.25,
            binary_rerank: 2,
            scan_threshold: 20_000,
            multi_cell_scan_threshold: 500_000,
            graph_expansion: 3,
            build_seed: 0x5052_4953_4d41_4e4e,
        };
        assert_eq!(
            prism_config_hash(&config),
            [
                95, 143, 137, 222, 77, 172, 87, 119, 176, 84, 108, 111, 103, 214, 3, 209, 96, 209,
                230, 6, 56, 166, 208, 21, 181, 51, 92, 164, 118, 45, 183, 135,
            ]
        );
    }
}
