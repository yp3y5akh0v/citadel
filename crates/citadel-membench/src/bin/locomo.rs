//! Live LoCoMo runner. Requires a real OpenAI key and a local BGE-small model;
//! gated behind `openai` + `candle-embed` so default/CI builds never compile it.
//!
//! Usage:
//!   OPENAI_API_KEY=...  CITADEL_AI_BGE_SMALL_DIR=/path/to/bge-small  \
//!     cargo run -p citadeldb-membench --features openai,candle-embed \
//!     --bin locomo -- path/to/locomo10.json
//!
//! Dataset path: argv[1] or LOCOMO_DATASET. Env knobs:
//!   LOCOMO_READER_MODEL=m     answer-generation model (default gpt-4o-mini)
//!   LOCOMO_JUDGE_MODEL=m      scoring model (default gpt-4o-mini)
//!   LOCOMO_RERANK_STRATEGY    replace|rrf (default rrf)
//!   LOCOMO_READER_CONCURRENCY reader calls in flight (default 3)
//!   LOCOMO_JUDGE_CONCURRENCY  judge calls in flight (default 12, mini)
//!   LOCOMO_CONCURRENCY=1      force TRUE serial (both caps -> 1); else legacy floor
//!   LOCOMO_READER_TPM / LOCOMO_JUDGE_TPM  per-model token/min cap (30000 / 1000000)
//!   LOCOMO_RETRY_MAX_ELAPSED_SECS  per-call retry wall-clock budget (default 240)
//!   LOCOMO_MAX_SAMPLES=N      cap the run to the first N conversations
//!   LOCOMO_LIVE_TRACE=path    stream one JSON line per scored question
//!   LOCOMO_AUDIT_PATH=path    per-question audit JSON written at the end
//!   LOCOMO_DRY_RUN=1          load + print dataset stats, then exit (no LLM/key)
//!   LOCOMO_RETRIEVAL_DIAG=1   token-free layered evidence recall@k, then exit
//!                             (needs bge, no key; pinpoints the lossy layer)

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::{LLMClient, OpenAiClient};
use citadel_mem::{
    AtomHit, CandleEmbedder, CrossEncoder, Embedder, FusionWeights, MemoryEngine, RecallQuery,
    RerankStrategy, Reranker,
};
use citadel_membench::{
    aggregate, ingest_sample, provenance, run_sample_observed, BenchConfig, Category,
    QuestionResult, Sample,
};
use rustc_hash::FxHashMap;

/// Reader generates answers, judge scores them (distinct roles, may differ). Override
/// via LOCOMO_READER_MODEL / LOCOMO_JUDGE_MODEL; both are pinned in Provenance.
const DEFAULT_READER_MODEL: &str = "gpt-4o-mini";
const DEFAULT_JUDGE_MODEL: &str = "gpt-4o-mini";

fn main() -> Result<(), Box<dyn Error>> {
    let dataset_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("LOCOMO_DATASET").ok())
        .ok_or("dataset path required: argv[1] or LOCOMO_DATASET")?;

    let config = BenchConfig::default();
    let (mut samples, dataset_sha256) = citadel_membench::load_with_hash(&dataset_path)?;

    if let Ok(raw) = std::env::var("LOCOMO_MAX_SAMPLES") {
        let n: usize = raw
            .parse()
            .map_err(|_| "LOCOMO_MAX_SAMPLES must be a non-negative integer")?;
        samples.truncate(n);
    }

    print_dataset_stats(&samples, &dataset_path, &dataset_sha256);

    if std::env::var("LOCOMO_DRY_RUN").is_ok() {
        eprintln!("dry run: dataset parsed OK, no LLM calls made.");
        return Ok(());
    }

    // LOCOMO_MOCK_EMBED uses a deterministic embedder: fine for the DB dump
    // (embedder-independent), NOT for the diag or full run (need real bge semantics).
    let embedder: Arc<dyn Embedder> = if std::env::var("LOCOMO_MOCK_EMBED").is_ok() {
        Arc::new(citadel_mem::MockEmbedder::new(384))
    } else {
        let bge_dir = std::env::var("CITADEL_AI_BGE_SMALL_DIR")
            .map_err(|_| "CITADEL_AI_BGE_SMALL_DIR not set")?;
        // LOCOMO_EMBEDDER selects the model (default bge-small); dim/layers come from
        // its config.json, and the choice is recorded in Provenance.
        let ce = match std::env::var("LOCOMO_EMBEDDER")
            .unwrap_or_default()
            .as_str()
        {
            "bge-base" => CandleEmbedder::bge_base(&bge_dir)?,
            "bge-large" => CandleEmbedder::bge_large(&bge_dir)?,
            "e5-large" => CandleEmbedder::e5_large(&bge_dir)?,
            _ => CandleEmbedder::bge_small(&bge_dir)?,
        };
        Arc::new(ce)
    };

    // LOCOMO_ENCRYPTED seals atoms per-key and enables per-atom/region erasure.
    // LOCOMO_DB_PATH persists the (encrypted) DB for table/sidecar inspection; else temp.
    let encrypted =
        citadel_membench::encrypted_regions() || std::env::var("LOCOMO_ERASURE_DEMO").is_ok();
    let db_path = std::env::var("LOCOMO_DB_PATH").ok();
    if let Some(p) = &db_path {
        if std::path::Path::new(p).exists() {
            return Err(format!("LOCOMO_DB_PATH exists: {p} (remove it for a fresh run)").into());
        }
    }
    let tmp = if db_path.is_none() {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let db_file = match &db_path {
        Some(p) => std::path::PathBuf::from(p),
        None => tmp.as_ref().unwrap().path().join("membench.cdl"),
    };
    let mut builder = DatabaseBuilder::new(&db_file)
        .passphrase(b"membench")
        .argon2_profile(Argon2Profile::Iot);
    if encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = Arc::new(builder.create()?);
    eprintln!("db: {} (encrypted_regions={encrypted})", db_file.display());
    let mut eng = MemoryEngine::open(db.clone())?;

    // Inspect what is actually stored: ingest one conversation, dump its atoms.
    if std::env::var("LOCOMO_DUMP_DB").is_ok() {
        return run_db_dump(&eng, &samples, embedder);
    }

    // Token-free: does top-k recall surface the gold evidence turns? No key.
    if std::env::var("LOCOMO_RETRIEVAL_DIAG").is_ok() {
        return run_retrieval_diag(&mut eng, &samples, embedder);
    }

    // Behind-the-scenes encryption + erasure verification (token-free, no key).
    if std::env::var("LOCOMO_ERASURE_DEMO").is_ok() {
        return run_erasure_demo(&eng, db, &samples, embedder);
    }

    // Optional cross-encoder reranker re-orders the candidate pool before the reader's
    // top-k. LOCOMO_RERANK_STRATEGY=replace|rrf (default rrf blends cross-encoder + fusion).
    let reranker_model = match std::env::var("CITADEL_AI_RERANKER_DIR") {
        Ok(rr_dir) => {
            let ce = CrossEncoder::ms_marco_minilm_l6(&rr_dir)?;
            let model = ce.model_id().to_string();
            let strategy = rerank_strategy_from_env();
            eng.set_reranker(Arc::new(ce), strategy);
            eprintln!("reranker: {model} (from {rr_dir}) strategy={strategy:?}");
            format!("{model} ({strategy:?})")
        }
        Err(_) => {
            eprintln!("reranker: none (set CITADEL_AI_RERANKER_DIR to enable)");
            "none".to_string()
        }
    };

    let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| "OPENAI_API_KEY not set")?;
    let reader_model =
        std::env::var("LOCOMO_READER_MODEL").unwrap_or_else(|_| DEFAULT_READER_MODEL.to_string());
    let judge_model =
        std::env::var("LOCOMO_JUDGE_MODEL").unwrap_or_else(|_| DEFAULT_JUDGE_MODEL.to_string());
    let reader: Arc<dyn LLMClient> = Arc::new(OpenAiClient::new(&reader_model, api_key.clone()));
    let judge: Arc<dyn LLMClient> = if judge_model == reader_model {
        Arc::clone(&reader)
    } else {
        Arc::new(OpenAiClient::new(&judge_model, api_key))
    };
    eprintln!("reader: {reader_model}  judge: {judge_model}");

    // Per-model TPM pacing keeps submissions under the OpenAI limit so a burst can't
    // trigger a 429 storm. Override via LOCOMO_READER_TPM / LOCOMO_JUDGE_TPM.
    const DEFAULT_READER_TPM: u64 = 30_000;
    const DEFAULT_JUDGE_TPM: u64 = 1_000_000;
    let reader_tpm = std::env::var("LOCOMO_READER_TPM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_READER_TPM);
    let judge_tpm = std::env::var("LOCOMO_JUDGE_TPM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_JUDGE_TPM);
    let pacer = citadel_membench::Pacer::new(&reader_model, reader_tpm, &judge_model, judge_tpm);

    // LOCOMO_LIVE_TRACE=path writes one JSON line per question (a plain File, so each
    // writeln is a direct, tailable syscall).
    let mut live_trace = std::env::var("LOCOMO_LIVE_TRACE")
        .ok()
        .map(std::fs::File::create)
        .transpose()?;
    let total_q: usize = samples.iter().map(|s| s.qa.len()).sum();
    let mut prog = LiveProgress::new(total_q);

    let mut results = Vec::new();
    for (i, sample) in samples.iter().enumerate() {
        eprintln!(
            "[{}/{}] {} starting",
            i + 1,
            samples.len(),
            sample.sample_id
        );
        let conv_id = sample.sample_id.clone();
        let rs = run_sample_observed(
            &eng,
            sample,
            Arc::clone(&embedder),
            reader.as_ref(),
            judge.as_ref(),
            config.top_k,
            &pacer,
            &mut |r| prog.observe(r, &conv_id, live_trace.as_mut()),
        )?;
        results.extend(rs);
    }

    let mut prov = provenance(
        &reader_model,
        &judge_model,
        embedder.model_id(),
        config,
        format!("{} ({} conversations)", dataset_path, samples.len()),
        dataset_sha256,
    );
    prov.reranker_model = reranker_model;
    let report = aggregate(&results, prov);

    // Per-question audit trail (question, gold, predicted, verdict) for spot-checking.
    if let Ok(path) = std::env::var("LOCOMO_AUDIT_PATH") {
        std::fs::write(&path, serde_json::to_string_pretty(&results)?)?;
        eprintln!(
            "per-question audit ({} questions) written to {path}",
            results.len()
        );
    }

    println!("{}", serde_json::to_string_pretty(&report)?);
    print_summary(&report);
    Ok(())
}

/// Live running tally (per-category correct/total), printed and optionally streamed
/// to a JSONL trace as each question scores.
struct LiveProgress {
    total: usize,
    done: usize,
    cat: BTreeMap<&'static str, (usize, usize)>,
    adv_total: usize,
    adv_abstained: usize,
}

impl LiveProgress {
    fn new(total: usize) -> Self {
        Self {
            total,
            done: 0,
            cat: BTreeMap::new(),
            adv_total: 0,
            adv_abstained: 0,
        }
    }

    fn observe(
        &mut self,
        r: &QuestionResult,
        conv_id: &str,
        trace: Option<&mut std::fs::File>,
    ) -> citadel_membench::Result<()> {
        self.done += 1;
        if r.category == Category::Adversarial {
            self.adv_total += 1;
            if r.correct {
                self.adv_abstained += 1;
            }
        } else if r.scorable {
            let e = self.cat.entry(r.category.label()).or_insert((0, 0));
            e.1 += 1;
            if r.correct {
                e.0 += 1;
            }
        }

        // One JSON line per question (direct, tail-able write).
        if let Some(w) = trace {
            let line = serde_json::json!({
                "conv": conv_id,
                "category": r.category.label(),
                "scorable": r.scorable,
                "correct": r.correct,
                "question": r.question,
                "gold": r.gold,
                "predicted": r.predicted,
                "retrieved": r.retrieved,
                "gold_evidence": r.gold_evidence,
            });
            writeln!(w, "{line}")?;
        }

        // Running table every 25 questions (and on the last).
        if self.done % 25 == 0 || self.done == self.total {
            let (mut c, mut t) = (0usize, 0usize);
            let mut parts = Vec::new();
            for (label, (ok, n)) in &self.cat {
                c += ok;
                t += n;
                parts.push(format!("{label} {:.0}%({ok}/{n})", pct(*ok, *n)));
            }
            eprintln!(
                "  [{}/{} done] overall {:.1}%({c}/{t})  {}  adv-abstain {:.0}%({}/{})",
                self.done,
                self.total,
                pct(c, t),
                parts.join("  "),
                pct(self.adv_abstained, self.adv_total),
                self.adv_abstained,
                self.adv_total
            );
        }
        Ok(())
    }
}

/// Parse `LOCOMO_RERANK_STRATEGY` (replace|rrf, default rrf) into a strategy.
fn rerank_strategy_from_env() -> RerankStrategy {
    match std::env::var("LOCOMO_RERANK_STRATEGY")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "replace" => RerankStrategy::Replace,
        _ => RerankStrategy::Rrf { k: 60.0 },
    }
}

/// Token-free evidence recall@k, computed four ways to localize the lossy layer:
///   A: exact cosine over raw bge (the embedder ceiling).
///   B: semantic-only recall (citadel-vector PRISM).
///   C: default fusion recall (+keyword/recency/score).
///   D: fusion + cross-encoder reranker (the order the reader sees).
/// Deltas localize the fault: low A = representation; A>>B = vector; B>>C = fusion.
fn run_retrieval_diag(
    eng: &mut MemoryEngine,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
) -> Result<(), Box<dyn Error>> {
    const KS: [usize; 3] = [10, 30, 50];
    const MAX_K: usize = 50;
    // D is only populated when a reranker dir is set; its name reflects the strategy.
    let rr_dir = std::env::var("CITADEL_AI_RERANKER_DIR").ok();
    let d_name = match &rr_dir {
        Some(_) => format!(
            "D: fusion + reranker {:?} (the order the reader sees)",
            rerank_strategy_from_env()
        ),
        None => "D: (skipped; set CITADEL_AI_RERANKER_DIR to measure)".to_string(),
    };
    let mode_names = [
        "A: exact-cosine (bge ceiling, no citadel)".to_string(),
        "B: semantic-only via recall (citadel-vector PRISM)".to_string(),
        "C: default fusion via recall (citadel-mem)".to_string(),
        d_name,
    ];
    let labels = ["multi_hop", "temporal", "open_domain", "single_hop"];
    let semantic_only = FusionWeights {
        semantic: 1.0,
        keyword: 0.0,
        recency: 0.0,
        importance: 0.0,
    };

    // Per mode: category label -> ([hit@KS[0], hit@KS[1], hit@KS[2]], measured_count).
    let mut acc: Vec<BTreeMap<&str, ([usize; 3], usize)>> =
        mode_names.iter().map(|_| BTreeMap::new()).collect();

    // Pass 1: A/B/C, with NO reranker attached (so C is pure fusion).
    for s in samples {
        citadel_membench::create_bench_region(eng, &s.sample_id, Arc::clone(&embedder))?;
        ingest_sample(eng, &s.sample_id, s)?;

        // Embed every turn once for the exact-cosine ground truth (mode A).
        let turn_texts: Vec<&str> = s.turns.iter().map(|t| t.text.as_str()).collect();
        let turn_embs = embedder.embed(&turn_texts)?;
        let turn_dia: Vec<&str> = s.turns.iter().map(|t| t.dia_id.as_str()).collect();

        // Embed every scored question ONCE in a single batch (GPU-efficient), then reuse
        // the vector across A/B/C via by_embedding so no recall re-embeds the query.
        let scored_qa: Vec<_> =
            s.qa.iter()
                .filter(|qa| qa.category != Category::Adversarial && !qa.evidence.is_empty())
                .collect();
        let q_texts: Vec<&str> = scored_qa.iter().map(|qa| qa.question.as_str()).collect();
        let q_embs = embedder.embed(&q_texts)?;

        for (qi, &qa) in scored_qa.iter().enumerate() {
            let label = qa.category.label();
            let q_emb = &q_embs[qi];

            // A: exact cosine over raw bge embeddings (no citadel-vector/mem).
            let mut scored: Vec<(f32, &str)> = turn_embs
                .iter()
                .zip(&turn_dia)
                .map(|(e, d)| (cosine(q_emb, e), *d))
                .collect();
            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            let a_ranked: Vec<&str> = scored.iter().map(|(_, d)| *d).collect();
            record(&mut acc[0], label, &a_ranked, &qa.evidence, KS);

            // B: semantic-only through recall (reuse the embedding; keyword inert here).
            let b = eng.recall(
                &s.sample_id,
                RecallQuery::by_embedding(q_emb.clone(), MAX_K)
                    .with_text(qa.question.as_str())
                    .with_weights(semantic_only),
            )?;
            record(&mut acc[1], label, &hit_dia_ids(&b), &qa.evidence, KS);

            // C: default fusion (reuse the embedding; text drives the keyword signal).
            let c = eng.recall(
                &s.sample_id,
                RecallQuery::by_embedding(q_emb.clone(), MAX_K).with_text(qa.question.as_str()),
            )?;
            record(&mut acc[2], label, &hit_dia_ids(&c), &qa.evidence, KS);
        }
    }

    // Pass 2 (mode D): attach the reranker and re-recall, isolating rerank from fusion.
    if let Some(dir) = &rr_dir {
        let ce = CrossEncoder::ms_marco_minilm_l6(dir)?;
        eng.set_reranker(Arc::new(ce), rerank_strategy_from_env());
        for s in samples {
            let scored_qa: Vec<_> =
                s.qa.iter()
                    .filter(|qa| qa.category != Category::Adversarial && !qa.evidence.is_empty())
                    .collect();
            let q_texts: Vec<&str> = scored_qa.iter().map(|qa| qa.question.as_str()).collect();
            let q_embs = embedder.embed(&q_texts)?;
            for (qi, &qa) in scored_qa.iter().enumerate() {
                let d = eng.recall(
                    &s.sample_id,
                    RecallQuery::by_embedding(q_embs[qi].clone(), MAX_K)
                        .with_text(qa.question.as_str()),
                )?;
                record(
                    &mut acc[3],
                    qa.category.label(),
                    &hit_dia_ids(&d),
                    &qa.evidence,
                    KS,
                );
            }
        }
    }

    eprintln!(
        "\n=== layered retrieval diagnostic: evidence recall@{}/{}/{} (pinpoints the lossy layer) ===",
        KS[0], KS[1], KS[2]
    );
    for (mi, name) in mode_names.iter().enumerate() {
        if mi == 3 && rr_dir.is_none() {
            continue;
        }
        eprintln!("\n[{name}]");
        let mut tot = [0usize; 3];
        let mut tot_n = 0usize;
        for label in labels {
            if let Some((h, n)) = acc[mi].get(label) {
                eprintln!(
                    "  {label:>12} (n={n}): @{}={:.1}%  @{}={:.1}%  @{}={:.1}%",
                    KS[0],
                    pct(h[0], *n),
                    KS[1],
                    pct(h[1], *n),
                    KS[2],
                    pct(h[2], *n)
                );
                for ki in 0..3 {
                    tot[ki] += h[ki];
                }
                tot_n += n;
            }
        }
        eprintln!(
            "  {:>12} (n={tot_n}): @{}={:.1}%  @{}={:.1}%  @{}={:.1}%",
            "OVERALL",
            KS[0],
            pct(tot[0], tot_n),
            KS[1],
            pct(tot[1], tot_n),
            KS[2],
            pct(tot[2], tot_n)
        );
    }
    Ok(())
}

/// Inspect stored atoms: ingest each conversation and print the raw rows, to verify
/// what the DB actually holds (text + caption/query markers).
fn run_db_dump(
    eng: &MemoryEngine,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
) -> Result<(), Box<dyn Error>> {
    if samples.is_empty() {
        return Err("no samples to dump".into());
    }

    // Markers confirming ingest folded the caption + image-search query into the text.
    let cap_marker = "[shared a photo:";
    let qry_marker = "[image search:";
    let (mut tot_turns, mut tot_ingested, mut tot_atoms) = (0usize, 0usize, 0usize);
    let (mut tot_caption, mut tot_query) = (0usize, 0usize);

    // One region per conversation: dump every conversation's ingested atoms.
    for s in samples {
        citadel_membench::create_bench_region(eng, &s.sample_id, Arc::clone(&embedder))?;
        let ids = ingest_sample(eng, &s.sample_id, s)?;
        let atoms = eng.fetch(&s.sample_id, "turn", None, 100_000)?;

        let with_caption = atoms.iter().filter(|a| a.text.contains(cap_marker)).count();
        let with_query = atoms.iter().filter(|a| a.text.contains(qry_marker)).count();
        tot_turns += s.turns.len();
        tot_ingested += ids.len();
        tot_atoms += atoms.len();
        tot_caption += with_caption;
        tot_query += with_query;

        eprintln!("\n=== DB dump: region {} ===", s.sample_id);
        eprintln!(
            "turns={}  ingested={}  fetched={}",
            s.turns.len(),
            ids.len(),
            atoms.len()
        );
        eprintln!(
            "indexed signals (over ALL {} atoms): caption {with_caption} ({:.0}%)  query {with_query} ({:.0}%)",
            atoms.len(),
            pct(with_caption, atoms.len()),
            pct(with_query, atoms.len())
        );

        // A compact aligned table (truncated), then full untruncated text below so the
        // caption/query markers are visible.
        let show = atoms.len().min(15);
        eprintln!("\nfirst {show} stored atoms:");
        eprintln!(
            "{:>5}  {:>4}  {:>3}  {:>3}  {:<14}  text (truncated to 70)",
            "id", "sess", "cap", "qry", "speaker/dia"
        );
        eprintln!("{}", "-".repeat(100));
        for a in &atoms[..show] {
            let session = a
                .payload
                .get("session")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let dia = a
                .payload
                .get("dia_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            eprintln!(
                "{:>5}  {:>4}  {:>3}  {:>3}  {:<14}  {}",
                a.id,
                session,
                if a.text.contains(cap_marker) {
                    "Y"
                } else {
                    "-"
                },
                if a.text.contains(qry_marker) {
                    "Y"
                } else {
                    "-"
                },
                truncate(dia, 14),
                truncate(&a.text, 70)
            );
        }

        // Full text of the first 5 caption/query atoms, so the markers are fully visible.
        eprintln!("\nfull text of first atoms carrying a caption or query marker:");
        for a in atoms
            .iter()
            .filter(|a| a.text.contains(cap_marker) || a.text.contains(qry_marker))
            .take(5)
        {
            eprintln!("  [id={}] {}", a.id, a.text);
        }
    }

    eprintln!("\n=== DB dump: all {} conversations ===", samples.len());
    eprintln!(
        "turns={tot_turns}  ingested={tot_ingested}  fetched={tot_atoms}  \
         caption {tot_caption} ({:.0}%)  query {tot_query} ({:.0}%)",
        pct(tot_caption, tot_atoms),
        pct(tot_query, tot_atoms)
    );
    Ok(())
}

/// Token-free encryption + erasure verification on one real conversation: ingest into
/// an ENCRYPTED region (per-atom sealed), prove PRISM-over-sealed recall, then exercise
/// per-atom (`forget_atom`) and per-region (`drop_region`) erasure, printing the on-disk
/// key-store sidecar bytes before/after each so the destruction is visible.
fn run_erasure_demo(
    eng: &MemoryEngine,
    db: Arc<citadel::Database>,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
) -> Result<(), Box<dyn Error>> {
    let s = samples.first().ok_or("no samples for erasure demo")?;
    let region = s.sample_id.as_str();
    let atom_sidecar = db.atom_store_path();
    let region_sidecar = db.region_store_path();
    let len = |p: &std::path::Path| std::fs::read(p).unwrap_or_default().len();
    let bytes = |p: &std::path::Path| std::fs::read(p).unwrap_or_default();

    eprintln!("\n=== ERASURE DEMO: region '{region}' (ENCRYPTED, per-atom sealed) ===");
    eng.create_encrypted_region(region, Arc::clone(&embedder))?;
    let ids = ingest_sample(eng, region, s)?;
    eprintln!(
        "ingested {} atoms (each sealed under its own ACK)\n  atomkeys sidecar:  {} = {} bytes\n  regions sidecar:   {} = {} bytes",
        ids.len(),
        atom_sidecar.display(),
        len(&atom_sidecar),
        region_sidecar.display(),
        len(&region_sidecar)
    );

    // PRISM over sealed content: a real turn must surface through decrypt-then-rank.
    let needle = &s.turns[s.turns.len() / 2];
    let needle_text = format!("{}: {}", needle.speaker, needle.text);
    let hits = eng.recall(region, RecallQuery::by_text(&needle_text, 3))?;
    let target = hits
        .first()
        .map(|h| h.id)
        .ok_or("recall returned no hits")?;
    eprintln!(
        "\n[PRISM over sealed] recall(\"{}\") -> {} hits; top id={target} \"{}\"",
        truncate(&needle_text, 40),
        hits.len(),
        truncate(&hits[0].text, 50)
    );

    // Per-atom erasure: forget the top hit, show recall drops it + the ACK slot changes.
    let before = bytes(&atom_sidecar);
    eprintln!(
        "\n[per-atom erasure] forgetting atom {target} (present_before={})",
        eng.fetch_one(region, target)?.is_some()
    );
    eng.forget_atom(region, target)?;
    let after = bytes(&atom_sidecar);
    let recalled_again = eng
        .recall(region, RecallQuery::by_text(&needle_text, 5))?
        .iter()
        .any(|h| h.id == target);
    eprintln!(
        "  atomkeys sidecar overwritten={} ({} -> {} bytes)\n  fetch_one(target)={:?}  recalled_again={recalled_again}",
        before != after,
        before.len(),
        after.len(),
        eng.fetch_one(region, target)?.map(|a| a.text)
    );
    if let Some(sib) = ids.iter().copied().find(|&i| i != target) {
        eprintln!(
            "  sibling atom {sib} survives={}",
            eng.fetch_one(region, sib)?.is_some()
        );
    }

    // Per-region erasure: drop the region, show recall empties + the RCK slot changes.
    let rbefore = bytes(&region_sidecar);
    eng.drop_region(region)?;
    let rafter = bytes(&region_sidecar);
    eng.create_encrypted_region(region, Arc::clone(&embedder))?;
    let after_drop = eng.recall(region, RecallQuery::by_text(&needle_text, 5))?;
    eprintln!(
        "\n[per-region erasure] drop_region('{region}')\n  regions sidecar overwritten={} ({} -> {} bytes)\n  re-create by name -> recall returns {} hits (content unrecoverable)",
        rbefore != rafter,
        rbefore.len(),
        rafter.len(),
        after_drop.len()
    );
    eprintln!("\n=== ERASURE DEMO complete ===");
    Ok(())
}

/// Truncate `s` to `max` chars with an ellipsis, on a char boundary (no panic).
fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}...")
}

/// Cosine similarity, computed fully (robust regardless of normalization).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..a.len().min(b.len()) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

/// Record whether any gold evidence dia_id lands in the top-{KS} of `ranked`.
fn record(
    acc: &mut BTreeMap<&str, ([usize; 3], usize)>,
    label: &'static str,
    ranked: &[&str],
    evidence: &[String],
    ks: [usize; 3],
) {
    let entry = acc.entry(label).or_insert(([0; 3], 0));
    entry.1 += 1;
    for (ki, &k) in ks.iter().enumerate() {
        let top = &ranked[..k.min(ranked.len())];
        if evidence.iter().any(|e| top.contains(&e.as_str())) {
            entry.0[ki] += 1;
        }
    }
}

fn hit_dia_ids(hits: &[AtomHit]) -> Vec<&str> {
    hits.iter()
        .filter_map(|h| h.payload.get("dia_id").and_then(|v| v.as_str()))
        .collect()
}

fn pct(a: usize, b: usize) -> f64 {
    if b == 0 {
        0.0
    } else {
        100.0 * a as f64 / b as f64
    }
}

fn print_dataset_stats(samples: &[Sample], path: &str, sha: &str) {
    let total_qa: usize = samples.iter().map(|s| s.qa.len()).sum();
    let mut by_cat: FxHashMap<&str, usize> = FxHashMap::default();
    for s in samples {
        for q in &s.qa {
            *by_cat.entry(q.category.label()).or_insert(0) += 1;
        }
    }
    eprintln!("=== dataset ===");
    eprintln!("path: {path}");
    eprintln!("sha256: {sha}");
    eprintln!("conversations: {}  questions: {total_qa}", samples.len());
    let mut cats: Vec<_> = by_cat.into_iter().collect();
    cats.sort_by(|a, b| a.0.cmp(b.0));
    for (label, n) in cats {
        eprintln!("  {label:>12}: {n}");
    }
}

fn print_summary(report: &citadel_membench::BenchReport) {
    eprintln!("\n=== LoCoMo summary ===");
    eprintln!(
        "overall (scored cats): {:.1}% ({}/{})",
        report.overall_accuracy * 100.0,
        report.overall_correct,
        report.overall_total
    );
    let mut cats: Vec<_> = report.per_category.iter().collect();
    cats.sort_by(|a, b| a.0.cmp(b.0));
    for (label, stats) in cats {
        eprintln!(
            "  {label:>12}: {:.1}% ({}/{})",
            stats.accuracy * 100.0,
            stats.correct,
            stats.total
        );
    }
    eprintln!(
        "adversarial abstention (secondary): {:.1}% ({} questions)",
        report.adversarial_abstention * 100.0,
        report.adversarial_total
    );
    if report.unscorable_total > 0 {
        eprintln!("unscorable (empty gold key): {}", report.unscorable_total);
    }
    eprintln!("recall p95: {} us", report.recall_p95_micros);
    eprintln!(
        "tokens: in {} / out {}  est cost ~${:.4}",
        report.total_input_tokens, report.total_output_tokens, report.estimated_cost_usd
    );
}
