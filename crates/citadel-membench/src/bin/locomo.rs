//! LoCoMo scored evaluation and offline diagnostics. See `run.ps1` and
//! `benchmarks::locomo::config` for validated modes and parameters.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;

use citadel_mem::{
    AtomHit, CrossEncoder, Embedder, FusionWeights, MemoryEngine, RecallQuery, RerankStrategy,
    Reranker,
};
use citadel_membench::benchmarks::locomo::config::{RunConfig, RunMode};
use citadel_membench::core::retrieval::{baseline_recall, validate_embeddings};
use citadel_membench::{
    aggregate, ingest_sample, provenance, run_sample_observed, turn_content, Category,
    QuestionEvent, QuestionResult, Sample,
};
use rustc_hash::FxHashMap;

/// Reader generates answers, judge scores them (distinct roles, may differ).
/// Override via CITADEL_LOCOMO_READER_MODEL / CITADEL_LOCOMO_JUDGE_MODEL; both
/// are pinned in Provenance.
const DEFAULT_READER_MODEL: &str = "gpt-4o-mini";
const DEFAULT_JUDGE_MODEL: &str = "gpt-4o-mini";

fn main() -> Result<(), Box<dyn Error>> {
    let dataset_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("CITADEL_LOCOMO_DATASET").ok())
        .ok_or("dataset path required: argv[1] or CITADEL_LOCOMO_DATASET")?;

    let launch = RunConfig::from_env()?;
    if launch.mode == RunMode::Scored {
        citadel_membench::core::config::preflight_llm(
            "CITADEL_LOCOMO_READER",
            DEFAULT_READER_MODEL,
        )?;
        citadel_membench::core::config::preflight_llm("CITADEL_LOCOMO_JUDGE", DEFAULT_JUDGE_MODEL)?;
    }
    let config = launch.bench;
    let (mut samples, dataset_sha256) = citadel_membench::load_with_hash(&dataset_path)?;

    if let Some(n) = launch.max_samples {
        samples.truncate(n);
    }
    if samples.is_empty() {
        return Err("dataset contains no selected conversations".into());
    }
    print_dataset_stats(&samples, &dataset_path, &dataset_sha256);
    if launch.mode == RunMode::DryRun {
        eprintln!("dry run: dataset parsed OK, no models loaded.");
        return Ok(());
    }
    let scoring_clients = if launch.mode == RunMode::Scored {
        let reader =
            citadel_llm::factory::from_env("CITADEL_LOCOMO_READER", "openai", DEFAULT_READER_MODEL)
                .map_err(|e| format!("reader LLM: {e}"))?;
        let judge =
            citadel_llm::factory::from_env("CITADEL_LOCOMO_JUDGE", "openai", DEFAULT_JUDGE_MODEL)
                .map_err(|e| format!("judge LLM: {e}"))?;
        Some((reader, judge))
    } else {
        None
    };
    let reranker = if matches!(launch.mode, RunMode::Dump | RunMode::Erasure) {
        None
    } else {
        match std::env::var("CITADEL_RERANKER_DIR") {
            Ok(dir) => Some(Arc::new(
                CrossEncoder::ms_marco_minilm_l6(&dir)
                    .map_err(|e| format!("reranker model: {e}"))?,
            )),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => return Err("CITADEL_RERANKER_DIR must be Unicode text".into()),
        }
    };
    let embedder: Arc<dyn Embedder> = if launch.mode == RunMode::Dump {
        Arc::new(citadel_mem::MockEmbedder::new(384))
    } else {
        let model_dir =
            std::env::var("CITADEL_EMBEDDER_DIR").map_err(|_| "CITADEL_EMBEDDER_DIR not set")?;
        Arc::new(launch.embedder.load(&model_dir)?)
    };

    let encrypted = launch.encrypted;
    let bench_db = citadel_membench::open_bench_db("CITADEL_LOCOMO_DB_PATH", encrypted)?;
    let db = Arc::clone(&bench_db.db);
    eprintln!(
        "db: {} (reuse={}, encrypted_regions={encrypted})",
        bench_db.path.display(),
        bench_db.reuse
    );
    let mut eng = MemoryEngine::open(db.clone())?;
    match launch.mode {
        RunMode::Dump => return run_db_dump(&eng, &samples, embedder),
        RunMode::RetrievalDiag => {
            return run_retrieval_diag(
                &mut eng,
                &samples,
                embedder,
                launch.rerank_strategy,
                config.top_k,
                reranker,
            );
        }
        RunMode::ParamSweep => {
            return run_param_sweep(&mut eng, &samples, embedder, config.top_k, reranker)
        }
        RunMode::Erasure => return run_erasure_demo(&eng, db, &samples, embedder),
        RunMode::Scored => {}
        RunMode::DryRun => unreachable!("dry-run returned before database creation"),
    }

    let reranker_model = match reranker {
        Some(ce) => {
            let model = ce.model_id().to_string();
            let strategy = launch.rerank_strategy;
            eng.set_reranker(ce, strategy);
            eprintln!("reranker: {model} strategy={strategy:?}");
            format!("{model} ({strategy:?})")
        }
        None => {
            eprintln!("reranker: none (set CITADEL_RERANKER_DIR to enable)");
            "none".to_string()
        }
    };
    let (reader, judge) = scoring_clients.ok_or("scored mode requires reader and judge clients")?;
    let reader_model = reader.model_id().to_string();
    let judge_model = judge.model_id().to_string();
    eprintln!("reader: {reader_model}  judge: {judge_model}");

    let pacer = citadel_membench::Pacer::new(
        &reader_model,
        launch.reader_tpm,
        &judge_model,
        launch.judge_tpm,
    );

    // CITADEL_LOCOMO_LIVE_TRACE=path writes one JSON line per question (a plain
    // File, so each writeln is a direct, tailable syscall).
    let mut live_trace = std::env::var("CITADEL_LOCOMO_LIVE_TRACE")
        .ok()
        .map(std::fs::File::create_new)
        .transpose()?;
    let mut audit_file = std::env::var("CITADEL_LOCOMO_AUDIT_PATH")
        .ok()
        .map(std::fs::File::create_new)
        .transpose()?;
    let receipt_path = std::env::var("CITADEL_LOCOMO_AUDIT_PATH")
        .or_else(|_| std::env::var("CITADEL_LOCOMO_LIVE_TRACE"))
        .map(|path| format!("{path}.events.jsonl"))
        .unwrap_or_else(|_| "locomo-events.jsonl".into());
    let mut receipts = std::fs::File::create_new(&receipt_path)?;
    eprintln!("question receipts: {receipt_path}");
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
        let rs = run_sample_observed(
            &eng,
            sample,
            Arc::clone(&embedder),
            reader.as_ref(),
            judge.as_ref(),
            config,
            bench_db.reuse,
            &pacer,
            &mut |event| match event {
                QuestionEvent::Completed(result) => {
                    prog.observe(result, live_trace.as_mut())?;
                    result.completion_receipt().write_json_line(&mut receipts)
                }
                QuestionEvent::Failed(failure) => failure.write_json_line(&mut receipts),
            },
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

    // Per-question audit trail (question, gold, predicted, verdict) for
    // spot-checking.
    if let Some(mut audit) = audit_file.take() {
        serde_json::to_writer_pretty(&mut audit, &results)?;
        audit.flush()?;
        eprintln!("per-question audit: {} questions", results.len());
    }

    println!("{}", serde_json::to_string_pretty(&report)?);
    print_summary(&report);
    Ok(())
}

/// Live running tally (per-category correct/total), printed and optionally
/// streamed to a JSONL trace as each question scores.
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
                "conv": r.sample_id,
                "qa_index": r.qa_index,
                "category": r.category.label(),
                "scorable": r.scorable,
                "correct": r.correct,
                "question": r.question,
                "gold": r.gold,
                "predicted": r.predicted,
                "reader_finish_reasons": r.reader_finish_reasons,
                "reader_calls": r.reader_calls,
                "judge": r.judge,
                "unknown_usage_attempts": r.unknown_usage_attempts,
                "retrieved": r.retrieved,
                "retrieved_atom_ids": r.retrieved_atom_ids,
                "gold_evidence": r.gold_evidence,
                "gold_turn_texts": r.gold_turn_texts,
                "gold_in_view": r.gold_in_view,
            });
            writeln!(w, "{line}")?;
            w.flush()?;
        }

        // Running table every 25 questions (and on the last).
        if self.done.is_multiple_of(25) || self.done == self.total {
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

/// Token-free evidence recall@k per layer, to localize the lossy stage: A exact
/// cosine (exact ranking), B semantic-only PRISM, C default fusion,
/// C-asof fusion graded at conversation end, D/D-asof + cross-encoder reranker.
/// Each cell reports any-evidence and all-evidence recall.
fn run_retrieval_diag(
    eng: &mut MemoryEngine,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
    strategy: RerankStrategy,
    top_k: usize,
    reranker: Option<Arc<CrossEncoder>>,
) -> Result<(), Box<dyn Error>> {
    let ks = [10.min(top_k), 30.min(top_k), top_k];
    // D rows are only populated when a reranker dir is set; names pin the
    // strategy.
    let (d_name, d_asof_name) = match &reranker {
        Some(_) => (
            format!("D: fusion + reranker {strategy:?} (retrieval rank before reader ordering)"),
            format!("D-asof: fusion as of conversation end + reranker {strategy:?}"),
        ),
        None => (
            "D: (skipped; set CITADEL_RERANKER_DIR to measure)".to_string(),
            "D-asof: (skipped; set CITADEL_RERANKER_DIR to measure)".to_string(),
        ),
    };
    let mode_names = [
        "A: exact-cosine reference over the indexed text".to_string(),
        "B: semantic-only via recall (citadel-vector PRISM)".to_string(),
        "C: default fusion via recall (citadel-mem)".to_string(),
        "C-asof: default fusion as of conversation end".to_string(),
        d_name,
        d_asof_name,
    ];
    let labels = ["multi_hop", "temporal", "open_domain", "single_hop"];
    let semantic_only = FusionWeights {
        semantic: 1.0,
        keyword: 0.0,
        recency: 0.0,
        importance: 0.0,
    };

    let mut acc: Vec<BTreeMap<&str, Tally>> = mode_names.iter().map(|_| BTreeMap::new()).collect();

    // Pass 1: A/B/C/C-asof, with NO reranker attached (so C is pure fusion).
    for s in samples {
        citadel_membench::create_bench_region(eng, &s.sample_id, Arc::clone(&embedder))?;
        ingest_sample(eng, &s.sample_id, s)?;
        let as_of = s.as_of_micros();

        // Embed each turn's indexed text once for the exact-cosine reference
        // (mode A).
        let turn_texts: Vec<String> = s.turns.iter().map(turn_content).collect();
        let turn_refs: Vec<&str> = turn_texts.iter().map(String::as_str).collect();
        let turn_embs = embedder.embed(&turn_refs)?;
        validate_embeddings(&turn_embs, turn_refs.len(), embedder.dim())?;
        let turn_dia: Vec<&str> = s.turns.iter().map(|t| t.dia_id.as_str()).collect();

        // Embed every scored question once in a single batch (GPU-efficient) on
        // the query side (asymmetric models prefix here), then reuse the vector
        // across modes via by_embedding so no recall re-embeds the query.
        let scored_qa: Vec<_> = s.qa.iter().filter(|qa| qa.has_scored_evidence()).collect();
        let q_texts: Vec<&str> = scored_qa.iter().map(|qa| qa.question.as_str()).collect();
        let q_embs = embedder.embed_queries(&q_texts)?;
        validate_embeddings(&q_embs, q_texts.len(), embedder.dim())?;

        for (qi, &qa) in scored_qa.iter().enumerate() {
            let label = qa.category.label();
            let q_emb = &q_embs[qi];

            // A: exact cosine over the same embeddings the index holds.
            let mut scored: Vec<(f64, &str)> = turn_embs
                .iter()
                .zip(&turn_dia)
                .map(|(e, d)| (cosine(q_emb, e), *d))
                .collect();
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
            let a_ranked: Vec<&str> = scored.iter().map(|(_, d)| *d).collect();
            record(&mut acc[0], label, &a_ranked, &qa.evidence, ks);

            // B: semantic-only recall (reuse the embedding; keyword inert
            // here).
            let b = eng.recall(
                &s.sample_id,
                RecallQuery::by_embedding(q_emb.clone(), top_k)
                    .with_text(qa.question.as_str())
                    .with_weights(semantic_only),
            )?;
            record(&mut acc[1], label, &hit_dia_ids(&b), &qa.evidence, ks);

            // C: default fusion (reuse the embedding; text drives the keyword
            // signal).
            let c_query =
                RecallQuery::by_embedding(q_emb.clone(), top_k).with_text(qa.question.as_str());
            let c = baseline_recall(eng, &s.sample_id, c_query.clone())?;
            record(&mut acc[2], label, &hit_dia_ids(&c), &qa.evidence, ks);

            // C-asof: identical query graded as of the conversation's end.
            if let Some(t) = as_of {
                let ca = baseline_recall(eng, &s.sample_id, c_query.with_as_of(t))?;
                record(&mut acc[3], label, &hit_dia_ids(&ca), &qa.evidence, ks);
            }
        }
    }

    // Pass 2 (D rows): attach the reranker and re-recall, isolating rerank from
    // fusion.
    if let Some(ce) = &reranker {
        eng.set_reranker(ce.clone(), strategy);
        for s in samples {
            let as_of = s.as_of_micros();
            let scored_qa: Vec<_> = s.qa.iter().filter(|qa| qa.has_scored_evidence()).collect();
            let q_texts: Vec<&str> = scored_qa.iter().map(|qa| qa.question.as_str()).collect();
            let q_embs = embedder.embed_queries(&q_texts)?;
            validate_embeddings(&q_embs, q_texts.len(), embedder.dim())?;
            for (qi, &qa) in scored_qa.iter().enumerate() {
                let d_query = RecallQuery::by_embedding(q_embs[qi].clone(), top_k)
                    .with_text(qa.question.as_str());
                let d = baseline_recall(eng, &s.sample_id, d_query.clone())?;
                record(
                    &mut acc[4],
                    qa.category.label(),
                    &hit_dia_ids(&d),
                    &qa.evidence,
                    ks,
                );
                if let Some(t) = as_of {
                    let da = baseline_recall(eng, &s.sample_id, d_query.with_as_of(t))?;
                    record(
                        &mut acc[5],
                        qa.category.label(),
                        &hit_dia_ids(&da),
                        &qa.evidence,
                        ks,
                    );
                }
            }
        }
    }
    eprintln!(
        "\n=== layered retrieval diagnostic: evidence recall@{}/{}/{} as any%/all% ===",
        ks[0], ks[1], ks[2]
    );
    for (mi, name) in mode_names.iter().enumerate() {
        if mi >= 4 && reranker.is_none() {
            continue;
        }
        eprintln!("\n[{name}]");
        let mut tot = Tally::default();
        for label in labels {
            if let Some(t) = acc[mi].get(label) {
                eprintln!("  {label:>12} (n={:>4}): {}", t.n, t.cells(ks));
                for ki in 0..ks.len() {
                    tot.any[ki] += t.any[ki];
                    tot.all[ki] += t.all[ki];
                }
                tot.n += t.n;
            }
        }
        eprintln!("  {:>12} (n={:>4}): {}", "OVERALL", tot.n, tot.cells(ks));
    }
    Ok(())
}

/// One conversation's cached sweep inputs: region name plus per-question
/// `(query embedding, question text, gold evidence)`.
type SweepCase = (String, Vec<(Vec<f32>, String, Vec<String>)>);

/// Recall every cached question under `w` and tally overall any/all evidence
/// recall.
fn sweep_combo(
    eng: &MemoryEngine,
    cases: &[SweepCase],
    w: FusionWeights,
    ks: [usize; 3],
    max_k: usize,
) -> Result<Tally, Box<dyn Error>> {
    let mut t = Tally::default();
    for (region, questions) in cases {
        for (emb, question, evidence) in questions {
            let hits = eng.recall(
                region,
                RecallQuery::by_embedding(emb.clone(), max_k)
                    .with_text(question.as_str())
                    .with_weights(w),
            )?;
            t.record(&hit_dia_ids(&hits), evidence, ks);
        }
    }
    Ok(t)
}

/// Token-free retrieval parameter sweep: phase 1 sweeps the semantic:keyword
/// ratio with no reranker (the ratio is the whole linear stage here), phase 2
/// crosses the two best ratios with RRF constants and Replace. Embeddings
/// computed once and reused; combos ranked by overall any@50.
fn run_param_sweep(
    eng: &mut MemoryEngine,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
    top_k: usize,
    reranker: Option<Arc<CrossEncoder>>,
) -> Result<(), Box<dyn Error>> {
    let ks = [10.min(top_k), 30.min(top_k), top_k];

    let mut cases: Vec<SweepCase> = Vec::new();
    for s in samples {
        citadel_membench::create_bench_region(eng, &s.sample_id, Arc::clone(&embedder))?;
        ingest_sample(eng, &s.sample_id, s)?;
        let scored_qa: Vec<_> = s.qa.iter().filter(|qa| qa.has_scored_evidence()).collect();
        let q_texts: Vec<&str> = scored_qa.iter().map(|qa| qa.question.as_str()).collect();
        let q_embs = embedder.embed_queries(&q_texts)?;
        validate_embeddings(&q_embs, q_texts.len(), embedder.dim())?;
        let questions = scored_qa
            .iter()
            .zip(q_embs)
            .map(|(qa, e)| (e, qa.question.clone(), qa.evidence.clone()))
            .collect();
        cases.push((s.sample_id.clone(), questions));
    }

    // 0.62:0.38 is the shipped default (0.40:0.25 normalized); 1:0 is layer B.
    let ratios: [(f32, f32); 8] = [
        (1.0, 0.0),
        (0.8, 0.2),
        (0.7, 0.3),
        (0.62, 0.38),
        (0.5, 0.5),
        (0.4, 0.6),
        (0.3, 0.7),
        (0.0, 1.0),
    ];

    eprintln!("\n=== sweep phase 1: semantic:keyword ratio, no reranker (any%/all%) ===");
    let mut phase1: Vec<((f32, f32), Tally)> = Vec::new();
    for &(sem, kw) in &ratios {
        let w = FusionWeights {
            semantic: sem,
            keyword: kw,
            recency: 0.0,
            importance: 0.0,
        };
        let t = sweep_combo(eng, &cases, w, ks, top_k)?;
        eprintln!(
            "  sem {sem:.2} / kw {kw:.2} (n={:>4}): {}",
            t.n,
            t.cells(ks)
        );
        phase1.push(((sem, kw), t));
    }

    let Some(ce) = reranker else {
        eprintln!("\nphase 2 skipped: set CITADEL_RERANKER_DIR to sweep rerank strategies");
        return Ok(());
    };
    phase1.sort_by(|a, b| b.1.any[2].cmp(&a.1.any[2]));
    let top: Vec<(f32, f32)> = phase1.iter().take(2).map(|(ratio, _)| *ratio).collect();

    let strategies = [
        RerankStrategy::Rrf { k: 20.0 },
        RerankStrategy::Rrf { k: 60.0 },
        RerankStrategy::Rrf { k: 100.0 },
        RerankStrategy::Replace,
    ];
    eprintln!("\n=== sweep phase 2: rerank strategy over the two best ratios (any%/all%) ===");
    for &(sem, kw) in &top {
        let w = FusionWeights {
            semantic: sem,
            keyword: kw,
            recency: 0.0,
            importance: 0.0,
        };
        for &strategy in &strategies {
            eng.set_reranker(ce.clone(), strategy);
            let t = sweep_combo(eng, &cases, w, ks, top_k)?;
            eprintln!("  sem {sem:.2} / kw {kw:.2}  {strategy:?}: {}", t.cells(ks));
        }
    }
    Ok(())
}

/// Inspect stored atoms: ingest each conversation and print the raw rows, to
/// verify what the DB actually holds (text + caption/query markers).
fn run_db_dump(
    eng: &MemoryEngine,
    samples: &[Sample],
    embedder: Arc<dyn Embedder>,
) -> Result<(), Box<dyn Error>> {
    if samples.is_empty() {
        return Err("no samples to dump".into());
    }

    // Markers confirming ingest folded the caption + image-search query into
    // the text.
    let cap_marker = "[shared a photo:";
    let qry_marker = "[image search:";
    let (mut tot_turns, mut tot_ingested, mut tot_atoms) = (0usize, 0usize, 0usize);
    let (mut tot_caption, mut tot_query) = (0usize, 0usize);

    let mut tot_event_exact = 0usize;

    // One region per conversation: dump every conversation's ingested atoms.
    for s in samples {
        citadel_membench::create_bench_region(eng, &s.sample_id, Arc::clone(&embedder))?;
        let ids = ingest_sample(eng, &s.sample_id, s)?;
        let atoms = eng.fetch(&s.sample_id, "turn", None, 100_000)?;

        let with_caption = atoms.iter().filter(|a| a.text.contains(cap_marker)).count();
        let with_query = atoms.iter().filter(|a| a.text.contains(qry_marker)).count();
        // Stored event time must equal the parsed session date, turn for turn
        // (ingest returns ids in turn order, so the zip aligns them exactly).
        let by_id: FxHashMap<_, _> = atoms.iter().map(|a| (a.id, a.created_at)).collect();
        let event_exact = ids
            .iter()
            .zip(&s.turns)
            .filter(|(id, t)| t.event_micros().is_some_and(|e| by_id.get(*id) == Some(&e)))
            .count();
        tot_turns += s.turns.len();
        tot_ingested += ids.len();
        tot_atoms += atoms.len();
        tot_caption += with_caption;
        tot_query += with_query;
        tot_event_exact += event_exact;

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
        eprintln!(
            "event-time created_at exact for {event_exact}/{} turns",
            s.turns.len()
        );

        // A compact aligned table (truncated), then full untruncated text below
        // so the caption/query markers are visible.
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

        // Full text of the first 5 caption/query atoms, so the markers are
        // fully visible.
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
         caption {tot_caption} ({:.0}%)  query {tot_query} ({:.0}%)  \
         event-time exact {tot_event_exact}/{tot_turns}",
        pct(tot_caption, tot_atoms),
        pct(tot_query, tot_atoms)
    );
    Ok(())
}

/// Inspect encrypted recall, atom deletion, and region deletion locally.
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
    if s.turns.is_empty() {
        return Err("erasure demo requires a nonempty conversation".into());
    }
    let bytes = |p: &std::path::Path| std::fs::read(p);

    eprintln!("\n=== ERASURE DEMO: region '{region}' (ENCRYPTED, per-atom sealed) ===");
    eng.create_encrypted_region(region, Arc::clone(&embedder))?;
    let ids = ingest_sample(eng, region, s)?;
    eprintln!(
        "ingested {} atoms (each sealed under its own ACK)\n  atomkeys sidecar:  {} = {} bytes\n  regions sidecar:   {} = {} bytes",
        ids.len(),
        atom_sidecar.display(),
        std::fs::metadata(&atom_sidecar)?.len(),
        region_sidecar.display(),
        std::fs::metadata(&region_sidecar)?.len()
    );

    // PRISM over sealed content: a real turn must surface through
    // decrypt-then-rank.
    let needle = &s.turns[s.turns.len() / 2];
    let needle_text = turn_content(needle);
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

    // Per-atom erasure: forget the top hit, show recall drops it + the ACK slot
    // changes.
    let before = bytes(&atom_sidecar)?;
    eprintln!(
        "\n[per-atom erasure] forgetting atom {target} (present_before={})",
        eng.fetch_one(region, target)?.is_some()
    );
    eng.forget_atom(region, target)?;
    let after = bytes(&atom_sidecar)?;
    let recalled_again = eng
        .recall(region, RecallQuery::by_text(&needle_text, 5))?
        .iter()
        .any(|h| h.id == target);
    if before == after || recalled_again || eng.fetch_one(region, target)?.is_some() {
        return Err("atom deletion did not remove the atom and update its key store".into());
    }
    eprintln!(
        "  atomkeys sidecar overwritten={} ({} -> {} bytes)\n  fetch_one(target)={:?}  recalled_again={recalled_again}",
        before != after,
        before.len(),
        after.len(),
        eng.fetch_one(region, target)?.map(|a| a.text)
    );
    if let Some(sib) = ids.iter().copied().find(|&i| i != target) {
        if eng.fetch_one(region, sib)?.is_none() {
            return Err("atom deletion removed an unrelated atom".into());
        }
        eprintln!(
            "  sibling atom {sib} survives={}",
            eng.fetch_one(region, sib)?.is_some()
        );
    }

    // Per-region erasure: drop the region, show recall empties + the RCK slot
    // changes.
    let rbefore = bytes(&region_sidecar)?;
    eng.drop_region(region)?;
    let rafter = bytes(&region_sidecar)?;
    eng.create_encrypted_region(region, Arc::clone(&embedder))?;
    let after_drop = eng.recall(region, RecallQuery::by_text(&needle_text, 5))?;
    if rbefore == rafter || !after_drop.is_empty() {
        return Err("region deletion did not clear recall and update its key store".into());
    }
    eprintln!(
        "\n[per-region erasure] drop_region('{region}')\n  regions sidecar overwritten={} ({} -> {} bytes)\n  re-create by name -> recall returns {} hits",
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
fn cosine(a: &[f32], b: &[f32]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (&a, &b) in a.iter().zip(b) {
        let (a, b) = (f64::from(a), f64::from(b));
        dot += a * b;
        na += a * a;
        nb += b * b;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

/// any/all evidence hit counts at each cutoff, plus the question count.
#[derive(Default)]
struct Tally {
    any: [usize; 3],
    all: [usize; 3],
    n: usize,
}

impl Tally {
    /// Count whether any / every gold evidence dia_id lands in the top-{ks}.
    fn record(&mut self, ranked: &[&str], evidence: &[String], ks: [usize; 3]) {
        self.n += 1;
        for (ki, &k) in ks.iter().enumerate() {
            let top = &ranked[..k.min(ranked.len())];
            let mut hit_any = false;
            let mut hit_all = true;
            for e in evidence {
                let present = top.contains(&e.as_str());
                hit_any |= present;
                hit_all &= present;
            }
            if hit_any {
                self.any[ki] += 1;
            }
            if hit_all {
                self.all[ki] += 1;
            }
        }
    }

    /// One report row: `@k any%/all%` per cutoff.
    fn cells(&self, ks: [usize; 3]) -> String {
        ks.iter()
            .enumerate()
            .map(|(ki, k)| {
                format!(
                    "@{k} {:.1}/{:.1}",
                    pct(self.any[ki], self.n),
                    pct(self.all[ki], self.n)
                )
            })
            .collect::<Vec<_>>()
            .join("  ")
    }
}

/// Record into the per-category tally for `label`.
fn record(
    acc: &mut BTreeMap<&str, Tally>,
    label: &'static str,
    ranked: &[&str],
    evidence: &[String],
    ks: [usize; 3],
) {
    acc.entry(label).or_default().record(ranked, evidence, ks);
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
    let cost = match report.estimated_cost_usd {
        Some(cost) => format!("~${cost:.4}"),
        None if report.unknown_usage_attempts != 0 => format!(
            "incomplete ({} attempts with unknown usage)",
            report.unknown_usage_attempts
        ),
        None => "unknown".to_string(),
    };
    eprintln!(
        "tokens: in {} / out {}  est cost {cost}",
        report.total_input_tokens, report.total_output_tokens
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_cosine_stays_finite_for_large_finite_vectors() {
        assert_eq!(cosine(&[f32::MAX, f32::MAX], &[f32::MAX, f32::MAX]), 1.0);
        assert_eq!(cosine(&[0.0, 0.0], &[1.0, 0.0]), 0.0);
    }

    #[test]
    fn erasure_demo_checks_atom_and_region_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            citadel::DatabaseBuilder::new(dir.path().join("demo.cdl"))
                .passphrase(b"test")
                .argon2_profile(citadel::Argon2Profile::Iot)
                .enable_region_keys(true)
                .create()
                .unwrap(),
        );
        let eng = MemoryEngine::open(db.clone()).unwrap();
        let sample = Sample {
            sample_id: "demo".into(),
            turns: (0..2)
                .map(|n| citadel_membench::Turn {
                    session: 1,
                    date_time: "1:56 pm on 8 May, 2023".into(),
                    speaker: "Alice".into(),
                    dia_id: format!("D1:{n}"),
                    text: format!("test turn {n}"),
                    blip_caption: String::new(),
                    query: String::new(),
                })
                .collect(),
            qa: Vec::new(),
        };
        run_erasure_demo(
            &eng,
            db,
            &[sample],
            Arc::new(citadel_mem::MockEmbedder::new(8)),
        )
        .unwrap();
    }
}
