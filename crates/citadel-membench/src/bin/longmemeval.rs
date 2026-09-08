//! LongMemEval predictions and offline retrieval diagnostics. The official
//! Python evaluator scores the emitted `{question_id, hypothesis}` JSONL.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use citadel_mem::{CrossEncoder, Embedder, MemoryEngine, RecallQuery, Reranker};
use citadel_membench::benchmarks::longmemeval::config::{RunConfig, RunMode};
use citadel_membench::benchmarks::longmemeval::retrieval::{
    distinct_session_ids, semantic_only_recall, Tally,
};
use citadel_membench::benchmarks::longmemeval::{dataset, ingest, run, LmevalConfig};
use citadel_membench::core::retrieval::baseline_recall;
use citadel_membench::{default_tpm_for_model, Pacer};

const DEFAULT_READER_MODEL: &str = "gpt-4o";

fn main() -> Result<(), Box<dyn Error>> {
    let launch = RunConfig::from_env()?;
    if launch.mode == RunMode::Scored {
        citadel_membench::core::config::preflight_llm(
            "CITADEL_LONGMEMEVAL_READER",
            DEFAULT_READER_MODEL,
        )?;
    }

    let dataset_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("CITADEL_LONGMEMEVAL_DATASET").ok())
        .ok_or("dataset path required: argv[1] or CITADEL_LONGMEMEVAL_DATASET")?;

    let (mut samples, dataset_sha256) = dataset::load_with_hash(&dataset_path)?;
    if let Ok(path) = std::env::var("CITADEL_LONGMEMEVAL_ONLY_QIDS") {
        let mut keep = rustc_hash::FxHashSet::default();
        for id in std::fs::read_to_string(&path)?
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !keep.insert(id.to_string()) {
                return Err(format!("duplicate question_id in {path}: {id}").into());
            }
        }
        if keep.is_empty() {
            return Err("CITADEL_LONGMEMEVAL_ONLY_QIDS contains no question IDs".into());
        }
        let known: rustc_hash::FxHashSet<_> =
            samples.iter().map(|s| s.question_id.as_str()).collect();
        for id in &keep {
            if !known.contains(id.as_str()) {
                return Err(
                    format!("question_id from {path} is missing from dataset: {id}").into(),
                );
            }
        }
        samples.retain(|s| keep.contains(&s.question_id));
    }
    if let Some(n) = launch.max_samples {
        samples.truncate(n);
    }
    if samples.is_empty() {
        return Err("dataset contains no selected questions".into());
    }
    let abstentions = samples.iter().filter(|s| s.abstention).count();
    eprintln!(
        "dataset: {dataset_path}  sha256={dataset_sha256}  questions={}  abstention={abstentions}",
        samples.len()
    );

    if launch.mode == RunMode::DryRun {
        eprintln!("dry run: dataset parsed OK, no LLM calls made.");
        return Ok(());
    }

    let reader = if launch.mode == RunMode::Scored {
        Some(
            citadel_llm::factory::from_env(
                "CITADEL_LONGMEMEVAL_READER",
                "openai",
                DEFAULT_READER_MODEL,
            )
            .map_err(|e| format!("reader LLM: {e}"))?,
        )
    } else {
        None
    };
    let reranker = match std::env::var("CITADEL_RERANKER_DIR") {
        Ok(dir) => Some(Arc::new(
            CrossEncoder::ms_marco_minilm_l6(&dir).map_err(|e| format!("reranker model: {e}"))?,
        )),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("CITADEL_RERANKER_DIR must be Unicode text".into()),
    };
    let t_embed = Instant::now();
    let model_dir =
        std::env::var("CITADEL_EMBEDDER_DIR").map_err(|_| "CITADEL_EMBEDDER_DIR not set")?;
    let embedder: Arc<dyn Embedder> = Arc::new(launch.embedder.load(&model_dir)?);
    eprintln!(
        "embedder: {} loaded in {:.1}s",
        embedder.model_id(),
        t_embed.elapsed().as_secs_f64()
    );

    let encrypted = launch.encrypted;
    let bench_db = citadel_membench::open_bench_db("CITADEL_LONGMEMEVAL_DB_PATH", encrypted)?;
    let eng = MemoryEngine::open(Arc::clone(&bench_db.db))?;
    if bench_db.reuse {
        eprintln!(
            "db: reuse {} (encrypted_regions={encrypted}) - corpus validation required",
            bench_db.path.display()
        );
    } else if std::env::var("CITADEL_LONGMEMEVAL_DB_PATH").is_ok() {
        eprintln!(
            "db: persist {} (encrypted_regions={encrypted}) - ingest once, reusable next run",
            bench_db.path.display()
        );
    } else {
        eprintln!("db: temp (encrypted_regions={encrypted})");
    }

    let reranker_model = match reranker {
        Some(ce) => {
            let model = ce.model_id().to_string();
            let strategy = launch.rerank_strategy;
            eng.set_reranker(ce, strategy);
            eprintln!("reranker: {model} strategy={strategy:?}");
            Some(model)
        }
        None => {
            eprintln!("reranker: none (set CITADEL_RERANKER_DIR to enable)");
            None
        }
    };

    // Token-free retrieval diagnostic: no reader, no key.
    if launch.mode == RunMode::RetrievalDiag {
        return run_retrieval_diag(
            &eng,
            &samples,
            embedder,
            encrypted,
            bench_db.reuse,
            launch.bench.top_k,
        );
    }

    let reader = reader.ok_or("scored mode requires a reader client")?;
    let reader_model = reader.model_id().to_string();
    let reader_tpm = launch
        .reader_tpm
        .unwrap_or_else(|| default_tpm_for_model(&reader_model));
    let pacer = Pacer::new(&reader_model, reader_tpm, &reader_model, reader_tpm);
    eprintln!("reader: {reader_model}  embedder: {}", embedder.model_id());

    let cfg = LmevalConfig {
        bench: launch.bench,
        encrypted,
        reuse: bench_db.reuse,
        reader_concurrency: launch.reader_concurrency,
    };
    let out_path =
        std::env::var("CITADEL_LONGMEMEVAL_OUT").unwrap_or_else(|_| "hypotheses.jsonl".to_string());
    let mut predictions = std::fs::File::create_new(&out_path)?;
    let mut audit = std::env::var("CITADEL_LONGMEMEVAL_AUDIT_PATH")
        .ok()
        .map(std::fs::File::create_new)
        .transpose()?;
    let total = samples.len();
    let mut done = 0usize;
    let pairs = run(
        &eng,
        &samples,
        Arc::clone(&embedder),
        reader.as_ref(),
        &pacer,
        &cfg,
        &mut |index, qid, outcome| {
            let prediction =
                serde_json::json!({ "question_id": qid, "hypothesis": outcome.answer });
            writeln!(predictions, "{prediction}")?;
            predictions.flush()?;
            if let Some(file) = audit.as_mut() {
                let record = serde_json::json!({
                    "question_id": qid,
                    "question": samples[index].question,
                    "hypothesis": outcome.answer,
                    "retrieved": outcome.retrieved,
                    "retrieved_atom_ids": outcome.retrieved_atom_ids,
                    "reader_finish_reasons": outcome.reader_finish_reasons,
                    "reader_calls": outcome.reader_calls,
                    "recall_micros": outcome.recall_micros,
                    "dataset_sha256": dataset_sha256,
                    "embedder_model": embedder.model_id(),
                    "encrypted": encrypted,
                    "top_k": launch.bench.top_k,
                    "neighbor_radius": launch.bench.neighbor_radius,
                    "agentic": launch.bench.agentic,
                    "rerank_strategy": format!("{:?}", launch.rerank_strategy),
                    "reranker_model": reranker_model,
                });
                writeln!(file, "{record}")?;
                file.flush()?;
            }
            done += 1;
            if done.is_multiple_of(10) || done == total {
                eprintln!("  answered {done}/{total} ({qid})");
            }
            Ok(())
        },
    )?;

    eprintln!("wrote {} predictions -> {out_path}", pairs.len());
    eprintln!(
        "score: python3 evaluate_qa.py gpt-4o {out_path} {dataset_path} \
         && python3 print_qa_metrics.py {out_path}.eval-results-gpt-4o {dataset_path}"
    );
    Ok(())
}

/// Token-free retrieval diagnostic: does scored recall surface the gold
/// evidence? Reports any%/all% @10/30/50 at session and turn granularity;
/// abstention/no-target questions are excluded.
fn run_retrieval_diag(
    eng: &MemoryEngine,
    samples: &[dataset::LmSample],
    embedder: Arc<dyn Embedder>,
    encrypted: bool,
    reuse: bool,
    top_k: usize,
) -> Result<(), Box<dyn Error>> {
    let ks = [10.min(top_k), 30.min(top_k), top_k];
    let labels = [
        "single_session_user",
        "single_session_assistant",
        "single_session_preference",
        "multi_session",
        "temporal_reasoning",
        "knowledge_update",
    ];
    let mut sess: BTreeMap<&str, Tally> = BTreeMap::new();
    let mut turn: BTreeMap<&str, Tally> = BTreeMap::new();
    let mut turn_sem: BTreeMap<&str, Tally> = BTreeMap::new();
    // Optional per-question dump (TSV: qid, #gold sessions, session all@50,
    // turn all@50).
    let mut dump = match std::env::var("CITADEL_LONGMEMEVAL_DIAG_DUMP") {
        Ok(p) => Some(std::io::BufWriter::new(std::fs::File::create_new(&p)?)),
        Err(_) => None,
    };

    // Score only the answerable, has-target questions (the official
    // exclusions).
    let scored: Vec<&dataset::LmSample> = samples
        .iter()
        .filter(|s| !s.abstention && !s.evidence.is_empty())
        .collect();

    // Ingest all regions first (a complete, reusable cache), then recall in a
    // separate pass: each write purges the ANN segment, so pass 2 builds it
    // once on first recall.
    let t_ing = std::time::Instant::now();
    for s in samples {
        if reuse {
            citadel_membench::core::db::attach_reused_region(
                eng,
                &s.question_id,
                Arc::clone(&embedder),
                encrypted,
            )?;
            ingest::validate_reuse(eng, &s.question_id, s)?;
        } else {
            if encrypted {
                eng.create_encrypted_region(&s.question_id, Arc::clone(&embedder))?;
            } else {
                eng.create_region(&s.question_id, Arc::clone(&embedder))?;
            }
            ingest::ingest_sample(eng, &s.question_id, s)?;
        }
    }
    eprintln!(
        "  {} {} regions in {:.1}s",
        if reuse { "re-attached" } else { "ingested" },
        samples.len(),
        t_ing.elapsed().as_secs_f64()
    );

    // Pass 2: recall + score (no writes between, so the ANN segment is stable).
    let (mut win_rc, mut win_rs, mut win_n) = (0u128, 0u128, 0usize);
    for (done, &s) in scored.iter().enumerate() {
        let t = std::time::Instant::now();
        let hits = baseline_recall(
            eng,
            &s.question_id,
            RecallQuery::by_text(&s.question, top_k),
        )?;
        win_rc += t.elapsed().as_micros();
        let label = s.kind.label();

        let ranked_sessions = distinct_session_ids(&hits);
        let evidence: Vec<&str> = s.evidence.iter().map(String::as_str).collect();
        sess.entry(label)
            .or_default()
            .record_membership(&ranked_sessions, &evidence, ks);

        let total_answer = s.turns.iter().filter(|t| t.has_answer).count();
        turn.entry(label)
            .or_default()
            .record_has_answer(&hits, total_answer, ks);

        if let Some(w) = dump.as_mut() {
            let sess_all = evidence.iter().all(|g| ranked_sessions.contains(g));
            let got = hits
                .iter()
                .filter(|h| {
                    h.payload
                        .get("has_answer")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
                .count();
            let turn_all = total_answer > 0 && got == total_answer;
            writeln!(
                w,
                "{}\t{}\t{}\t{}",
                s.question_id,
                evidence.len(),
                sess_all,
                turn_all
            )?;
        }

        // Compare configured recall with vector-only retrieval.
        let t = std::time::Instant::now();
        let hits_sem = semantic_only_recall(eng, &s.question_id, &*embedder, &s.question, top_k)?;
        win_rs += t.elapsed().as_micros();
        turn_sem
            .entry(label)
            .or_default()
            .record_has_answer(&hits_sem, total_answer, ks);

        win_n += 1;
        if win_n == 50 || done + 1 == scored.len() {
            eprintln!(
                "  recalled {}/{}  [/q ms: recall {:.0}  recall_sem {:.0}]",
                done + 1,
                scored.len(),
                win_rc as f64 / 1e3 / win_n as f64,
                win_rs as f64 / 1e3 / win_n as f64
            );
            (win_rc, win_rs, win_n) = (0, 0, 0);
        }
    }

    print_diag("session-level (answer_session_ids)", &sess, &labels, ks);
    print_diag(
        "turn-level, configured recall (has_answer)",
        &turn,
        &labels,
        ks,
    );
    print_diag(
        "turn-level, semantic-only (has_answer)",
        &turn_sem,
        &labels,
        ks,
    );
    Ok(())
}

fn print_diag(title: &str, acc: &BTreeMap<&str, Tally>, labels: &[&str], ks: [usize; 3]) {
    eprintln!(
        "\n=== retrieval diag [{title}]: recall@{}/{}/{} as any%/all% ===",
        ks[0], ks[1], ks[2]
    );
    let mut tot = Tally::default();
    for &label in labels {
        if let Some(t) = acc.get(label) {
            eprintln!("  {label:>26} (n={:>4}): {}", t.n, t.cells(ks));
            for ki in 0..ks.len() {
                tot.any[ki] += t.any[ki];
                tot.all[ki] += t.all[ki];
            }
            tot.n += t.n;
        }
    }
    eprintln!("  {:>26} (n={:>4}): {}", "OVERALL", tot.n, tot.cells(ks));
}
