//! Live LongMemEval runner. Emit-only: writes a `{question_id, hypothesis}`
//! JSONL prediction file; the official score comes from the LongMemEval repo's
//! Python, not this binary. Gated behind `openai` + `candle-embed` so CI never
//! compiles it.
//!
//! Usage:
//!   OPENAI_API_KEY=...  CITADEL_EMBEDDER_DIR=/path/to/e5-large  \
//!     cargo run -p citadeldb-membench --features openai,candle-embed \
//!     --bin longmemeval -- path/to/longmemeval_oracle.json
//!
//! Then score with the official repo (gpt-4o-2024-08-06 judge):
//!   python3 evaluate_qa.py gpt-4o hypotheses.jsonl longmemeval_oracle.json
//!   python3 print_qa_metrics.py hypotheses.jsonl.eval-results-gpt-4o DATASET
//!
//! Dataset path: argv[1] or CITADEL_LONGMEMEVAL_DATASET. Env knobs:
//!   CITADEL_LONGMEMEVAL_OUT=path        predictions (def hypotheses.jsonl)
//!   CITADEL_LONGMEMEVAL_READER_MODEL=m  reader model (default gpt-4o)
//!   CITADEL_LONGMEMEVAL_TOP_K=n         memories per question (default 50)
//!   CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS=n  adjacent turns per hit (default 0)
//!   CITADEL_LONGMEMEVAL_READER_CONCURRENCY  reader calls in flight (default 3)
//!   CITADEL_LONGMEMEVAL_READER_TPM      tokens/min cap (default per model)
//!   CITADEL_MEMBENCH_MAX_TOKENS         reader output-token cap (default 800)
//!   CITADEL_LONGMEMEVAL_ENCRYPTED=true  seal atoms per-key (default false)
//!   CITADEL_LONGMEMEVAL_DB_PATH=path    persist + reuse the encrypted DB
//!                             (skip the ~2h ingest; ENCRYPTED must match)
//!   CITADEL_LONGMEMEVAL_MOCK_EMBED=1    deterministic embedder (smoke only)
//!   CITADEL_LONGMEMEVAL_EMBEDDER=m      e5-large|e5-large-v2|bge-*|granite-r2
//!   CITADEL_RERANKER_DIR=/path          cross-encoder reranker dir
//!   CITADEL_LONGMEMEVAL_RERANK_STRATEGY  replace|rrf (default rrf)
//!   CITADEL_LONGMEMEVAL_AGENTIC=1       agentic reader for aggregation Qs
//!                             (extract -> count/sort -> answer); labeled apart
//!   CITADEL_LONGMEMEVAL_ONLY_QIDS=path  keep only the listed question_ids
//!   CITADEL_LONGMEMEVAL_MAX_SAMPLES=N   cap to the first N questions
//!   CITADEL_LONGMEMEVAL_DRY_RUN=1       parse + print stats, then exit
//!   CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1  recall@k vs gold, no reader/key

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use citadel_llm::LLMClient;
use citadel_mem::{
    CandleEmbedder, CrossEncoder, Embedder, MemoryEngine, MockEmbedder, RecallProfile, RecallQuery,
    RerankStrategy, Reranker,
};
use citadel_membench::benchmarks::longmemeval::retrieval::{distinct_session_ids, Tally};
use citadel_membench::benchmarks::longmemeval::{dataset, ingest, run, LmevalConfig};
use citadel_membench::{default_tpm_for_model, BenchConfig, Pacer, ReaderOrder};

const DEFAULT_READER_MODEL: &str = "gpt-4o";

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn Error>> {
    if let Ok(value) = std::env::var("CITADEL_LONGMEMEVAL_READER_ORDER") {
        return Err(format!(
            "CITADEL_LONGMEMEVAL_READER_ORDER={value:?} is unsupported: LongMemEval uses one \
             canonical prompt order (sessions by date, turns by conversation order); unset it"
        )
        .into());
    }

    let dataset_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("CITADEL_LONGMEMEVAL_DATASET").ok())
        .ok_or("dataset path required: argv[1] or CITADEL_LONGMEMEVAL_DATASET")?;

    let (mut samples, dataset_sha256) = dataset::load_with_hash(&dataset_path)?;
    if let Ok(raw) = std::env::var("CITADEL_LONGMEMEVAL_MAX_SAMPLES") {
        let n: usize = raw
            .parse()
            .map_err(|_| "CITADEL_LONGMEMEVAL_MAX_SAMPLES must be a non-negative integer")?;
        samples.truncate(n);
    }
    if let Ok(path) = std::env::var("CITADEL_LONGMEMEVAL_ONLY_QIDS") {
        let keep: rustc_hash::FxHashSet<String> = std::fs::read_to_string(&path)?
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        samples.retain(|s| keep.contains(&s.question_id));
        eprintln!(
            "only-qids: {} of {} listed found",
            samples.len(),
            keep.len()
        );
    }
    let abstentions = samples.iter().filter(|s| s.abstention).count();
    eprintln!(
        "dataset: {dataset_path}  sha256={dataset_sha256}  questions={}  abstention={abstentions}",
        samples.len()
    );

    if std::env::var("CITADEL_LONGMEMEVAL_DRY_RUN").is_ok() {
        eprintln!("dry run: dataset parsed OK, no LLM calls made.");
        return Ok(());
    }

    let t_embed = Instant::now();
    let embedder: Arc<dyn Embedder> = if std::env::var("CITADEL_LONGMEMEVAL_MOCK_EMBED").is_ok() {
        Arc::new(MockEmbedder::new(384))
    } else {
        let model_dir =
            std::env::var("CITADEL_EMBEDDER_DIR").map_err(|_| "CITADEL_EMBEDDER_DIR not set")?;
        let ce = match std::env::var("CITADEL_LONGMEMEVAL_EMBEDDER")
            .unwrap_or_default()
            .as_str()
        {
            "bge-base" => CandleEmbedder::bge_base(&model_dir)?,
            "bge-large" => CandleEmbedder::bge_large(&model_dir)?,
            "e5-large" => CandleEmbedder::e5_large(&model_dir)?,
            "e5-large-v2" => CandleEmbedder::e5_large_v2(&model_dir)?,
            "granite-r2" => CandleEmbedder::granite_r2(&model_dir)?,
            _ => CandleEmbedder::e5_large(&model_dir)?,
        };
        Arc::new(ce)
    };
    eprintln!("embedder loaded in {:.1}s", t_embed.elapsed().as_secs_f64());

    let encrypted = std::env::var("CITADEL_LONGMEMEVAL_ENCRYPTED")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let bench_db = citadel_membench::open_bench_db("CITADEL_LONGMEMEVAL_DB_PATH", encrypted)?;
    let eng = MemoryEngine::open(Arc::clone(&bench_db.db))?;
    if bench_db.reuse {
        eprintln!(
            "db: reuse {} (encrypted_regions={encrypted}) - skipping ingest",
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

    // Loaded before the diag so its recall@k matches the reader's reranked
    // top-k.
    match std::env::var("CITADEL_RERANKER_DIR") {
        Ok(rr_dir) => {
            let ce = CrossEncoder::ms_marco_minilm_l6(&rr_dir)?;
            let model = ce.model_id().to_string();
            let strategy = rerank_strategy_from_env();
            eng.set_reranker(Arc::new(ce), strategy);
            eprintln!("reranker: {model} (from {rr_dir}) strategy={strategy:?}");
        }
        Err(_) => eprintln!("reranker: none (set CITADEL_RERANKER_DIR to enable)"),
    }

    // Token-free retrieval diagnostic: no reader, no key.
    if std::env::var("CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG").is_ok() {
        return run_retrieval_diag(&eng, &samples, embedder, encrypted, bench_db.reuse);
    }

    let reader: Arc<dyn LLMClient> = citadel_llm::factory::from_env(
        "CITADEL_LONGMEMEVAL_READER",
        "openai",
        DEFAULT_READER_MODEL,
    )
    .map_err(|e| format!("reader LLM: {e}"))?;
    let reader_model = reader.model_id().to_string();
    let reader_tpm = std::env::var("CITADEL_LONGMEMEVAL_READER_TPM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| default_tpm_for_model(&reader_model));
    let pacer = Pacer::new(&reader_model, reader_tpm, &reader_model, reader_tpm);
    eprintln!("reader: {reader_model}  embedder: {}", embedder.model_id());

    let cfg = LmevalConfig {
        bench: BenchConfig {
            top_k: env_usize("CITADEL_LONGMEMEVAL_TOP_K", 50),
            // Inert: build_reader_prompt regroups by session and re-sorts.
            reader_order: ReaderOrder::Relevance,
            neighbor_radius: env_usize("CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS", 0),
            // Official CoT gen_length; the reader's step-by-step answer needs
            // the headroom.
            reader_max_tokens: 800,
            agentic: std::env::var("CITADEL_LONGMEMEVAL_AGENTIC").is_ok(),
        },
        encrypted,
        reuse: bench_db.reuse,
        reader_concurrency: env_usize("CITADEL_LONGMEMEVAL_READER_CONCURRENCY", 3),
    };

    let out_path =
        std::env::var("CITADEL_LONGMEMEVAL_OUT").unwrap_or_else(|_| "hypotheses.jsonl".to_string());
    let total = samples.len();
    let mut done = 0usize;
    let pairs = run(
        &eng,
        &samples,
        Arc::clone(&embedder),
        reader.as_ref(),
        &pacer,
        &cfg,
        &mut |_, qid, _hyp| {
            done += 1;
            if done.is_multiple_of(10) || done == total {
                eprintln!("  answered {done}/{total} ({qid})");
            }
            Ok(())
        },
    )?;

    let mut f = std::fs::File::create(&out_path)?;
    for (question_id, hypothesis) in &pairs {
        let line = serde_json::json!({ "question_id": question_id, "hypothesis": hypothesis });
        writeln!(f, "{line}")?;
    }
    eprintln!("wrote {} predictions -> {out_path}", pairs.len());
    eprintln!(
        "score: python3 evaluate_qa.py gpt-4o {out_path} {dataset_path} \
         && python3 print_qa_metrics.py {out_path}.eval-results-gpt-4o {dataset_path}"
    );
    Ok(())
}

const DIAG_KS: [usize; 3] = [10, 30, 50];

/// Parse `CITADEL_LONGMEMEVAL_RERANK_STRATEGY` (replace|rrf, default rrf).
fn rerank_strategy_from_env() -> RerankStrategy {
    match std::env::var("CITADEL_LONGMEMEVAL_RERANK_STRATEGY")
        .unwrap_or_default()
        .as_str()
    {
        "replace" => RerankStrategy::Replace,
        _ => RerankStrategy::default(),
    }
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
) -> Result<(), Box<dyn Error>> {
    const MAX_K: usize = 50;
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
        Ok(p) => Some(std::io::BufWriter::new(std::fs::File::create(&p)?)),
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
        if encrypted {
            eng.create_encrypted_region(&s.question_id, Arc::clone(&embedder))?;
        } else {
            eng.create_region(&s.question_id, Arc::clone(&embedder))?;
        }
        if !reuse {
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
        let hits = eng.recall(
            &s.question_id,
            RecallProfile::default().apply(RecallQuery::by_text(&s.question, MAX_K)),
        )?;
        win_rc += t.elapsed().as_micros();
        let label = s.kind.label();

        let ranked_sessions = distinct_session_ids(&hits);
        let evidence: Vec<&str> = s.evidence.iter().map(String::as_str).collect();
        sess.entry(label)
            .or_default()
            .record_membership(&ranked_sessions, &evidence, DIAG_KS);

        let total_answer = s.turns.iter().filter(|t| t.has_answer).count();
        turn.entry(label)
            .or_default()
            .record_has_answer(&hits, total_answer, DIAG_KS);

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

        // Semantic-only ranking: isolates whether default fusion helps recall
        // vs similarity.
        let t = std::time::Instant::now();
        let hits_sem = eng.recall(
            &s.question_id,
            RecallProfile::semantic_only().apply(RecallQuery::by_text(&s.question, MAX_K)),
        )?;
        win_rs += t.elapsed().as_micros();
        turn_sem
            .entry(label)
            .or_default()
            .record_has_answer(&hits_sem, total_answer, DIAG_KS);

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

    print_diag("session-level (answer_session_ids)", &sess, &labels);
    print_diag("turn-level, default fusion (has_answer)", &turn, &labels);
    print_diag("turn-level, semantic-only (has_answer)", &turn_sem, &labels);
    Ok(())
}

fn print_diag(title: &str, acc: &BTreeMap<&str, Tally>, labels: &[&str]) {
    eprintln!("\n=== retrieval diag [{title}]: recall@10/30/50 as any%/all% ===");
    let mut tot = Tally::default();
    for &label in labels {
        if let Some(t) = acc.get(label) {
            eprintln!("  {label:>26} (n={:>4}): {}", t.n, t.cells(DIAG_KS));
            for ki in 0..DIAG_KS.len() {
                tot.any[ki] += t.any[ki];
                tot.all[ki] += t.all[ki];
            }
            tot.n += t.n;
        }
    }
    eprintln!(
        "  {:>26} (n={:>4}): {}",
        "OVERALL",
        tot.n,
        tot.cells(DIAG_KS)
    );
}
