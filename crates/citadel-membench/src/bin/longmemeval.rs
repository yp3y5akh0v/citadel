//! Live LongMemEval runner. Emit-only: writes a `{question_id, hypothesis}` JSONL
//! prediction file; the OFFICIAL score comes from the LongMemEval repo's Python, not
//! this binary. Gated behind `openai` + `candle-embed` so default/CI builds never
//! compile it.
//!
//! Usage:
//!   OPENAI_API_KEY=...  CITADEL_EMBEDDER_DIR=/path/to/bge-small  \
//!     cargo run -p citadeldb-membench --features openai,candle-embed \
//!     --bin longmemeval -- path/to/longmemeval_oracle.json
//!
//! Then score with the official repo (gpt-4o-2024-08-06 judge):
//!   python3 evaluate_qa.py gpt-4o hypotheses.jsonl longmemeval_oracle.json
//!   python3 print_qa_metrics.py hypotheses.jsonl.eval-results-gpt-4o longmemeval_oracle.json
//!
//! Dataset path: argv[1] or CITADEL_LONGMEMEVAL_DATASET. Env knobs:
//!   CITADEL_LONGMEMEVAL_OUT=path        prediction JSONL (default hypotheses.jsonl)
//!   CITADEL_LONGMEMEVAL_READER_MODEL=m  reader model (default gpt-4o-mini)
//!   CITADEL_LONGMEMEVAL_TOP_K=n         memories retrieved per question (default 50)
//!   CITADEL_LONGMEMEVAL_READER_CONCURRENCY  reader calls in flight (default 3)
//!   CITADEL_LONGMEMEVAL_READER_TPM      tokens/min cap (default per model, Tier-1)
//!   CITADEL_MEMBENCH_MAX_TOKENS         reader output-token cap override (default 800 = CoT)
//!   CITADEL_LONGMEMEVAL_ENCRYPTED=true  seal atoms per-key (default false)
//!   CITADEL_LONGMEMEVAL_MOCK_EMBED=1    deterministic embedder (no model dir; smoke only)
//!   CITADEL_LONGMEMEVAL_EMBEDDER=m      bge-small|bge-base|bge-large|e5-large|granite-r2
//!   CITADEL_LONGMEMEVAL_MAX_SAMPLES=N   cap to the first N questions
//!   CITADEL_LONGMEMEVAL_DRY_RUN=1       parse + print stats, then exit (no LLM/key)
//!   CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1  recall@k vs gold (session + turn), no reader/key

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_ai::LLMClient;
use citadel_mem::{
    CandleEmbedder, Embedder, MemoryEngine, MockEmbedder, RecallProfile, RecallQuery,
};
use citadel_membench::benchmarks::longmemeval::retrieval::{distinct_session_ids, Tally};
use citadel_membench::benchmarks::longmemeval::{dataset, ingest, run, LmevalConfig};
use citadel_membench::{default_tpm_for_model, BenchConfig, Pacer};

const DEFAULT_READER_MODEL: &str = "gpt-4o-mini";

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn Error>> {
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
        let bge_dir =
            std::env::var("CITADEL_EMBEDDER_DIR").map_err(|_| "CITADEL_EMBEDDER_DIR not set")?;
        let ce = match std::env::var("CITADEL_LONGMEMEVAL_EMBEDDER")
            .unwrap_or_default()
            .as_str()
        {
            "bge-base" => CandleEmbedder::bge_base(&bge_dir)?,
            "bge-large" => CandleEmbedder::bge_large(&bge_dir)?,
            "e5-large" => CandleEmbedder::e5_large(&bge_dir)?,
            "granite-r2" => CandleEmbedder::granite_r2(&bge_dir)?,
            _ => CandleEmbedder::bge_small(&bge_dir)?,
        };
        Arc::new(ce)
    };
    eprintln!("embedder loaded in {:.1}s", t_embed.elapsed().as_secs_f64());

    let encrypted = std::env::var("CITADEL_LONGMEMEVAL_ENCRYPTED")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let tmp = tempfile::tempdir()?;
    let mut builder = DatabaseBuilder::new(tmp.path().join("membench.cdl"))
        .passphrase(b"membench")
        .argon2_profile(Argon2Profile::Iot);
    if encrypted {
        builder = builder.enable_region_keys(true);
    }
    let db = Arc::new(builder.create()?);
    let eng = MemoryEngine::open(db)?;
    eprintln!("db: temp (encrypted_regions={encrypted})");

    // Token-free: measure whether citadel's recall surfaces the gold evidence. No reader, no key.
    if std::env::var("CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG").is_ok() {
        return run_retrieval_diag(&eng, &samples, embedder, encrypted);
    }

    let reader: Arc<dyn LLMClient> =
        citadel_ai::factory::from_env("CITADEL_LONGMEMEVAL_READER", "openai", DEFAULT_READER_MODEL)
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
            // Official CoT gen_length; the reader's step-by-step answer needs the headroom.
            reader_max_tokens: 800,
            ..BenchConfig::default()
        },
        encrypted,
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
            if done % 10 == 0 || done == total {
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

/// Token-free retrieval diagnostic: does citadel's scored recall surface the gold
/// evidence? Reports recall any%/all% @10/30/50, mirroring the official LongMemEval
/// retrieval metric, at session granularity (answer_session_ids) and turn granularity
/// (has_answer). Abstention and no-target questions carry no gold and are excluded, as
/// the official harness does. No reader, no API key.
fn run_retrieval_diag(
    eng: &MemoryEngine,
    samples: &[dataset::LmSample],
    embedder: Arc<dyn Embedder>,
    encrypted: bool,
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

    // Score only the answerable, has-target questions (the official exclusions).
    let scored: Vec<&dataset::LmSample> = samples
        .iter()
        .filter(|s| !s.abstention && !s.evidence.is_empty())
        .collect();

    // Pass 1: ingest every region first. Each write purges the table's ANN segment, so
    // interleaving recall here would rebuild it every call; separating the passes lets
    // pass 2 build the segment once (on its first recall) and reuse it.
    let t_ing = std::time::Instant::now();
    for s in &scored {
        if encrypted {
            eng.create_encrypted_region(&s.question_id, Arc::clone(&embedder))?;
        } else {
            eng.create_region(&s.question_id, Arc::clone(&embedder))?;
        }
        ingest::ingest_sample(eng, &s.question_id, s)?;
    }
    eprintln!(
        "  ingested {} regions in {:.1}s",
        scored.len(),
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

        // Semantic-only ranking: isolates whether the default fusion (keyword + recency
        // weights) helps or hurts evidence recall vs plain similarity.
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
