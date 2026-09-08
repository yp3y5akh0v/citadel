//! LongMemEval runner: ingest each question's haystack, answer it, collect
//! predictions. Emit-only; the official Python scorer grades the JSONL.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use citadel_llm::{LLMClient, TokenUsage};
use citadel_mem::{Embedder, MemoryEngine};

use super::dataset::LmSample;
use super::{ingest, LongMemEval};
use crate::core::db::attach_reused_region;
use crate::core::error::{BenchError, Result};
use crate::core::eval::{answer_question, AnswerOutcome, Question};
use crate::core::ratelimit::{Gate, Pacer};
use crate::BenchConfig;

pub struct LmevalConfig {
    pub bench: BenchConfig,
    pub encrypted: bool,
    /// Reopened persisted DB: validate and re-attach regions without ingestion.
    pub reuse: bool,
    pub reader_concurrency: usize,
}

struct UsageTotal {
    input_tokens: u64,
    output_tokens: u64,
    cost_usd: Option<f64>,
}

impl Default for UsageTotal {
    fn default() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: Some(0.0),
        }
    }
}

impl UsageTotal {
    fn add(&mut self, usage: &TokenUsage) {
        self.input_tokens += u64::from(usage.input_tokens);
        self.output_tokens += u64::from(usage.output_tokens);
        self.cost_usd = self.cost_usd.zip(usage.cost_usd).map(|(a, b)| a + b);
    }
}

/// Ingest + answer every sample, returning `(question_id, hypothesis)` in
/// sample order. `on_emit` fires per answer in completion order (live trace);
/// an error from it aborts the run.
pub fn run(
    eng: &MemoryEngine,
    samples: &[LmSample],
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    cfg: &LmevalConfig,
    on_emit: &mut (dyn FnMut(usize, &str, &AnswerOutcome) -> Result<()> + Send),
) -> Result<Vec<(String, String)>> {
    cfg.bench.validate()?;
    if cfg.reader_concurrency == 0 {
        return Err(BenchError::Dataset(
            "reader_concurrency must be positive".into(),
        ));
    }
    super::dataset::validate_samples(samples)?;
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    // Phase 1: ingest each question's private haystack into its own region.
    // Writes are single-writer, so this stays sequential; questions then fan
    // out as reads.
    let t_ingest = Instant::now();
    let n = samples.len();
    for (i, s) in samples.iter().enumerate() {
        if cfg.reuse {
            attach_reused_region(eng, &s.question_id, Arc::clone(&embedder), cfg.encrypted)?;
            ingest::validate_reuse(eng, &s.question_id, s)?;
        } else {
            if cfg.encrypted {
                eng.create_encrypted_region(&s.question_id, Arc::clone(&embedder))?;
            } else {
                eng.create_region(&s.question_id, Arc::clone(&embedder))?;
            }
            ingest::ingest_sample(eng, &s.question_id, s)?;
        }
        if (i + 1) % 25 == 0 || i + 1 == n {
            let verb = if cfg.reuse { "re-attached" } else { "ingested" };
            eprintln!("  {verb} {}/{n}", i + 1);
        }
    }
    eprintln!(
        "  phase 1 ({} {n}) {:.1}s",
        if cfg.reuse { "re-attach" } else { "ingest" },
        t_ingest.elapsed().as_secs_f64()
    );

    // Phase 2: answer each question concurrently; results returned in sample
    // order.
    let t_answer = Instant::now();
    let bench = LongMemEval;
    let total = samples.len();
    let workers = cfg.reader_concurrency.max(1);
    let gate = Gate::new(workers);
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let observed = Mutex::new(on_emit);
    let err_slot: Mutex<Option<BenchError>> = Mutex::new(None);
    let spent: Mutex<UsageTotal> = Mutex::new(UsageTotal::default());
    let (tx, rx) = std::sync::mpsc::channel::<(usize, (String, String))>();
    let (next_r, failed_r, observed_r, err_r, gate_r, bench_r, spent_r) =
        (&next, &failed, &observed, &err_slot, &gate, &bench, &spent);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            scope.spawn(move || loop {
                if failed_r.load(Ordering::Relaxed) {
                    break;
                }
                let i = next_r.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let s = &samples[i];
                let outcome = {
                    let _permit = gate_r.acquire();
                    answer_question(
                        bench_r,
                        reader,
                        pacer,
                        eng,
                        &s.question_id,
                        Question {
                            text: &s.question,
                            date: &s.question_date,
                        },
                        cfg.bench,
                    )
                };
                match outcome {
                    Ok(o) => {
                        spent_r.lock().expect("usage poisoned").add(&o.usage);
                        let emit =
                            (*observed_r.lock().expect("observer poisoned"))(i, &s.question_id, &o);
                        match emit {
                            Ok(()) => {
                                let _ = tx.send((i, (s.question_id.clone(), o.answer)));
                            }
                            Err(e) => {
                                *err_r.lock().expect("err slot poisoned") = Some(e);
                                failed_r.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        *err_r.lock().expect("err slot poisoned") = Some(e);
                        failed_r.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });
        }
        drop(tx);
    });

    if let Some(e) = err_slot.into_inner().expect("err slot poisoned") {
        return Err(e);
    }
    eprintln!(
        "  phase 2 (answer {total}) {:.1}s",
        t_answer.elapsed().as_secs_f64()
    );
    let spent = spent.into_inner().expect("usage poisoned");
    // None for local models and for snapshots absent from the pricing table.
    let cost = match spent.cost_usd {
        Some(usd) => format!("est cost ~${usd:.4}"),
        None => "cost unpriced".into(),
    };
    eprintln!(
        "  tokens: in {} / out {}  (mean {:.0} / {:.0} per question)  {cost}",
        spent.input_tokens,
        spent.output_tokens,
        spent.input_tokens as f64 / total as f64,
        spent.output_tokens as f64 / total as f64,
    );
    let mut slots: Vec<Option<(String, String)>> = (0..total).map(|_| None).collect();
    for (i, pair) in rx {
        slots[i] = Some(pair);
    }
    Ok(slots
        .into_iter()
        .map(|o| o.expect("every question produced a result"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_usage_keeps_unknown_costs_and_wide_token_counts() {
        let priced = TokenUsage {
            input_tokens: u32::MAX,
            output_tokens: u32::MAX,
            cost_usd: Some(0.5),
        };
        let unknown = TokenUsage {
            cost_usd: None,
            ..priced
        };
        for calls in [[priced, unknown], [unknown, priced]] {
            let mut total = UsageTotal::default();
            assert_eq!(total.cost_usd, Some(0.0));
            for call in calls {
                total.add(&call);
            }
            assert_eq!(total.cost_usd, None);
            assert_eq!(total.input_tokens, 2 * u64::from(u32::MAX));
            assert_eq!(total.output_tokens, 2 * u64::from(u32::MAX));
        }
        let mut total = UsageTotal::default();
        total.add(&priced);
        total.add(&priced);
        assert_eq!(total.cost_usd, Some(1.0));
    }
}
