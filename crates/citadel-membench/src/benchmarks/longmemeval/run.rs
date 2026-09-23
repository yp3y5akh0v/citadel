//! LongMemEval runner: ingest each question's haystack, answer it, collect
//! predictions. Emit-only; the official Python scorer grades the JSONL.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use citadel_llm::LLMClient;
use citadel_mem::{Embedder, MemoryEngine};

use super::dataset::LmSample;
use super::{ingest, LongMemEval};
use crate::core::error::{
    observe_failure, BenchError, CompletedOutput, QuestionBatchFailure, QuestionCompletion,
    QuestionEvent, QuestionFailure, QuestionIdentity, QuestionObserver, QuestionStage, Result,
    UsageAccounting,
};
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

/// A completed prediction, with its reader receipts and stable dataset index.
pub struct PredictionOutcome {
    pub index: usize,
    pub question_id: String,
    pub outcome: AnswerOutcome,
}

impl PredictionOutcome {
    pub fn completion_receipt(&self) -> QuestionCompletion {
        QuestionCompletion {
            identity: QuestionIdentity::LongMemEval {
                question_id: self.question_id.clone(),
            },
            calls: self.outcome.reader_calls.clone(),
            output: CompletedOutput {
                answer: self.outcome.answer.clone(),
                judge: None,
            },
        }
    }
}

struct UsageTotal {
    input_tokens: u64,
    output_tokens: u64,
    cost_usd: Option<f64>,
    unknown_usage_attempts: u64,
}

impl Default for UsageTotal {
    fn default() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: Some(0.0),
            unknown_usage_attempts: 0,
        }
    }
}

impl UsageTotal {
    fn add_outcome(&mut self, outcome: &AnswerOutcome) {
        self.add(&UsageAccounting::from_calls(&outcome.reader_calls));
    }

    fn add(&mut self, usage: &UsageAccounting) {
        self.input_tokens = self
            .input_tokens
            .saturating_add(usage.observed_input_tokens);
        self.output_tokens = self
            .output_tokens
            .saturating_add(usage.observed_output_tokens);
        self.unknown_usage_attempts = self
            .unknown_usage_attempts
            .saturating_add(usage.unknown_usage_attempts);
        self.cost_usd = crate::core::error::sum_costs(self.cost_usd, usage.estimated_cost_usd);
    }
}

/// Ingest + answer every sample, returning `(question_id, hypothesis)` in
/// sample order. `on_emit` sees completed and failed questions (live trace);
/// an error from it aborts the run.
pub fn run(
    eng: &MemoryEngine,
    samples: &[LmSample],
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    cfg: &LmevalConfig,
    on_emit: &mut QuestionObserver<'_, PredictionOutcome>,
) -> Result<Vec<(String, String)>> {
    cfg.bench.validate()?;
    if cfg.reader_concurrency == 0 {
        return Err(BenchError::Dataset(
            "reader_concurrency must be positive".into(),
        ));
    }
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    // Phase 1: ingest each question's private haystack into its own region.
    // Writes are single-writer, so this stays sequential; questions then fan
    // out as reads.
    ingest::prepare_regions(eng, samples, embedder, cfg.encrypted, cfg.reuse)?;

    // Phase 2: answer each question concurrently; results returned in sample
    // order.
    let t_answer = Instant::now();
    let bench = LongMemEval::new(cfg.bench.temporal_glosses);
    let total = samples.len();
    eprintln!("phase 2: answer {total} questions: started");
    let workers = cfg.reader_concurrency.max(1);
    let gate = Gate::new(workers);
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let observed = Mutex::new(on_emit);
    let err_slot: Mutex<Vec<QuestionFailure>> = Mutex::new(Vec::new());
    let spent: Mutex<UsageTotal> = Mutex::new(UsageTotal::default());
    let (tx, rx) = std::sync::mpsc::channel::<PredictionOutcome>();
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
                        spent_r.lock().expect("usage poisoned").add_outcome(&o);
                        let completed = PredictionOutcome {
                            index: i,
                            question_id: s.question_id.clone(),
                            outcome: o,
                        };
                        let emit = {
                            let mut observer = observed_r.lock().expect("observer poisoned");
                            (*observer)(QuestionEvent::Completed(&completed))
                        };
                        match emit {
                            Ok(()) => {
                                let _ = tx.send(completed);
                            }
                            Err(e) => {
                                failed_r.store(true, Ordering::Relaxed);
                                let failure = QuestionFailure::new(
                                    QuestionIdentity::LongMemEval {
                                        question_id: s.question_id.clone(),
                                    },
                                    QuestionStage::Observer,
                                    e,
                                )
                                .prepend_calls(completed.outcome.reader_calls)
                                .with_completed_output(completed.outcome.answer, None);
                                observe_failure(failure, observed_r, err_r);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        failed_r.store(true, Ordering::Relaxed);
                        observe_failure(
                            QuestionFailure::new(
                                QuestionIdentity::LongMemEval {
                                    question_id: s.question_id.clone(),
                                },
                                QuestionStage::Reader,
                                e,
                            ),
                            observed_r,
                            err_r,
                        );
                        break;
                    }
                }
            });
        }
        drop(tx);
    });

    let failures = err_slot.into_inner().expect("err slot poisoned");
    if !failures.is_empty() {
        let mut completed: Vec<_> = rx.into_iter().collect();
        completed.sort_by_key(|completed| completed.index);
        return Err(BenchError::Questions(Box::new(QuestionBatchFailure {
            failures,
            completed: completed
                .iter()
                .map(PredictionOutcome::completion_receipt)
                .collect(),
        })));
    }
    eprintln!(
        "  phase 2 (answer {total}) {:.1}s",
        t_answer.elapsed().as_secs_f64()
    );
    let spent = spent.into_inner().expect("usage poisoned");
    // None for local models and for snapshots absent from the pricing table.
    let cost = match spent.cost_usd {
        Some(usd) => format!("est cost ~${usd:.4}"),
        None if spent.unknown_usage_attempts != 0 => format!(
            "cost incomplete ({} attempts with unknown usage)",
            spent.unknown_usage_attempts
        ),
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
    for completed in rx {
        slots[completed.index] = Some((completed.question_id, completed.outcome.answer));
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
        let priced = UsageAccounting {
            observed_input_tokens: u64::from(u32::MAX),
            observed_output_tokens: u64::from(u32::MAX),
            unknown_usage_attempts: 0,
            estimated_cost_usd: Some(0.5),
        };
        let unknown = UsageAccounting {
            estimated_cost_usd: None,
            ..priced
        };
        for calls in [[&priced, &unknown], [&unknown, &priced]] {
            let mut total = UsageTotal::default();
            assert_eq!(total.cost_usd, Some(0.0));
            for call in calls {
                total.add(call);
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
