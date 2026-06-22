//! LongMemEval runner: ingest each question's haystack, answer it, collect predictions.
//! Emit-only; the official Python scorer grades the JSONL.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use citadel_ai::LLMClient;
use citadel_mem::{Embedder, MemoryEngine};

use super::dataset::LmSample;
use super::{ingest, LongMemEval};
use crate::core::error::{BenchError, Result};
use crate::core::eval::{answer_question, Question};
use crate::core::ratelimit::{Gate, Pacer};
use crate::BenchConfig;

pub struct LmevalConfig {
    pub bench: BenchConfig,
    pub encrypted: bool,
    pub reader_concurrency: usize,
}

/// Ingest + answer every sample, returning `(question_id, hypothesis)` in sample order.
/// `on_emit(index, question_id, hypothesis)` fires per answer in completion order (for a
/// live trace); an error from it aborts the run.
pub fn run(
    eng: &MemoryEngine,
    samples: &[LmSample],
    embedder: Arc<dyn Embedder>,
    reader: &dyn LLMClient,
    pacer: &Pacer,
    cfg: &LmevalConfig,
    on_emit: &mut (dyn FnMut(usize, &str, &str) -> Result<()> + Send),
) -> Result<Vec<(String, String)>> {
    // Region names are case-folded by the engine, so a duplicate (case-insensitive)
    // question_id would merge two haystacks; fail loud rather than contaminate.
    let mut seen = rustc_hash::FxHashSet::default();
    for s in samples {
        if !seen.insert(s.question_id.to_ascii_lowercase()) {
            return Err(BenchError::Dataset(format!(
                "duplicate question_id (case-insensitive): {}",
                s.question_id
            )));
        }
    }

    // Phase 1: ingest each question's private haystack into its own region. Writes are
    // single-writer, so this stays sequential; questions then fan out as reads.
    let t_ingest = Instant::now();
    let n = samples.len();
    for (i, s) in samples.iter().enumerate() {
        if cfg.encrypted {
            eng.create_encrypted_region(&s.question_id, Arc::clone(&embedder))?;
        } else {
            eng.create_region(&s.question_id, Arc::clone(&embedder))?;
        }
        ingest::ingest_sample(eng, &s.question_id, s)?;
        if (i + 1) % 25 == 0 || i + 1 == n {
            eprintln!("  ingested {}/{n}", i + 1);
        }
    }
    eprintln!(
        "  phase 1 (ingest {n}) {:.1}s",
        t_ingest.elapsed().as_secs_f64()
    );

    // Phase 2: answer each question concurrently; results returned in sample order.
    let t_answer = Instant::now();
    let bench = LongMemEval;
    let total = samples.len();
    let workers = cfg.reader_concurrency.max(1);
    let gate = Gate::new(workers);
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let observed = Mutex::new(on_emit);
    let err_slot: Mutex<Option<BenchError>> = Mutex::new(None);
    let (tx, rx) = std::sync::mpsc::channel::<(usize, (String, String))>();
    let (next_r, failed_r, observed_r, err_r, gate_r, bench_r) =
        (&next, &failed, &observed, &err_slot, &gate, &bench);

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
                        let emit = (*observed_r.lock().expect("observer poisoned"))(
                            i,
                            &s.question_id,
                            &o.answer,
                        );
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
    let mut slots: Vec<Option<(String, String)>> = (0..total).map(|_| None).collect();
    for (i, pair) in rx {
        slots[i] = Some(pair);
    }
    Ok(slots
        .into_iter()
        .map(|o| o.expect("every question produced a result"))
        .collect())
}
