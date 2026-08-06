//! Client-side concurrency + rate-limit pacing for the live runner. Timing-only
//! primitives so reader and judge each run at their own rate under OpenAI's TPM limit:
//!   - [`Gate`]: counting semaphore capping requests-in-flight per role.
//!   - [`TpmBucket`]: refilling token bucket capping tokens/minute per model.
//!   - [`Pacer`]: the per-model buckets, keyed by model id.
//!
//! These change only when a call is admitted, never the request/response.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

/// Floor on every paced sleep so a tiny deficit cannot busy-spin the CPU.
const MIN_SLEEP: Duration = Duration::from_millis(5);

/// Default tokens/minute per model (OpenAI Tier-1): gpt-4o-mini/nano allow ~10x
/// gpt-4o, so the pacer must not flat-cap them low. Override with the bin's *_TPM env.
pub fn default_tpm_for_model(model: &str) -> u64 {
    let m = model.to_ascii_lowercase();
    if m.contains("mini") || m.contains("nano") {
        2_000_000
    } else {
        200_000
    }
}

/// Fair counting semaphore; a permit is held around one round-trip (incl. its retry
/// backoff), so the count equals requests-in-flight for that role.
pub struct Gate {
    free: Mutex<usize>,
    cv: Condvar,
}

impl Gate {
    pub fn new(permits: usize) -> Self {
        Self {
            free: Mutex::new(permits.max(1)),
            cv: Condvar::new(),
        }
    }

    /// Block until a permit is free, then take it. The permit is returned on drop.
    pub fn acquire(&self) -> Permit<'_> {
        let mut free = self.free.lock().expect("gate poisoned");
        while *free == 0 {
            free = self.cv.wait(free).expect("gate poisoned");
        }
        *free -= 1;
        Permit { gate: self }
    }
}

/// RAII permit: releases its [`Gate`] slot when dropped.
pub struct Permit<'a> {
    gate: &'a Gate,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        *self.gate.free.lock().expect("gate poisoned") += 1;
        self.gate.cv.notify_one();
    }
}

/// Refilling token bucket for a TPM limit: capacity = one burst, refill = `tpm/60`
/// per second. The lock is held only for the arithmetic, never the sleep.
pub struct TpmBucket {
    tpm: u64,
    refill_per_sec: f64,
    capacity: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    available: f64,
    last_refill: Instant,
}

impl TpmBucket {
    pub fn new(tpm: u64, burst_frac: f64) -> Self {
        let tpm = tpm.max(1); // never 0 -> refill_per_sec > 0, no NaN/inf sleep
        let rate = tpm as f64;
        let capacity = (rate * burst_frac).max(1.0);
        Self {
            tpm,
            refill_per_sec: rate / 60.0,
            capacity,
            state: Mutex::new(BucketState {
                available: capacity,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Block until `cost` tokens are available, then consume them. A `cost` above
    /// capacity is clamped (it drains the bucket rather than deadlocking).
    pub fn acquire(&self, cost: usize) {
        let cost = (cost as f64).min(self.capacity);
        loop {
            let wait = {
                let mut s = self.state.lock().expect("tpm bucket poisoned");
                let now = Instant::now();
                let elapsed = now.duration_since(s.last_refill).as_secs_f64();
                s.available = (s.available + elapsed * self.refill_per_sec).min(self.capacity);
                s.last_refill = now;
                if s.available >= cost {
                    s.available -= cost;
                    return;
                }
                Duration::from_secs_f64((cost - s.available) / self.refill_per_sec)
            }; // lock dropped before sleeping
            sleep(wait.max(MIN_SLEEP));
        }
    }

    /// Empty the bucket so the whole pool backs off in unison after a residual 429.
    pub fn penalize(&self) {
        let mut s = self.state.lock().expect("tpm bucket poisoned");
        s.available = 0.0;
        s.last_refill = Instant::now();
    }
}

/// Per-model TPM buckets keyed by model id; equal reader/judge models share one.
/// A model with no bucket makes [`Pacer::acquire`] a no-op.
#[derive(Clone)]
pub struct Pacer {
    buckets: FxHashMap<String, Arc<TpmBucket>>,
}

/// Reader and judge can share a model id; the tighter limit has to win, or the
/// looser one over-admits into the provider's real quota.
fn insert_tighter_bucket(
    buckets: &mut FxHashMap<String, Arc<TpmBucket>>,
    key: String,
    tpm: u64,
    burst: f64,
) {
    let tpm = tpm.max(1);
    match buckets.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if tpm < entry.get().tpm {
                entry.insert(Arc::new(TpmBucket::new(tpm, burst)));
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(Arc::new(TpmBucket::new(tpm, burst)));
        }
    }
}

impl Pacer {
    /// Build buckets for the reader and judge models. Equal ids collapse to one
    /// shared bucket. `CITADEL_MEMBENCH_TPM_BURST_FRAC` (default 1.0) scales capacity.
    pub fn new(reader_model: &str, reader_tpm: u64, judge_model: &str, judge_tpm: u64) -> Self {
        let burst = std::env::var("CITADEL_MEMBENCH_TPM_BURST_FRAC")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            // A non-finite fraction would make capacity infinite, silently
            // disabling the limiter it was meant to scale.
            .filter(|f| f.is_finite() && *f > 0.0)
            .unwrap_or(1.0);
        let mut buckets = FxHashMap::default();
        insert_tighter_bucket(&mut buckets, reader_model.to_string(), reader_tpm, burst);
        insert_tighter_bucket(&mut buckets, judge_model.to_string(), judge_tpm, burst);
        Self { buckets }
    }

    /// A pacer with no buckets: every [`acquire`](Self::acquire) is a no-op.
    pub fn unbounded() -> Self {
        Self {
            buckets: FxHashMap::default(),
        }
    }

    /// Block until `cost` tokens are available for `model` (no-op if untracked).
    pub fn acquire(&self, model: &str, cost: usize) {
        if let Some(b) = self.buckets.get(model) {
            b.acquire(cost);
        }
    }

    /// Drain `model`'s bucket after a residual 429 (no-op if untracked).
    pub fn penalize(&self, model: &str) {
        if let Some(b) = self.buckets.get(model) {
            b.penalize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that mutate process-global env vars.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn gate_caps_in_flight_and_releases_on_drop() {
        let g = Gate::new(2);
        let p1 = g.acquire();
        let _p2 = g.acquire();
        assert_eq!(*g.free.lock().unwrap(), 0, "both permits taken");
        drop(p1);
        assert_eq!(*g.free.lock().unwrap(), 1, "permit returned on drop");
    }

    #[test]
    fn bucket_admits_initial_burst_then_throttles() {
        // capacity = 600 tokens, refill 10/s. First 600 are instant; the next
        // costing 60 must wait ~6s, so the second acquire is measurably delayed.
        let b = TpmBucket::new(600, 1.0);
        b.acquire(600); // drains the burst instantly
        let t = Instant::now();
        b.acquire(60);
        assert!(
            t.elapsed() >= Duration::from_secs(5),
            "throttled after burst"
        );
    }

    #[test]
    fn unbounded_pacer_is_noop() {
        let p = Pacer::unbounded();
        p.acquire("anything", 1_000_000); // returns immediately, no bucket
        p.penalize("anything");
    }

    #[test]
    fn same_model_reader_and_judge_share_one_bucket() {
        let p = Pacer::new("m", 1000, "m", 1000);
        assert_eq!(p.buckets.len(), 1, "equal model ids collapse to one bucket");
    }

    #[test]
    fn default_tpm_gives_mini_more_than_full_models() {
        assert_eq!(default_tpm_for_model("gpt-4o-mini"), 2_000_000);
        assert_eq!(default_tpm_for_model("gpt-4o"), 200_000);
        assert!(default_tpm_for_model("gpt-4o-mini") > default_tpm_for_model("gpt-4o"));
    }

    #[test]
    fn a_shared_model_keeps_the_tighter_limit_whichever_role_is_first() {
        // The judge is tighter here, and it is inserted second.
        let judge_second = Pacer::new("m", 900_000, "m", 1_000);
        assert_eq!(judge_second.buckets.len(), 1);
        assert_eq!(judge_second.buckets["m"].tpm, 1_000);

        // Reader tighter, inserted first: still the tighter one.
        let reader_first = Pacer::new("m", 1_000, "m", 900_000);
        assert_eq!(reader_first.buckets["m"].tpm, 1_000);
    }

    #[test]
    fn a_non_finite_burst_fraction_cannot_disable_the_limiter() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::set_var("CITADEL_MEMBENCH_TPM_BURST_FRAC", "inf");
        let p = Pacer::new("m", 600, "m", 600);
        std::env::remove_var("CITADEL_MEMBENCH_TPM_BURST_FRAC");
        assert!(
            p.buckets["m"].capacity.is_finite(),
            "an infinite burst would make every acquire free"
        );
    }
}
