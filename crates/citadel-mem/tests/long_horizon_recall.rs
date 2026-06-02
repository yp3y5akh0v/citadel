//! Long-horizon recall: a needle written early stays in top-k after many writes,
//! amid near distractors. Uses MockEmbedder (CI-safe). Metric: recall@k membership.

use std::sync::Arc;
use std::time::{Duration, Instant};

use citadel::{Argon2Profile, DatabaseBuilder};
use citadel_mem::{AtomId, AtomInput, FusionWeights, MemoryEngine, MockEmbedder, RecallQuery};

const REGION: &str = "longhorizon";
const DIM: usize = 384;
const HARD_NEGATIVES: usize = 9;
const K: usize = 5;
const GATE: f64 = 0.80;

fn open_engine() -> (tempfile::TempDir, MemoryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseBuilder::new(dir.path().join("m.db"))
            .passphrase(b"test-passphrase")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap(),
    );
    let eng = MemoryEngine::open(db).unwrap();
    eng.create_region(REGION, Arc::new(MockEmbedder::new(DIM)))
        .unwrap();
    (dir, eng)
}

/// `{f}` suffix makes each family's tokens unique so families don't cross-contaminate.
fn needle_text(f: usize) -> String {
    format!("project apollo{f} deadline november{f} seventeenth conference room zenith{f}")
}

/// Needle subject minus the answer token, so the wrong-date negative stays in contention.
fn probe_text(f: usize) -> String {
    format!("project apollo{f} deadline conference room zenith{f}")
}

/// Nine near negatives: each flips one answer token but shares the probe tokens.
fn hard_negatives(f: usize) -> Vec<String> {
    vec![
        format!("project apollo{f} deadline december{f} third conference room zenith{f}"),
        format!("project gemini{f} deadline november{f} seventeenth conference room zenith{f}"),
        format!("project apollo{f} deadline november{f} seventeenth conference room atlas{f}"),
        format!("project apollo{f} kickoff november{f} seventeenth conference room zenith{f}"),
        format!("project apollo{f} deadline october{f} ninth conference room zenith{f}"),
        format!("project mercury{f} deadline november{f} seventeenth conference room zenith{f}"),
        format!("project apollo{f} deadline november{f} seventeenth conference room olympus{f}"),
        format!("project apollo{f} deadline november{f} eleventh conference room zenith{f}"),
        format!("project apollo{f} review november{f} seventeenth conference room zenith{f}"),
    ]
}

/// Filler with vocabulary disjoint from every needle: corpus size, not rank competition.
fn filler_text(i: usize) -> String {
    format!("logistics ledger entry{i} warehouse shelf{i} inventory pallet{i} shipment manifest{i}")
}

fn atom(text: String) -> AtomInput {
    AtomInput::new("fact", text)
}

fn needle_in_topk(
    eng: &MemoryEngine,
    f: usize,
    needle_id: AtomId,
    weights: Option<FusionWeights>,
) -> bool {
    let mut q = RecallQuery::by_text(probe_text(f), K);
    if let Some(w) = weights {
        q = q.with_weights(w);
    }
    let hits = eng.recall(REGION, q).unwrap();
    hits.iter().any(|h| h.id == needle_id)
}

/// Plant `families` needles then all distractors (one ANN build, no interleaved
/// writes); returns the needle ids in family order.
fn plant_corpus(eng: &MemoryEngine, families: usize, filler: usize) -> Vec<AtomId> {
    let mut needle_ids = Vec::with_capacity(families);
    for f in 0..families {
        needle_ids.push(eng.remember(REGION, atom(needle_text(f))).unwrap());
    }
    let mut rest = Vec::new();
    for f in 0..families {
        for hn in hard_negatives(f) {
            rest.push(atom(hn));
        }
    }
    for i in 0..filler {
        rest.push(atom(filler_text(i)));
    }
    eng.remember_batch(REGION, rest).unwrap();
    needle_ids
}

fn accuracy(eng: &MemoryEngine, needle_ids: &[AtomId], weights: Option<FusionWeights>) -> f64 {
    let hits = (0..needle_ids.len())
        .filter(|&f| needle_in_topk(eng, f, needle_ids[f], weights))
        .count();
    hits as f64 / needle_ids.len() as f64
}

const NEUTRAL: FusionWeights = FusionWeights {
    semantic: 0.5,
    keyword: 0.4,
    recency: 0.0,
    importance: 0.1,
};

#[test]
fn step1_recalled_at_long_horizon() {
    // 12 needles + 108 hard negatives + 40 filler = 160 atoms.
    let families = 12;
    let (_dir, eng) = open_engine();
    let needle_ids = plant_corpus(&eng, families, 40);

    // Two warm-up recalls build the ANN + FTS path before any timing/assertion.
    let _ = eng
        .recall(REGION, RecallQuery::by_text(probe_text(0), K))
        .unwrap();
    let _ = eng
        .recall(REGION, RecallQuery::by_text(probe_text(0), K))
        .unwrap();

    // Mode A: shipped default weights - the old fact survives top-k by default.
    let default_acc = accuracy(&eng, &needle_ids, None);
    // Mode B: recency neutralized - a targeted recall of an old fact is recency-independent.
    let neutral_acc = accuracy(&eng, &needle_ids, Some(NEUTRAL));

    // Sample many recalls (no writes between), gate the median, print p95.
    let mut times = Vec::new();
    for _ in 0..8 {
        for f in 0..families {
            let t = Instant::now();
            let _ = eng
                .recall(REGION, RecallQuery::by_text(probe_text(f), K))
                .unwrap();
            times.push(t.elapsed());
        }
    }
    times.sort();
    let median = times[times.len() / 2];
    let p95 = times[(times.len() * 95) / 100];

    println!(
        "[long-horizon] recall@{K} default={default_acc:.3} recency_neutral={neutral_acc:.3} \
         median={median:?} p95={p95:?} (n={})",
        times.len()
    );

    assert!(
        default_acc >= GATE,
        "default recall@{K} = {default_acc:.3} below floor {GATE}"
    );
    assert!(
        neutral_acc >= GATE,
        "recency-neutral recall@{K} = {neutral_acc:.3} below floor {GATE}"
    );
    assert!(
        median < Duration::from_millis(100),
        "median recall latency {median:?} exceeds 100ms"
    );
}

#[test]
fn recall_accuracy_flat_over_horizon() {
    // Accuracy must not decay as the corpus grows. 6 needles; filler scales N up.
    let families = 6;
    for n in [90usize, 180] {
        let filler = n - families - families * HARD_NEGATIVES;
        let (_dir, eng) = open_engine();
        let needle_ids = plant_corpus(&eng, families, filler);
        let _ = eng
            .recall(REGION, RecallQuery::by_text(probe_text(0), K))
            .unwrap();
        let acc = accuracy(&eng, &needle_ids, None);
        println!("[long-horizon] horizon N={n}: recall@{K}={acc:.3}");
        assert!(
            acc >= GATE,
            "recall@{K} at horizon {n} = {acc:.3} below floor {GATE}"
        );
    }
}

#[test]
fn recall_unbiased_by_needle_depth() {
    // Needles at depths {0,25,50,75,100}% must all be recalled (no positional bias).
    let total = 200;
    let depths = [0usize, 50, 100, 150, 199];
    let (_dir, eng) = open_engine();

    // Build the whole corpus as one ordered batch; capture ids by position.
    let mut atoms = Vec::with_capacity(total);
    let mut needle_family = Vec::new();
    let mut hn_iter: Vec<String> = (0..depths.len()).flat_map(hard_negatives).collect();
    let mut filler_i = 0usize;
    for i in 0..total {
        if let Some(j) = depths.iter().position(|&d| d == i) {
            atoms.push(atom(needle_text(j)));
            needle_family.push((i, j));
        } else if let Some(hn) = hn_iter.pop() {
            atoms.push(atom(hn));
        } else {
            atoms.push(atom(filler_text(filler_i)));
            filler_i += 1;
        }
    }
    let ids = eng.remember_batch(REGION, atoms).unwrap();
    let _ = eng
        .recall(REGION, RecallQuery::by_text(probe_text(0), K))
        .unwrap();

    let hits = needle_family
        .iter()
        .filter(|&&(pos, j)| needle_in_topk(&eng, j, ids[pos], None))
        .count();
    let acc = hits as f64 / depths.len() as f64;
    println!(
        "[long-horizon] depth buckets: recall@{K}={acc:.3} ({hits}/{})",
        depths.len()
    );
    assert!(
        acc >= GATE,
        "depth recall@{K} = {acc:.3} below floor {GATE}"
    );
}

#[test]
fn trivial_unique_needle_is_top_hit() {
    // A unique-token needle with no hard negatives must be rank-1.
    let (_dir, eng) = open_engine();
    let needle_id = eng
        .remember(
            REGION,
            atom("singular beacon tokens quetzal vermilion obelisk".into()),
        )
        .unwrap();
    let filler: Vec<AtomInput> = (0..30).map(|i| atom(filler_text(i))).collect();
    eng.remember_batch(REGION, filler).unwrap();

    let hits = eng
        .recall(
            REGION,
            RecallQuery::by_text("singular beacon quetzal vermilion obelisk", 1),
        )
        .unwrap();
    assert_eq!(
        hits.first().map(|h| h.id),
        Some(needle_id),
        "unique needle must be the top hit"
    );
}
