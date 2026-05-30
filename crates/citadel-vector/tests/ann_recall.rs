//! ANN recall vs brute-force ground truth on seeded synthetic data. The 100K
//! case is `#[ignore]` (heavy); run it with --release.

use citadel_vector::{AnnIndex, Metric};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn random_dataset(seed: u64, n: usize, dim: u16) -> Vec<(u64, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            let v: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0_f32)).collect();
            ((i as u64) + 1, v)
        })
        .collect()
}

fn random_queries(seed: u64, n: usize, dim: u16) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0..1.0_f32)).collect())
        .collect()
}

/// SIFT-like synthetic: noisy combinations of `intrinsic_dim` basis vectors.
fn sift_like_dataset(
    seed: u64,
    n: usize,
    dim: u16,
    intrinsic_dim: usize,
    noise: f32,
) -> Vec<(u64, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let basis: Vec<Vec<f32>> = (0..intrinsic_dim)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0..1.0_f32)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let coeffs: Vec<f32> = (0..intrinsic_dim)
                .map(|_| rng.gen_range(0.0..1.0_f32))
                .collect();
            let mut v = vec![0.0_f32; dim as usize];
            for (b, &c) in basis.iter().zip(coeffs.iter()) {
                for (vi, &bi) in v.iter_mut().zip(b.iter()) {
                    *vi += c * bi;
                }
            }
            for vi in v.iter_mut() {
                *vi += rng.gen_range(-noise..noise);
            }
            ((i as u64) + 1, v)
        })
        .collect()
}

fn brute_force_top_k(
    rows: &[(u64, Vec<f32>)],
    query: &[f32],
    metric: Metric,
    k: usize,
) -> Vec<u64> {
    let mut scored: Vec<(u64, f32)> = rows
        .iter()
        .map(|(id, v)| (*id, distance(query, v, metric)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

fn distance(a: &[f32], b: &[f32], metric: Metric) -> f32 {
    match metric {
        Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum::<f32>(),
        Metric::InnerProduct => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        Metric::Cosine => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if na == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - dot / (na * nb)
            }
        }
    }
}

fn recall_at_k(predicted: &[u64], truth: &[u64]) -> f32 {
    let truth_set: std::collections::HashSet<u64> = truth.iter().copied().collect();
    let hits = predicted.iter().filter(|id| truth_set.contains(id)).count();
    hits as f32 / truth.len() as f32
}

struct RecallSpec {
    n: usize,
    dim: u16,
    num_queries: usize,
    k: usize,
    ef: usize,
    metric: Metric,
    target: f32,
    /// `Some(intrinsic_dim)` for SIFT-like clustered data, `None` for uniform random.
    intrinsic_dim: Option<usize>,
}

fn run_recall_eval(spec: RecallSpec) {
    let (rows, queries) = match spec.intrinsic_dim {
        None => (
            random_dataset(1, spec.n, spec.dim),
            random_queries(2, spec.num_queries, spec.dim),
        ),
        Some(idim) => (
            sift_like_dataset(1, spec.n, spec.dim, idim, 0.05),
            sift_like_dataset(2, spec.num_queries, spec.dim, idim, 0.05)
                .into_iter()
                .map(|(_, v)| v)
                .collect(),
        ),
    };
    let index = AnnIndex::build(rows.clone(), spec.metric, spec.dim).expect("build");

    let mut sum_recall = 0.0_f32;
    for q in &queries {
        let truth = brute_force_top_k(&rows, q, spec.metric, spec.k);
        let hits = index.search_with_ef(q, spec.k, spec.ef);
        let predicted: Vec<u64> = hits.into_iter().map(|(id, _)| id).collect();
        sum_recall += recall_at_k(&predicted, &truth);
    }
    let mean_recall = sum_recall / spec.num_queries as f32;
    assert!(
        mean_recall >= spec.target,
        "recall@{k} = {mean_recall:.3} on n={n} dim={d} ef={ef} metric={m:?} intrinsic={i:?} (target {t})",
        k = spec.k,
        n = spec.n,
        d = spec.dim,
        ef = spec.ef,
        m = spec.metric,
        i = spec.intrinsic_dim,
        t = spec.target,
    );
}

#[test]
fn recall_at_10_l2_small_uniform_random() {
    run_recall_eval(RecallSpec {
        n: 2_000,
        dim: 64,
        num_queries: 50,
        k: 10,
        ef: 200,
        metric: Metric::L2,
        target: 0.95,
        intrinsic_dim: None,
    });
}

#[test]
fn recall_at_10_cosine_small_uniform_random() {
    run_recall_eval(RecallSpec {
        n: 2_000,
        dim: 64,
        num_queries: 50,
        k: 10,
        ef: 200,
        metric: Metric::Cosine,
        target: 0.95,
        intrinsic_dim: None,
    });
}

/// Target case: recall@10 >= 0.95 at ef=200, dim=128, SIFT-like clustered data.
#[test]
fn recall_at_10_l2_medium_sift_like() {
    run_recall_eval(RecallSpec {
        n: 10_000,
        dim: 128,
        num_queries: 25,
        k: 10,
        ef: 200,
        metric: Metric::L2,
        target: 0.95,
        intrinsic_dim: Some(16),
    });
}

/// Uniform dim=128: SQ8 is lossy at full intrinsic dim, so a lower target.
#[test]
fn recall_at_10_l2_medium_uniform_random() {
    run_recall_eval(RecallSpec {
        n: 10_000,
        dim: 128,
        num_queries: 25,
        k: 10,
        ef: 200,
        metric: Metric::L2,
        target: 0.70,
        intrinsic_dim: None,
    });
}

#[test]
#[ignore = "100K vector SIFT-like recall — heavy build; run with --release locally"]
fn recall_at_10_l2_100k_sift_like() {
    run_recall_eval(RecallSpec {
        n: 100_000,
        dim: 128,
        num_queries: 100,
        k: 10,
        ef: 200,
        metric: Metric::L2,
        target: 0.95,
        intrinsic_dim: Some(16),
    });
}
