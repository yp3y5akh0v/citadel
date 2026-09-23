#![cfg(feature = "cuda-embed")]

use citadel_mem::{CandleEmbedder, CrossEncoder, Reranker};
use serde_json::{json, Value};
use std::{fs, process::Command};

const CASE: &str = "CITADEL_CUDA_PRECISION_CHILD";
const OUTPUT: &str = "CITADEL_CUDA_PRECISION_OUTPUT";
const TEST: &str = "cross_encoder_math_mode_is_independent_of_constructor_order";

fn child(case: &str) {
    let initial = candle_core::cuda::gemm_reduced_precision_f32();
    assert!(
        !initial,
        "fresh child must start with Candle's default f32 mode"
    );
    let after_embedder = match case {
        "standalone" => None,
        "after_embedder" => {
            let directory = std::env::var("CITADEL_EMBEDDER_DIR")
                .expect("CITADEL_EMBEDDER_DIR must name a local e5-large-v1 model");
            let embedder = CandleEmbedder::e5_large(directory).unwrap();
            let observed = candle_core::cuda::gemm_reduced_precision_f32();
            drop(embedder);
            Some(observed)
        }
        _ => panic!("unknown precision child case"),
    };
    let directory = std::env::var("CITADEL_RERANKER_DIR")
        .expect("CITADEL_RERANKER_DIR must name a local ms-marco-MiniLM-L-6-v2 model");
    let reranker = CrossEncoder::ms_marco_minilm_l6(directory).unwrap();
    let after_reranker = candle_core::cuda::gemm_reduced_precision_f32();
    let passages = [
        "Maya took the train to Boston to attend a jazz concert.",
        "The red bicycle was repaired in the garage yesterday.",
        "Sam planted tomatoes in the garden before the rain.",
        "Maya bought two tickets for the evening concert in Boston.",
    ];
    let queries = ["Where did Maya travel?", "What did Sam plant?"];
    let scores = queries
        .iter()
        .map(|query| reranker.rerank(query, &passages).unwrap())
        .collect::<Vec<_>>();
    assert!(scores
        .iter()
        .all(|row| row.len() == passages.len() && row.iter().all(|score| score.is_finite())));
    let bits = scores
        .iter()
        .map(|row| row.iter().map(|score| score.to_bits()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let ranks = scores
        .iter()
        .map(|row| {
            let mut order = (0..row.len()).collect::<Vec<_>>();
            order.sort_by(|&a, &b| row[b].total_cmp(&row[a]).then(a.cmp(&b)));
            order
        })
        .collect::<Vec<_>>();
    let receipt = json!({"case":case,"pid":std::process::id(),"initial_tf32":initial,
        "after_embedder_tf32":after_embedder,"after_cross_encoder_tf32":after_reranker,
        "after_inference_tf32":candle_core::cuda::gemm_reduced_precision_f32(),
        "score_bits":bits,"rankings":ranks});
    fs::write(
        std::env::var(OUTPUT).expect("child output path"),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    assert!(
        after_reranker,
        "CrossEncoder must establish the declared CUDA f32 GEMM mode"
    );
    assert!(candle_core::cuda::gemm_reduced_precision_f32());
    assert!(after_embedder.is_none_or(|mode| mode));
}

/// Requires a GPU and local assets. Each child starts with a fresh Candle global;
/// no test mutates the precision flag to manufacture the desired result.
#[test]
#[ignore = "requires CUDA, CITADEL_EMBEDDER_DIR and CITADEL_RERANKER_DIR local models"]
fn cross_encoder_math_mode_is_independent_of_constructor_order() {
    match std::env::var(CASE) {
        Ok(case) => return child(&case),
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => panic!("invalid child case: {error}"),
    }
    let temporary = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut results = Vec::new();
    let mut failed = Vec::new();
    for case in ["standalone", "after_embedder"] {
        let output_path = temporary.path().join(format!("{case}.json"));
        let output = Command::new(&executable)
            .args([
                "--exact",
                TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CASE, case)
            .env(OUTPUT, &output_path)
            .output()
            .unwrap();
        if output_path.exists() {
            let result: Value = serde_json::from_slice(&fs::read(&output_path).unwrap()).unwrap();
            println!("CUDA constructor-order receipt: {result}");
            results.push(result);
        }
        if !output.status.success() {
            failed.push(format!(
                "{case}: {}\n{}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
    assert_eq!(results.len(), 2);
    assert_ne!(results[0]["pid"], std::process::id());
    assert_ne!(results[1]["pid"], std::process::id());
    assert_eq!(results[0]["score_bits"], results[1]["score_bits"]);
    assert_eq!(results[0]["rankings"], results[1]["rankings"]);
    assert_eq!(results[0]["after_cross_encoder_tf32"], true);
    assert_eq!(results[1]["after_cross_encoder_tf32"], true);
}
