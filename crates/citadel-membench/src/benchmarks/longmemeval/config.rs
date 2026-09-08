//! Validated LongMemEval launch options.

use citadel_mem::RerankStrategy;

use crate::core::config::{
    boolean, environment, invalid, number, rerank_strategy, validate_llm_options, EmbedderModel,
};
use crate::{BenchConfig, BenchError, ReaderOrder, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    Scored,
    RetrievalDiag,
    DryRun,
}

#[derive(Debug)]
pub struct RunConfig {
    pub mode: RunMode,
    pub bench: BenchConfig,
    pub embedder: EmbedderModel,
    pub rerank_strategy: RerankStrategy,
    pub max_samples: Option<usize>,
    pub encrypted: bool,
    pub reader_concurrency: usize,
    pub reader_tpm: Option<u64>,
}

impl RunConfig {
    pub fn from_env() -> Result<Self> {
        let values = environment("CITADEL_LONGMEMEVAL_")?;
        Self::from_lookup(|key| values.get(key).cloned())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        for suffix in [
            "JUDGE_MODEL",
            "JUDGE_PROVIDER",
            "JUDGE_CONCURRENCY",
            "JUDGE_TPM",
            "GRAPH_SLOTS",
            "GRAPH_SWEEP_SLOTS",
            "GRAPH_SEEDS",
            "GRAPH_POOL",
            "GRAPH_EDGES",
            "GRAPH_MAX_DF",
            "GRAPH_MIN_ENTITY_LEN",
            "GRAPH_DIAG",
        ] {
            let key = format!("CITADEL_LONGMEMEVAL_{suffix}");
            if get(&key).is_some() {
                return Err(BenchError::Dataset(format!(
                    "{key} is not supported by LongMemEval"
                )));
            }
        }
        validate_llm_options(&get, "CITADEL_LONGMEMEVAL_READER")?;
        for suffix in ["DRY_RUN", "RETRIEVAL_DIAG", "MOCK_EMBED"] {
            let key = format!("CITADEL_LONGMEMEVAL_{suffix}");
            if get(&key).is_some() {
                return Err(BenchError::Dataset(format!(
                    "{key} is unsupported; use CITADEL_LONGMEMEVAL_MODE with a real embedder"
                )));
            }
        }
        if get("CITADEL_LONGMEMEVAL_READER_ORDER").is_some() {
            return Err(BenchError::Dataset("LongMemEval uses canonical session/date order; unset CITADEL_LONGMEMEVAL_READER_ORDER".into()));
        }
        let mode = match get("CITADEL_LONGMEMEVAL_MODE")
            .as_deref()
            .unwrap_or("scored")
        {
            "scored" => RunMode::Scored,
            "retrieval-diag" => RunMode::RetrievalDiag,
            "dry-run" => RunMode::DryRun,
            _ => {
                return Err(invalid(
                    "CITADEL_LONGMEMEVAL_MODE",
                    "scored|retrieval-diag|dry-run",
                ))
            }
        };
        let max_samples = get("CITADEL_LONGMEMEVAL_MAX_SAMPLES")
            .map(|_| number(&get, "CITADEL_LONGMEMEVAL_MAX_SAMPLES", 0, 1))
            .transpose()?;
        let reader_tpm = get("CITADEL_LONGMEMEVAL_READER_TPM")
            .map(|_| number(&get, "CITADEL_LONGMEMEVAL_READER_TPM", 0, 1).map(|n| n as u64))
            .transpose()?;
        let max_tokens = number(&get, "CITADEL_MEMBENCH_MAX_TOKENS", 800, 1)?;
        let config = Self {
            mode,
            bench: BenchConfig {
                top_k: number(&get, "CITADEL_LONGMEMEVAL_TOP_K", 50, 1)?,
                reader_order: ReaderOrder::Relevance,
                neighbor_radius: number(&get, "CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS", 0, 0)?,
                reader_max_tokens: u32::try_from(max_tokens)
                    .map_err(|_| invalid("CITADEL_MEMBENCH_MAX_TOKENS", "a positive u32"))?,
                agentic: boolean(&get, "CITADEL_LONGMEMEVAL_AGENTIC", false)?,
            },
            embedder: EmbedderModel::parse(
                get("CITADEL_LONGMEMEVAL_EMBEDDER").as_deref(),
                "CITADEL_LONGMEMEVAL_EMBEDDER",
            )?,
            rerank_strategy: rerank_strategy(
                get("CITADEL_LONGMEMEVAL_RERANK_STRATEGY").as_deref(),
                "CITADEL_LONGMEMEVAL_RERANK_STRATEGY",
            )?,
            max_samples,
            encrypted: boolean(&get, "CITADEL_LONGMEMEVAL_ENCRYPTED", false)?,
            reader_concurrency: number(&get, "CITADEL_LONGMEMEVAL_READER_CONCURRENCY", 3, 1)?,
            reader_tpm,
        };
        config.bench.validate()?;
        if mode == RunMode::DryRun && get("CITADEL_LONGMEMEVAL_DB_PATH").is_some() {
            return Err(BenchError::Dataset(
                "CITADEL_LONGMEMEVAL_DB_PATH is unused in dry-run mode".into(),
            ));
        }
        if mode != RunMode::Scored && config.bench.agentic {
            return Err(BenchError::Dataset(
                "CITADEL_LONGMEMEVAL_AGENTIC requires scored mode".into(),
            ));
        }
        if mode == RunMode::RetrievalDiag && config.bench.neighbor_radius != 0 {
            return Err(BenchError::Dataset("retrieval-diag measures recall before neighbor expansion; set CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS=0".into()));
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<RunConfig> {
        RunConfig::from_lookup(|key| {
            values
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).into())
        })
    }

    #[test]
    fn defaults_are_scored_and_model_based() {
        let c = config(&[]).unwrap();
        assert_eq!(c.mode, RunMode::Scored);
        assert_eq!(c.embedder, EmbedderModel::E5Large);
        assert_eq!(c.bench.reader_max_tokens, 800);
        assert!(!c.bench.agentic);
    }

    #[test]
    fn malformed_options_never_fall_back() {
        for (key, value) in [
            ("CITADEL_LONGMEMEVAL_TOP_K", "0"),
            ("CITADEL_LONGMEMEVAL_READER_CONCURRENCY", "0"),
            ("CITADEL_LONGMEMEVAL_READER_TPM", "bad"),
            ("CITADEL_LONGMEMEVAL_RERANK_STRATEGY", "bad"),
            ("CITADEL_LONGMEMEVAL_EMBEDDER", "bad"),
            ("CITADEL_LONGMEMEVAL_ENCRYPTED", "maybe"),
            ("CITADEL_LONGMEMEVAL_AGENTIC", "maybe"),
            ("CITADEL_LONGMEMEVAL_MODE", "graph-diag"),
            ("CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG", "0"),
            ("CITADEL_LONGMEMEVAL_MOCK_EMBED", "1"),
            ("CITADEL_LONGMEMEVAL_READER_ORDER", "sessions"),
            ("CITADEL_MEMBENCH_MAX_TOKENS", "4294967296"),
        ] {
            assert!(config(&[(key, value)]).is_err(), "accepted {key}={value}");
        }
    }

    #[test]
    fn retrieval_diagnostic_reuses_validated_corpora_without_reader_calls() {
        let c = config(&[
            ("CITADEL_LONGMEMEVAL_MODE", "retrieval-diag"),
            ("CITADEL_LONGMEMEVAL_DB_PATH", "corpus.cdl"),
        ])
        .unwrap();
        assert_eq!(c.mode, RunMode::RetrievalDiag);
        assert!(config(&[
            ("CITADEL_LONGMEMEVAL_MODE", "retrieval-diag"),
            ("CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS", "1")
        ])
        .is_err());
    }

    #[test]
    fn zero_neighbors_and_false_agentic_are_explicit() {
        let c = config(&[
            ("CITADEL_LONGMEMEVAL_NEIGHBOR_RADIUS", "0"),
            ("CITADEL_LONGMEMEVAL_AGENTIC", "false"),
        ])
        .unwrap();
        assert_eq!(c.bench.neighbor_radius, 0);
        assert!(!c.bench.agentic);
    }

    #[test]
    fn locomo_only_options_and_inactive_options_are_rejected() {
        for suffix in ["GRAPH_SLOTS", "JUDGE_MODEL", "JUDGE_PROVIDER"] {
            assert!(config(&[(&format!("CITADEL_LONGMEMEVAL_{suffix}"), "0")]).is_err());
        }
        assert!(config(&[
            ("CITADEL_LONGMEMEVAL_MODE", "dry-run"),
            ("CITADEL_LONGMEMEVAL_DB_PATH", "corpus.cdl"),
        ])
        .is_err());
        assert!(config(&[
            ("CITADEL_LONGMEMEVAL_MODE", "retrieval-diag"),
            ("CITADEL_LONGMEMEVAL_AGENTIC", "true"),
        ])
        .is_err());
    }
}
