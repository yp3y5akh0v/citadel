//! Validated launch configuration for the LoCoMo runner.

use citadel_mem::RerankStrategy;

use crate::core::config::{
    boolean, environment, invalid, number, rerank_strategy, validate_llm_options, EmbedderModel,
};
use crate::{BenchConfig, BenchError, ReaderOrder, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    Scored,
    RetrievalDiag,
    ParamSweep,
    Dump,
    Erasure,
    DryRun,
}

impl RunMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "scored" => Ok(Self::Scored),
            "retrieval-diag" => Ok(Self::RetrievalDiag),
            "param-sweep" => Ok(Self::ParamSweep),
            "dump" => Ok(Self::Dump),
            "erasure" => Ok(Self::Erasure),
            "dry-run" => Ok(Self::DryRun),
            _ => Err(invalid(
                "CITADEL_LOCOMO_MODE",
                "scored|retrieval-diag|param-sweep|dump|erasure|dry-run",
            )),
        }
    }
}

#[derive(Debug)]
pub struct RunConfig {
    pub mode: RunMode,
    pub bench: BenchConfig,
    pub embedder: EmbedderModel,
    pub rerank_strategy: RerankStrategy,
    pub max_samples: Option<usize>,
    pub encrypted: bool,
    pub reader_tpm: u64,
    pub judge_tpm: u64,
}

impl RunConfig {
    pub fn from_env() -> Result<Self> {
        let values = environment("CITADEL_LOCOMO_")?;
        Self::from_lookup(|key| values.get(key).cloned())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        for suffix in [
            "GRAPH_DIAG",
            "GRAPH_SLOTS",
            "GRAPH_SWEEP_SLOTS",
            "GRAPH_SEEDS",
            "GRAPH_POOL",
            "GRAPH_EDGES",
            "GRAPH_MAX_DF",
            "GRAPH_MIN_ENTITY_LEN",
        ] {
            let key = format!("CITADEL_LOCOMO_{suffix}");
            if get(&key).is_some() {
                return Err(BenchError::Dataset(format!(
                    "{key} is not supported by the baseline runner"
                )));
            }
        }
        for key in [
            "DRY_RUN",
            "RETRIEVAL_DIAG",
            "PARAM_SWEEP",
            "DUMP_DB",
            "ERASURE_DEMO",
            "MOCK_EMBED",
        ] {
            let key = format!("CITADEL_LOCOMO_{key}");
            if get(&key).is_some() {
                return Err(BenchError::Dataset(format!(
                    "{key} was replaced by CITADEL_LOCOMO_MODE"
                )));
            }
        }
        let mode = RunMode::parse(&get("CITADEL_LOCOMO_MODE").unwrap_or_else(|| "scored".into()))?;
        validate_llm_options(&get, "CITADEL_LOCOMO_READER")?;
        validate_llm_options(&get, "CITADEL_LOCOMO_JUDGE")?;
        if mode != RunMode::Scored && get("CITADEL_LOCOMO_DB_PATH").is_some() {
            return Err(BenchError::Dataset("CITADEL_LOCOMO_DB_PATH is only supported in scored mode; diagnostics use a temporary database".into()));
        }
        let top_k = number(&get, "CITADEL_LOCOMO_TOP_K", 50, 1)?;
        let reader_order = match get("CITADEL_LOCOMO_READER_ORDER")
            .as_deref()
            .unwrap_or("sessions")
        {
            "sessions" => ReaderOrder::Sessions,
            "chrono" => ReaderOrder::Chrono,
            "relevance" => ReaderOrder::Relevance,
            _ => {
                return Err(invalid(
                    "CITADEL_LOCOMO_READER_ORDER",
                    "sessions|chrono|relevance",
                ))
            }
        };
        let rerank_strategy = rerank_strategy(
            get("CITADEL_LOCOMO_RERANK_STRATEGY").as_deref(),
            "CITADEL_LOCOMO_RERANK_STRATEGY",
        )?;
        for key in [
            "CITADEL_LOCOMO_READER_CONCURRENCY",
            "CITADEL_LOCOMO_JUDGE_CONCURRENCY",
        ] {
            number(&get, key, 1, 1)?;
        }
        if get("CITADEL_LOCOMO_CONCURRENCY").is_some_and(|v| v != "1") {
            return Err(invalid(
                "CITADEL_LOCOMO_CONCURRENCY",
                "1 (serial); use READER_CONCURRENCY and JUDGE_CONCURRENCY otherwise",
            ));
        }
        let max_samples = get("CITADEL_LOCOMO_MAX_SAMPLES")
            .map(|_| number(&get, "CITADEL_LOCOMO_MAX_SAMPLES", 0, 1))
            .transpose()?;
        let max_tokens = number(&get, "CITADEL_MEMBENCH_MAX_TOKENS", 512, 1)?;
        let reader_max_tokens = u32::try_from(max_tokens)
            .map_err(|_| invalid("CITADEL_MEMBENCH_MAX_TOKENS", "a positive u32"))?;
        let config = Self {
            mode,
            bench: BenchConfig {
                top_k,
                reader_order,
                neighbor_radius: number(&get, "CITADEL_LOCOMO_NEIGHBOR_RADIUS", 0, 0)?,
                agentic: boolean(&get, "CITADEL_LOCOMO_AGENTIC", false)?,
                reader_max_tokens,
            },
            embedder: EmbedderModel::parse(
                get("CITADEL_LOCOMO_EMBEDDER").as_deref(),
                "CITADEL_LOCOMO_EMBEDDER",
            )?,
            rerank_strategy,
            max_samples,
            encrypted: boolean(&get, "CITADEL_LOCOMO_ENCRYPTED", false)?
                || mode == RunMode::Erasure,
            reader_tpm: number(&get, "CITADEL_LOCOMO_READER_TPM", 30_000, 1)? as u64,
            judge_tpm: number(&get, "CITADEL_LOCOMO_JUDGE_TPM", 1_000_000, 1)? as u64,
        };
        config.bench.validate()?;
        if mode != RunMode::Scored && config.bench.agentic {
            return Err(BenchError::Dataset(
                "CITADEL_LOCOMO_AGENTIC requires scored mode".into(),
            ));
        }
        if matches!(mode, RunMode::RetrievalDiag | RunMode::ParamSweep)
            && config.bench.neighbor_radius != 0
        {
            return Err(BenchError::Dataset("diagnostics measure recall before neighbor expansion; set CITADEL_LOCOMO_NEIGHBOR_RADIUS=0".into()));
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
    fn default_is_scored_with_real_embedder() {
        let c = config(&[]).unwrap();
        assert_eq!(c.mode, RunMode::Scored);
        assert_eq!(c.embedder, EmbedderModel::E5Large);
    }

    #[test]
    fn invalid_options_never_fall_back() {
        for (key, value) in [
            ("CITADEL_LOCOMO_GRAPH_SLOTS", "1,2,5"),
            ("CITADEL_LOCOMO_GRAPH_SLOTS", "bad"),
            ("CITADEL_LOCOMO_TOP_K", "0"),
            ("CITADEL_LOCOMO_READER_CONCURRENCY", "0"),
            ("CITADEL_LOCOMO_READER_TPM", "no"),
            ("CITADEL_LOCOMO_ENCRYPTED", "maybe"),
            ("CITADEL_LOCOMO_RERANK_STRATEGY", "unknown"),
            ("CITADEL_LOCOMO_EMBEDDER", "unknown"),
            ("CITADEL_LOCOMO_READER_ORDER", "unknown"),
            ("CITADEL_LOCOMO_MODE", "unknown"),
            ("CITADEL_LOCOMO_GRAPH_DIAG", "0"),
            ("CITADEL_LOCOMO_MOCK_EMBED", "1"),
        ] {
            assert!(config(&[(key, value)]).is_err(), "accepted {key}={value}");
        }
    }

    #[test]
    fn diagnostics_refuse_even_a_new_persistent_database_path() {
        for mode in [
            "retrieval-diag",
            "param-sweep",
            "dump",
            "erasure",
            "dry-run",
        ] {
            assert!(config(&[
                ("CITADEL_LOCOMO_MODE", mode),
                ("CITADEL_LOCOMO_DB_PATH", "new.cdl")
            ])
            .is_err());
        }
    }

    #[test]
    fn unsupported_graph_settings_are_not_silently_ignored() {
        assert!(config(&[("CITADEL_LOCOMO_MODE", "graph-diag")]).is_err());
        assert!(config(&[("CITADEL_LOCOMO_GRAPH_SLOTS", "0")]).is_err());
        assert!(config(&[("CITADEL_LOCOMO_GRAPH_SWEEP_SLOTS", "1,2")]).is_err());
    }

    #[test]
    fn bge_small_is_supported_and_false_is_not_presence_true() {
        let c = config(&[
            ("CITADEL_LOCOMO_EMBEDDER", "bge-small"),
            ("CITADEL_LOCOMO_AGENTIC", "false"),
        ])
        .unwrap();
        assert_eq!(c.embedder, EmbedderModel::BgeSmall);
        assert!(!c.bench.agentic);
    }

    #[test]
    fn reader_and_judge_settings_do_not_silently_fall_back() {
        for (key, value) in [
            ("CITADEL_LOCOMO_READER_PROVIDER", "mock"),
            ("CITADEL_LOCOMO_JUDGE_PROVIDER", "unknown"),
            ("CITADEL_LOCOMO_READER_MODEL", " "),
            ("CITADEL_LOCOMO_JUDGE_MODEL", ""),
        ] {
            assert!(config(&[(key, value)]).is_err());
        }
        assert!(config(&[
            ("CITADEL_LOCOMO_MODE", "retrieval-diag"),
            ("CITADEL_LOCOMO_AGENTIC", "true"),
        ])
        .is_err());
    }
}
