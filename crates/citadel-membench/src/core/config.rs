//! Shared validation for benchmark launch parameters.

use std::collections::BTreeMap;
use std::ffi::OsString;

use citadel_mem::RerankStrategy;

use crate::{BenchError, Result};

pub fn environment(prefix: &str) -> Result<BTreeMap<String, String>> {
    environment_entries(prefix, std::env::vars_os())
}

fn environment_entries(
    prefix: &str,
    entries: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for (key, value) in entries {
        let Some(key) = key.to_str() else { continue };
        let key = if cfg!(windows) {
            key.to_ascii_uppercase()
        } else {
            key.to_owned()
        };
        if key.starts_with(prefix) || key == "CITADEL_MEMBENCH_MAX_TOKENS" {
            let value = value
                .into_string()
                .map_err(|_| invalid(&key, "Unicode text"))?;
            values.insert(key, value);
        }
    }
    Ok(values)
}

pub fn invalid(key: &str, expected: &str) -> BenchError {
    BenchError::Dataset(format!("{key} must be {expected}"))
}

pub fn number(
    get: &impl Fn(&str) -> Option<String>,
    key: &str,
    default: usize,
    min: usize,
) -> Result<usize> {
    let value = match get(key) {
        Some(raw) => raw
            .parse()
            .map_err(|_| invalid(key, "an unsigned integer"))?,
        None => default,
    };
    if value < min {
        return Err(invalid(key, &format!("at least {min}")));
    }
    Ok(value)
}

pub fn boolean(get: &impl Fn(&str) -> Option<String>, key: &str, default: bool) -> Result<bool> {
    match get(key).as_deref().map(str::to_ascii_lowercase).as_deref() {
        None => Ok(default),
        Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        Some(_) => Err(invalid(key, "true|false|1|0")),
    }
}

pub fn rerank_strategy(value: Option<&str>, key: &str) -> Result<RerankStrategy> {
    match value.unwrap_or("rrf") {
        "rrf" => Ok(RerankStrategy::default()),
        "replace" => Ok(RerankStrategy::Replace),
        _ => Err(invalid(key, "rrf|replace")),
    }
}

pub fn validate_llm_options(get: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<()> {
    let provider_key = format!("{prefix}_PROVIDER");
    let model_key = format!("{prefix}_MODEL");
    let provider = get(&provider_key).unwrap_or_else(|| "openai".into());
    if !matches!(provider.as_str(), "openai" | "gemini" | "claude" | "ollama") {
        return Err(invalid(&provider_key, "openai|gemini|claude|ollama"));
    }
    match get(&model_key) {
        Some(model) if model.trim().is_empty() => Err(invalid(&model_key, "nonempty text")),
        None if provider != "openai" => Err(invalid(
            &model_key,
            "an explicit model for a non-OpenAI provider",
        )),
        _ => Ok(()),
    }
}

/// Check compiled provider support and request identity without reading API keys.
pub fn preflight_llm(prefix: &str, default_model: &str) -> Result<()> {
    citadel_llm::factory::request_identity_from_env(prefix, "openai", default_model)
        .map(|_| ())
        .map_err(|error| BenchError::Dataset(format!("{prefix}: {error}")))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbedderModel {
    BgeSmall,
    BgeBase,
    BgeLarge,
    E5Large,
    E5LargeV2,
    GraniteR2,
    Arctic,
    ModernBert,
}

impl EmbedderModel {
    pub fn parse(value: Option<&str>, key: &str) -> Result<Self> {
        match value.unwrap_or("e5-large") {
            "bge-small" => Ok(Self::BgeSmall),
            "bge-base" => Ok(Self::BgeBase),
            "bge-large" => Ok(Self::BgeLarge),
            "e5-large" => Ok(Self::E5Large),
            "e5-large-v2" => Ok(Self::E5LargeV2),
            "granite-r2" => Ok(Self::GraniteR2),
            "arctic" => Ok(Self::Arctic),
            "modernbert-embed" => Ok(Self::ModernBert),
            _ => Err(invalid(key, "bge-small|bge-base|bge-large|e5-large|e5-large-v2|granite-r2|arctic|modernbert-embed")),
        }
    }

    #[cfg(feature = "candle-embed")]
    pub fn load(self, dir: &str) -> Result<citadel_mem::CandleEmbedder> {
        use citadel_mem::{CandleEmbedder, MemError};
        if !std::path::Path::new(dir).is_dir() {
            return Err(invalid(
                "CITADEL_EMBEDDER_DIR",
                "an existing model directory",
            ));
        }
        let model = match self {
            Self::BgeSmall => CandleEmbedder::bge_small(dir),
            Self::BgeBase => CandleEmbedder::bge_base(dir),
            Self::BgeLarge => CandleEmbedder::bge_large(dir),
            Self::E5Large => CandleEmbedder::e5_large(dir),
            Self::E5LargeV2 => CandleEmbedder::e5_large_v2(dir),
            Self::GraniteR2 => CandleEmbedder::granite_r2(dir),
            Self::Arctic => CandleEmbedder::arctic(dir),
            Self::ModernBert => CandleEmbedder::modernbert_embed(dir),
        }
        .map_err(MemError::from)?;
        Ok(model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_names_follow_platform_case_rules() {
        let entries = [
            ("CITADEL_LOCOMO_TOP_K", "17"),
            ("citadel_LoCoMo_MODE", "dry-run"),
            ("citadel_membench_max_tokens", "123"),
            ("citadel_LongMemEval_MODE", "scored"),
        ]
        .map(|(key, value)| (OsString::from(key), OsString::from(value)));
        let values = environment_entries("CITADEL_LOCOMO_", entries).unwrap();
        assert_eq!(
            values.get("CITADEL_LOCOMO_TOP_K").map(String::as_str),
            Some("17")
        );
        assert!(!values.contains_key("CITADEL_LONGMEMEVAL_MODE"));
        if cfg!(windows) {
            assert_eq!(
                values.get("CITADEL_LOCOMO_MODE").map(String::as_str),
                Some("dry-run")
            );
            assert_eq!(
                values
                    .get("CITADEL_MEMBENCH_MAX_TOKENS")
                    .map(String::as_str),
                Some("123")
            );
            assert_eq!(values.len(), 3);
        } else {
            assert_eq!(values.len(), 1);
        }
    }

    #[test]
    fn provider_and_model_are_validated_without_credentials() {
        let check = |provider: Option<&str>, model: Option<&str>| {
            validate_llm_options(
                &|key| match key {
                    "READER_PROVIDER" => provider.map(str::to_owned),
                    "READER_MODEL" => model.map(str::to_owned),
                    _ => None,
                },
                "READER",
            )
        };
        assert!(check(None, None).is_ok());
        assert!(check(Some("ollama"), Some("local-model")).is_ok());
        assert!(check(Some("claude"), None).is_err());
        assert!(check(Some("unknown"), Some("model")).is_err());
        assert!(check(Some("mock"), Some("model")).is_err());
        assert!(check(None, Some(" \t")).is_err());
    }
}
