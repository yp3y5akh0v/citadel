//! USD pricing per 1M tokens (verified 2026-07); unknown models: None, no guesses.

use super::openai_models::{gpt5_model, Gpt5Model};
#[cfg(any(
    test,
    all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai")
    )
))]
use super::TokenUsage;

#[cfg(any(
    test,
    all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai")
    )
))]
const PER_MTOK: f64 = 1_000_000.0;

#[derive(Debug, Clone, Copy)]
pub(super) struct ModelPricing {
    pub input_per_mtok: f64,
    pub output_per_mtok: f64,
}

/// Pricing for `model_id`; GPT-5 closed-list: descendants are priced independently.
pub(super) fn pricing_for(model_id: &str) -> Option<ModelPricing> {
    if let Some(model) = gpt5_model(model_id) {
        let (input_per_mtok, output_per_mtok) = match model {
            Gpt5Model::Standard => (1.25, 10.0),
            Gpt5Model::Mini => (0.25, 2.0),
        };
        return Some(ModelPricing {
            input_per_mtok,
            output_per_mtok,
        });
    }

    let (input_per_mtok, output_per_mtok) = if model_id.starts_with("claude-fable-5") {
        (10.0, 50.0)
    } else if model_id.starts_with("claude-opus-4") {
        (5.0, 25.0)
    } else if model_id.starts_with("claude-sonnet-4") {
        (3.0, 15.0)
    } else if model_id.starts_with("claude-haiku-4") {
        (1.0, 5.0)
    // gpt-4o-mini before gpt-4o: the longer family shares the shorter prefix.
    } else if model_id.starts_with("gpt-4o-mini") {
        (0.15, 0.6)
    } else if model_id.starts_with("gpt-4o") {
        (2.5, 10.0)
    } else if model_id.starts_with("gemini-3.5-flash") {
        (1.5, 9.0)
    } else {
        return None;
    };
    Some(ModelPricing {
        input_per_mtok,
        output_per_mtok,
    })
}

/// Cost in USD for `usage` under `model_id`, or `None` for an unpriced model.
#[cfg(any(
    test,
    all(
        not(target_arch = "wasm32"),
        any(feature = "claude", feature = "openai")
    )
))]
pub(super) fn cost_for(model_id: &str, usage: &TokenUsage) -> Option<f64> {
    let p = pricing_for(model_id)?;
    let input = f64::from(usage.input_tokens) / PER_MTOK * p.input_per_mtok;
    let output = f64::from(usage.output_tokens) / PER_MTOK * p.output_per_mtok;
    Some(input + output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_model_prices_from_usage() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            cost_usd: None,
        };
        assert_eq!(cost_for("claude-opus-4-8", &usage), Some(5.0 + 25.0));
        assert_eq!(cost_for("claude-fable-5", &usage), Some(10.0 + 50.0));
        assert_eq!(cost_for("claude-fable-5-20260609", &usage), Some(60.0));
        assert_eq!(
            cost_for("claude-haiku-4-5-20251001", &usage),
            Some(1.0 + 5.0),
            "family prefix matches a date-suffixed id"
        );
        assert_eq!(
            cost_for("gpt-4o-mini", &usage),
            Some(0.15 + 0.6),
            "mini is not swallowed by the gpt-4o prefix"
        );
        assert_eq!(cost_for("gpt-4o-mini-2024-07-18", &usage), Some(0.15 + 0.6));
        assert_eq!(cost_for("gpt-4o", &usage), Some(2.5 + 10.0));
        assert_eq!(cost_for("gpt-4o-2024-08-06", &usage), Some(2.5 + 10.0));
        assert_eq!(cost_for("gpt-5-mini", &usage), Some(0.25 + 2.0));
        assert_eq!(cost_for("gpt-5-mini-2025-08-07", &usage), Some(0.25 + 2.0));
        assert_eq!(cost_for("gpt-5", &usage), Some(1.25 + 10.0));
        assert_eq!(cost_for("gpt-5-2025-08-07", &usage), Some(1.25 + 10.0));
        assert_eq!(cost_for("gemini-3.5-flash", &usage), Some(1.5 + 9.0));
    }

    #[test]
    fn unknown_model_has_no_price() {
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 100,
            cost_usd: None,
        };
        assert_eq!(cost_for("some-unlisted-model", &usage), None);
        assert_eq!(cost_for("gpt-5.4-mini", &usage), None);
        assert_eq!(cost_for("gpt-5-chat-latest", &usage), None);
        assert_eq!(cost_for("gpt-5-mini-future-snapshot", &usage), None);
    }
}
