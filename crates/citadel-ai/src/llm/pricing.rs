//! Per-model USD pricing for filling [`TokenUsage::cost_usd`].
//!
//! Rates are USD per 1M tokens, point-in-time. An unknown model returns `None`
//! (no guessed price); only models with confident rates are listed.

use super::TokenUsage;

const PER_MTOK: f64 = 1_000_000.0;

#[derive(Debug, Clone, Copy)]
pub(super) struct ModelPricing {
    pub input_per_mtok: f64,
    pub output_per_mtok: f64,
}

/// Known pricing for `model_id`, or `None` if we have no confident rate for it.
/// Matched by family prefix so date-suffixed ids resolve.
pub(super) fn pricing_for(model_id: &str) -> Option<ModelPricing> {
    let (input_per_mtok, output_per_mtok) = if model_id.starts_with("claude-opus-4") {
        (5.0, 25.0)
    } else if model_id.starts_with("claude-sonnet-4") {
        (3.0, 15.0)
    } else if model_id.starts_with("claude-haiku-4") {
        (1.0, 5.0)
    } else {
        return None;
    };
    Some(ModelPricing {
        input_per_mtok,
        output_per_mtok,
    })
}

/// Cost in USD for `usage` under `model_id`, or `None` for an unpriced model.
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
        assert_eq!(
            cost_for("claude-haiku-4-5-20251001", &usage),
            Some(1.0 + 5.0),
            "family prefix matches a date-suffixed id"
        );
    }

    #[test]
    fn unknown_model_has_no_price() {
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 100,
            cost_usd: None,
        };
        assert_eq!(cost_for("some-unlisted-model", &usage), None);
    }
}
