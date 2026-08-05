//! Exact ids only: a `gpt-5` prefix gives `gpt-5.4-mini` the wrong price and caps.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Gpt5Model {
    Standard,
    Mini,
}

/// Maps only OpenAI-documented GPT-5 ids; never guess an undocumented model.
pub(super) fn gpt5_model(model_id: &str) -> Option<Gpt5Model> {
    match model_id {
        "gpt-5" | "gpt-5-2025-08-07" => Some(Gpt5Model::Standard),
        "gpt-5-mini" | "gpt-5-mini-2025-08-07" => Some(Gpt5Model::Mini),
        _ => None,
    }
}
