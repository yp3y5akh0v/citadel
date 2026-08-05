//! Provider output ceilings; low caps truncate silently, so caps derive from these.

/// Published max output tokens, or `None`; prefix order matters (mini before 4o).
pub(super) fn max_output_tokens(model_id: &str) -> Option<u32> {
    // GPT-5 snapshots vary; unknown beats a guessed ceiling.
    if model_id.starts_with("gpt-4o-mini") || model_id.starts_with("gpt-4o") {
        return Some(16_384);
    }
    if model_id.starts_with("gpt-4.1") {
        return Some(32_768);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_ceilings_by_family_prefix() {
        assert_eq!(max_output_tokens("gpt-4o-2024-08-06"), Some(16_384));
        assert_eq!(
            max_output_tokens("gpt-4o-mini-2024-07-18"),
            Some(16_384),
            "mini is not swallowed by the gpt-4o prefix"
        );
        assert_eq!(max_output_tokens("gpt-4.1-mini"), Some(32_768));
        assert_eq!(
            max_output_tokens("some-local-model"),
            None,
            "unknown models never inherit a guessed ceiling"
        );
    }
}
