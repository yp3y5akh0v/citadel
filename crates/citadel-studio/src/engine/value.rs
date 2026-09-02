//! Bounded grid rendering for engine values.
//!
//! Formatting remains delegated to `Value::Display`; this module distinguishes NULL and
//! caps cell width without cutting UTF-8.

use citadel_sql::Value;

/// NULL placeholder, distinct from an empty text value.
pub const NULL: &str = "\u{2013}";

/// Longest cell text rendered before elision.
const MAX: usize = 96;

/// Render one bounded cell while retaining its full character count after elision.
pub fn cell(value: &Value) -> Cell {
    if matches!(value, Value::Null) {
        return Cell {
            text: NULL.to_owned(),
            elided: None,
        };
    }
    let full = value.to_string();
    // Character boundaries keep truncation valid for non-ASCII text.
    let count = full.chars().count();
    if count <= MAX {
        return Cell {
            text: full,
            elided: None,
        };
    }
    Cell {
        text: full.chars().take(MAX).collect(),
        elided: Some(count),
    }
}

/// A rendered cell, and what was left out of it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cell {
    pub text: String,
    /// The full length in characters, present only when `text` is a prefix of it.
    pub elided: Option<usize>,
}

impl Cell {
    /// Cell text with an elision marker when truncated.
    pub fn display(&self) -> String {
        match self.elided {
            Some(_) => format!("{}\u{2026}", self.text),
            None => self.text.clone(),
        }
    }

    /// The hover text, when there is more than the cell shows.
    pub fn hover(&self) -> Option<String> {
        self.elided
            .map(|n| format!("{n} characters, showing the first {MAX}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn null_is_a_placeholder_not_the_word() {
        let out = cell(&Value::Null);
        assert_eq!(out.text, NULL);
        assert!(out.elided.is_none());
        // "NULL" as text is a value; the placeholder must not collide with it.
        assert_ne!(out.text, "NULL");
        assert_eq!(cell(&Value::Text("NULL".into())).text, "NULL");
    }

    #[test]
    fn an_empty_string_is_not_null() {
        assert_eq!(cell(&Value::Text("".into())).text, "");
    }

    #[test]
    fn a_long_value_is_capped_and_says_how_long_it_really_is() {
        let long = "x".repeat(500);
        let out = cell(&Value::Text(long.as_str().into()));
        assert_eq!(out.text.chars().count(), MAX);
        assert_eq!(out.elided, Some(500));
        assert!(out.display().ends_with('\u{2026}'));
        assert!(out.hover().expect("elided cells hover").contains("500"));
    }

    #[test]
    fn capping_counts_characters_not_bytes() {
        let wide = "\u{8a18}".repeat(300);
        let out = cell(&Value::Text(wide.as_str().into()));
        assert_eq!(out.text.chars().count(), MAX);
        assert_eq!(out.elided, Some(300));
    }

    #[test]
    fn formatting_is_the_engines_own() {
        for value in [
            Value::Integer(42),
            Value::Boolean(true),
            Value::Real(1.5),
            Value::Vector(Arc::from(vec![1.0f32, 2.0].as_slice())),
        ] {
            assert_eq!(cell(&value).text, value.to_string());
        }
    }
}
