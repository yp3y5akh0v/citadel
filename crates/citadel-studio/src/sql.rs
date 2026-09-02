//! Lexical SQL tokenization for syntax highlighting; it does not validate statements.

/// What a run of source text is, for colouring purposes only.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Token {
    Keyword,
    /// A function or a bind parameter: `count(`, `$1`.
    Function,
    Literal,
    Number,
    Comment,
    /// Identifiers, operators, punctuation, whitespace.
    Plain,
}

/// Compared uppercased, so `select` and `SELECT` colour alike.
const KEYWORDS: &[&str] = &[
    "ALL", "ALTER", "AND", "AS", "ASC", "BEGIN", "BETWEEN", "BY", "CASE", "COMMIT", "CREATE",
    "CROSS", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "END", "EXISTS", "FALSE", "FROM",
    "FULL", "GROUP", "HAVING", "IN", "INDEX", "INNER", "INSERT", "INTO", "IS", "JOIN", "LEFT",
    "LIKE", "LIMIT", "NOT", "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "RIGHT", "ROLLBACK",
    "SELECT", "SET", "TABLE", "THEN", "TRUE", "UNION", "UPDATE", "USING", "VALUES", "WHEN",
    "WHERE", "WITH",
];

/// Split `src` into coloured runs covering every byte exactly once. A dropped or
/// duplicated byte shifts every glyph after it and the caret draws off its own position.
pub fn tokenize(src: &str) -> Vec<(std::ops::Range<usize>, Token)> {
    let bytes = src.as_bytes();
    let mut out: Vec<(std::ops::Range<usize>, Token)> = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let start = i;
        let kind = match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = src[i..].find('\n').map_or(bytes.len(), |n| i + n);
                Token::Comment
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = src[i + 2..]
                    .find("*/")
                    .map_or(bytes.len(), |n| i + 2 + n + 2);
                Token::Comment
            }
            // A doubled quote escapes rather than terminates the quoted run.
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        if bytes.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                Token::Literal
            }
            b'$' => {
                if let Some(end) = dollar_quote_end(bytes, i) {
                    i = end;
                    Token::Literal
                } else {
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    Token::Function
                }
            }
            c if c.is_ascii_digit() => {
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                    i += 1;
                }
                Token::Number
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = src[start..i].to_ascii_uppercase();
                let called = bytes[i..].iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'(');
                if KEYWORDS.contains(&word.as_str()) {
                    Token::Keyword
                } else if called {
                    Token::Function
                } else {
                    Token::Plain
                }
            }
            _ => {
                while i < bytes.len() && !starts_token(bytes, i) {
                    i += 1;
                }
                Token::Plain
            }
        };
        // Every branch must consume at least one byte.
        debug_assert!(i > start, "tokenizer stalled at byte {start}");
        out.push((start..i, kind));
    }
    out
}

/// End of a PostgreSQL dollar-quoted literal beginning at `start`, including its
/// closing delimiter. An unterminated literal owns the rest of the source.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut delimiter_end = start + 1;
    while delimiter_end < bytes.len()
        && (bytes[delimiter_end].is_ascii_alphanumeric() || bytes[delimiter_end] == b'_')
    {
        delimiter_end += 1;
    }
    if bytes.get(delimiter_end) != Some(&b'$') {
        return None;
    }

    let delimiter = &bytes[start..=delimiter_end];
    let body_start = delimiter_end + 1;
    Some(
        bytes[body_start..]
            .windows(delimiter.len())
            .position(|window| window == delimiter)
            .map_or(bytes.len(), |offset| body_start + offset + delimiter.len()),
    )
}

/// True when byte `i` could begin a token of its own, used to end a plain run.
fn starts_token(bytes: &[u8], i: usize) -> bool {
    let c = bytes[i];
    c.is_ascii_alphanumeric()
        || matches!(c, b'_' | b'\'' | b'"' | b'`' | b'$')
        || (c == b'-' && bytes.get(i + 1) == Some(&b'-'))
        || (c == b'/' && bytes.get(i + 1) == Some(&b'*'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract every caller depends on.
    fn covers(src: &str) {
        let spans = tokenize(src);
        let mut at = 0;
        let mut rebuilt = String::new();
        for (range, _) in &spans {
            assert_eq!(range.start, at, "gap or overlap in {src:?}");
            rebuilt.push_str(&src[range.clone()]);
            at = range.end;
        }
        assert_eq!(at, src.len(), "did not reach the end of {src:?}");
        assert_eq!(rebuilt, src, "spans do not reproduce {src:?}");
    }

    #[test]
    fn every_byte_is_covered_exactly_once() {
        for src in [
            "",
            "SELECT 1",
            "select * from t where a = 'x' and b <= 2.5;",
            "-- trailing comment",
            "/* unterminated",
            "'unterminated",
            "a /* mid */ b",
            "count(*) OVER ()",
            "$1 $22",
            "SELECT $$-- not a comment$$, $tag$/* neither is this */$tag$",
            "\"quoted id\" `back` 'it''s'",
            "SELECT\n  a,\n  b\nFROM t;\n",
            "-- unicode: naïve café 中文\nSELECT 1",
        ] {
            covers(src);
        }
    }

    fn kinds(src: &str) -> Vec<(&str, Token)> {
        tokenize(src)
            .into_iter()
            .map(|(r, t)| (&src[r.start..r.end], t))
            .filter(|(s, _)| !s.trim().is_empty())
            .collect()
    }

    #[test]
    fn classifies_the_shapes_that_matter() {
        assert_eq!(
            kinds("SELECT a FROM t"),
            [
                ("SELECT", Token::Keyword),
                ("a", Token::Plain),
                ("FROM", Token::Keyword),
                ("t", Token::Plain),
            ]
        );
        assert_eq!(kinds("select")[0].1, Token::Keyword);
        assert_eq!(kinds("'it''s'"), [("'it''s'", Token::Literal)]);
        assert_eq!(kinds("mine(1)")[0].1, Token::Function);
        assert_eq!(kinds("$1")[0].1, Token::Function);
        assert_eq!(kinds("$$-- text$$")[0].1, Token::Literal);
        assert_eq!(kinds("$tag$/* text */$tag$")[0].1, Token::Literal);
        assert_eq!(kinds("2.5")[0].1, Token::Number);
        assert_eq!(kinds("-- x")[0].1, Token::Comment);
    }

    #[test]
    fn operators_stay_one_run() {
        for (src, op) in [("a <=> b", "<=>"), ("a != b", "!="), ("a || b", "||")] {
            let whole = tokenize(src)
                .into_iter()
                .any(|(r, t)| t == Token::Plain && src[r].contains(op));
            assert!(whole, "{op} was split into separate runs in {src:?}");
        }
    }
}
