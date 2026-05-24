use citadel_sql::fts::{
    op_match, parse_tsquery, TsQueryAst, TsVectorBuilder, Weight, MAX_POSITION,
};
use citadel_sql::Value;

fn tsvector(lexs: &[(&[u8], u16, Weight)]) -> Vec<u8> {
    let mut b = TsVectorBuilder::new();
    for (lex, pos, w) in lexs {
        b.push(lex, *pos, *w);
    }
    b.build().to_vec()
}

fn matched(v: &[u8], q: &str) -> bool {
    let ast = parse_tsquery(q).unwrap();
    matches!(op_match(v, &ast.encode()).unwrap(), Value::Boolean(true))
}

#[test]
fn parser_precedence_and_over_or() {
    // a & b | c  ==  (a & b) | c
    let v_a_b = tsvector(&[(b"a", 1, Weight::D), (b"b", 2, Weight::D)]);
    let v_c = tsvector(&[(b"c", 1, Weight::D)]);
    assert!(matched(&v_a_b, "a & b | c"));
    assert!(matched(&v_c, "a & b | c"));
    let v_a_only = tsvector(&[(b"a", 1, Weight::D)]);
    assert!(!matched(&v_a_only, "a & b | c"));
}

#[test]
fn parser_parens_override_precedence() {
    // a & (b | c) — needs a AND one of (b, c)
    let v_ab = tsvector(&[(b"a", 1, Weight::D), (b"b", 2, Weight::D)]);
    let v_ac = tsvector(&[(b"a", 1, Weight::D), (b"c", 2, Weight::D)]);
    let v_only_a = tsvector(&[(b"a", 1, Weight::D)]);
    assert!(matched(&v_ab, "a & (b | c)"));
    assert!(matched(&v_ac, "a & (b | c)"));
    assert!(!matched(&v_only_a, "a & (b | c)"));
}

#[test]
fn parser_phrase_basic_and_distance_n() {
    // hello at pos 1, world at pos 2 → 'hello <-> world' matches
    let v = tsvector(&[(b"hello", 1, Weight::D), (b"world", 2, Weight::D)]);
    assert!(matched(&v, "hello <-> world"));
    // 'hello <2> world' needs pos diff of 2; positions are 1,2 (diff 1) → no
    assert!(!matched(&v, "hello <2> world"));

    let v_far = tsvector(&[(b"hello", 1, Weight::D), (b"world", 4, Weight::D)]);
    assert!(matched(&v_far, "hello <3> world"));
}

#[test]
fn parser_phrase_nested_left_associative() {
    // a <-> b <-> c against positions 1,2,3
    let v = tsvector(&[
        (b"a", 1, Weight::D),
        (b"b", 2, Weight::D),
        (b"c", 3, Weight::D),
    ]);
    assert!(matched(&v, "a <-> b <-> c"));
    // Same vector, c at position 4 — gap of 2 from b → no match for <->
    let v_gap = tsvector(&[
        (b"a", 1, Weight::D),
        (b"b", 2, Weight::D),
        (b"c", 4, Weight::D),
    ]);
    assert!(!matched(&v_gap, "a <-> b <-> c"));
}

#[test]
fn parser_weight_filtering() {
    // cat is at weight A; ":B" filter rejects, ":A" accepts.
    let v = tsvector(&[(b"cat", 1, Weight::A)]);
    assert!(matched(&v, "cat:A"));
    assert!(!matched(&v, "cat:B"));
    assert!(matched(&v, "cat:AB")); // mask covers A or B
}

#[test]
fn parser_prefix_wildcard_matches_multiple_lexemes() {
    let v = tsvector(&[
        (b"category", 1, Weight::D),
        (b"caterpillar", 2, Weight::D),
        (b"dog", 3, Weight::D),
    ]);
    assert!(matched(&v, "cat:*"));
    assert!(!matched(&v, "zoo:*"));
}

#[test]
fn parser_not_negates_match() {
    let v = tsvector(&[(b"cat", 1, Weight::D)]);
    assert!(matched(&v, "!mouse"));
    assert!(!matched(&v, "!cat"));
    assert!(matched(&v, "cat & !mouse"));
    assert!(!matched(&v, "cat & !cat"));
}

#[test]
fn parser_quoted_lexeme_handles_operator_chars() {
    // A quoted lexeme is taken literally — the '&' inside isn't an operator.
    let q = parse_tsquery("'foo&bar'").unwrap();
    if let TsQueryAst::Lexeme { lexeme, .. } = q {
        assert_eq!(lexeme, b"foo&bar".to_vec());
    } else {
        panic!("expected quoted lexeme");
    }
}

#[test]
fn parser_rejects_invalid_input() {
    assert!(parse_tsquery("").is_err());
    assert!(parse_tsquery("&cat").is_err());
    assert!(parse_tsquery("(cat & dog").is_err());
    assert!(parse_tsquery("cat <foo> dog").is_err());
    assert!(parse_tsquery("cat <0> dog").is_err());
    let too_far = format!("a <{}> b", MAX_POSITION as u32 + 1);
    assert!(parse_tsquery(&too_far).is_err());
}

#[test]
fn parser_phrase_distance_one_dash_form() {
    // Both `<->` and `<1>` mean distance 1.
    let q1 = parse_tsquery("a <-> b").unwrap();
    let q2 = parse_tsquery("a <1> b").unwrap();
    assert_eq!(q1, q2);
}

#[test]
fn op_match_position_overflow_refuses_phrase() {
    let mut b = TsVectorBuilder::new();
    b.push(b"hello", 1, Weight::D);
    b.push(b"world", 2, Weight::D);
    b.push(b"junk", MAX_POSITION + 5, Weight::D); // sets overflow flag
    let v = b.build();

    let q_phrase = parse_tsquery("hello <-> world").unwrap().encode();
    let err = op_match(&v, &q_phrase).unwrap_err().to_string();
    assert!(
        err.contains("position overflow") || err.contains("unreliable"),
        "expected overflow error, got: {err}"
    );

    // Non-phrase queries still answer.
    let q_simple = parse_tsquery("hello & world").unwrap().encode();
    assert_eq!(op_match(&v, &q_simple).unwrap(), Value::Boolean(true));
}
