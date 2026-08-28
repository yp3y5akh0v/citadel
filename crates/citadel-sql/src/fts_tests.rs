use super::*;
use crate::types::Value;

fn many_lexeme_vector(count: usize) -> Arc<[u8]> {
    let mut builder = TsVectorBuilder::new();
    for index in 0..count {
        builder
            .push(format!("term{index:05}").as_bytes(), 1, Weight::D)
            .unwrap();
    }
    builder.build()
}

#[test]
fn deeply_nested_text_queries_are_rejected_before_the_stack_is_exhausted() {
    let not_chain = format!("{}term", "!".repeat(MAX_TSQUERY_DEPTH + 32));
    let error = parse_tsquery(&not_chain).expect_err("deep NOT chain was accepted");
    assert!(error.to_string().contains("complexity limit"));

    let parenthesized = format!(
        "{}term{}",
        "(".repeat(MAX_TSQUERY_DEPTH + 32),
        ")".repeat(MAX_TSQUERY_DEPTH + 32)
    );
    let error = parse_tsquery(&parenthesized).expect_err("deep parentheses were accepted");
    assert!(error.to_string().contains("complexity limit"));
}

#[test]
fn deeply_nested_wire_queries_are_rejected_before_recursive_decode() {
    let mut bytes = vec![TSQ_TAG_NOT; MAX_TSQUERY_DEPTH + 32];
    bytes.extend_from_slice(&[TSQ_TAG_LEXEME, 1, 0, b'x', 0, 0]);

    let error = TsQueryAst::decode(&bytes).expect_err("deep wire query was accepted");
    assert!(error.to_string().contains("complexity limit"));
}

#[test]
fn public_encode_rejects_an_externally_built_deep_ast() {
    let mut ast = TsQueryAst::Lexeme {
        lexeme: b"term".to_vec(),
        weight_mask: 0,
        prefix: false,
    };
    for _ in 0..(MAX_TSQUERY_DEPTH + 32) {
        ast = TsQueryAst::Not(Box::new(ast));
    }

    let error = ast.encode().expect_err("deep external AST was encoded");
    assert!(error.to_string().contains("complexity limit"));
}

#[test]
fn associative_generated_queries_are_balanced_but_phrase_chains_are_bounded() {
    let text = "term ".repeat(MAX_TSQUERY_DEPTH + 32);
    for value in [
        fn_plainto_tsquery_with(TokenizerKind::Simple, &text).unwrap(),
        fn_websearch_to_tsquery_with(TokenizerKind::Simple, &text).unwrap(),
    ] {
        let Value::TsQuery(bytes) = value else {
            panic!("query constructor returned the wrong value type");
        };
        let ast = TsQueryAst::decode(&bytes).unwrap();
        validate_tsquery(&ast).unwrap();
    }

    let error = fn_phraseto_tsquery_with(TokenizerKind::Simple, &text)
        .expect_err("phrase query built an over-deep AST");
    assert!(error.to_string().contains("complexity limit"));
}

#[test]
fn lexeme_wire_lengths_accept_u16_max_and_reject_the_next_byte() {
    let maximum = "x".repeat(MAX_LEXEME_BYTES);

    let Value::TsVector(vector) =
        fn_to_tsvector_with(TokenizerKind::Simple, &maximum).expect("maximum TSVECTOR lexeme")
    else {
        panic!("vector constructor returned the wrong value type");
    };
    let (_, mut reader) = TsVectorReader::open(&vector).unwrap();
    assert_eq!(reader.next().unwrap().unwrap().0.len(), MAX_LEXEME_BYTES);

    let Value::TsQuery(query) =
        fn_to_tsquery_with(TokenizerKind::Simple, &maximum).expect("maximum TSQUERY lexeme")
    else {
        panic!("query constructor returned the wrong value type");
    };
    let TsQueryAst::Lexeme { lexeme, .. } = TsQueryAst::decode(&query).unwrap() else {
        panic!("single lexeme decoded to a compound query");
    };
    assert_eq!(lexeme.len(), MAX_LEXEME_BYTES);

    let overlong = "x".repeat(MAX_LEXEME_BYTES + 1);
    for error in [
        fn_to_tsvector_with(TokenizerKind::Simple, &overlong)
            .expect_err("TSVECTOR accepted an overlong lexeme"),
        fn_to_tsquery_with(TokenizerKind::Simple, &overlong)
            .expect_err("TSQUERY accepted an overlong lexeme"),
    ] {
        assert!(error.to_string().contains("maximum is 65535"));
    }
}

#[test]
fn public_fts_encoders_return_errors_for_overlong_lexemes() {
    let overlong = vec![b'x'; MAX_LEXEME_BYTES + 1];
    let mut builder = TsVectorBuilder::new();
    let error = builder
        .push(&overlong, 1, Weight::D)
        .expect_err("builder accepted an overlong lexeme");
    assert!(error.to_string().contains("maximum is 65535"));

    let query = TsQueryAst::Lexeme {
        lexeme: overlong,
        weight_mask: 0,
        prefix: false,
    };
    let error = query
        .encode()
        .expect_err("query encoder accepted an overlong lexeme");
    assert!(error.to_string().contains("maximum is 65535"));
}

#[test]
fn websearch_propagates_cancellation_from_a_quoted_phrase() {
    let token = citadel::CancelToken::new();
    let _cancel = cancel_tokenize_after(token.clone(), CANCEL_CHECK_INTERVAL + 1);
    let text = format!("\"{}\"", "searchable ".repeat(CANCEL_CHECK_INTERVAL));

    let error = fn_websearch_to_tsquery_with_cancel(TokenizerKind::Simple, &text, Some(&token))
        .expect_err("quoted-phrase cancellation was swallowed");

    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn match_length_and_display_poll_inside_large_fts_values() {
    let vector = many_lexeme_vector(CANCEL_CHECK_INTERVAL * 3);
    let query = TsQueryAst::Lexeme {
        lexeme: b"term".to_vec(),
        weight_mask: 0,
        prefix: true,
    }
    .encode()
    .unwrap();

    let match_token = citadel::CancelToken::new();
    let _match_cancel = cancel_on_poll_after(match_token.clone(), 3);
    let match_error = op_match_with_cancel(&vector, &query, Some(&match_token))
        .expect_err("matching ignored in-work cancellation");
    assert!(matches!(
        match_error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    drop(_match_cancel);

    let length_token = citadel::CancelToken::new();
    let _length_cancel = cancel_on_poll_after(length_token.clone(), 3);
    let length_error = fn_length_tsvector_with_cancel(&vector, Some(&length_token))
        .expect_err("length ignored in-work cancellation");
    assert!(matches!(
        length_error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    drop(_length_cancel);

    let vector_display_token = citadel::CancelToken::new();
    let _vector_display_cancel = cancel_on_poll_after(vector_display_token.clone(), 3);
    let display_error = tsvector_display_with_cancel(&vector, Some(&vector_display_token))
        .expect_err("TSVECTOR display ignored in-work cancellation");
    assert!(matches!(
        display_error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
    drop(_vector_display_cancel);

    let query = and_chain(vec![b"term".to_vec(); CANCEL_CHECK_INTERVAL * 2])
        .unwrap()
        .encode()
        .unwrap();
    let query_display_token = citadel::CancelToken::new();
    let _query_display_cancel = cancel_on_poll_after(query_display_token.clone(), 3);
    let display_error = tsquery_display_with_cancel(&query, Some(&query_display_token))
        .expect_err("TSQUERY display ignored in-work cancellation");
    assert!(matches!(
        display_error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn untripped_tokens_preserve_match_concat_and_length_results() {
    let token = citadel::CancelToken::new();
    let left = many_lexeme_vector(64);
    let right = many_lexeme_vector(96);
    let query = parse_tsquery("term00001 | term00095")
        .unwrap()
        .encode()
        .unwrap();

    assert_eq!(
        op_match_with_cancel(&right, &query, Some(&token)).unwrap(),
        op_match(&right, &query).unwrap()
    );
    assert_eq!(
        op_concat_with_cancel(&left, &right, Some(&token)).unwrap(),
        op_concat(&left, &right).unwrap()
    );
    assert_eq!(
        fn_length_tsvector_with_cancel(&right, Some(&token)).unwrap(),
        fn_length_tsvector(&right).unwrap()
    );
}

#[test]
fn cancellable_tokenizer_matches_the_fast_path() {
    let token = citadel::CancelToken::new();
    let text = "The \u{fb01}le\u{301}d cats were RUNNING; ΟΣ ΟΣΑ";

    assert_eq!(
        tokenize_with_cancel(TokenizerKind::English, text, Some(&token)).unwrap(),
        tokenize(TokenizerKind::English, text)
    );
}

#[test]
fn tokenizer_observes_cancellation_inside_one_large_value() {
    let token = citadel::CancelToken::new();
    let _cancel = cancel_tokenize_after(token.clone(), CANCEL_CHECK_INTERVAL + 1);
    let text = "a".repeat(CANCEL_CHECK_INTERVAL * 4);

    let error = tokenize_with_cancel(TokenizerKind::Simple, &text, Some(&token))
        .expect_err("tokenization completed after its in-value hook tripped");

    assert!(token.is_cancelled());
    assert!(matches!(
        error,
        SqlError::Storage(citadel_core::Error::Interrupted)
    ));
}

#[test]
fn tsvector_builder_sorts_and_dedups() {
    let mut b = TsVectorBuilder::new();
    b.push(b"dog", 2, Weight::D).unwrap();
    b.push(b"cat", 1, Weight::A).unwrap();
    b.push(b"cat", 1, Weight::A).unwrap(); // dup
    b.push(b"cat", 5, Weight::B).unwrap();
    let bytes = b.build();
    assert!(!tsvector_overflowed(&bytes));
    let s = tsvector_display(&bytes);
    assert_eq!(s, "'cat':1A,5B 'dog':2");
}

#[test]
fn tsvector_canonical_byte_equality() {
    let mut a = TsVectorBuilder::new();
    a.push(b"foo", 3, Weight::B).unwrap();
    a.push(b"bar", 1, Weight::A).unwrap();
    let ab = a.build();

    let mut b = TsVectorBuilder::new();
    b.push(b"bar", 1, Weight::A).unwrap();
    b.push(b"foo", 3, Weight::B).unwrap();
    let bb = b.build();

    assert_eq!(ab.as_ref(), bb.as_ref());
}

#[test]
fn tsvector_position_overflow_flag() {
    let mut b = TsVectorBuilder::new();
    b.push(b"cat", 1, Weight::D).unwrap();
    b.push(b"dog", MAX_POSITION + 1, Weight::D).unwrap(); // overflow
    let bytes = b.build();
    assert!(tsvector_overflowed(&bytes));
    assert!(tsvector_display(&bytes).contains("'cat'"));
}

#[test]
fn tsvector_per_lexeme_position_cap() {
    let mut b = TsVectorBuilder::new();
    for p in 1..=300 {
        b.push(b"cat", p, Weight::D).unwrap();
    }
    let bytes = b.build();
    let (_flags, reader) = TsVectorReader::open(&bytes).unwrap();
    let entries: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.len(), MAX_POSITIONS_PER_LEXEME as usize);
}

#[test]
fn tsvector_reader_round_trip() {
    let mut b = TsVectorBuilder::new();
    b.push(b"hello", 1, Weight::A).unwrap();
    b.push(b"world", 2, Weight::B).unwrap();
    b.push(b"world", 3, Weight::D).unwrap();
    let bytes = b.build();
    let (flags, reader) = TsVectorReader::open(&bytes).unwrap();
    assert_eq!(flags, 0);
    let entries: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, b"hello");
    assert_eq!(entries[0].1, vec![pack_position(1, Weight::A)]);
    assert_eq!(entries[1].0, b"world");
    assert_eq!(
        entries[1].1,
        vec![pack_position(2, Weight::B), pack_position(3, Weight::D)]
    );
}

#[test]
fn tsvector_no_position_lexemes() {
    let mut b = TsVectorBuilder::new();
    b.push_no_position(b"cat").unwrap();
    b.push_no_position(b"dog").unwrap();
    let bytes = b.build();
    assert_eq!(tsvector_display(&bytes), "'cat' 'dog'");
}

#[test]
fn weight_pack_unpack() {
    for &w in &[Weight::D, Weight::C, Weight::B, Weight::A] {
        for p in [1u16, 100, 16383] {
            let packed = pack_position(p, w);
            let (up, uw) = unpack_position(packed);
            assert_eq!(p, up);
            assert_eq!(w, uw);
        }
    }
}

#[test]
fn tsquery_codec_round_trip_lexeme() {
    let q = TsQueryAst::Lexeme {
        lexeme: b"cat".to_vec(),
        weight_mask: 0b1010, // A | C
        prefix: true,
    };
    let bytes = q.encode().unwrap();
    let decoded = TsQueryAst::decode(&bytes).unwrap();
    assert_eq!(decoded, q);
}

#[test]
fn tsquery_codec_round_trip_combinators() {
    let q = TsQueryAst::And(
        Box::new(TsQueryAst::Lexeme {
            lexeme: b"cat".to_vec(),
            weight_mask: 0,
            prefix: false,
        }),
        Box::new(TsQueryAst::Or(
            Box::new(TsQueryAst::Not(Box::new(TsQueryAst::Lexeme {
                lexeme: b"dog".to_vec(),
                weight_mask: 0,
                prefix: false,
            }))),
            Box::new(TsQueryAst::Phrase {
                distance: 3,
                left: Box::new(TsQueryAst::Lexeme {
                    lexeme: b"fox".to_vec(),
                    weight_mask: 0,
                    prefix: false,
                }),
                right: Box::new(TsQueryAst::Lexeme {
                    lexeme: b"jumps".to_vec(),
                    weight_mask: 0,
                    prefix: false,
                }),
            }),
        )),
    );
    let bytes = q.encode().unwrap();
    let decoded = TsQueryAst::decode(&bytes).unwrap();
    assert_eq!(decoded, q);
}

#[test]
fn tsquery_decode_rejects_garbage() {
    assert!(TsQueryAst::decode(&[99]).is_err());
    assert!(TsQueryAst::decode(&[]).is_err());
    assert!(TsQueryAst::decode(&[TSQ_TAG_LEXEME, 5, 0]).is_err()); // truncated
}

#[test]
fn tsquery_display_handles_prefix_and_weights() {
    let q = TsQueryAst::Lexeme {
        lexeme: b"cat".to_vec(),
        weight_mask: 0b1000, // A
        prefix: true,
    };
    let s = tsquery_display(&q.encode().unwrap());
    assert_eq!(s, "'cat':*A");
}

#[test]
fn tsquery_display_phrase() {
    let q = TsQueryAst::Phrase {
        distance: 2,
        left: Box::new(TsQueryAst::Lexeme {
            lexeme: b"hello".to_vec(),
            weight_mask: 0,
            prefix: false,
        }),
        right: Box::new(TsQueryAst::Lexeme {
            lexeme: b"world".to_vec(),
            weight_mask: 0,
            prefix: false,
        }),
    };
    assert_eq!(tsquery_display(&q.encode().unwrap()), "'hello' <2> 'world'");
}

fn lex(name: &str) -> TsQueryAst {
    TsQueryAst::Lexeme {
        lexeme: name.as_bytes().to_vec(),
        weight_mask: 0,
        prefix: false,
    }
}

#[test]
fn parse_simple_lexeme() {
    assert_eq!(parse_tsquery("cat").unwrap(), lex("cat"));
    assert_eq!(parse_tsquery("'cat'").unwrap(), lex("cat"));
    assert_eq!(parse_tsquery("  cat  ").unwrap(), lex("cat"));
}

#[test]
fn parse_and_precedence_over_or() {
    let q = parse_tsquery("a & b | c").unwrap();
    let expected = TsQueryAst::Or(
        Box::new(TsQueryAst::And(Box::new(lex("a")), Box::new(lex("b")))),
        Box::new(lex("c")),
    );
    assert_eq!(q, expected);
}

#[test]
fn parse_not_prefix() {
    assert_eq!(
        parse_tsquery("!cat").unwrap(),
        TsQueryAst::Not(Box::new(lex("cat")))
    );
    assert_eq!(
        parse_tsquery("!!cat").unwrap(),
        TsQueryAst::Not(Box::new(TsQueryAst::Not(Box::new(lex("cat")))))
    );
}

#[test]
fn parse_parens_override_precedence() {
    let q = parse_tsquery("a & (b | c)").unwrap();
    let expected = TsQueryAst::And(
        Box::new(lex("a")),
        Box::new(TsQueryAst::Or(Box::new(lex("b")), Box::new(lex("c")))),
    );
    assert_eq!(q, expected);
}

#[test]
fn parse_phrase_distance_one() {
    let q = parse_tsquery("hello <-> world").unwrap();
    assert!(matches!(q, TsQueryAst::Phrase { distance: 1, .. }));
}

#[test]
fn parse_phrase_distance_n() {
    let q = parse_tsquery("hello <3> world").unwrap();
    assert!(matches!(q, TsQueryAst::Phrase { distance: 3, .. }));
}

#[test]
fn parse_weight_label() {
    let q = parse_tsquery("cat:A").unwrap();
    if let TsQueryAst::Lexeme { weight_mask, .. } = q {
        assert_eq!(weight_mask, 0b1000);
    } else {
        panic!();
    }
}

#[test]
fn parse_weight_multi_letter() {
    let q = parse_tsquery("cat:AB").unwrap();
    if let TsQueryAst::Lexeme { weight_mask, .. } = q {
        assert_eq!(weight_mask, 0b1100);
    } else {
        panic!();
    }
}

#[test]
fn parse_prefix_wildcard() {
    let q = parse_tsquery("cat:*").unwrap();
    if let TsQueryAst::Lexeme {
        prefix,
        weight_mask,
        ..
    } = q
    {
        assert!(prefix);
        assert_eq!(weight_mask, 0);
    } else {
        panic!();
    }
}

#[test]
fn parse_prefix_with_weight() {
    let q = parse_tsquery("cat:*A").unwrap();
    if let TsQueryAst::Lexeme {
        prefix,
        weight_mask,
        ..
    } = q
    {
        assert!(prefix);
        assert_eq!(weight_mask, 0b1000);
    } else {
        panic!();
    }
}

#[test]
fn parse_rejects_garbage() {
    assert!(parse_tsquery("").is_err());
    assert!(parse_tsquery("&").is_err());
    assert!(parse_tsquery("(cat").is_err());
    assert!(parse_tsquery("cat <foo> dog").is_err());
}

#[test]
fn parse_rejects_phrase_distance_over_cap() {
    assert!(parse_tsquery("cat <20000> dog").is_err());
}

#[test]
fn parse_phrase_chains_left_associative() {
    let q = parse_tsquery("a <-> b <-> c").unwrap();
    if let TsQueryAst::Phrase { right, .. } = &q {
        assert_eq!(**right, lex("c"));
    }
    if let TsQueryAst::Phrase { left, .. } = &q {
        assert!(matches!(**left, TsQueryAst::Phrase { .. }));
    }
}

type PosWeight = (u16, Weight);
type LexEntry<'a> = (&'a [u8], &'a [PosWeight]);

fn tsv_with(lexemes: &[LexEntry<'_>]) -> Vec<u8> {
    let mut b = TsVectorBuilder::new();
    for (lex, positions) in lexemes {
        if positions.is_empty() {
            b.push_no_position(lex).unwrap();
        } else {
            for (p, w) in *positions {
                b.push(lex, *p, *w).unwrap();
            }
        }
    }
    b.build().to_vec()
}

#[test]
fn op_match_simple_lexeme() {
    let v = tsv_with(&[(b"cat", &[(1, Weight::A)]), (b"dog", &[(2, Weight::D)])]);
    let q = parse_tsquery("cat").unwrap().encode().unwrap();
    assert!(matches!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    ));
    let q2 = parse_tsquery("mouse").unwrap().encode().unwrap();
    assert!(matches!(
        op_match(&v, &q2).unwrap(),
        crate::types::Value::Boolean(false)
    ));
}

#[test]
fn op_match_and_combinator() {
    let v = tsv_with(&[(b"cat", &[(1, Weight::D)]), (b"dog", &[(2, Weight::D)])]);
    let q_both = parse_tsquery("cat & dog").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_both).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q_miss = parse_tsquery("cat & mouse").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_miss).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_or_combinator() {
    let v = tsv_with(&[(b"cat", &[(1, Weight::D)])]);
    let q = parse_tsquery("dog | cat").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q2 = parse_tsquery("dog | mouse").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q2).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_not_combinator() {
    let v = tsv_with(&[(b"cat", &[(1, Weight::D)])]);
    let q = parse_tsquery("!mouse").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q2 = parse_tsquery("!cat").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q2).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_phrase_distance_one() {
    let v = tsv_with(&[(b"hello", &[(1, Weight::D)]), (b"world", &[(2, Weight::D)])]);
    let q = parse_tsquery("hello <-> world").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );

    // Reversed order: world appears before hello → no match
    let v2 = tsv_with(&[(b"hello", &[(5, Weight::D)]), (b"world", &[(2, Weight::D)])]);
    assert_eq!(
        op_match(&v2, &q).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_phrase_distance_n() {
    let v = tsv_with(&[(b"hello", &[(1, Weight::D)]), (b"world", &[(4, Weight::D)])]);
    let q = parse_tsquery("hello <3> world").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q2 = parse_tsquery("hello <2> world").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q2).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_phrase_overflow_refused() {
    let mut b = TsVectorBuilder::new();
    b.push(b"hello", 1, Weight::D).unwrap();
    b.push(b"world", 2, Weight::D).unwrap();
    b.push(b"junk", MAX_POSITION + 1, Weight::D).unwrap(); // sets overflow flag
    let v = b.build();

    let q_phrase = parse_tsquery("hello <-> world").unwrap().encode().unwrap();
    assert!(op_match(&v, &q_phrase).is_err());

    // Non-phrase queries still work on overflowed tsvectors.
    let q_simple = parse_tsquery("hello & world").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_simple).unwrap(),
        crate::types::Value::Boolean(true)
    );
}

#[test]
fn op_match_prefix_wildcard() {
    let v = tsv_with(&[
        (b"category", &[(1, Weight::D)]),
        (b"caterpillar", &[(2, Weight::D)]),
        (b"dog", &[(3, Weight::D)]),
    ]);
    let q = parse_tsquery("cat:*").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q2 = parse_tsquery("zebr:*").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q2).unwrap(),
        crate::types::Value::Boolean(false)
    );
}

#[test]
fn op_match_weight_filter() {
    let v = tsv_with(&[(b"cat", &[(1, Weight::B), (3, Weight::A)])]);
    let q_a = parse_tsquery("cat:A").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_a).unwrap(),
        crate::types::Value::Boolean(true)
    );
    let q_c = parse_tsquery("cat:C").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_c).unwrap(),
        crate::types::Value::Boolean(false)
    );
    let q_ab = parse_tsquery("cat:AB").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q_ab).unwrap(),
        crate::types::Value::Boolean(true)
    );
}

#[test]
fn op_match_nested_phrase() {
    // 'a <-> b <-> c' against 'a b c'
    let v = tsv_with(&[
        (b"a", &[(1, Weight::D)]),
        (b"b", &[(2, Weight::D)]),
        (b"c", &[(3, Weight::D)]),
    ]);
    let q = parse_tsquery("a <-> b <-> c").unwrap().encode().unwrap();
    assert_eq!(
        op_match(&v, &q).unwrap(),
        crate::types::Value::Boolean(true)
    );
}

#[test]
fn strip_drops_positions_and_weights() {
    let v = tsv_with(&[
        (b"alpha", &[(1, Weight::A), (3, Weight::B)]),
        (b"beta", &[(2, Weight::C)]),
    ]);
    let stripped = match fn_strip(&v).unwrap() {
        crate::types::Value::TsVector(b) => b,
        _ => panic!("strip must return TsVector"),
    };
    let display = tsvector_display(&stripped);
    assert_eq!(display, "'alpha' 'beta'");
}

#[test]
fn strip_preserves_lexeme_dedup() {
    let v = tsv_with(&[
        (b"x", &[(1, Weight::D), (2, Weight::A)]),
        (b"y", &[(5, Weight::D)]),
    ]);
    let stripped = match fn_strip(&v).unwrap() {
        crate::types::Value::TsVector(b) => b,
        _ => panic!(),
    };
    assert_eq!(tsvector_display(&stripped), "'x' 'y'");
}

#[test]
fn op_concat_merges_disjoint_lexemes() {
    let a = tsv_with(&[(b"alpha", &[(1, Weight::A)])]);
    let b = tsv_with(&[(b"beta", &[(1, Weight::B)])]);
    let merged = match op_concat(&a, &b).unwrap() {
        crate::types::Value::TsVector(out) => out,
        _ => panic!(),
    };
    let display = tsvector_display(&merged);
    assert!(display.contains("'alpha'"));
    assert!(display.contains("'beta'"));
}

#[test]
fn op_concat_merges_overlapping_lexeme_positions() {
    let a = tsv_with(&[(b"shared", &[(1, Weight::A)])]);
    let b = tsv_with(&[(b"shared", &[(5, Weight::D)])]);
    let merged = match op_concat(&a, &b).unwrap() {
        crate::types::Value::TsVector(out) => out,
        _ => panic!(),
    };
    let display = tsvector_display(&merged);
    // Both positions present, sorted ascending; weight A printed, weight D suppressed
    assert_eq!(display, "'shared':1A,5");
}

#[test]
fn op_concat_idempotent_for_identical_inputs() {
    let v = tsv_with(&[(b"x", &[(1, Weight::A)]), (b"y", &[(2, Weight::B)])]);
    let merged = match op_concat(&v, &v).unwrap() {
        crate::types::Value::TsVector(out) => out,
        _ => panic!(),
    };
    assert_eq!(merged.as_ref(), v.as_slice());
}

#[test]
fn setweight_selective_reweights_only_matching_lexemes() {
    use crate::types::Value;
    let v = tsv_with(&[
        (b"foo", &[(1, Weight::D)]),
        (b"bar", &[(2, Weight::D)]),
        (b"baz", &[(3, Weight::D)]),
    ]);
    let filter = vec![Value::Text("foo".into()), Value::Text("baz".into())];
    let out = match fn_setweight_selective(&v, Weight::A, &filter).unwrap() {
        Value::TsVector(b) => b,
        _ => panic!(),
    };
    let display = tsvector_display(&out);
    assert_eq!(display, "'bar':2 'baz':3A 'foo':1A");
}

#[test]
fn setweight_selective_empty_filter_is_noop() {
    use crate::types::Value;
    let v = tsv_with(&[(b"foo", &[(1, Weight::D)])]);
    let out = match fn_setweight_selective(&v, Weight::A, &[]).unwrap() {
        Value::TsVector(b) => b,
        _ => panic!(),
    };
    assert_eq!(out.as_ref(), v.as_slice());
}

#[test]
fn setweight_selective_unknown_lexeme_in_filter_is_ignored() {
    use crate::types::Value;
    let v = tsv_with(&[(b"foo", &[(1, Weight::D)])]);
    let filter = vec![Value::Text("not_in_vector".into())];
    let out = match fn_setweight_selective(&v, Weight::A, &filter).unwrap() {
        Value::TsVector(b) => b,
        _ => panic!(),
    };
    assert_eq!(out.as_ref(), v.as_slice());
}
