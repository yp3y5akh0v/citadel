//! Full-text search: tsvector/tsquery binary codec + evaluator.
//!
//! Wire format is canonical: identical content produces byte-identical bytes,
//! enabling hash + equality + B-tree ordering without re-parsing.

use crate::error::{Result, SqlError};
use std::sync::Arc;

pub const MAX_POSITION: u16 = 16_383;
pub const MAX_POSITIONS_PER_LEXEME: u16 = 255;

pub const TSV_FLAG_POSITION_OVERFLOW: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Weight {
    D = 0,
    C = 1,
    B = 2,
    A = 3,
}

impl Weight {
    pub fn as_bits(self) -> u16 {
        (self as u16) << 14
    }

    pub fn from_bits(packed: u16) -> Self {
        match packed >> 14 {
            0 => Weight::D,
            1 => Weight::C,
            2 => Weight::B,
            _ => Weight::A,
        }
    }

    pub fn label(self) -> char {
        match self {
            Weight::D => 'D',
            Weight::C => 'C',
            Weight::B => 'B',
            Weight::A => 'A',
        }
    }
}

#[inline]
pub fn pack_position(pos: u16, weight: Weight) -> u16 {
    weight.as_bits() | (pos & MAX_POSITION)
}

#[inline]
pub fn unpack_position(packed: u16) -> (u16, Weight) {
    (packed & MAX_POSITION, Weight::from_bits(packed))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexemePos {
    pub lexeme: Vec<u8>,
    pub positions: Vec<u16>, // packed (pos|weight); sorted ascending
}

#[derive(Debug, Clone)]
pub struct TsVectorBuilder {
    by_lex: std::collections::BTreeMap<Vec<u8>, Vec<u16>>,
    overflowed: bool,
}

impl Default for TsVectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TsVectorBuilder {
    pub fn new() -> Self {
        Self {
            by_lex: std::collections::BTreeMap::new(),
            overflowed: false,
        }
    }

    pub fn push(&mut self, lexeme: &[u8], position: u16, weight: Weight) {
        if position == 0 || position > MAX_POSITION {
            self.overflowed = true;
            return;
        }
        let entry = self.by_lex.entry(lexeme.to_vec()).or_default();
        if entry.len() >= MAX_POSITIONS_PER_LEXEME as usize {
            return;
        }
        let packed = pack_position(position, weight);
        let key = (position, weight as u8);
        let insert_at = entry
            .binary_search_by(|p| {
                let (pp, pw) = unpack_position(*p);
                (pp, pw as u8).cmp(&key)
            })
            .unwrap_or_else(|e| e);
        if insert_at < entry.len() {
            let (ep, ew) = unpack_position(entry[insert_at]);
            if ep == position && ew == weight {
                return;
            }
        }
        entry.insert(insert_at, packed);
    }

    pub fn push_no_position(&mut self, lexeme: &[u8]) {
        self.by_lex.entry(lexeme.to_vec()).or_default();
    }

    pub fn build(self) -> Arc<[u8]> {
        let mut buf = Vec::with_capacity(8 + self.by_lex.len() * 16);
        let flags = if self.overflowed {
            TSV_FLAG_POSITION_OVERFLOW
        } else {
            0
        };
        buf.push(flags);
        buf.extend_from_slice(&(self.by_lex.len() as u32).to_le_bytes());
        for (lex, positions) in self.by_lex {
            buf.extend_from_slice(&(lex.len() as u16).to_le_bytes());
            buf.extend_from_slice(&lex);
            buf.extend_from_slice(&(positions.len() as u16).to_le_bytes());
            for p in positions {
                buf.extend_from_slice(&p.to_le_bytes());
            }
        }
        Arc::from(buf)
    }
}

pub struct TsVectorReader<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: u32,
}

impl<'a> TsVectorReader<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<(u8, Self)> {
        if bytes.is_empty() {
            return Err(SqlError::InvalidValue("empty tsvector".into()));
        }
        let flags = bytes[0];
        if bytes.len() < 5 {
            return Err(SqlError::InvalidValue("truncated tsvector header".into()));
        }
        let count = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        Ok((
            flags,
            Self {
                bytes,
                pos: 5,
                remaining: count,
            },
        ))
    }
}

impl<'a> Iterator for TsVectorReader<'a> {
    type Item = Result<(&'a [u8], Vec<u16>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        if self.pos + 2 > self.bytes.len() {
            return Some(Err(SqlError::InvalidValue("truncated tsvector lex".into())));
        }
        let lex_len = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]) as usize;
        self.pos += 2;
        if self.pos + lex_len > self.bytes.len() {
            return Some(Err(SqlError::InvalidValue("truncated lex bytes".into())));
        }
        let lex = &self.bytes[self.pos..self.pos + lex_len];
        self.pos += lex_len;
        if self.pos + 2 > self.bytes.len() {
            return Some(Err(SqlError::InvalidValue("truncated pos count".into())));
        }
        let pc = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]) as usize;
        self.pos += 2;
        if self.pos + pc * 2 > self.bytes.len() {
            return Some(Err(SqlError::InvalidValue("truncated positions".into())));
        }
        let mut positions = Vec::with_capacity(pc);
        for _ in 0..pc {
            positions.push(u16::from_le_bytes([
                self.bytes[self.pos],
                self.bytes[self.pos + 1],
            ]));
            self.pos += 2;
        }
        Some(Ok((lex, positions)))
    }
}

pub fn tsvector_overflowed(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] & TSV_FLAG_POSITION_OVERFLOW != 0
}

pub fn tsvector_display(bytes: &[u8]) -> String {
    let (_flags, reader) = match TsVectorReader::open(bytes) {
        Ok(v) => v,
        Err(_) => return "<invalid tsvector>".into(),
    };
    let mut out = String::new();
    let mut first = true;
    for item in reader {
        let (lex, positions) = match item {
            Ok(v) => v,
            Err(_) => return "<invalid tsvector>".into(),
        };
        if !first {
            out.push(' ');
        }
        first = false;
        out.push('\'');
        out.push_str(&String::from_utf8_lossy(lex));
        out.push('\'');
        if !positions.is_empty() {
            out.push(':');
            for (i, packed) in positions.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let (p, w) = unpack_position(*packed);
                out.push_str(&p.to_string());
                if w != Weight::D {
                    out.push(w.label());
                }
            }
        }
    }
    out
}

/// Tsquery AST. Wire format (preorder):
/// ```text
/// [u8 tag]
///   0 Lexeme [u16 len][bytes][u8 weight_mask][u8 flags(prefix=1)]
///   1 And    [child][child]
///   2 Or     [child][child]
///   3 Not    [child]
///   4 Phrase [u16 distance][child][child]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TsQueryAst {
    Lexeme {
        lexeme: Vec<u8>,
        weight_mask: u8,
        prefix: bool,
    },
    And(Box<TsQueryAst>, Box<TsQueryAst>),
    Or(Box<TsQueryAst>, Box<TsQueryAst>),
    Not(Box<TsQueryAst>),
    Phrase {
        distance: u16,
        left: Box<TsQueryAst>,
        right: Box<TsQueryAst>,
    },
}

pub const TSQ_TAG_LEXEME: u8 = 0;
pub const TSQ_TAG_AND: u8 = 1;
pub const TSQ_TAG_OR: u8 = 2;
pub const TSQ_TAG_NOT: u8 = 3;
pub const TSQ_TAG_PHRASE: u8 = 4;

pub const TSQ_FLAG_PREFIX: u8 = 0x01;

impl TsQueryAst {
    pub fn encode(&self) -> Arc<[u8]> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        Arc::from(buf)
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        match self {
            TsQueryAst::Lexeme {
                lexeme,
                weight_mask,
                prefix,
            } => {
                buf.push(TSQ_TAG_LEXEME);
                buf.extend_from_slice(&(lexeme.len() as u16).to_le_bytes());
                buf.extend_from_slice(lexeme);
                buf.push(*weight_mask);
                buf.push(if *prefix { TSQ_FLAG_PREFIX } else { 0 });
            }
            TsQueryAst::And(l, r) => {
                buf.push(TSQ_TAG_AND);
                l.encode_into(buf);
                r.encode_into(buf);
            }
            TsQueryAst::Or(l, r) => {
                buf.push(TSQ_TAG_OR);
                l.encode_into(buf);
                r.encode_into(buf);
            }
            TsQueryAst::Not(c) => {
                buf.push(TSQ_TAG_NOT);
                c.encode_into(buf);
            }
            TsQueryAst::Phrase {
                distance,
                left,
                right,
            } => {
                buf.push(TSQ_TAG_PHRASE);
                buf.extend_from_slice(&distance.to_le_bytes());
                left.encode_into(buf);
                right.encode_into(buf);
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let ast = Self::decode_at(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(SqlError::InvalidValue("trailing tsquery bytes".into()));
        }
        Ok(ast)
    }

    fn decode_at(bytes: &[u8], cursor: &mut usize) -> Result<Self> {
        if *cursor >= bytes.len() {
            return Err(SqlError::InvalidValue("truncated tsquery".into()));
        }
        let tag = bytes[*cursor];
        *cursor += 1;
        match tag {
            TSQ_TAG_LEXEME => {
                if *cursor + 2 > bytes.len() {
                    return Err(SqlError::InvalidValue("truncated tsquery lex".into()));
                }
                let len = u16::from_le_bytes([bytes[*cursor], bytes[*cursor + 1]]) as usize;
                *cursor += 2;
                if *cursor + len + 2 > bytes.len() {
                    return Err(SqlError::InvalidValue("truncated tsquery lex body".into()));
                }
                let lexeme = bytes[*cursor..*cursor + len].to_vec();
                *cursor += len;
                let weight_mask = bytes[*cursor];
                let flags = bytes[*cursor + 1];
                *cursor += 2;
                Ok(TsQueryAst::Lexeme {
                    lexeme,
                    weight_mask,
                    prefix: flags & TSQ_FLAG_PREFIX != 0,
                })
            }
            TSQ_TAG_AND => {
                let l = Self::decode_at(bytes, cursor)?;
                let r = Self::decode_at(bytes, cursor)?;
                Ok(TsQueryAst::And(Box::new(l), Box::new(r)))
            }
            TSQ_TAG_OR => {
                let l = Self::decode_at(bytes, cursor)?;
                let r = Self::decode_at(bytes, cursor)?;
                Ok(TsQueryAst::Or(Box::new(l), Box::new(r)))
            }
            TSQ_TAG_NOT => {
                let c = Self::decode_at(bytes, cursor)?;
                Ok(TsQueryAst::Not(Box::new(c)))
            }
            TSQ_TAG_PHRASE => {
                if *cursor + 2 > bytes.len() {
                    return Err(SqlError::InvalidValue("truncated phrase distance".into()));
                }
                let distance = u16::from_le_bytes([bytes[*cursor], bytes[*cursor + 1]]);
                *cursor += 2;
                let l = Self::decode_at(bytes, cursor)?;
                let r = Self::decode_at(bytes, cursor)?;
                Ok(TsQueryAst::Phrase {
                    distance,
                    left: Box::new(l),
                    right: Box::new(r),
                })
            }
            other => Err(SqlError::InvalidValue(format!(
                "unknown tsquery tag: {other}"
            ))),
        }
    }
}

pub fn tsquery_display(bytes: &[u8]) -> String {
    match TsQueryAst::decode(bytes) {
        Ok(ast) => display_ast(&ast),
        Err(_) => "<invalid tsquery>".into(),
    }
}

fn display_ast(ast: &TsQueryAst) -> String {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            let mut s = format!("'{}'", String::from_utf8_lossy(lexeme));
            if *prefix || *weight_mask != 0 {
                s.push(':');
                if *prefix {
                    s.push('*');
                }
                for (bit, label) in [(8, 'A'), (4, 'B'), (2, 'C'), (1, 'D')] {
                    if weight_mask & bit != 0 {
                        s.push(label);
                    }
                }
            }
            s
        }
        TsQueryAst::And(l, r) => format!("{} & {}", display_ast(l), display_ast(r)),
        TsQueryAst::Or(l, r) => format!("({} | {})", display_ast(l), display_ast(r)),
        TsQueryAst::Not(c) => format!("!{}", display_ast(c)),
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => {
            if *distance == 1 {
                format!("{} <-> {}", display_ast(left), display_ast(right))
            } else {
                format!(
                    "{} <{}> {}",
                    display_ast(left),
                    distance,
                    display_ast(right)
                )
            }
        }
    }
}

pub fn parse_tsquery(input: &str) -> Result<TsQueryAst> {
    let mut p = TsQueryParser::new(input);
    let ast = p.parse_or()?;
    p.skip_ws();
    if p.cursor < p.input.len() {
        return Err(SqlError::InvalidValue(format!(
            "unexpected trailing input in tsquery: {}",
            &p.input[p.cursor..]
        )));
    }
    Ok(ast)
}

struct TsQueryParser<'a> {
    input: &'a str,
    cursor: usize,
}

impl<'a> TsQueryParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, cursor: 0 }
    }

    fn skip_ws(&mut self) {
        let bytes = self.input.as_bytes();
        while self.cursor < bytes.len() && bytes[self.cursor].is_ascii_whitespace() {
            self.cursor += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.cursor).copied()
    }

    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn parse_or(&mut self) -> Result<TsQueryAst> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_ws();
            if !self.eat(b'|') {
                break;
            }
            let right = self.parse_and()?;
            left = TsQueryAst::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<TsQueryAst> {
        let mut left = self.parse_not()?;
        loop {
            self.skip_ws();
            if !self.eat(b'&') {
                break;
            }
            let right = self.parse_not()?;
            left = TsQueryAst::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<TsQueryAst> {
        self.skip_ws();
        if self.eat(b'!') {
            let inner = self.parse_not()?;
            return Ok(TsQueryAst::Not(Box::new(inner)));
        }
        self.parse_phrase()
    }

    fn parse_phrase(&mut self) -> Result<TsQueryAst> {
        let mut left = self.parse_atom()?;
        loop {
            self.skip_ws();
            if self.peek() != Some(b'<') {
                break;
            }
            let dist = self.parse_phrase_distance()?;
            let right = self.parse_atom()?;
            left = TsQueryAst::Phrase {
                distance: dist,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_phrase_distance(&mut self) -> Result<u16> {
        if !self.eat(b'<') {
            return Err(SqlError::InvalidValue("expected '<'".into()));
        }
        if self.eat(b'-') {
            if !self.eat(b'>') {
                return Err(SqlError::InvalidValue("expected '<->' phrase op".into()));
            }
            return Ok(1);
        }
        let start = self.cursor;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.cursor += 1;
            } else {
                break;
            }
        }
        if start == self.cursor {
            return Err(SqlError::InvalidValue(
                "expected distance after '<' in phrase op".into(),
            ));
        }
        let dist_str = &self.input[start..self.cursor];
        let dist: u16 = dist_str
            .parse()
            .map_err(|_| SqlError::InvalidValue(format!("invalid phrase distance: {dist_str}")))?;
        if !(1..=MAX_POSITION).contains(&dist) {
            return Err(SqlError::InvalidValue(format!(
                "phrase distance {dist} out of range 1..={MAX_POSITION}"
            )));
        }
        if !self.eat(b'>') {
            return Err(SqlError::InvalidValue("expected '>' after distance".into()));
        }
        Ok(dist)
    }

    fn parse_atom(&mut self) -> Result<TsQueryAst> {
        self.skip_ws();
        if self.eat(b'(') {
            let inner = self.parse_or()?;
            self.skip_ws();
            if !self.eat(b')') {
                return Err(SqlError::InvalidValue("missing closing paren".into()));
            }
            return Ok(inner);
        }
        let lexeme = self.parse_lexeme_word()?;
        let (weight_mask, prefix) = self.parse_weight_and_prefix()?;
        Ok(TsQueryAst::Lexeme {
            lexeme: lexeme.into_bytes(),
            weight_mask,
            prefix,
        })
    }

    fn parse_lexeme_word(&mut self) -> Result<String> {
        self.skip_ws();
        if self.eat(b'\'') {
            let start = self.cursor;
            let bytes = self.input.as_bytes();
            while self.cursor < bytes.len() && bytes[self.cursor] != b'\'' {
                self.cursor += 1;
            }
            if self.cursor >= bytes.len() {
                return Err(SqlError::InvalidValue(
                    "unterminated quoted lexeme in tsquery".into(),
                ));
            }
            let word = self.input[start..self.cursor].to_string();
            self.cursor += 1; // closing quote
            if word.is_empty() {
                return Err(SqlError::InvalidValue("empty lexeme in tsquery".into()));
            }
            return Ok(word);
        }
        let start = self.cursor;
        for (i, ch) in self.input[self.cursor..].char_indices() {
            if ch.is_alphanumeric() || ch == '_' {
                self.cursor = start + i + ch.len_utf8();
            } else {
                break;
            }
        }
        if self.cursor == start {
            return Err(SqlError::InvalidValue(format!(
                "expected lexeme at: {}",
                &self.input[self.cursor..]
            )));
        }
        Ok(self.input[start..self.cursor].to_string())
    }

    fn parse_weight_and_prefix(&mut self) -> Result<(u8, bool)> {
        if !self.eat(b':') {
            return Ok((0, false));
        }
        let mut prefix = false;
        let mut mask: u8 = 0;
        loop {
            match self.peek() {
                Some(b'*') => {
                    prefix = true;
                    self.cursor += 1;
                }
                Some(c) if matches!(c, b'A' | b'B' | b'C' | b'D' | b'a' | b'b' | b'c' | b'd') => {
                    let bit = match c.to_ascii_uppercase() {
                        b'A' => 0b1000,
                        b'B' => 0b0100,
                        b'C' => 0b0010,
                        b'D' => 0b0001,
                        _ => unreachable!(),
                    };
                    mask |= bit;
                    self.cursor += 1;
                }
                _ => break,
            }
        }
        Ok((mask, prefix))
    }
}

pub fn op_match(tsvector_bytes: &[u8], tsquery_bytes: &[u8]) -> Result<crate::types::Value> {
    let ast = TsQueryAst::decode(tsquery_bytes)?;
    let (flags, reader) = TsVectorReader::open(tsvector_bytes)?;
    let mut entries: Vec<(Vec<u8>, Vec<u16>)> = Vec::new();
    for item in reader {
        let (lex, positions) = item?;
        entries.push((lex.to_vec(), positions));
    }
    let overflowed = flags & TSV_FLAG_POSITION_OVERFLOW != 0;
    let matched = eval_match(&ast, &entries, overflowed)?;
    Ok(crate::types::Value::Boolean(matched))
}

fn eval_match(ast: &TsQueryAst, entries: &[(Vec<u8>, Vec<u16>)], overflowed: bool) -> Result<bool> {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => Ok(!collect_lex_positions(entries, lexeme, *weight_mask, *prefix).is_empty()),
        TsQueryAst::And(l, r) => {
            Ok(eval_match(l, entries, overflowed)? && eval_match(r, entries, overflowed)?)
        }
        TsQueryAst::Or(l, r) => {
            Ok(eval_match(l, entries, overflowed)? || eval_match(r, entries, overflowed)?)
        }
        TsQueryAst::Not(c) => Ok(!eval_match(c, entries, overflowed)?),
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => {
            if overflowed {
                return Err(SqlError::Unsupported(
                    "tsvector position overflow; phrase queries unreliable".into(),
                ));
            }
            let left_pos = phrase_positions(left, entries)?;
            let right_pos = phrase_positions(right, entries)?;
            Ok(positions_at_offset(&left_pos, &right_pos, *distance))
        }
    }
}

fn phrase_positions(ast: &TsQueryAst, entries: &[(Vec<u8>, Vec<u16>)]) -> Result<Vec<u16>> {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => Ok(collect_lex_positions(
            entries,
            lexeme,
            *weight_mask,
            *prefix,
        )),
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => {
            let lp = phrase_positions(left, entries)?;
            let rp = phrase_positions(right, entries)?;
            Ok(positions_pairing_right(&lp, &rp, *distance))
        }
        _ => Err(SqlError::Unsupported(
            "tsquery: AND/OR/NOT inside phrase operator not supported".into(),
        )),
    }
}

fn positions_at_offset(left: &[u16], right: &[u16], distance: u16) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        let lp = left[i] & MAX_POSITION;
        let rp = right[j] & MAX_POSITION;
        if rp == lp.saturating_add(distance) {
            return true;
        }
        if rp < lp + distance {
            j += 1;
        } else {
            i += 1;
        }
    }
    false
}

fn positions_pairing_right(left: &[u16], right: &[u16], distance: u16) -> Vec<u16> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        let lp = left[i] & MAX_POSITION;
        let rp = right[j] & MAX_POSITION;
        let target = lp.saturating_add(distance);
        if rp == target {
            if out.last().copied() != Some(right[j]) {
                out.push(right[j]);
            }
            j += 1;
        } else if rp < target {
            j += 1;
        } else {
            i += 1;
        }
    }
    out
}

fn collect_lex_positions(
    entries: &[(Vec<u8>, Vec<u16>)],
    query_lex: &[u8],
    weight_mask: u8,
    prefix: bool,
) -> Vec<u16> {
    let mut out: Vec<u16> = Vec::new();
    let weight_to_bit = |w: Weight| -> u8 {
        match w {
            Weight::A => 0b1000,
            Weight::B => 0b0100,
            Weight::C => 0b0010,
            Weight::D => 0b0001,
        }
    };
    let collect_from = |positions: &[u16], out: &mut Vec<u16>| {
        for &p in positions {
            if weight_mask != 0 {
                let (_pos, w) = unpack_position(p);
                if weight_to_bit(w) & weight_mask == 0 {
                    continue;
                }
            }
            out.push(p);
        }
    };
    if prefix {
        let start = entries.partition_point(|(lex, _)| lex.as_slice() < query_lex);
        for (lex, positions) in entries[start..].iter() {
            if !lex.starts_with(query_lex) {
                break;
            }
            collect_from(positions, &mut out);
        }
        out.sort_unstable();
        out.dedup();
    } else {
        if let Ok(i) = entries.binary_search_by(|(lex, _)| lex.as_slice().cmp(query_lex)) {
            collect_from(&entries[i].1, &mut out);
        }
    }
    out
}

pub fn fn_length_tsvector(bytes: &[u8]) -> Result<crate::types::Value> {
    let (_flags, reader) = TsVectorReader::open(bytes)?;
    let count = reader.count() as i64;
    Ok(crate::types::Value::Integer(count))
}

pub fn fn_numnode(bytes: &[u8]) -> Result<crate::types::Value> {
    let ast = TsQueryAst::decode(bytes)?;
    Ok(crate::types::Value::Integer(count_nodes(&ast) as i64))
}

fn count_nodes(ast: &TsQueryAst) -> usize {
    match ast {
        TsQueryAst::Lexeme { .. } => 1,
        TsQueryAst::And(l, r) | TsQueryAst::Or(l, r) => 1 + count_nodes(l) + count_nodes(r),
        TsQueryAst::Not(c) => 1 + count_nodes(c),
        TsQueryAst::Phrase { left, right, .. } => 1 + count_nodes(left) + count_nodes(right),
    }
}

fn weight_default(w: Weight) -> f64 {
    match w {
        Weight::A => 1.0,
        Weight::B => 0.4,
        Weight::C => 0.2,
        Weight::D => 0.1,
    }
}

pub fn fn_ts_rank(tsv: &[u8], tsq: &[u8], norm: i64) -> Result<crate::types::Value> {
    let entries = decode_entries(tsv)?;
    let ast = TsQueryAst::decode(tsq)?;
    let mut score = 0.0_f64;
    accumulate_rank(&ast, &entries, &mut score, true);
    score = apply_norm(score, &entries, norm);
    Ok(crate::types::Value::Real(score))
}

pub fn fn_ts_rank_cd(tsv: &[u8], tsq: &[u8], norm: i64) -> Result<crate::types::Value> {
    let entries = decode_entries(tsv)?;
    let ast = TsQueryAst::decode(tsq)?;
    let mut atom_lists: Vec<Vec<u16>> = Vec::new();
    collect_positive_atoms(&ast, &entries, &mut atom_lists);
    if atom_lists.is_empty() || atom_lists.iter().any(|l| l.is_empty()) {
        return Ok(crate::types::Value::Real(0.0));
    }
    let score = shortest_cover_score(&atom_lists);
    let score = apply_norm(score, &entries, norm);
    Ok(crate::types::Value::Real(score))
}

fn decode_entries(tsv: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u16>)>> {
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut out = Vec::new();
    for item in reader {
        let (lex, positions) = item?;
        out.push((lex.to_vec(), positions));
    }
    Ok(out)
}

fn accumulate_rank(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    out: &mut f64,
    positive: bool,
) {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            if !positive {
                return;
            }
            let positions = collect_lex_positions(entries, lexeme, *weight_mask, *prefix);
            if positions.is_empty() {
                return;
            }
            let weight_sum: f64 = positions
                .iter()
                .map(|&p| weight_default(Weight::from_bits(p)))
                .sum();
            let tf = (positions.len() as f64).ln_1p();
            *out += weight_sum * (1.0 + tf);
        }
        TsQueryAst::And(l, r) | TsQueryAst::Or(l, r) => {
            accumulate_rank(l, entries, out, positive);
            accumulate_rank(r, entries, out, positive);
        }
        TsQueryAst::Not(c) => accumulate_rank(c, entries, out, !positive),
        TsQueryAst::Phrase { left, right, .. } => {
            accumulate_rank(left, entries, out, positive);
            accumulate_rank(right, entries, out, positive);
        }
    }
}

fn collect_positive_atoms(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    out: &mut Vec<Vec<u16>>,
) {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            let positions = collect_lex_positions(entries, lexeme, *weight_mask, *prefix);
            out.push(positions);
        }
        TsQueryAst::And(l, r)
        | TsQueryAst::Or(l, r)
        | TsQueryAst::Phrase {
            left: l, right: r, ..
        } => {
            collect_positive_atoms(l, entries, out);
            collect_positive_atoms(r, entries, out);
        }
        TsQueryAst::Not(_) => {} // negated atoms don't contribute to cover
    }
}

fn shortest_cover_score(atom_lists: &[Vec<u16>]) -> f64 {
    if atom_lists.is_empty() {
        return 0.0;
    }
    let mut events: Vec<(u16, usize, u16)> = Vec::new();
    for (i, list) in atom_lists.iter().enumerate() {
        for &packed in list {
            let (pos, _w) = unpack_position(packed);
            events.push((pos, i, packed));
        }
    }
    events.sort_unstable_by_key(|e| e.0);

    let k = atom_lists.len();
    let mut count_per_atom = vec![0usize; k];
    let mut covered_count = 0usize;
    let mut best_score = 0.0_f64;
    let mut l = 0usize;
    for r in 0..events.len() {
        let ai = events[r].1;
        if count_per_atom[ai] == 0 {
            covered_count += 1;
        }
        count_per_atom[ai] += 1;
        while covered_count == k {
            let window_len = (events[r].0 - events[l].0 + 1) as f64;
            let weight_sum: f64 = events[l..=r]
                .iter()
                .map(|e| weight_default(Weight::from_bits(e.2)))
                .sum();
            let candidate = weight_sum / window_len;
            if candidate > best_score {
                best_score = candidate;
            }
            let la = events[l].1;
            count_per_atom[la] -= 1;
            if count_per_atom[la] == 0 {
                covered_count -= 1;
            }
            l += 1;
        }
    }
    best_score
}

fn apply_norm(mut score: f64, entries: &[(Vec<u8>, Vec<u16>)], norm: i64) -> f64 {
    let doc_len: f64 = entries.iter().map(|e| e.1.len()).sum::<usize>() as f64;
    let unique = entries.len() as f64;
    if (norm & 1) != 0 && doc_len > 1.0 {
        score /= 1.0 + doc_len.ln();
    }
    if (norm & 2) != 0 && doc_len > 0.0 {
        score /= doc_len;
    }
    if (norm & 8) != 0 && unique > 0.0 {
        score /= unique;
    }
    if (norm & 16) != 0 && unique > 1.0 {
        score /= 1.0 + unique.ln();
    }
    if (norm & 32) != 0 {
        score /= score + 1.0;
    }
    score
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub lexeme: String,
    pub position: u16,
    pub stopped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizerKind {
    Simple,
    English,
}

impl TokenizerKind {
    pub fn from_name(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "simple" => Ok(TokenizerKind::Simple),
            "english" | "pg_catalog.english" => Ok(TokenizerKind::English),
            other => Err(SqlError::Unsupported(format!(
                "unknown text search configuration: {other}"
            ))),
        }
    }

    pub fn as_config_id(self) -> u8 {
        match self {
            TokenizerKind::Simple => 0,
            TokenizerKind::English => 1,
        }
    }

    pub fn from_config_id(id: u8) -> Result<Self> {
        match id {
            0 => Ok(TokenizerKind::Simple),
            1 => Ok(TokenizerKind::English),
            _ => Err(SqlError::InvalidValue(format!(
                "unknown FTS config_id: {id}"
            ))),
        }
    }
}

// Vendored from `postgres/src/backend/snowball/stopwords/english.stop`.
const ENGLISH_STOP_WORDS: &[&str] = &[
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "did",
    "do",
    "does",
    "doing",
    "don",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "me",
    "more",
    "most",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "now",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "s",
    "same",
    "she",
    "should",
    "so",
    "some",
    "such",
    "t",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
];

fn is_english_stopword(word: &str) -> bool {
    ENGLISH_STOP_WORDS.binary_search(&word).is_ok()
}

pub fn tokenize(kind: TokenizerKind, text: &str) -> Vec<Token> {
    use unicode_normalization::UnicodeNormalization;
    use unicode_segmentation::UnicodeSegmentation;

    let normalized: String = text.nfkc().collect();
    let lowered = normalized.to_lowercase();

    let mut out = Vec::new();
    let mut position: u32 = 0;
    for word in lowered.unicode_words() {
        position += 1;
        let pos_u16 = if position <= MAX_POSITION as u32 {
            position as u16
        } else {
            MAX_POSITION + 1 // signals overflow downstream
        };
        let mut stopped = false;
        let lexeme = match kind {
            TokenizerKind::Simple => word.to_string(),
            TokenizerKind::English => {
                if is_english_stopword(word) {
                    stopped = true;
                    String::new()
                } else {
                    tantivy_stemmers::algorithms::english_porter_2(word).into_owned()
                }
            }
        };
        if lexeme.is_empty() && !stopped {
            position -= 1;
            continue;
        }
        out.push(Token {
            lexeme,
            position: pos_u16,
            stopped,
        });
    }
    out
}

fn stem_one(kind: TokenizerKind, word: &str) -> Option<String> {
    use unicode_normalization::UnicodeNormalization;
    let normalized: String = word.nfkc().collect();
    let lowered = normalized.to_lowercase();
    match kind {
        TokenizerKind::Simple => Some(lowered),
        TokenizerKind::English => {
            if is_english_stopword(&lowered) {
                None
            } else {
                Some(tantivy_stemmers::algorithms::english_porter_2(&lowered).into_owned())
            }
        }
    }
}

pub fn fn_to_tsvector_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let tokens = tokenize(kind, text);
    let mut b = TsVectorBuilder::new();
    for t in tokens {
        if t.stopped {
            continue;
        }
        b.push(t.lexeme.as_bytes(), t.position, Weight::D);
    }
    Ok(crate::types::Value::TsVector(b.build()))
}

pub fn fn_to_tsvector(text: &str) -> Result<crate::types::Value> {
    fn_to_tsvector_with(TokenizerKind::English, text)
}

pub fn fn_to_tsquery_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let raw = parse_tsquery(text)?;
    let stemmed = stem_ast(&raw, kind)?;
    Ok(crate::types::Value::TsQuery(stemmed.encode()))
}

fn stem_ast(ast: &TsQueryAst, kind: TokenizerKind) -> Result<TsQueryAst> {
    Ok(match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            let s = std::str::from_utf8(lexeme)
                .map_err(|_| SqlError::InvalidValue("tsquery lexeme has invalid UTF-8".into()))?;
            let stemmed = stem_one(kind, s).ok_or_else(|| {
                SqlError::InvalidValue(format!("tsquery: lexeme '{s}' is a stop-word"))
            })?;
            TsQueryAst::Lexeme {
                lexeme: stemmed.into_bytes(),
                weight_mask: *weight_mask,
                prefix: *prefix,
            }
        }
        TsQueryAst::And(l, r) => {
            TsQueryAst::And(Box::new(stem_ast(l, kind)?), Box::new(stem_ast(r, kind)?))
        }
        TsQueryAst::Or(l, r) => {
            TsQueryAst::Or(Box::new(stem_ast(l, kind)?), Box::new(stem_ast(r, kind)?))
        }
        TsQueryAst::Not(c) => TsQueryAst::Not(Box::new(stem_ast(c, kind)?)),
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => TsQueryAst::Phrase {
            distance: *distance,
            left: Box::new(stem_ast(left, kind)?),
            right: Box::new(stem_ast(right, kind)?),
        },
    })
}

pub fn fn_plainto_tsquery_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let tokens = tokenize(kind, text);
    let lexemes: Vec<Vec<u8>> = tokens
        .into_iter()
        .filter(|t| !t.stopped && !t.lexeme.is_empty())
        .map(|t| t.lexeme.into_bytes())
        .collect();
    let ast = and_chain(&lexemes)?;
    Ok(crate::types::Value::TsQuery(ast.encode()))
}

fn and_chain(lexemes: &[Vec<u8>]) -> Result<TsQueryAst> {
    if lexemes.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    let mut iter = lexemes.iter().map(|l| TsQueryAst::Lexeme {
        lexeme: l.clone(),
        weight_mask: 0,
        prefix: false,
    });
    let first = iter.next().unwrap();
    Ok(iter.fold(first, |acc, x| TsQueryAst::And(Box::new(acc), Box::new(x))))
}

pub fn fn_phraseto_tsquery_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let tokens = tokenize(kind, text);
    let mut lex_positions: Vec<(Vec<u8>, u16)> = Vec::new();
    for t in tokens {
        if t.stopped || t.lexeme.is_empty() {
            continue;
        }
        lex_positions.push((t.lexeme.into_bytes(), t.position));
    }
    if lex_positions.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    let mut iter = lex_positions.into_iter();
    let (first_lex, mut prev_pos) = iter.next().unwrap();
    let mut acc = TsQueryAst::Lexeme {
        lexeme: first_lex,
        weight_mask: 0,
        prefix: false,
    };
    for (lex, pos) in iter {
        let dist = pos.saturating_sub(prev_pos).max(1);
        let right = TsQueryAst::Lexeme {
            lexeme: lex,
            weight_mask: 0,
            prefix: false,
        };
        acc = TsQueryAst::Phrase {
            distance: dist,
            left: Box::new(acc),
            right: Box::new(right),
        };
        prev_pos = pos;
    }
    Ok(crate::types::Value::TsQuery(acc.encode()))
}

pub fn fn_websearch_to_tsquery_with(
    kind: TokenizerKind,
    text: &str,
) -> Result<crate::types::Value> {
    let mut groups: Vec<TsQueryAst> = Vec::new();
    let mut current_terms: Vec<TsQueryAst> = Vec::new();
    let mut cursor = 0usize;
    let bytes = text.as_bytes();

    let flush_group = |terms: &mut Vec<TsQueryAst>, groups: &mut Vec<TsQueryAst>| {
        if terms.is_empty() {
            return;
        }
        let mut iter = std::mem::take(terms).into_iter();
        let first = iter.next().unwrap();
        let combined = iter.fold(first, |acc, x| TsQueryAst::And(Box::new(acc), Box::new(x)));
        groups.push(combined);
    };

    while cursor < bytes.len() {
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            break;
        }
        let negate = if bytes[cursor] == b'-' {
            cursor += 1;
            true
        } else {
            false
        };
        if cursor < bytes.len() && bytes[cursor] == b'"' {
            cursor += 1;
            let start = cursor;
            while cursor < bytes.len() && bytes[cursor] != b'"' {
                cursor += 1;
            }
            let inner = &text[start..cursor];
            if cursor < bytes.len() {
                cursor += 1; // closing quote
            }
            if let Ok(crate::types::Value::TsQuery(q)) = fn_phraseto_tsquery_with(kind, inner) {
                let mut ast = TsQueryAst::decode(&q)?;
                if negate {
                    ast = TsQueryAst::Not(Box::new(ast));
                }
                current_terms.push(ast);
            }
            continue;
        }
        let start = cursor;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let word = &text[start..cursor];
        if word.eq_ignore_ascii_case("or") {
            flush_group(&mut current_terms, &mut groups);
            continue;
        }
        if let Some(stemmed) = stem_one(kind, word) {
            let mut ast = TsQueryAst::Lexeme {
                lexeme: stemmed.into_bytes(),
                weight_mask: 0,
                prefix: false,
            };
            if negate {
                ast = TsQueryAst::Not(Box::new(ast));
            }
            current_terms.push(ast);
        }
    }
    flush_group(&mut current_terms, &mut groups);
    if groups.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    let mut iter = groups.into_iter();
    let first = iter.next().unwrap();
    let combined = iter.fold(first, |acc, x| TsQueryAst::Or(Box::new(acc), Box::new(x)));
    Ok(crate::types::Value::TsQuery(combined.encode()))
}

pub fn fn_ts_headline_with(
    kind: TokenizerKind,
    text: &str,
    tsq_bytes: &[u8],
) -> Result<crate::types::Value> {
    use unicode_segmentation::UnicodeSegmentation;
    let ast = TsQueryAst::decode(tsq_bytes)?;
    let positive_lexemes = collect_query_atoms(&ast);
    let mut out = String::with_capacity(text.len() + 16);
    let mut last_end = 0usize;
    for (idx, word) in text.split_word_bound_indices() {
        let word_lower: String = word.to_lowercase();
        let stemmed = stem_one(kind, &word_lower);
        let matched = stemmed.as_ref().is_some_and(|s| {
            positive_lexemes
                .iter()
                .any(|q| q == s.as_bytes() || word_lower.as_bytes() == q)
        });
        if matched {
            out.push_str(&text[last_end..idx]);
            out.push_str("<b>");
            out.push_str(word);
            out.push_str("</b>");
            last_end = idx + word.len();
        }
    }
    out.push_str(&text[last_end..]);
    Ok(crate::types::Value::Text(out.into()))
}

fn collect_query_atoms(ast: &TsQueryAst) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    fn walk(ast: &TsQueryAst, positive: bool, out: &mut Vec<Vec<u8>>) {
        match ast {
            TsQueryAst::Lexeme { lexeme, .. } => {
                if positive {
                    out.push(lexeme.clone());
                }
            }
            TsQueryAst::And(l, r) | TsQueryAst::Or(l, r) => {
                walk(l, positive, out);
                walk(r, positive, out);
            }
            TsQueryAst::Not(c) => walk(c, !positive, out),
            TsQueryAst::Phrase { left, right, .. } => {
                walk(left, positive, out);
                walk(right, positive, out);
            }
        }
    }
    walk(ast, true, &mut out);
    out
}

pub fn fn_ts_lexize_with(kind: TokenizerKind, word: &str) -> Result<crate::types::Value> {
    match stem_one(kind, word) {
        Some(s) => Ok(crate::types::Value::Text(s.into())),
        None => Ok(crate::types::Value::Null),
    }
}

pub fn fn_to_tsquery(text: &str) -> Result<crate::types::Value> {
    fn_to_tsquery_with(TokenizerKind::English, text)
}

pub fn fn_plainto_tsquery(text: &str) -> Result<crate::types::Value> {
    fn_plainto_tsquery_with(TokenizerKind::English, text)
}

pub fn fn_phraseto_tsquery(text: &str) -> Result<crate::types::Value> {
    fn_phraseto_tsquery_with(TokenizerKind::English, text)
}

pub fn fn_websearch_to_tsquery(text: &str) -> Result<crate::types::Value> {
    fn_websearch_to_tsquery_with(TokenizerKind::English, text)
}

pub fn fn_ts_headline(text: &str, tsq: &[u8]) -> Result<crate::types::Value> {
    fn_ts_headline_with(TokenizerKind::English, text, tsq)
}

pub fn fn_ts_lexize(word: &str) -> Result<crate::types::Value> {
    fn_ts_lexize_with(TokenizerKind::English, word)
}

pub fn parse_weight_char(s: &str) -> Result<Weight> {
    let bytes = s.as_bytes();
    if bytes.len() == 1 {
        match bytes[0].to_ascii_uppercase() {
            b'A' => return Ok(Weight::A),
            b'B' => return Ok(Weight::B),
            b'C' => return Ok(Weight::C),
            b'D' => return Ok(Weight::D),
            _ => {}
        }
    }
    Err(SqlError::InvalidValue(format!(
        "unrecognized weight: {}",
        bytes.first().copied().unwrap_or(0)
    )))
}

pub fn fn_setweight(tsv: &[u8], weight: Weight) -> Result<crate::types::Value> {
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut b = TsVectorBuilder::new();
    for item in reader {
        let (lex, positions) = item?;
        if positions.is_empty() {
            b.push_no_position(lex);
            continue;
        }
        for packed in positions {
            let pos = packed & MAX_POSITION;
            b.push(lex, pos, weight);
        }
    }
    Ok(crate::types::Value::TsVector(b.build()))
}

/// Apply `weight` only to lexemes appearing in `filter`; leave others unchanged.
pub fn fn_setweight_selective(
    tsv: &[u8],
    weight: Weight,
    filter: &[crate::types::Value],
) -> Result<crate::types::Value> {
    let mut filter_set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for v in filter {
        match v {
            crate::types::Value::Text(s) => {
                filter_set.insert(s.as_bytes().to_vec());
            }
            crate::types::Value::Null => continue,
            other => {
                return Err(crate::error::SqlError::TypeMismatch {
                    expected: "TEXT[]".into(),
                    got: other.data_type().to_string(),
                });
            }
        }
    }
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut b = TsVectorBuilder::new();
    for item in reader {
        let (lex, positions) = item?;
        let should_reweight = filter_set.contains(lex);
        if positions.is_empty() {
            b.push_no_position(lex);
            continue;
        }
        for packed in positions {
            let pos = packed & MAX_POSITION;
            if should_reweight {
                b.push(lex, pos, weight);
            } else {
                let (_p, w) = unpack_position(packed);
                b.push(lex, pos, w);
            }
        }
    }
    Ok(crate::types::Value::TsVector(b.build()))
}

/// Strip positions and weights from a TSVECTOR, keeping only the distinct lexeme set.
pub fn fn_strip(tsv: &[u8]) -> Result<crate::types::Value> {
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut b = TsVectorBuilder::new();
    for item in reader {
        let (lex, _positions) = item?;
        b.push_no_position(lex);
    }
    Ok(crate::types::Value::TsVector(b.build()))
}

/// `tsvector || tsvector`: union the lexeme sets, merging positions per lexeme.
pub fn op_concat(a: &[u8], b: &[u8]) -> Result<crate::types::Value> {
    let (_, reader_a) = TsVectorReader::open(a)?;
    let (_, reader_b) = TsVectorReader::open(b)?;
    let mut builder = TsVectorBuilder::new();
    for item in reader_a {
        let (lex, positions) = item?;
        if positions.is_empty() {
            builder.push_no_position(lex);
            continue;
        }
        for packed in positions {
            let (pos, w) = unpack_position(packed);
            builder.push(lex, pos, w);
        }
    }
    for item in reader_b {
        let (lex, positions) = item?;
        if positions.is_empty() {
            builder.push_no_position(lex);
            continue;
        }
        for packed in positions {
            let (pos, w) = unpack_position(packed);
            builder.push(lex, pos, w);
        }
    }
    Ok(crate::types::Value::TsVector(builder.build()))
}

#[cfg(test)]
#[path = "fts_tests.rs"]
mod tests;
