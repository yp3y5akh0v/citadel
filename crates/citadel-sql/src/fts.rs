//! Full-text search: tsvector/tsquery binary codec + evaluator.
//!
//! Wire format is canonical: identical content produces byte-identical bytes,
//! enabling hash + equality + B-tree ordering without re-parsing.

use crate::error::{Result, SqlError};
use std::sync::Arc;

pub const MAX_POSITION: u16 = 16_383;
pub const MAX_POSITIONS_PER_LEXEME: u16 = 255;
pub const MAX_LEXEME_BYTES: usize = u16::MAX as usize;

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

    pub fn push(&mut self, lexeme: &[u8], position: u16, weight: Weight) -> Result<()> {
        validate_lexeme_length(lexeme)?;
        if position == 0 || position > MAX_POSITION {
            self.overflowed = true;
            return Ok(());
        }
        let entry = self.by_lex.entry(lexeme.to_vec()).or_default();
        if entry.len() >= MAX_POSITIONS_PER_LEXEME as usize {
            return Ok(());
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
                return Ok(());
            }
        }
        entry.insert(insert_at, packed);
        Ok(())
    }

    pub fn push_no_position(&mut self, lexeme: &[u8]) -> Result<()> {
        validate_lexeme_length(lexeme)?;
        self.by_lex.entry(lexeme.to_vec()).or_default();
        Ok(())
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
            let lexeme_len = u16::try_from(lex.len()).expect("FTS lexeme exceeds 65,535 bytes");
            buf.extend_from_slice(&lexeme_len.to_le_bytes());
            buf.extend_from_slice(&lex);
            buf.extend_from_slice(&(positions.len() as u16).to_le_bytes());
            for p in positions {
                buf.extend_from_slice(&p.to_le_bytes());
            }
        }
        Arc::from(buf)
    }

    fn build_with_cancel(self, cancel: Option<&citadel::CancelToken>) -> Result<Arc<[u8]>> {
        if cancel.is_none() {
            return Ok(self.build());
        }
        check_cancel(cancel)?;
        let mut buf = Vec::with_capacity(8 + self.by_lex.len() * 16);
        let flags = if self.overflowed {
            TSV_FLAG_POSITION_OVERFLOW
        } else {
            0
        };
        buf.push(flags);
        buf.extend_from_slice(&(self.by_lex.len() as u32).to_le_bytes());
        let mut work = 0usize;
        for (lexeme, positions) in self.by_lex {
            check_cancel_at(cancel, work)?;
            work = work.wrapping_add(1);
            validate_lexeme_length(&lexeme)?;
            let lexeme_len = u16::try_from(lexeme.len()).expect("length validated above");
            buf.extend_from_slice(&lexeme_len.to_le_bytes());
            buf.extend_from_slice(&lexeme);
            buf.extend_from_slice(&(positions.len() as u16).to_le_bytes());
            for position in positions {
                check_cancel_at(cancel, work)?;
                work = work.wrapping_add(1);
                buf.extend_from_slice(&position.to_le_bytes());
            }
        }
        check_cancel(cancel)?;
        Ok(Arc::from(buf))
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

/// Diagnostic display; SQL text conversion must use the fallible renderer.
pub fn tsvector_display(bytes: &[u8]) -> String {
    tsvector_to_text_with_cancel(bytes, None).unwrap_or_else(|_| "<invalid tsvector>".into())
}

/// Render SQL text without substituting a diagnostic string for a decode error.
pub(crate) fn tsvector_to_text_with_cancel(
    bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<String> {
    check_cancel(cancel)?;
    let (_flags, reader) = TsVectorReader::open(bytes)?;
    let mut out = String::new();
    let mut first = true;
    let mut work = 0usize;
    for item in reader {
        check_cancel_at(cancel, work)?;
        work = work.wrapping_add(1);
        let (lexeme, positions) = item?;
        if !first {
            out.push(' ');
        }
        first = false;
        out.push('\'');
        out.push_str(&String::from_utf8_lossy(lexeme));
        out.push('\'');
        if !positions.is_empty() {
            out.push(':');
            for (index, packed) in positions.into_iter().enumerate() {
                check_cancel_at(cancel, work)?;
                work = work.wrapping_add(1);
                if index > 0 {
                    out.push(',');
                }
                let (position, weight) = unpack_position(packed);
                out.push_str(&position.to_string());
                if weight != Weight::D {
                    out.push(weight.label());
                }
            }
        }
    }
    check_cancel(cancel)?;
    Ok(out)
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

// Keep recursive consumers within the 1 MiB stack budget exercised by the
// depth tests. The node cap also bounds adversarial, broadly branching inputs.
const MAX_TSQUERY_DEPTH: usize = 64;
const MAX_TSQUERY_NODES: usize = 4_096;

fn lexeme_length_error(len: usize) -> SqlError {
    SqlError::InvalidValue(format!(
        "FTS lexeme is {len} bytes; the maximum is {MAX_LEXEME_BYTES}"
    ))
}

fn validate_lexeme_length(lexeme: &[u8]) -> Result<()> {
    if lexeme.len() > MAX_LEXEME_BYTES {
        return Err(lexeme_length_error(lexeme.len()));
    }
    Ok(())
}

fn tsquery_complexity_error() -> SqlError {
    SqlError::InvalidValue(format!(
        "tsquery exceeds the complexity limit ({MAX_TSQUERY_NODES} nodes, depth {MAX_TSQUERY_DEPTH})"
    ))
}

fn validate_tsquery(ast: &TsQueryAst) -> Result<()> {
    let mut stack = vec![(ast, 1usize)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_TSQUERY_NODES || depth > MAX_TSQUERY_DEPTH {
            return Err(tsquery_complexity_error());
        }
        match node {
            TsQueryAst::Lexeme { lexeme, .. } => validate_lexeme_length(lexeme)?,
            TsQueryAst::And(left, right)
            | TsQueryAst::Or(left, right)
            | TsQueryAst::Phrase { left, right, .. } => {
                stack.push((right, depth + 1));
                stack.push((left, depth + 1));
            }
            TsQueryAst::Not(child) => stack.push((child, depth + 1)),
        }
    }
    Ok(())
}

impl TsQueryAst {
    pub fn encode(&self) -> Result<Arc<[u8]>> {
        validate_tsquery(self)?;
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        Ok(Arc::from(buf))
    }

    pub(crate) fn encode_with_cancel(
        &self,
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Arc<[u8]>> {
        if cancel.is_none() {
            return self.encode();
        }
        validate_tsquery(self)?;
        let mut buf = Vec::new();
        let mut work = 0;
        self.encode_into_with_cancel(&mut buf, cancel, &mut work)?;
        check_cancel(cancel)?;
        Ok(Arc::from(buf))
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        let mut stack = vec![self];
        while let Some(node) = stack.pop() {
            match node {
                TsQueryAst::Lexeme {
                    lexeme,
                    weight_mask,
                    prefix,
                } => {
                    buf.push(TSQ_TAG_LEXEME);
                    let lexeme_len =
                        u16::try_from(lexeme.len()).expect("FTS lexeme exceeds 65,535 bytes");
                    buf.extend_from_slice(&lexeme_len.to_le_bytes());
                    buf.extend_from_slice(lexeme);
                    buf.push(*weight_mask);
                    buf.push(if *prefix { TSQ_FLAG_PREFIX } else { 0 });
                }
                TsQueryAst::And(left, right) => {
                    buf.push(TSQ_TAG_AND);
                    stack.push(right);
                    stack.push(left);
                }
                TsQueryAst::Or(left, right) => {
                    buf.push(TSQ_TAG_OR);
                    stack.push(right);
                    stack.push(left);
                }
                TsQueryAst::Not(child) => {
                    buf.push(TSQ_TAG_NOT);
                    stack.push(child);
                }
                TsQueryAst::Phrase {
                    distance,
                    left,
                    right,
                } => {
                    buf.push(TSQ_TAG_PHRASE);
                    buf.extend_from_slice(&distance.to_le_bytes());
                    stack.push(right);
                    stack.push(left);
                }
            }
        }
    }

    fn encode_into_with_cancel(
        &self,
        buf: &mut Vec<u8>,
        cancel: Option<&citadel::CancelToken>,
        work: &mut usize,
    ) -> Result<()> {
        let mut stack = vec![self];
        while let Some(node) = stack.pop() {
            check_cancel_at(cancel, *work)?;
            *work = work.wrapping_add(1);
            if *work > MAX_TSQUERY_NODES {
                return Err(tsquery_complexity_error());
            }
            match node {
                TsQueryAst::Lexeme {
                    lexeme,
                    weight_mask,
                    prefix,
                } => {
                    buf.push(TSQ_TAG_LEXEME);
                    validate_lexeme_length(lexeme)?;
                    let lexeme_len = u16::try_from(lexeme.len()).expect("length validated above");
                    buf.extend_from_slice(&lexeme_len.to_le_bytes());
                    buf.extend_from_slice(lexeme);
                    buf.push(*weight_mask);
                    buf.push(if *prefix { TSQ_FLAG_PREFIX } else { 0 });
                }
                TsQueryAst::And(left, right) => {
                    buf.push(TSQ_TAG_AND);
                    stack.push(right);
                    stack.push(left);
                }
                TsQueryAst::Or(left, right) => {
                    buf.push(TSQ_TAG_OR);
                    stack.push(right);
                    stack.push(left);
                }
                TsQueryAst::Not(child) => {
                    buf.push(TSQ_TAG_NOT);
                    stack.push(child);
                }
                TsQueryAst::Phrase {
                    distance,
                    left,
                    right,
                } => {
                    buf.push(TSQ_TAG_PHRASE);
                    buf.extend_from_slice(&distance.to_le_bytes());
                    stack.push(right);
                    stack.push(left);
                }
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;
        let mut nodes = 0usize;
        let ast = Self::decode_at(bytes, &mut cursor, 1, &mut nodes)?;
        if cursor != bytes.len() {
            return Err(SqlError::InvalidValue("trailing tsquery bytes".into()));
        }
        Ok(ast)
    }

    pub(crate) fn decode_with_cancel(
        bytes: &[u8],
        cancel: Option<&citadel::CancelToken>,
    ) -> Result<Self> {
        if cancel.is_none() {
            return Self::decode(bytes);
        }
        let mut cursor = 0;
        let mut work = 0;
        let ast = Self::decode_at_with_cancel(bytes, &mut cursor, cancel, 1, &mut work)?;
        if cursor != bytes.len() {
            return Err(SqlError::InvalidValue("trailing tsquery bytes".into()));
        }
        check_cancel(cancel)?;
        Ok(ast)
    }

    fn decode_at_with_cancel(
        bytes: &[u8],
        cursor: &mut usize,
        cancel: Option<&citadel::CancelToken>,
        depth: usize,
        work: &mut usize,
    ) -> Result<Self> {
        if depth > MAX_TSQUERY_DEPTH || *work >= MAX_TSQUERY_NODES {
            return Err(tsquery_complexity_error());
        }
        check_cancel_at(cancel, *work)?;
        *work += 1;
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
            TSQ_TAG_AND => Ok(TsQueryAst::And(
                Box::new(Self::decode_at_with_cancel(
                    bytes,
                    cursor,
                    cancel,
                    depth + 1,
                    work,
                )?),
                Box::new(Self::decode_at_with_cancel(
                    bytes,
                    cursor,
                    cancel,
                    depth + 1,
                    work,
                )?),
            )),
            TSQ_TAG_OR => Ok(TsQueryAst::Or(
                Box::new(Self::decode_at_with_cancel(
                    bytes,
                    cursor,
                    cancel,
                    depth + 1,
                    work,
                )?),
                Box::new(Self::decode_at_with_cancel(
                    bytes,
                    cursor,
                    cancel,
                    depth + 1,
                    work,
                )?),
            )),
            TSQ_TAG_NOT => Ok(TsQueryAst::Not(Box::new(Self::decode_at_with_cancel(
                bytes,
                cursor,
                cancel,
                depth + 1,
                work,
            )?))),
            TSQ_TAG_PHRASE => {
                if *cursor + 2 > bytes.len() {
                    return Err(SqlError::InvalidValue("truncated phrase distance".into()));
                }
                let distance = u16::from_le_bytes([bytes[*cursor], bytes[*cursor + 1]]);
                *cursor += 2;
                Ok(TsQueryAst::Phrase {
                    distance,
                    left: Box::new(Self::decode_at_with_cancel(
                        bytes,
                        cursor,
                        cancel,
                        depth + 1,
                        work,
                    )?),
                    right: Box::new(Self::decode_at_with_cancel(
                        bytes,
                        cursor,
                        cancel,
                        depth + 1,
                        work,
                    )?),
                })
            }
            other => Err(SqlError::InvalidValue(format!(
                "unknown tsquery tag: {other}"
            ))),
        }
    }

    fn decode_at(
        bytes: &[u8],
        cursor: &mut usize,
        depth: usize,
        nodes: &mut usize,
    ) -> Result<Self> {
        if depth > MAX_TSQUERY_DEPTH || *nodes >= MAX_TSQUERY_NODES {
            return Err(tsquery_complexity_error());
        }
        *nodes += 1;
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
                let l = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                let r = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                Ok(TsQueryAst::And(Box::new(l), Box::new(r)))
            }
            TSQ_TAG_OR => {
                let l = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                let r = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                Ok(TsQueryAst::Or(Box::new(l), Box::new(r)))
            }
            TSQ_TAG_NOT => {
                let c = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                Ok(TsQueryAst::Not(Box::new(c)))
            }
            TSQ_TAG_PHRASE => {
                if *cursor + 2 > bytes.len() {
                    return Err(SqlError::InvalidValue("truncated phrase distance".into()));
                }
                let distance = u16::from_le_bytes([bytes[*cursor], bytes[*cursor + 1]]);
                *cursor += 2;
                let l = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
                let r = Self::decode_at(bytes, cursor, depth + 1, nodes)?;
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

/// Diagnostic display; SQL text conversion must use the fallible renderer.
pub fn tsquery_display(bytes: &[u8]) -> String {
    tsquery_to_text_with_cancel(bytes, None).unwrap_or_else(|_| "<invalid tsquery>".into())
}

/// Render SQL text without substituting a diagnostic string for a decode error.
pub(crate) fn tsquery_to_text_with_cancel(
    bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<String> {
    if cancel.is_none() {
        return TsQueryAst::decode(bytes).map(|ast| display_ast(&ast));
    }
    let ast = TsQueryAst::decode_with_cancel(bytes, cancel)?;
    let mut work = 0usize;
    let result = display_ast_with_cancel(&ast, cancel, &mut work)?;
    check_cancel(cancel)?;
    Ok(result)
}

fn display_ast_with_cancel(
    ast: &TsQueryAst,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<String> {
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            let mut output = format!("'{}'", String::from_utf8_lossy(lexeme));
            if *prefix || *weight_mask != 0 {
                output.push(':');
                if *prefix {
                    output.push('*');
                }
                for (bit, label) in [(8, 'A'), (4, 'B'), (2, 'C'), (1, 'D')] {
                    if weight_mask & bit != 0 {
                        output.push(label);
                    }
                }
            }
            Ok(output)
        }
        TsQueryAst::And(left, right) => Ok(format!(
            "{} & {}",
            display_ast_with_cancel(left, cancel, work)?,
            display_ast_with_cancel(right, cancel, work)?
        )),
        TsQueryAst::Or(left, right) => Ok(format!(
            "({} | {})",
            display_ast_with_cancel(left, cancel, work)?,
            display_ast_with_cancel(right, cancel, work)?
        )),
        TsQueryAst::Not(child) => Ok(format!(
            "!{}",
            display_ast_with_cancel(child, cancel, work)?
        )),
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => {
            let left = display_ast_with_cancel(left, cancel, work)?;
            let right = display_ast_with_cancel(right, cancel, work)?;
            if *distance == 1 {
                Ok(format!("{left} <-> {right}"))
            } else {
                Ok(format!("{left} <{distance}> {right}"))
            }
        }
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
    parse_tsquery_with_cancel(input, None)
}

pub(crate) fn parse_tsquery_with_cancel(
    input: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<TsQueryAst> {
    let mut p = TsQueryParser::new(input, cancel)?;
    let (ast, _) = p.parse_or()?;
    p.skip_ws()?;
    if p.cursor < p.input.len() {
        return Err(SqlError::InvalidValue(format!(
            "unexpected trailing input in tsquery: {}",
            &p.input[p.cursor..]
        )));
    }
    validate_tsquery(&ast)?;
    check_cancel(cancel)?;
    Ok(ast)
}

struct TsQueryParser<'a> {
    input: &'a str,
    cursor: usize,
    cancel: Option<&'a citadel::CancelToken>,
    work: usize,
    syntax_depth: usize,
    nodes: usize,
}

type ParsedTsQuery = (TsQueryAst, usize);

impl<'a> TsQueryParser<'a> {
    fn new(input: &'a str, cancel: Option<&'a citadel::CancelToken>) -> Result<Self> {
        check_cancel(cancel)?;
        Ok(Self {
            input,
            cursor: 0,
            cancel,
            work: 0,
            syntax_depth: 1,
            nodes: 0,
        })
    }

    fn tick(&mut self) -> Result<()> {
        check_cancel_at(self.cancel, self.work)?;
        self.work += 1;
        Ok(())
    }

    fn enter_syntax(&mut self) -> Result<()> {
        self.tick()?;
        if self.syntax_depth >= MAX_TSQUERY_DEPTH {
            return Err(tsquery_complexity_error());
        }
        self.syntax_depth += 1;
        Ok(())
    }

    fn finish_node(&mut self, node: TsQueryAst, depth: usize) -> Result<ParsedTsQuery> {
        self.tick()?;
        self.nodes += 1;
        if self.nodes > MAX_TSQUERY_NODES || depth > MAX_TSQUERY_DEPTH {
            return Err(tsquery_complexity_error());
        }
        Ok((node, depth))
    }

    fn skip_ws(&mut self) -> Result<()> {
        let bytes = self.input.as_bytes();
        while self.cursor < bytes.len() && bytes[self.cursor].is_ascii_whitespace() {
            self.tick()?;
            self.cursor += 1;
        }
        Ok(())
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

    fn parse_or(&mut self) -> Result<ParsedTsQuery> {
        let (mut left, mut left_depth) = self.parse_and()?;
        let mut terms = 1usize;
        loop {
            self.skip_ws()?;
            if !self.eat(b'|') {
                break;
            }
            let (right, right_depth) = self.parse_and()?;
            terms += 1;
            if terms > MAX_TSQUERY_DEPTH {
                return Err(tsquery_complexity_error());
            }
            (left, left_depth) = self.finish_node(
                TsQueryAst::Or(Box::new(left), Box::new(right)),
                left_depth.max(right_depth) + 1,
            )?;
        }
        Ok((left, left_depth))
    }

    fn parse_and(&mut self) -> Result<ParsedTsQuery> {
        let (mut left, mut left_depth) = self.parse_not()?;
        let mut terms = 1usize;
        loop {
            self.skip_ws()?;
            if !self.eat(b'&') {
                break;
            }
            let (right, right_depth) = self.parse_not()?;
            terms += 1;
            if terms > MAX_TSQUERY_DEPTH {
                return Err(tsquery_complexity_error());
            }
            (left, left_depth) = self.finish_node(
                TsQueryAst::And(Box::new(left), Box::new(right)),
                left_depth.max(right_depth) + 1,
            )?;
        }
        Ok((left, left_depth))
    }

    fn parse_not(&mut self) -> Result<ParsedTsQuery> {
        self.skip_ws()?;
        if self.eat(b'!') {
            self.enter_syntax()?;
            let parsed = self.parse_not();
            self.syntax_depth -= 1;
            let (inner, inner_depth) = parsed?;
            return self.finish_node(TsQueryAst::Not(Box::new(inner)), inner_depth + 1);
        }
        self.parse_phrase()
    }

    fn parse_phrase(&mut self) -> Result<ParsedTsQuery> {
        let (mut left, mut left_depth) = self.parse_atom()?;
        let mut terms = 1usize;
        loop {
            self.skip_ws()?;
            if self.peek() != Some(b'<') {
                break;
            }
            let dist = self.parse_phrase_distance()?;
            let (right, right_depth) = self.parse_atom()?;
            terms += 1;
            if terms > MAX_TSQUERY_DEPTH {
                return Err(tsquery_complexity_error());
            }
            (left, left_depth) = self.finish_node(
                TsQueryAst::Phrase {
                    distance: dist,
                    left: Box::new(left),
                    right: Box::new(right),
                },
                left_depth.max(right_depth) + 1,
            )?;
        }
        Ok((left, left_depth))
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
                self.tick()?;
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

    fn parse_atom(&mut self) -> Result<ParsedTsQuery> {
        self.skip_ws()?;
        if self.eat(b'(') {
            self.enter_syntax()?;
            let parsed = self.parse_or();
            self.syntax_depth -= 1;
            let inner = parsed?;
            self.skip_ws()?;
            if !self.eat(b')') {
                return Err(SqlError::InvalidValue("missing closing paren".into()));
            }
            return Ok(inner);
        }
        let lexeme = self.parse_lexeme_word()?;
        let (weight_mask, prefix) = self.parse_weight_and_prefix()?;
        self.finish_node(
            TsQueryAst::Lexeme {
                lexeme: lexeme.into_bytes(),
                weight_mask,
                prefix,
            },
            1,
        )
    }

    fn parse_lexeme_word(&mut self) -> Result<String> {
        self.skip_ws()?;
        if self.eat(b'\'') {
            let start = self.cursor;
            let bytes = self.input.as_bytes();
            while self.cursor < bytes.len() && bytes[self.cursor] != b'\'' {
                self.tick()?;
                self.cursor += 1;
            }
            if self.cursor >= bytes.len() {
                return Err(SqlError::InvalidValue(
                    "unterminated quoted lexeme in tsquery".into(),
                ));
            }
            let word = &self.input[start..self.cursor];
            validate_lexeme_length(word.as_bytes())?;
            let word = word.to_string();
            self.cursor += 1; // closing quote
            if word.is_empty() {
                return Err(SqlError::InvalidValue("empty lexeme in tsquery".into()));
            }
            return Ok(word);
        }
        let start = self.cursor;
        for (i, ch) in self.input[self.cursor..].char_indices() {
            if ch.is_alphanumeric() || ch == '_' {
                self.tick()?;
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
        let word = &self.input[start..self.cursor];
        validate_lexeme_length(word.as_bytes())?;
        Ok(word.to_string())
    }

    fn parse_weight_and_prefix(&mut self) -> Result<(u8, bool)> {
        if !self.eat(b':') {
            return Ok((0, false));
        }
        let mut prefix = false;
        let mut mask: u8 = 0;
        loop {
            self.tick()?;
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

pub(crate) fn op_match_with_cancel(
    tsvector_bytes: &[u8],
    tsquery_bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return op_match(tsvector_bytes, tsquery_bytes);
    }
    check_cancel(cancel)?;
    let ast = TsQueryAst::decode_with_cancel(tsquery_bytes, cancel)?;
    let (flags, reader) = TsVectorReader::open(tsvector_bytes)?;
    let mut entries = Vec::new();
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        let (lexeme, positions) = item?;
        entries.push((lexeme.to_vec(), positions));
    }
    let mut work = 0usize;
    let overflowed = flags & TSV_FLAG_POSITION_OVERFLOW != 0;
    let matched = eval_match_with_cancel(&ast, &entries, overflowed, cancel, &mut work)?;
    check_cancel(cancel)?;
    Ok(crate::types::Value::Boolean(matched))
}

fn eval_match(ast: &TsQueryAst, entries: &[(Vec<u8>, Vec<u16>)], overflowed: bool) -> Result<bool> {
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => lexeme_matches(entries, lexeme, *weight_mask, *prefix, None, &mut 0),
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

fn eval_match_with_cancel(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    overflowed: bool,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<bool> {
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => lexeme_matches(entries, lexeme, *weight_mask, *prefix, cancel, work),
        TsQueryAst::And(left, right) => {
            if !eval_match_with_cancel(left, entries, overflowed, cancel, work)? {
                return Ok(false);
            }
            eval_match_with_cancel(right, entries, overflowed, cancel, work)
        }
        TsQueryAst::Or(left, right) => {
            if eval_match_with_cancel(left, entries, overflowed, cancel, work)? {
                return Ok(true);
            }
            eval_match_with_cancel(right, entries, overflowed, cancel, work)
        }
        TsQueryAst::Not(child) => Ok(!eval_match_with_cancel(
            child, entries, overflowed, cancel, work,
        )?),
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
            let left_positions = phrase_positions_with_cancel(left, entries, cancel, work)?;
            let right_positions = phrase_positions_with_cancel(right, entries, cancel, work)?;
            positions_at_offset_with_cancel(
                &left_positions,
                &right_positions,
                *distance,
                cancel,
                work,
            )
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

fn phrase_positions_with_cancel(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<Vec<u16>> {
    check_cancel_at(cancel, *work)?;
    *work = work.wrapping_add(1);
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => {
            collect_lex_positions_with_cancel(entries, lexeme, *weight_mask, *prefix, cancel, work)
        }
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => {
            let left_positions = phrase_positions_with_cancel(left, entries, cancel, work)?;
            let right_positions = phrase_positions_with_cancel(right, entries, cancel, work)?;
            positions_pairing_right_with_cancel(
                &left_positions,
                &right_positions,
                *distance,
                cancel,
                work,
            )
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

fn positions_at_offset_with_cancel(
    left: &[u16],
    right: &[u16],
    distance: u16,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<bool> {
    if left.is_empty() || right.is_empty() {
        return Ok(false);
    }
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        check_cancel_at(cancel, *work)?;
        *work = work.wrapping_add(1);
        let left_position = left[left_index] & MAX_POSITION;
        let right_position = right[right_index] & MAX_POSITION;
        let target = left_position.saturating_add(distance);
        if right_position == target {
            return Ok(true);
        }
        if right_position < target {
            right_index += 1;
        } else {
            left_index += 1;
        }
    }
    Ok(false)
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

fn positions_pairing_right_with_cancel(
    left: &[u16],
    right: &[u16],
    distance: u16,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<Vec<u16>> {
    let mut out = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        check_cancel_at(cancel, *work)?;
        *work = work.wrapping_add(1);
        let left_position = left[left_index] & MAX_POSITION;
        let right_position = right[right_index] & MAX_POSITION;
        let target = left_position.saturating_add(distance);
        if right_position == target {
            if out.last().copied() != Some(right[right_index]) {
                out.push(right[right_index]);
            }
            right_index += 1;
        } else if right_position < target {
            right_index += 1;
        } else {
            left_index += 1;
        }
    }
    Ok(out)
}

fn position_has_weight(packed: u16, weight_mask: u8) -> bool {
    weight_mask == 0 || (1 << (packed >> 14)) & weight_mask != 0
}

fn lexeme_matches(
    entries: &[(Vec<u8>, Vec<u16>)],
    query_lex: &[u8],
    weight_mask: u8,
    prefix: bool,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<bool> {
    let candidates = if prefix {
        let start = entries.partition_point(|(lexeme, _)| lexeme.as_slice() < query_lex);
        &entries[start..]
    } else {
        match entries.binary_search_by(|(lexeme, _)| lexeme.as_slice().cmp(query_lex)) {
            Ok(index) => &entries[index..index + 1],
            Err(_) => return Ok(false),
        }
    };
    for (lexeme, positions) in candidates {
        check_cancel_at(cancel, *work)?;
        *work += 1;
        if prefix && !lexeme.starts_with(query_lex) {
            break;
        }
        // STRIP removes positions and weights, not lexeme membership.
        if weight_mask == 0 || positions.is_empty() {
            return Ok(true);
        }
        for &position in positions {
            check_cancel_at(cancel, *work)?;
            *work += 1;
            if position_has_weight(position, weight_mask) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn collect_lex_positions(
    entries: &[(Vec<u8>, Vec<u16>)],
    query_lex: &[u8],
    weight_mask: u8,
    prefix: bool,
) -> Vec<u16> {
    let mut out: Vec<u16> = Vec::new();
    let collect_from = |positions: &[u16], out: &mut Vec<u16>| {
        for &p in positions {
            if !position_has_weight(p, weight_mask) {
                continue;
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
    let mut count = 0i64;
    for item in reader {
        item?;
        count += 1;
    }
    Ok(crate::types::Value::Integer(count))
}

pub(crate) fn fn_length_tsvector_with_cancel(
    bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_length_tsvector(bytes);
    }
    check_cancel(cancel)?;
    let (_flags, reader) = TsVectorReader::open(bytes)?;
    let mut count = 0i64;
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        item?;
        count += 1;
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::Integer(count))
}

pub fn fn_numnode(bytes: &[u8]) -> Result<crate::types::Value> {
    let ast = TsQueryAst::decode(bytes)?;
    Ok(crate::types::Value::Integer(count_nodes(&ast) as i64))
}

pub(crate) fn fn_numnode_with_cancel(
    bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_numnode(bytes);
    }
    let ast = TsQueryAst::decode_with_cancel(bytes, cancel)?;
    let mut work = 0;
    Ok(crate::types::Value::Integer(
        count_nodes_with_cancel(&ast, cancel, &mut work)? as i64,
    ))
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

fn count_nodes_with_cancel(
    ast: &TsQueryAst,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<usize> {
    check_cancel_at(cancel, *work)?;
    *work += 1;
    Ok(match ast {
        TsQueryAst::Lexeme { .. } => 1,
        TsQueryAst::And(left, right) | TsQueryAst::Or(left, right) => {
            1 + count_nodes_with_cancel(left, cancel, work)?
                + count_nodes_with_cancel(right, cancel, work)?
        }
        TsQueryAst::Not(child) => 1 + count_nodes_with_cancel(child, cancel, work)?,
        TsQueryAst::Phrase { left, right, .. } => {
            1 + count_nodes_with_cancel(left, cancel, work)?
                + count_nodes_with_cancel(right, cancel, work)?
        }
    })
}

pub(crate) fn fn_ts_rank_with_cancel(
    tsv: &[u8],
    tsq: &[u8],
    norm: i64,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_ts_rank(tsv, tsq, norm);
    }
    let entries = decode_entries_with_cancel(tsv, cancel)?;
    let ast = TsQueryAst::decode_with_cancel(tsq, cancel)?;
    let mut score = 0.0_f64;
    let mut work = 0;
    accumulate_rank_with_cancel(&ast, &entries, &mut score, true, cancel, &mut work)?;
    score = apply_norm_with_cancel(score, &entries, norm, cancel)?;
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

pub(crate) fn fn_ts_rank_cd_with_cancel(
    tsv: &[u8],
    tsq: &[u8],
    norm: i64,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_ts_rank_cd(tsv, tsq, norm);
    }
    let entries = decode_entries_with_cancel(tsv, cancel)?;
    let ast = TsQueryAst::decode_with_cancel(tsq, cancel)?;
    let mut atom_lists = Vec::new();
    let mut work = 0;
    collect_positive_atoms_with_cancel(&ast, &entries, &mut atom_lists, cancel, &mut work)?;
    if atom_lists.is_empty() || atom_lists.iter().any(Vec::is_empty) {
        return Ok(crate::types::Value::Real(0.0));
    }
    let score = shortest_cover_score_with_cancel(&atom_lists, cancel)?;
    let score = apply_norm_with_cancel(score, &entries, norm, cancel)?;
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

fn accumulate_rank_with_cancel(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    out: &mut f64,
    positive: bool,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<()> {
    check_cancel_at(cancel, *work)?;
    *work += 1;
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } if positive => {
            let positions = collect_lex_positions_with_cancel(
                entries,
                lexeme,
                *weight_mask,
                *prefix,
                cancel,
                work,
            )?;
            if !positions.is_empty() {
                let mut weight_sum = 0.0;
                for packed in &positions {
                    check_cancel_at(cancel, *work)?;
                    *work += 1;
                    weight_sum += weight_default(Weight::from_bits(*packed));
                }
                *out += weight_sum * (1.0 + (positions.len() as f64).ln_1p());
            }
        }
        TsQueryAst::Lexeme { .. } => {}
        TsQueryAst::And(left, right) | TsQueryAst::Or(left, right) => {
            accumulate_rank_with_cancel(left, entries, out, positive, cancel, work)?;
            accumulate_rank_with_cancel(right, entries, out, positive, cancel, work)?;
        }
        TsQueryAst::Not(child) => {
            accumulate_rank_with_cancel(child, entries, out, !positive, cancel, work)?;
        }
        TsQueryAst::Phrase { left, right, .. } => {
            accumulate_rank_with_cancel(left, entries, out, positive, cancel, work)?;
            accumulate_rank_with_cancel(right, entries, out, positive, cancel, work)?;
        }
    }
    Ok(())
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

fn collect_positive_atoms_with_cancel(
    ast: &TsQueryAst,
    entries: &[(Vec<u8>, Vec<u16>)],
    out: &mut Vec<Vec<u16>>,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<()> {
    check_cancel_at(cancel, *work)?;
    *work += 1;
    match ast {
        TsQueryAst::Lexeme {
            lexeme,
            weight_mask,
            prefix,
        } => out.push(collect_lex_positions_with_cancel(
            entries,
            lexeme,
            *weight_mask,
            *prefix,
            cancel,
            work,
        )?),
        TsQueryAst::And(left, right)
        | TsQueryAst::Or(left, right)
        | TsQueryAst::Phrase { left, right, .. } => {
            collect_positive_atoms_with_cancel(left, entries, out, cancel, work)?;
            collect_positive_atoms_with_cancel(right, entries, out, cancel, work)?;
        }
        TsQueryAst::Not(_) => {}
    }
    Ok(())
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

fn shortest_cover_score_with_cancel(
    atom_lists: &[Vec<u16>],
    cancel: Option<&citadel::CancelToken>,
) -> Result<f64> {
    if atom_lists.is_empty() {
        return Ok(0.0);
    }
    let mut events = Vec::new();
    let mut work = 0;
    for (atom_index, list) in atom_lists.iter().enumerate() {
        for &packed in list {
            check_cancel_at(cancel, work)?;
            work += 1;
            let (position, _) = unpack_position(packed);
            events.push((position, atom_index, packed));
        }
    }
    events =
        crate::executor::helpers::sort_vec_by(events, cancel, |left, right| left.0.cmp(&right.0))?;

    let atom_count = atom_lists.len();
    let mut count_per_atom = vec![0usize; atom_count];
    let mut covered_count = 0usize;
    let mut best_score = 0.0_f64;
    let mut left = 0usize;
    for right in 0..events.len() {
        check_cancel_at(cancel, work)?;
        work += 1;
        let atom_index = events[right].1;
        if count_per_atom[atom_index] == 0 {
            covered_count += 1;
        }
        count_per_atom[atom_index] += 1;
        while covered_count == atom_count {
            let window_len = (events[right].0 - events[left].0 + 1) as f64;
            let mut weight_sum = 0.0;
            for event in &events[left..=right] {
                check_cancel_at(cancel, work)?;
                work += 1;
                weight_sum += weight_default(Weight::from_bits(event.2));
            }
            best_score = best_score.max(weight_sum / window_len);
            let left_atom = events[left].1;
            count_per_atom[left_atom] -= 1;
            if count_per_atom[left_atom] == 0 {
                covered_count -= 1;
            }
            left += 1;
        }
    }
    check_cancel(cancel)?;
    Ok(best_score)
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

fn apply_norm_with_cancel(
    mut score: f64,
    entries: &[(Vec<u8>, Vec<u16>)],
    norm: i64,
    cancel: Option<&citadel::CancelToken>,
) -> Result<f64> {
    let mut doc_len = 0usize;
    for (work, entry) in entries.iter().enumerate() {
        check_cancel_at(cancel, work)?;
        doc_len += entry.1.len();
    }
    let doc_len = doc_len as f64;
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
    check_cancel(cancel)?;
    Ok(score)
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

fn decode_entries_with_cancel(
    tsv: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<(Vec<u8>, Vec<u16>)>> {
    check_cancel(cancel)?;
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut out = Vec::new();
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        let (lexeme, positions) = item?;
        out.push((lexeme.to_vec(), positions));
    }
    check_cancel(cancel)?;
    Ok(out)
}

const CANCEL_CHECK_INTERVAL: usize = 256;

#[inline]
fn check_cancel(cancel: Option<&citadel::CancelToken>) -> Result<()> {
    match cancel {
        Some(token) => token.check().map_err(SqlError::Storage),
        None => Ok(()),
    }
}

#[inline]
fn check_cancel_at(cancel: Option<&citadel::CancelToken>, work: usize) -> Result<()> {
    let Some(token) = cancel else {
        return Ok(());
    };
    #[cfg(test)]
    cancellation_poll_work();
    if work.is_multiple_of(CANCEL_CHECK_INTERVAL) {
        token.check().map_err(SqlError::Storage)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static POLL_CANCEL_HOOK: std::cell::RefCell<Option<(citadel::CancelToken, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct PollCancelGuard;

#[cfg(test)]
impl Drop for PollCancelGuard {
    fn drop(&mut self) {
        POLL_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = None);
    }
}

#[cfg(test)]
fn cancel_on_poll_after(token: citadel::CancelToken, work: usize) -> PollCancelGuard {
    assert!(work > 0, "the hook must trip after FTS work starts");
    POLL_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = Some((token, work)));
    PollCancelGuard
}

#[cfg(test)]
fn cancellation_poll_work() {
    POLL_CANCEL_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        let Some((token, remaining)) = hook.as_mut() else {
            return;
        };
        *remaining -= 1;
        if *remaining == 0 {
            token.cancel();
            *hook = None;
        }
    });
}

#[cfg(test)]
thread_local! {
    static TOKENIZE_CANCEL_HOOK: std::cell::RefCell<Option<(citadel::CancelToken, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct TokenizeCancelGuard;

#[cfg(test)]
impl Drop for TokenizeCancelGuard {
    fn drop(&mut self) {
        TOKENIZE_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = None);
    }
}

#[cfg(test)]
pub(crate) fn cancel_tokenize_after(
    token: citadel::CancelToken,
    work: usize,
) -> TokenizeCancelGuard {
    assert!(work > 0, "the hook must trip after tokenization starts");
    TOKENIZE_CANCEL_HOOK.with(|hook| *hook.borrow_mut() = Some((token, work)));
    TokenizeCancelGuard
}

#[inline]
fn tokenization_work() {
    #[cfg(test)]
    TOKENIZE_CANCEL_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        let Some((token, remaining)) = hook.as_mut() else {
            return;
        };
        *remaining -= 1;
        if *remaining == 0 {
            token.cancel();
            *hook = None;
        }
    });
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

fn collect_lex_positions_with_cancel(
    entries: &[(Vec<u8>, Vec<u16>)],
    query_lex: &[u8],
    weight_mask: u8,
    prefix: bool,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<Vec<u16>> {
    let mut out = Vec::new();
    if prefix {
        let start = entries.partition_point(|(lexeme, _)| lexeme.as_slice() < query_lex);
        for (lexeme, positions) in &entries[start..] {
            check_cancel_at(cancel, *work)?;
            *work += 1;
            if !lexeme.starts_with(query_lex) {
                break;
            }
            collect_positions_with_cancel(positions, weight_mask, cancel, work, &mut out)?;
        }
        out = crate::executor::helpers::sort_vec_by(out, cancel, |left, right| left.cmp(right))?;
        out.dedup();
    } else if let Ok(index) =
        entries.binary_search_by(|(lexeme, _)| lexeme.as_slice().cmp(query_lex))
    {
        collect_positions_with_cancel(&entries[index].1, weight_mask, cancel, work, &mut out)?;
    }
    Ok(out)
}

fn collect_positions_with_cancel(
    positions: &[u16],
    weight_mask: u8,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
    out: &mut Vec<u16>,
) -> Result<()> {
    for &packed in positions {
        check_cancel_at(cancel, *work)?;
        *work += 1;
        if !position_has_weight(packed, weight_mask) {
            continue;
        }
        out.push(packed);
    }
    Ok(())
}

/// Tokenize while polling `cancel`. The no-token path is exactly [`tokenize`], so
/// ordinary indexing keeps the existing fast path.
pub(crate) fn tokenize_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<Vec<Token>> {
    use unicode_normalization::UnicodeNormalization;
    use unicode_segmentation::UnicodeSegmentation;

    if cancel.is_none() {
        let tokens = tokenize(kind, text);
        for token in &tokens {
            validate_lexeme_length(token.lexeme.as_bytes())?;
        }
        return Ok(tokens);
    }
    check_cancel(cancel)?;

    let mut normalized = String::with_capacity(text.len());
    for (work, ch) in text.nfkc().enumerate() {
        tokenization_work();
        check_cancel_at(cancel, work)?;
        normalized.push(ch);
    }
    check_cancel(cancel)?;
    // Keep whole-string lowercasing: unlike `char::to_lowercase`, this applies
    // context-sensitive mappings such as Greek final sigma.
    let lowered = normalized.to_lowercase();
    check_cancel(cancel)?;

    let mut out = Vec::new();
    let mut position: u32 = 0;
    for (work, word) in lowered.unicode_words().enumerate() {
        tokenization_work();
        check_cancel_at(cancel, work)?;
        position += 1;
        let pos_u16 = if position <= MAX_POSITION as u32 {
            position as u16
        } else {
            MAX_POSITION + 1
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
        validate_lexeme_length(lexeme.as_bytes())?;
        out.push(Token {
            lexeme,
            position: pos_u16,
            stopped,
        });
    }
    check_cancel(cancel)?;
    Ok(out)
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
    fn_to_tsvector_with_cancel(kind, text, None)
}

pub(crate) fn fn_to_tsvector_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    let tokens = tokenize_with_cancel(kind, text, cancel)?;
    let mut b = TsVectorBuilder::new();
    for (work, t) in tokens.into_iter().enumerate() {
        check_cancel_at(cancel, work)?;
        if t.stopped {
            continue;
        }
        b.push(t.lexeme.as_bytes(), t.position, Weight::D)?;
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsVector(b.build_with_cancel(cancel)?))
}

pub fn fn_to_tsvector(text: &str) -> Result<crate::types::Value> {
    fn_to_tsvector_with(TokenizerKind::English, text)
}

pub fn fn_to_tsquery_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let raw = parse_tsquery(text)?;
    let stemmed = stem_ast(&raw, kind)?;
    Ok(crate::types::Value::TsQuery(stemmed.encode()?))
}

pub(crate) fn fn_to_tsquery_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_to_tsquery_with(kind, text);
    }
    let raw = parse_tsquery_with_cancel(text, cancel)?;
    let mut work = 0;
    let stemmed = stem_ast_with_cancel(&raw, kind, cancel, &mut work)?;
    Ok(crate::types::Value::TsQuery(
        stemmed.encode_with_cancel(cancel)?,
    ))
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
            validate_lexeme_length(stemmed.as_bytes())?;
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

fn stem_ast_with_cancel(
    ast: &TsQueryAst,
    kind: TokenizerKind,
    cancel: Option<&citadel::CancelToken>,
    work: &mut usize,
) -> Result<TsQueryAst> {
    check_cancel_at(cancel, *work)?;
    *work += 1;
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
            validate_lexeme_length(stemmed.as_bytes())?;
            check_cancel(cancel)?;
            TsQueryAst::Lexeme {
                lexeme: stemmed.into_bytes(),
                weight_mask: *weight_mask,
                prefix: *prefix,
            }
        }
        TsQueryAst::And(l, r) => TsQueryAst::And(
            Box::new(stem_ast_with_cancel(l, kind, cancel, work)?),
            Box::new(stem_ast_with_cancel(r, kind, cancel, work)?),
        ),
        TsQueryAst::Or(l, r) => TsQueryAst::Or(
            Box::new(stem_ast_with_cancel(l, kind, cancel, work)?),
            Box::new(stem_ast_with_cancel(r, kind, cancel, work)?),
        ),
        TsQueryAst::Not(c) => {
            TsQueryAst::Not(Box::new(stem_ast_with_cancel(c, kind, cancel, work)?))
        }
        TsQueryAst::Phrase {
            distance,
            left,
            right,
        } => TsQueryAst::Phrase {
            distance: *distance,
            left: Box::new(stem_ast_with_cancel(left, kind, cancel, work)?),
            right: Box::new(stem_ast_with_cancel(right, kind, cancel, work)?),
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
    let ast = and_chain(lexemes)?;
    Ok(crate::types::Value::TsQuery(ast.encode()?))
}

pub(crate) fn fn_plainto_tsquery_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_plainto_tsquery_with(kind, text);
    }
    let tokens = tokenize_with_cancel(kind, text, cancel)?;
    let mut lexemes = Vec::new();
    for (work, token) in tokens.into_iter().enumerate() {
        check_cancel_at(cancel, work)?;
        if !token.stopped && !token.lexeme.is_empty() {
            lexemes.push(token.lexeme.into_bytes());
        }
    }
    check_cancel(cancel)?;
    let ast = and_chain(lexemes)?;
    Ok(crate::types::Value::TsQuery(
        ast.encode_with_cancel(cancel)?,
    ))
}

fn and_chain(lexemes: Vec<Vec<u8>>) -> Result<TsQueryAst> {
    if lexemes.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    let nodes = lexemes.into_iter().map(|lexeme| TsQueryAst::Lexeme {
        lexeme,
        weight_mask: 0,
        prefix: false,
    });
    fold_tsquery_nodes(nodes.collect(), TsQueryAst::And)?.ok_or_else(|| {
        SqlError::InvalidValue("tsquery would be empty (input had only stop-words?)".into())
    })
}

fn fold_tsquery_nodes(
    mut nodes: Vec<TsQueryAst>,
    join: fn(Box<TsQueryAst>, Box<TsQueryAst>) -> TsQueryAst,
) -> Result<Option<TsQueryAst>> {
    if nodes.is_empty() {
        return Ok(None);
    }
    while nodes.len() > 1 {
        let mut next_level = Vec::with_capacity(nodes.len().div_ceil(2));
        let mut iter = nodes.into_iter();
        while let Some(left) = iter.next() {
            match iter.next() {
                Some(right) => next_level.push(join(Box::new(left), Box::new(right))),
                None => next_level.push(left),
            }
        }
        nodes = next_level;
    }
    let result = nodes.pop().expect("non-empty query reduction");
    validate_tsquery(&result)?;
    Ok(Some(result))
}

fn check_left_deep_query_size(leaves: usize) -> Result<()> {
    let nodes = leaves.saturating_mul(2).saturating_sub(1);
    if leaves > MAX_TSQUERY_DEPTH || nodes > MAX_TSQUERY_NODES {
        return Err(tsquery_complexity_error());
    }
    Ok(())
}

pub fn fn_phraseto_tsquery_with(kind: TokenizerKind, text: &str) -> Result<crate::types::Value> {
    let tokens = tokenize(kind, text);
    let mut lex_positions: Vec<(Vec<u8>, u16)> = Vec::new();
    for t in tokens {
        if t.stopped || t.lexeme.is_empty() {
            continue;
        }
        validate_lexeme_length(t.lexeme.as_bytes())?;
        lex_positions.push((t.lexeme.into_bytes(), t.position));
    }
    if lex_positions.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    check_left_deep_query_size(lex_positions.len())?;
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
    Ok(crate::types::Value::TsQuery(acc.encode()?))
}

pub(crate) fn fn_phraseto_tsquery_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_phraseto_tsquery_with(kind, text);
    }
    let tokens = tokenize_with_cancel(kind, text, cancel)?;
    let mut lex_positions: Vec<(Vec<u8>, u16)> = Vec::new();
    for (work, token) in tokens.into_iter().enumerate() {
        check_cancel_at(cancel, work)?;
        if !token.stopped && !token.lexeme.is_empty() {
            lex_positions.push((token.lexeme.into_bytes(), token.position));
        }
    }
    if lex_positions.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    check_left_deep_query_size(lex_positions.len())?;
    let mut iter = lex_positions.into_iter();
    let (first_lex, mut prev_pos) = iter.next().unwrap();
    let mut acc = TsQueryAst::Lexeme {
        lexeme: first_lex,
        weight_mask: 0,
        prefix: false,
    };
    for (work, (lexeme, position)) in iter.enumerate() {
        check_cancel_at(cancel, work)?;
        let distance = position.saturating_sub(prev_pos).max(1);
        acc = TsQueryAst::Phrase {
            distance,
            left: Box::new(acc),
            right: Box::new(TsQueryAst::Lexeme {
                lexeme,
                weight_mask: 0,
                prefix: false,
            }),
        };
        prev_pos = position;
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsQuery(
        acc.encode_with_cancel(cancel)?,
    ))
}

pub fn fn_websearch_to_tsquery_with(
    kind: TokenizerKind,
    text: &str,
) -> Result<crate::types::Value> {
    fn_websearch_to_tsquery_with_cancel(kind, text, None)
}

pub(crate) fn fn_websearch_to_tsquery_with_cancel(
    kind: TokenizerKind,
    text: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    check_cancel(cancel)?;
    let mut groups: Vec<TsQueryAst> = Vec::new();
    let mut current_terms: Vec<TsQueryAst> = Vec::new();
    let mut cursor = 0usize;
    let bytes = text.as_bytes();

    let flush_group = |terms: &mut Vec<TsQueryAst>, groups: &mut Vec<TsQueryAst>| -> Result<()> {
        if let Some(combined) = fold_tsquery_nodes(std::mem::take(terms), TsQueryAst::And)? {
            groups.push(combined);
        }
        Ok(())
    };

    while cursor < bytes.len() {
        check_cancel_at(cancel, cursor)?;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            check_cancel_at(cancel, cursor)?;
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
                check_cancel_at(cancel, cursor)?;
            }
            let inner = &text[start..cursor];
            if cursor < bytes.len() {
                cursor += 1; // closing quote
            }
            match fn_phraseto_tsquery_with_cancel(kind, inner, cancel) {
                Ok(crate::types::Value::TsQuery(q)) => {
                    let mut ast = TsQueryAst::decode_with_cancel(&q, cancel)?;
                    if negate {
                        ast = TsQueryAst::Not(Box::new(ast));
                    }
                    validate_tsquery(&ast)?;
                    current_terms.push(ast);
                }
                Err(error @ SqlError::Storage(citadel_core::Error::Interrupted)) => {
                    return Err(error);
                }
                Ok(_) | Err(_) => {}
            }
            continue;
        }
        let start = cursor;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            check_cancel_at(cancel, cursor)?;
        }
        let word = &text[start..cursor];
        if word.eq_ignore_ascii_case("or") {
            flush_group(&mut current_terms, &mut groups)?;
            continue;
        }
        if let Some(stemmed) = stem_one(kind, word) {
            check_cancel(cancel)?;
            let mut ast = TsQueryAst::Lexeme {
                lexeme: stemmed.into_bytes(),
                weight_mask: 0,
                prefix: false,
            };
            if negate {
                ast = TsQueryAst::Not(Box::new(ast));
            }
            validate_tsquery(&ast)?;
            current_terms.push(ast);
        }
    }
    flush_group(&mut current_terms, &mut groups)?;
    if groups.is_empty() {
        return Err(SqlError::InvalidValue(
            "tsquery would be empty (input had only stop-words?)".into(),
        ));
    }
    let combined = fold_tsquery_nodes(groups, TsQueryAst::Or)?.expect("groups checked non-empty");
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsQuery(
        combined.encode_with_cancel(cancel)?,
    ))
}

pub fn fn_ts_headline_with(
    kind: TokenizerKind,
    text: &str,
    tsq_bytes: &[u8],
) -> Result<crate::types::Value> {
    fn_ts_headline_with_cancel(kind, text, tsq_bytes, None)
}

pub(crate) fn fn_ts_headline_with_cancel(
    kind: TokenizerKind,
    text: &str,
    tsq_bytes: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    use unicode_segmentation::UnicodeSegmentation;
    check_cancel(cancel)?;
    let ast = TsQueryAst::decode_with_cancel(tsq_bytes, cancel)?;
    let positive_lexemes = collect_query_atoms(&ast);
    let mut out = String::with_capacity(text.len() + 16);
    let mut last_end = 0usize;
    for (work, (idx, word)) in text.split_word_bound_indices().enumerate() {
        check_cancel_at(cancel, work)?;
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
    check_cancel(cancel)?;
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

pub(crate) fn fn_ts_lexize_with_cancel(
    kind: TokenizerKind,
    word: &str,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return fn_ts_lexize_with(kind, word);
    }
    check_cancel(cancel)?;
    let value = fn_ts_lexize_with(kind, word)?;
    check_cancel(cancel)?;
    Ok(value)
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
    fn_setweight_with_cancel(tsv, weight, None)
}

pub(crate) fn fn_setweight_with_cancel(
    tsv: &[u8],
    weight: Weight,
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    check_cancel(cancel)?;
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut b = TsVectorBuilder::new();
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        let (lex, positions) = item?;
        if positions.is_empty() {
            b.push_no_position(lex)?;
            continue;
        }
        for packed in positions {
            let pos = packed & MAX_POSITION;
            b.push(lex, pos, weight)?;
        }
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsVector(b.build_with_cancel(cancel)?))
}

/// Apply `weight` only to lexemes appearing in `filter`; leave others unchanged.
pub fn fn_setweight_selective(
    tsv: &[u8],
    weight: Weight,
    filter: &[crate::types::Value],
) -> Result<crate::types::Value> {
    fn_setweight_selective_with_cancel(tsv, weight, filter, None)
}

pub(crate) fn fn_setweight_selective_with_cancel(
    tsv: &[u8],
    weight: Weight,
    filter: &[crate::types::Value],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    check_cancel(cancel)?;
    let mut filter_set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for (work, v) in filter.iter().enumerate() {
        check_cancel_at(cancel, work)?;
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
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        let (lex, positions) = item?;
        let should_reweight = filter_set.contains(lex);
        if positions.is_empty() {
            b.push_no_position(lex)?;
            continue;
        }
        for packed in positions {
            let pos = packed & MAX_POSITION;
            if should_reweight {
                b.push(lex, pos, weight)?;
            } else {
                let (_p, w) = unpack_position(packed);
                b.push(lex, pos, w)?;
            }
        }
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsVector(b.build_with_cancel(cancel)?))
}

/// Strip positions and weights from a TSVECTOR, keeping only the distinct lexeme set.
pub fn fn_strip(tsv: &[u8]) -> Result<crate::types::Value> {
    fn_strip_with_cancel(tsv, None)
}

pub(crate) fn fn_strip_with_cancel(
    tsv: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    check_cancel(cancel)?;
    let (_flags, reader) = TsVectorReader::open(tsv)?;
    let mut b = TsVectorBuilder::new();
    for (work, item) in reader.enumerate() {
        check_cancel_at(cancel, work)?;
        let (lex, _positions) = item?;
        b.push_no_position(lex)?;
    }
    check_cancel(cancel)?;
    Ok(crate::types::Value::TsVector(b.build_with_cancel(cancel)?))
}

/// `tsvector || tsvector`: union the lexeme sets, merging positions per lexeme.
pub fn op_concat(a: &[u8], b: &[u8]) -> Result<crate::types::Value> {
    let (_, reader_a) = TsVectorReader::open(a)?;
    let (_, reader_b) = TsVectorReader::open(b)?;
    let mut builder = TsVectorBuilder::new();
    for item in reader_a {
        let (lex, positions) = item?;
        if positions.is_empty() {
            builder.push_no_position(lex)?;
            continue;
        }
        for packed in positions {
            let (pos, w) = unpack_position(packed);
            builder.push(lex, pos, w)?;
        }
    }
    for item in reader_b {
        let (lex, positions) = item?;
        if positions.is_empty() {
            builder.push_no_position(lex)?;
            continue;
        }
        for packed in positions {
            let (pos, w) = unpack_position(packed);
            builder.push(lex, pos, w)?;
        }
    }
    Ok(crate::types::Value::TsVector(builder.build()))
}

pub(crate) fn op_concat_with_cancel(
    left: &[u8],
    right: &[u8],
    cancel: Option<&citadel::CancelToken>,
) -> Result<crate::types::Value> {
    if cancel.is_none() {
        return op_concat(left, right);
    }
    check_cancel(cancel)?;
    let (_, left_reader) = TsVectorReader::open(left)?;
    let (_, right_reader) = TsVectorReader::open(right)?;
    let mut builder = TsVectorBuilder::new();
    let mut work = 0usize;
    for item in left_reader.chain(right_reader) {
        check_cancel_at(cancel, work)?;
        work = work.wrapping_add(1);
        let (lexeme, positions) = item?;
        if positions.is_empty() {
            builder.push_no_position(lexeme)?;
            continue;
        }
        for packed in positions {
            check_cancel_at(cancel, work)?;
            work = work.wrapping_add(1);
            let (position, weight) = unpack_position(packed);
            builder.push(lexeme, position, weight)?;
        }
    }
    Ok(crate::types::Value::TsVector(
        builder.build_with_cancel(cancel)?,
    ))
}

#[cfg(test)]
#[path = "fts_tests.rs"]
mod tests;
