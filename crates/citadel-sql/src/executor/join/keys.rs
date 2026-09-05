use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

use rustc_hash::{FxHashMap, FxHasher};

use super::{hash_join_value, join_key_hash, EquiJoin, JoinCancel};
use crate::error::Result;
use crate::types::{Collation, DataType, Value};

type Buckets = FxHashMap<u64, Vec<usize>>;

#[derive(Clone, Copy, Default)]
struct Families(u8);

impl Families {
    const DATE: u8 = 1;
    const TIME: u8 = 2;
    const TIMESTAMP: u8 = 4;
    const INTERVAL: u8 = 8;
    const TEXT: u8 = 16;
    const INTEGER: u8 = 32;
    const TEMPORAL: u8 = Self::DATE | Self::TIME | Self::TIMESTAMP | Self::INTERVAL;
    const CONVERTIBLE: u8 = Self::TEXT | Self::INTEGER;

    fn add(&mut self, value: &Value) {
        self.0 |= match value {
            Value::Date(_) => Self::DATE,
            Value::Time(_) => Self::TIME,
            Value::Timestamp(_) => Self::TIMESTAMP,
            Value::Interval { .. } => Self::INTERVAL,
            Value::Text(_) => Self::TEXT,
            Value::Integer(_) => Self::INTEGER,
            _ => 0,
        };
    }

    fn needs_coercion(self, outer: &Value) -> bool {
        let other = match outer {
            Value::Date(_) => Self::TIMESTAMP | Self::CONVERTIBLE,
            Value::Timestamp(_) => Self::DATE | Self::CONVERTIBLE,
            Value::Time(_) | Value::Interval { .. } => Self::CONVERTIBLE,
            Value::Text(_) | Value::Integer(_) => Self::TEMPORAL,
            _ => 0,
        };
        self.0 & other != 0
    }
}

pub(in crate::executor) struct ProbeTable {
    tuples: Buckets,
    families: Vec<Families>,
    coerced_tuples: OnceLock<Option<CoercedTupleIndex>>,
    comparison: OnceLock<ComparisonIndex>,
}

impl ProbeTable {
    pub(super) fn build(
        inner_rows: &[Vec<Value>],
        equi: &EquiJoin,
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Self> {
        let columns = equi.inner_cols();
        let mut tuples = Buckets::with_capacity_and_hasher(inner_rows.len(), Default::default());
        let mut families = vec![Families::default(); columns.len()];
        for (index, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if columns.iter().any(|&column| inner[column].is_null()) {
                continue;
            }
            tuples
                .entry(join_key_hash(inner, &columns, &equi.key_colls))
                .or_default()
                .push(index);
            for (family, &column) in families.iter_mut().zip(&columns) {
                family.add(&inner[column]);
            }
        }
        cancel.check()?;
        Ok(Self {
            tuples,
            families,
            coerced_tuples: OnceLock::new(),
            comparison: OnceLock::new(),
        })
    }

    #[inline]
    pub(super) fn cached_candidates(&self, outer: &[Value], equi: &EquiJoin) -> Option<&[usize]> {
        if let Some(Some(index)) = self.coerced_tuples.get() {
            if index.matches_signature(outer, equi) {
                return Some(index.lookup(outer, equi));
            }
        }
        let mut needs_comparison = false;
        for (family, pair) in self.families.iter().zip(&equi.pairs) {
            let value = &outer[pair.outer];
            if value.is_null() {
                return Some(&[]);
            }
            needs_comparison |= family.needs_coercion(value);
        }
        if needs_comparison {
            return None;
        }
        let mut hash = FxHasher::default();
        equi.len().hash(&mut hash);
        for (pair, &collation) in equi.pairs.iter().zip(&equi.key_colls) {
            hash_join_value(&outer[pair.outer], collation, &mut hash);
        }
        Some(self.tuples.get(&hash.finish()).map_or(&[], Vec::as_slice))
    }

    pub(super) fn comparison_candidates(
        &self,
        outer: &[Value],
        equi: &EquiJoin,
        inner_rows: &[Vec<Value>],
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Cow<'_, [usize]>> {
        let coerced = match self.coerced_tuples.get() {
            Some(index) => index,
            None => {
                let built = CoercedTupleIndex::build(inner_rows, outer, equi, cancel)?;
                cancel.check()?;
                self.coerced_tuples.get_or_init(|| built)
            }
        };
        if let Some(index) = coerced {
            if index.matches_signature(outer, equi) {
                return Ok(Cow::Borrowed(index.lookup(outer, equi)));
            }
        }
        let index = match self.comparison.get() {
            Some(index) => index,
            None => {
                let built = ComparisonIndex::build(inner_rows, equi, cancel)?;
                cancel.check()?;
                self.comparison.get_or_init(|| built)
            }
        };
        index.candidates(outer, equi, cancel)
    }

    #[cfg(test)]
    fn candidates(
        &self,
        outer: &[Value],
        equi: &EquiJoin,
        inner_rows: &[Vec<Value>],
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Cow<'_, [usize]>> {
        match self.cached_candidates(outer, equi) {
            Some(rows) => Ok(Cow::Borrowed(rows)),
            None => self.comparison_candidates(outer, equi, inner_rows, cancel),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct KeyCoercion {
    outer: Option<DataType>,
    inner: Option<DataType>,
}

impl KeyCoercion {
    fn for_types(outer: DataType, inner: DataType) -> Self {
        let temporal = |kind| {
            matches!(
                kind,
                DataType::Date | DataType::Time | DataType::Timestamp | DataType::Interval
            )
        };
        match (outer, inner) {
            (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date) => Self {
                outer: Some(DataType::Timestamp),
                inner: Some(DataType::Timestamp),
            },
            (target, DataType::Text | DataType::Integer) if temporal(target) => Self {
                outer: None,
                inner: Some(target),
            },
            (DataType::Text | DataType::Integer, target) if temporal(target) => Self {
                outer: Some(target),
                inner: None,
            },
            _ => Self::default(),
        }
    }
}

struct CoercedTupleIndex {
    outer_types: Vec<DataType>,
    coercions: Vec<KeyCoercion>,
    tuples: Buckets,
}

impl CoercedTupleIndex {
    fn build(
        inner_rows: &[Vec<Value>],
        outer: &[Value],
        equi: &EquiJoin,
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Option<Self>> {
        let mut inner_types: Option<Vec<DataType>> = None;
        for inner in inner_rows {
            cancel.work()?;
            if equi.pairs.iter().any(|pair| inner[pair.inner].is_null()) {
                continue;
            }
            if let Some(types) = &inner_types {
                if equi
                    .pairs
                    .iter()
                    .zip(types)
                    .any(|(pair, &kind)| inner[pair.inner].data_type() != kind)
                {
                    cancel.check()?;
                    return Ok(None);
                }
            } else {
                inner_types = Some(
                    equi.pairs
                        .iter()
                        .map(|pair| inner[pair.inner].data_type())
                        .collect(),
                );
            }
        }
        cancel.check()?;
        let Some(inner_types) = inner_types else {
            return Ok(None);
        };
        let outer_types: Vec<_> = equi
            .pairs
            .iter()
            .map(|pair| outer[pair.outer].data_type())
            .collect();
        let coercions = outer_types
            .iter()
            .zip(inner_types)
            .map(|(&outer, inner)| KeyCoercion::for_types(outer, inner))
            .collect();
        let mut index = Self {
            outer_types,
            coercions,
            tuples: Buckets::with_capacity_and_hasher(inner_rows.len(), Default::default()),
        };
        for (row, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if let Some(key) = index.hash_row(inner, equi, false) {
                index.tuples.entry(key).or_default().push(row);
            }
        }
        cancel.check()?;
        Ok(Some(index))
    }

    fn matches_signature(&self, outer: &[Value], equi: &EquiJoin) -> bool {
        self.outer_types
            .iter()
            .zip(&equi.pairs)
            .all(|(&kind, pair)| outer[pair.outer].data_type() == kind)
    }

    fn lookup(&self, outer: &[Value], equi: &EquiJoin) -> &[usize] {
        self.hash_row(outer, equi, true)
            .and_then(|key| self.tuples.get(&key))
            .map_or(&[], Vec::as_slice)
    }

    fn hash_row(&self, row: &[Value], equi: &EquiJoin, outer: bool) -> Option<u64> {
        let mut hash = FxHasher::default();
        equi.len().hash(&mut hash);
        for ((pair, coercion), &collation) in
            equi.pairs.iter().zip(&self.coercions).zip(&equi.key_colls)
        {
            let (column, target) = if outer {
                (pair.outer, coercion.outer)
            } else {
                (pair.inner, coercion.inner)
            };
            let value = &row[column];
            if value.is_null() {
                return None;
            }
            if let Some(target) = target {
                let converted = value.clone().coerce_into(target)?;
                hash_join_value(&converted, Collation::Binary, &mut hash);
            } else {
                hash_join_value(value, collation, &mut hash);
            }
        }
        Some(hash.finish())
    }
}

#[derive(Clone, Copy)]
enum Domain {
    Date,
    Time,
    Timestamp,
    Interval,
}

impl Domain {
    const ALL: [Self; 4] = [Self::Date, Self::Time, Self::Timestamp, Self::Interval];

    fn data_type(self) -> DataType {
        match self {
            Self::Date => DataType::Date,
            Self::Time => DataType::Time,
            Self::Timestamp => DataType::Timestamp,
            Self::Interval => DataType::Interval,
        }
    }

    fn of(value: &Value) -> Option<Self> {
        match value {
            Value::Date(_) => Some(Self::Date),
            Value::Time(_) => Some(Self::Time),
            Value::Timestamp(_) => Some(Self::Timestamp),
            Value::Interval { .. } => Some(Self::Interval),
            _ => None,
        }
    }
}

fn scalar_hash(value: &Value, collation: Collation) -> u64 {
    join_key_hash(std::slice::from_ref(value), &[0], &[collation])
}

#[derive(Default)]
struct DomainIndex {
    actual: Buckets,
    converted: Buckets,
}

#[derive(Default)]
struct ColumnIndex {
    native: Buckets,
    domains: [DomainIndex; 4],
}

impl ColumnIndex {
    fn insert(&mut self, value: &Value, collation: Collation, row: usize) {
        self.native
            .entry(scalar_hash(value, collation))
            .or_default()
            .push(row);
        if let Some(domain) = Domain::of(value) {
            self.domains[domain as usize]
                .actual
                .entry(scalar_hash(value, Collation::Binary))
                .or_default()
                .push(row);
            if let Value::Date(_) = value {
                if let Some(timestamp) = value.clone().coerce_into(DataType::Timestamp) {
                    self.domains[Domain::Timestamp as usize]
                        .actual
                        .entry(scalar_hash(&timestamp, Collation::Binary))
                        .or_default()
                        .push(row);
                }
            }
        } else if matches!(value, Value::Text(_) | Value::Integer(_)) {
            for domain in Domain::ALL {
                if let Some(converted) = value.clone().coerce_into(domain.data_type()) {
                    self.domains[domain as usize]
                        .converted
                        .entry(scalar_hash(&converted, Collation::Binary))
                        .or_default()
                        .push(row);
                }
            }
        }
    }

    fn lists(&self, outer: &Value, collation: Collation) -> CandidateLists<'_> {
        let mut lists = CandidateLists::default();
        lists.push(self.native.get(&scalar_hash(outer, collation)));
        if let Some(domain) = Domain::of(outer) {
            let key = scalar_hash(outer, Collation::Binary);
            lists.push(self.domains[domain as usize].converted.get(&key));
            if matches!(outer, Value::Date(_) | Value::Timestamp(_)) {
                if let Some(timestamp) = outer.clone().coerce_into(DataType::Timestamp) {
                    lists.push(
                        self.domains[Domain::Timestamp as usize]
                            .actual
                            .get(&scalar_hash(&timestamp, Collation::Binary)),
                    );
                }
            }
        } else if matches!(outer, Value::Text(_) | Value::Integer(_)) {
            for domain in Domain::ALL {
                if let Some(converted) = outer.clone().coerce_into(domain.data_type()) {
                    // Converted values only probe actual temporals, never other conversions.
                    lists.push(
                        self.domains[domain as usize]
                            .actual
                            .get(&scalar_hash(&converted, Collation::Binary)),
                    );
                }
            }
        }
        lists
    }
}

struct ComparisonIndex {
    columns: Vec<ColumnIndex>,
    pair_columns: Vec<usize>,
}

impl ComparisonIndex {
    fn build(
        inner_rows: &[Vec<Value>],
        equi: &EquiJoin,
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Self> {
        let mut distinct = FxHashMap::default();
        let mut definitions = Vec::new();
        let mut pair_columns = Vec::with_capacity(equi.len());
        for (pair, &collation) in equi.pairs.iter().zip(&equi.key_colls) {
            cancel.work()?;
            let definition = (pair.inner, collation);
            let next = definitions.len();
            let column = *distinct.entry(definition).or_insert_with(|| {
                definitions.push(definition);
                next
            });
            pair_columns.push(column);
        }
        let mut columns: Vec<_> = definitions.iter().map(|_| ColumnIndex::default()).collect();
        for (row, inner) in inner_rows.iter().enumerate() {
            cancel.work()?;
            if equi.pairs.iter().any(|pair| inner[pair.inner].is_null()) {
                continue;
            }
            for (index, &(column, collation)) in columns.iter_mut().zip(&definitions) {
                cancel.work()?;
                index.insert(&inner[column], collation, row);
            }
        }
        cancel.check()?;
        Ok(Self {
            columns,
            pair_columns,
        })
    }

    fn candidates(
        &self,
        outer: &[Value],
        equi: &EquiJoin,
        cancel: &mut JoinCancel<'_>,
    ) -> Result<Cow<'_, [usize]>> {
        let mut smallest = CandidateLists::default();
        let mut shortest = usize::MAX;
        for ((&column, pair), &collation) in self
            .pair_columns
            .iter()
            .zip(&equi.pairs)
            .zip(&equi.key_colls)
        {
            cancel.work()?;
            let lists = self.columns[column].lists(&outer[pair.outer], collation);
            let count = lists.total_len();
            if count < shortest {
                smallest = lists;
                shortest = count;
            }
            if count == 0 {
                break;
            }
        }
        smallest.merge(cancel)
    }
}

#[derive(Default)]
struct CandidateLists<'a> {
    lists: [&'a [usize]; 5],
    count: usize,
}

impl<'a> CandidateLists<'a> {
    fn push(&mut self, values: Option<&'a Vec<usize>>) {
        if let Some(values) = values {
            self.lists[self.count] = values;
            self.count += 1;
        }
    }

    fn total_len(&self) -> usize {
        self.lists[..self.count]
            .iter()
            .fold(0usize, |sum, values| sum.saturating_add(values.len()))
    }

    fn merge(mut self, cancel: &mut JoinCancel<'_>) -> Result<Cow<'a, [usize]>> {
        if self.count <= 1 {
            return Ok(Cow::Borrowed(self.lists[0]));
        }
        let mut merged = Vec::new();
        loop {
            cancel.work()?;
            let Some(next) = self.lists[..self.count]
                .iter()
                .filter_map(|values| values.first())
                .min()
                .copied()
            else {
                break;
            };
            merged.push(next);
            for list in &mut self.lists[..self.count] {
                if list.first() == Some(&next) {
                    *list = &list[1..];
                }
            }
        }
        cancel.check()?;
        Ok(Cow::Owned(merged))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::join::KeyPair;

    fn keys(collations: &[Collation]) -> EquiJoin {
        EquiJoin {
            pairs: (0..collations.len())
                .map(|column| KeyPair {
                    outer: column,
                    inner: column,
                    left_is_outer: true,
                })
                .collect(),
            pure: true,
            key_colls: collations.to_vec(),
        }
    }

    fn interval(months: i32, days: i32, micros: i64) -> Value {
        Value::Interval {
            months,
            days,
            micros,
        }
    }

    #[test]
    fn ordinary_lookup_borrows_tuple_bucket_without_comparison_index() {
        for value in [
            Value::Integer(1),
            Value::Real(1.0),
            Value::Text("one".into()),
            Value::Date(1),
            Value::Time(1),
            Value::Timestamp(1),
            interval(1, 0, 0),
        ] {
            let equi = keys(&[Collation::Binary]);
            let inner = vec![vec![value.clone()]];
            let mut cancel = JoinCancel::new(None).unwrap();
            let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
            let found = probe
                .candidates(&[value], &equi, &inner, &mut cancel)
                .unwrap();
            assert!(matches!(found, Cow::Borrowed(_)));
            assert_eq!(found.as_ref(), &[0]);
            assert!(probe.comparison.get().is_none());
            assert!(probe.coerced_tuples.get().is_none());
        }
    }

    #[test]
    fn ordinary_lookup_preserves_key_order_and_collations() {
        let mut equi = keys(&[Collation::NoCase, Collation::Rtrim]);
        equi.pairs[0].outer = 1;
        equi.pairs[1].outer = 0;
        let inner = vec![vec![Value::Text("A".into()), Value::Text("b ".into())]];
        let probe = ProbeTable::build(&inner, &equi, &mut JoinCancel::new(None).unwrap()).unwrap();
        assert_eq!(
            probe.cached_candidates(&[Value::Text("b".into()), Value::Text("a".into())], &equi),
            Some([0usize].as_slice())
        );
        assert_eq!(
            probe.cached_candidates(&[Value::Text("c".into()), Value::Text("a".into())], &equi),
            Some([].as_slice())
        );
        assert!(probe.coerced_tuples.get().is_none());
        assert!(probe.comparison.get().is_none());
    }

    #[test]
    fn null_after_coercing_key_does_not_initialize_comparison_caches() {
        let equi = keys(&[Collation::Binary; 2]);
        let inner = vec![vec![Value::Text("1970-01-01".into()), Value::Integer(1)]];
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        let outer = [Value::Timestamp(0), Value::Null];
        assert_eq!(probe.cached_candidates(&outer, &equi), Some([].as_slice()));
        assert!(probe
            .candidates(&outer, &equi, &inner, &mut cancel)
            .unwrap()
            .is_empty());
        assert!(probe.coerced_tuples.get().is_none());
        assert!(probe.comparison.get().is_none());
    }

    #[test]
    fn cached_coerced_lookup_preserves_nulls_and_conversion_failures() {
        let equi = keys(&[Collation::Binary]);
        let inner = vec![vec![Value::Timestamp(0)]];
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        assert!(probe.cached_candidates(&[Value::Date(0)], &equi).is_none());
        assert_eq!(
            probe
                .candidates(&[Value::Date(0)], &equi, &inner, &mut cancel)
                .unwrap()
                .as_ref(),
            &[0]
        );
        let index = probe.coerced_tuples.get().unwrap().as_ref().unwrap();
        for (value, expected) in [
            (Value::Date(0), &[0usize][..]),
            (Value::Date(i32::MAX), &[][..]),
            (Value::Null, &[][..]),
            (Value::Timestamp(0), &[0usize][..]),
        ] {
            assert_eq!(probe.cached_candidates(&[value], &equi), Some(expected));
            assert!(std::ptr::eq(
                index,
                probe.coerced_tuples.get().unwrap().as_ref().unwrap()
            ));
            assert!(probe.comparison.get().is_none());
        }
    }

    fn temporal_values() -> Vec<Value> {
        vec![
            Value::Date(i32::MIN),
            Value::Date(0),
            Value::Date(1),
            Value::Date(i32::MAX),
            Value::Time(0),
            Value::Time(-1),
            Value::Time(86_400_000_000),
            Value::Timestamp(i64::MIN),
            Value::Timestamp(0),
            Value::Timestamp(1_000_000),
            Value::Timestamp(86_400_000_000),
            Value::Timestamp(i64::MAX),
            interval(1, 0, 0),
            interval(0, 30, 0),
            interval(0, 0, 0),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(i64::MAX),
            Value::Integer(1 << 53),
            Value::Integer((1 << 53) + 1),
            Value::Real(0.0),
            Value::Real((1u64 << 53) as f64),
            Value::Text("1970-01-01".into()),
            Value::Text("1970-01-01 00:00:00".into()),
            Value::Text("00:00:00".into()),
            Value::Text("30 days".into()),
            Value::Text("1 month".into()),
            Value::Text("not a date".into()),
            Value::Null,
        ]
    }

    #[test]
    fn temporal_candidates_cover_scalar_equality_without_duplicate_rows() {
        let values = temporal_values();
        let inner: Vec<_> = values.iter().cloned().map(|value| vec![value]).collect();
        for collation in [Collation::Binary, Collation::NoCase, Collation::Rtrim] {
            let equi = keys(&[collation]);
            let mut cancel = JoinCancel::new(None).unwrap();
            let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
            for value in &values {
                let outer = std::slice::from_ref(value);
                let candidates = probe.candidates(outer, &equi, &inner, &mut cancel).unwrap();
                assert!(candidates.windows(2).all(|pair| pair[0] < pair[1]));
                let actual: Vec<_> = candidates
                    .iter()
                    .copied()
                    .filter(|&row| equi.keys_match(outer, &inner[row]).unwrap())
                    .collect();
                let expected: Vec<_> = inner
                    .iter()
                    .enumerate()
                    .filter_map(|(row, inner)| {
                        equi.keys_match(outer, inner).unwrap().then_some(row)
                    })
                    .collect();
                assert_eq!(actual, expected, "{value:?}, {collation:?}");
            }
        }
    }

    #[test]
    fn converted_text_does_not_probe_other_converted_text() {
        let mut index = ColumnIndex::default();
        let date = Value::Text("1970-01-01".into());
        let timestamp = Value::Text("1970-01-01 00:00:00".into());
        index.insert(&date, Collation::Binary, 0);
        index.insert(&timestamp, Collation::Binary, 1);
        let found = index
            .lists(&date, Collation::Binary)
            .merge(&mut JoinCancel::new(None).unwrap())
            .unwrap();
        assert_eq!(found.as_ref(), &[0]);
    }

    #[test]
    fn composite_comparison_index_is_linear_and_selects_short_postings() {
        const ROWS: usize = 128;
        const KEYS: usize = 12;
        let inner: Vec<_> = (0..ROWS)
            .map(|row| {
                let mut values = vec![Value::Text("1970-01-01".into()); KEYS];
                values[KEYS - 1] = Value::Integer(row as i64);
                values
            })
            .collect();
        let equi = keys(&[Collation::Binary; KEYS]);
        let mut outer = vec![Value::Date(0); KEYS];
        outer[KEYS - 1] = Value::Integer(42);
        let mut cancel = JoinCancel::new(None).unwrap();
        let comparison = ComparisonIndex::build(&inner, &equi, &mut cancel).unwrap();
        let found = comparison.candidates(&outer, &equi, &mut cancel).unwrap();
        assert_eq!(found.as_ref(), &[42]);
        assert!(matches!(found, Cow::Borrowed(_)));
        assert_eq!(comparison.columns.len(), KEYS);
        assert_eq!(
            comparison.columns[0]
                .lists(&outer[0], Collation::Binary)
                .total_len(),
            ROWS
        );
        let entries: usize = comparison
            .columns
            .iter()
            .map(|column| {
                column.native.values().map(Vec::len).sum::<usize>()
                    + column
                        .domains
                        .iter()
                        .map(|domain| {
                            domain.actual.values().map(Vec::len).sum::<usize>()
                                + domain.converted.values().map(Vec::len).sum::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum();
        assert!(entries <= 5 * KEYS * ROWS);
    }

    #[test]
    fn temporal_parsing_preserves_original_text_under_collation() {
        let text = Value::Text("2024-01-15 12:00:00 America/New_York".into());
        let timestamp = text.clone().coerce_into(DataType::Timestamp).unwrap();
        for collation in [Collation::NoCase, Collation::Rtrim] {
            for (left, right) in [(&text, &timestamp), (&timestamp, &text)] {
                let equi = keys(&[collation]);
                let inner = vec![vec![right.clone()]];
                let mut cancel = JoinCancel::new(None).unwrap();
                let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
                let found = probe
                    .candidates(std::slice::from_ref(left), &equi, &inner, &mut cancel)
                    .unwrap();
                assert_eq!(found.as_ref(), &[0]);
                assert!(equi
                    .keys_match(std::slice::from_ref(left), &inner[0])
                    .unwrap());
            }
        }
    }

    #[test]
    fn repeated_predicates_share_columns_by_inner_position_and_collation() {
        let collations: Vec<_> = (0..12)
            .map(|position| [Collation::Binary, Collation::NoCase, Collation::Rtrim][position % 3])
            .collect();
        let mut equi = keys(&collations);
        for pair in &mut equi.pairs {
            pair.outer = 0;
            pair.inner = 0;
        }
        let inner = vec![vec![Value::Text("1970-01-01".into())]; 32];
        let mut cancel = JoinCancel::new(None).unwrap();
        let comparison = ComparisonIndex::build(&inner, &equi, &mut cancel).unwrap();
        let found = comparison
            .candidates(&[Value::Date(0)], &equi, &mut cancel)
            .unwrap();
        assert_eq!(found.as_ref(), &(0..32).collect::<Vec<_>>());
        assert_eq!(comparison.columns.len(), 3);
        assert_eq!(comparison.pair_columns, [0, 1, 2].repeat(4));
    }

    #[test]
    fn cancelled_comparison_build_does_not_install_partial_index() {
        for count in [1, 512] {
            let equi = keys(&[Collation::Binary]);
            let mut inner = vec![vec![Value::Integer(0)]; count];
            inner.push(vec![Value::Real(0.0)]);
            let probe =
                ProbeTable::build(&inner, &equi, &mut JoinCancel::new(None).unwrap()).unwrap();
            let homogeneous = CoercedTupleIndex::build(
                &inner,
                &[Value::Date(0)],
                &equi,
                &mut JoinCancel::new(None).unwrap(),
            )
            .unwrap();
            assert!(homogeneous.is_none());
            assert!(probe.coerced_tuples.set(homogeneous).is_ok());
            let token = citadel::CancelToken::new();
            let mut cancel = JoinCancel::new(Some(&token)).unwrap();
            token.cancel();
            assert!(probe
                .candidates(&[Value::Date(0)], &equi, &inner, &mut cancel)
                .is_err());
            assert!(probe.comparison.get().is_none());
            let mut retry = JoinCancel::new(None).unwrap();
            let first = probe
                .candidates(&[Value::Date(0)], &equi, &inner, &mut retry)
                .unwrap();
            assert_eq!(first.len(), count);
            let installed = probe.comparison.get().unwrap();
            let second = probe
                .candidates(&[Value::Timestamp(0)], &equi, &inner, &mut retry)
                .unwrap();
            assert_eq!(first, second);
            assert!(std::ptr::eq(installed, probe.comparison.get().unwrap()));
        }
    }

    #[test]
    fn coerced_tuple_index_rejects_anti_correlated_keys_without_candidates() {
        let inner: Vec<_> = (0..128)
            .map(|row| {
                vec![
                    Value::Text(if row < 64 {
                        "1970-01-01 00:00:00".into()
                    } else {
                        "1970-01-02 00:00:00".into()
                    }),
                    Value::Integer(i64::from(row >= 64)),
                ]
            })
            .collect();
        let equi = keys(&[Collation::Binary; 2]);
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        let outer = [Value::Timestamp(0), Value::Integer(1)];
        let candidates = probe
            .candidates(&outer, &equi, &inner, &mut cancel)
            .unwrap();
        assert!(candidates.is_empty());
        assert!(matches!(candidates, Cow::Borrowed(_)));
        assert!(probe.coerced_tuples.get().unwrap().is_some());
        assert!(probe.comparison.get().is_none());
        let index = probe.coerced_tuples.get().unwrap().as_ref().unwrap();
        assert_eq!(index.tuples.values().map(Vec::len).sum::<usize>(), 128);
    }

    #[test]
    fn homogeneous_coercion_hashes_cover_all_scalar_type_pairs() {
        let values = temporal_values();
        let mut kinds = Vec::new();
        for value in &values {
            if !kinds.contains(&value.data_type()) {
                kinds.push(value.data_type());
            }
        }
        for kind in kinds {
            let inner: Vec<_> = values
                .iter()
                .filter(|value| value.data_type() == kind || value.is_null())
                .cloned()
                .map(|value| vec![value])
                .collect();
            for collation in [Collation::Binary, Collation::NoCase, Collation::Rtrim] {
                let equi = keys(&[collation]);
                for value in &values {
                    let outer = std::slice::from_ref(value);
                    let mut cancel = JoinCancel::new(None).unwrap();
                    let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
                    let candidates = probe.candidates(outer, &equi, &inner, &mut cancel).unwrap();
                    let actual: Vec<_> = candidates
                        .iter()
                        .copied()
                        .filter(|&row| equi.keys_match(outer, &inner[row]).unwrap())
                        .collect();
                    let expected: Vec<_> = inner
                        .iter()
                        .enumerate()
                        .filter_map(|(row, inner)| {
                            equi.keys_match(outer, inner).unwrap().then_some(row)
                        })
                        .collect();
                    assert_eq!(actual, expected, "inner {kind:?}, outer {value:?}");
                    assert!(probe.comparison.get().is_none());
                }
            }
        }
    }

    #[test]
    fn real_inner_values_disqualify_integer_homogeneity() {
        let equi = keys(&[Collation::Binary]);
        let inner = vec![vec![Value::Integer(0)], vec![Value::Real(0.0)]];
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        let candidates = probe
            .candidates(&[Value::Date(0)], &equi, &inner, &mut cancel)
            .unwrap();
        assert_eq!(candidates.as_ref(), &[0]);
        assert!(probe.coerced_tuples.get().unwrap().is_none());
        assert!(probe.comparison.get().is_some());
    }

    #[test]
    fn changed_outer_signature_falls_back_without_replacing_coerced_cache() {
        let inner = vec![
            vec![Value::Text("1970-01-01".into())],
            vec![Value::Text("1970-01-01 00:00:00".into())],
        ];
        let equi = keys(&[Collation::Binary]);
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        for (outer, expected) in [
            (Value::Timestamp(0), vec![0, 1]),
            (Value::Date(0), vec![0]),
            (Value::Timestamp(0), vec![0, 1]),
        ] {
            let candidates = probe
                .candidates(&[outer], &equi, &inner, &mut cancel)
                .unwrap();
            assert_eq!(candidates.as_ref(), &expected);
            assert_eq!(
                probe
                    .coerced_tuples
                    .get()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .outer_types,
                [DataType::Timestamp]
            );
        }
        assert!(probe.comparison.get().is_some());
    }

    #[test]
    fn mixed_numeric_keys_stay_candidate_hashes_in_coerced_tuples() {
        let first = 1i64 << 53;
        let inner = vec![
            vec![Value::Text("1970-01-01".into()), Value::Integer(first)],
            vec![Value::Text("1970-01-01".into()), Value::Integer(first + 1)],
        ];
        let equi = keys(&[Collation::Binary; 2]);
        let mut cancel = JoinCancel::new(None).unwrap();
        let probe = ProbeTable::build(&inner, &equi, &mut cancel).unwrap();
        let outer = [Value::Date(0), Value::Real(first as f64)];
        let candidates = probe
            .candidates(&outer, &equi, &inner, &mut cancel)
            .unwrap();
        assert_eq!(candidates.as_ref(), &[0, 1]);
        assert!(candidates
            .iter()
            .all(|&row| equi.keys_match(&outer, &inner[row]).unwrap()));
        assert!(probe.coerced_tuples.get().unwrap().is_some());
    }

    #[test]
    fn cancelled_coerced_build_does_not_install_and_can_retry() {
        for count in [1, 512] {
            let inner = vec![vec![Value::Text("1970-01-01".into())]; count];
            let equi = keys(&[Collation::Binary]);
            let probe =
                ProbeTable::build(&inner, &equi, &mut JoinCancel::new(None).unwrap()).unwrap();
            let token = citadel::CancelToken::new();
            let mut cancel = JoinCancel::new(Some(&token)).unwrap();
            token.cancel();
            assert!(probe
                .candidates(&[Value::Date(0)], &equi, &inner, &mut cancel)
                .is_err());
            assert!(probe.coerced_tuples.get().is_none());
            assert!(probe.comparison.get().is_none());
            let candidates = probe
                .candidates(
                    &[Value::Date(0)],
                    &equi,
                    &inner,
                    &mut JoinCancel::new(None).unwrap(),
                )
                .unwrap();
            assert_eq!(candidates.len(), count);
            assert!(probe.coerced_tuples.get().unwrap().is_some());
            assert!(probe.comparison.get().is_none());
        }
    }
}
