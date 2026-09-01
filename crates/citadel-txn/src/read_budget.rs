//! Shared admission budget for values materialized by a read operation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use citadel_core::{Error, Result};

#[derive(Debug)]
struct ReadBudgetInner {
    remaining: AtomicUsize,
    max_value: usize,
}

/// A cloneable byte budget shared by every transaction participating in one
/// logical read.
///
/// The per-value limit is checked before an overflow buffer is allocated. The
/// total is shared atomically so parallel scan shards cannot each spend the
/// full allowance.
#[derive(Clone, Debug)]
pub struct ReadBudget {
    inner: Arc<ReadBudgetInner>,
}

impl ReadBudget {
    pub fn new(max_value: usize, total: usize) -> Self {
        Self {
            inner: Arc::new(ReadBudgetInner {
                remaining: AtomicUsize::new(total),
                max_value,
            }),
        }
    }

    pub fn max_value(&self) -> usize {
        self.inner.max_value
    }

    pub fn remaining(&self) -> usize {
        self.inner.remaining.load(Ordering::Relaxed)
    }

    /// Admit one materialized value into this budget.
    ///
    /// Higher layers use this for cached or decrypted content that does not
    /// pass through a transaction read. Failed admission does not consume any
    /// of the remaining total.
    pub fn try_charge(&self, size: usize) -> Result<()> {
        let remaining = self.remaining();
        if size > self.inner.max_value {
            return Err(Error::ReadBudgetExceeded {
                size,
                max_value: self.inner.max_value,
                remaining,
            });
        }
        self.try_charge_aggregate(size)
    }

    fn try_charge_aggregate(&self, size: usize) -> Result<()> {
        match self
            .inner
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(size)
            }) {
            Ok(_) => Ok(()),
            Err(remaining) => Err(Error::ReadBudgetExceeded {
                size,
                max_value: self.inner.max_value,
                remaining,
            }),
        }
    }

    /// Record bytes already materialized by a child reader.
    ///
    /// Unlike admission, an over-budget record drains the remaining allowance:
    /// the allocation has already happened and must not become spendable again.
    #[doc(hidden)]
    pub fn record_aggregate_spend(&self, size: usize) -> Result<()> {
        let remaining = self
            .inner
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                Some(remaining.saturating_sub(size))
            })
            .expect("aggregate-spend update always supplies a value");
        if size <= remaining {
            Ok(())
        } else {
            Err(Error::ReadBudgetExceeded {
                size,
                max_value: self.inner.max_value,
                remaining,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_one_atomic_total() {
        let budget = ReadBudget::new(8, 8);
        let left = budget.clone();
        let right = budget.clone();
        let threads = [left, right].map(|budget| std::thread::spawn(move || budget.try_charge(5)));
        let accepted = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(Result::is_ok)
            .count();

        assert_eq!(accepted, 1);
        assert_eq!(budget.remaining(), 3);
    }

    #[test]
    fn refused_admission_does_not_consume_the_shared_total() {
        let budget = ReadBudget::new(4, 8);

        assert!(matches!(
            budget.try_charge(5),
            Err(Error::ReadBudgetExceeded {
                size: 5,
                max_value: 4,
                remaining: 8,
            })
        ));
        assert_eq!(budget.remaining(), 8);

        budget.try_charge(4).unwrap();
        assert!(matches!(
            budget.try_charge(5),
            Err(Error::ReadBudgetExceeded { remaining: 4, .. })
        ));
        assert_eq!(budget.remaining(), 4);
    }

    #[test]
    fn aggregate_charge_enforces_only_the_shared_total() {
        let budget = ReadBudget::new(4, 10);
        budget.try_charge_aggregate(7).unwrap();
        assert_eq!(budget.remaining(), 3);
        assert!(matches!(
            budget.try_charge_aggregate(4),
            Err(Error::ReadBudgetExceeded {
                size: 4,
                max_value: 4,
                remaining: 3,
            })
        ));
        assert_eq!(budget.remaining(), 3);
    }

    #[test]
    fn recording_spend_drains_a_short_budget() {
        let budget = ReadBudget::new(4, 3);
        assert!(matches!(
            budget.record_aggregate_spend(5),
            Err(Error::ReadBudgetExceeded {
                size: 5,
                max_value: 4,
                remaining: 3,
            })
        ));
        assert_eq!(budget.remaining(), 0);
    }
}
