//! A cancel flag shared out of band with the work it cancels.
//!
//! A session pinned to one thread is inside the query when the stop request
//! arrives, so a queued message reaches it only once the query is over.
//! Relaxed ordering: one bool is published and nothing is ordered against it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cloning shares the flag rather than copying it, and once tripped it stays
/// tripped: a cancelled operation must not be resumable by a race.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// `Err(Error::Interrupted)` once cancelled.
    #[inline]
    pub fn check(&self) -> crate::Result<()> {
        if self.is_cancelled() {
            Err(crate::Error::Interrupted)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "cancel_tests.rs"]
mod tests;
