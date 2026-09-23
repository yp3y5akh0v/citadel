//! Hard budget caps for the agent loop.
//!
//! [`AgentBudget`] is the ceilings, [`BudgetUsage`] the running tally; `check` is
//! pure and the loop calls it before every transition, exiting through Done on breach.

#[derive(Debug, Clone, Copy)]
pub struct AgentBudget {
    pub max_steps: u32,
    pub max_tokens: u64,
    pub max_wall_secs: u64,
    /// When set, unavailable estimated cost accounting stops the run conservatively.
    pub max_cost_usd: Option<f64>,
    /// Discovery search: max proposal batches. Proposals/checker calls accrue no
    /// tokens or steps, so without these a search loop would be unbounded.
    pub max_proposals: u32,
    /// Discovery search: max checker invocations (deterministic, free of tokens/steps).
    pub max_checker_calls: u32,
}

impl Default for AgentBudget {
    fn default() -> Self {
        Self {
            max_steps: 50,
            max_tokens: 1_000_000,
            max_wall_secs: 600,
            max_cost_usd: None,
            // Generous: non-discovery runs never increment these; discovery sets its own.
            max_proposals: 100_000,
            max_checker_calls: 1_000_000,
        }
    }
}

/// Cumulative resources consumed so far this run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BudgetUsage {
    pub steps: u32,
    /// Complete cumulative token count; `None` if any call lacked valid counters.
    pub tokens: Option<u64>,
    pub wall_secs: u64,
    /// Complete cumulative estimated cost; `None` if any call was unpriced or invalid.
    pub cost_usd: Option<f64>,
    pub proposals: u32,
    pub checker_calls: u32,
}

impl Default for BudgetUsage {
    fn default() -> Self {
        Self {
            steps: 0,
            tokens: Some(0),
            wall_secs: 0,
            cost_usd: Some(0.0),
            proposals: 0,
            checker_calls: 0,
        }
    }
}

impl BudgetUsage {
    pub(crate) fn accrue(&mut self, usage: Option<citadel_llm::TokenUsage>) {
        self.tokens = self.tokens.zip(usage).and_then(|(total, usage)| {
            total.checked_add(u64::from(usage.input_tokens) + u64::from(usage.output_tokens))
        });
        self.cost_usd = self
            .cost_usd
            .zip(usage.and_then(|u| valid_cost(u.cost_usd)))
            .and_then(|(total, cost)| valid_cost(Some(total + cost)));
    }
}

pub(crate) fn valid_cost(cost: Option<f64>) -> Option<f64> {
    cost.filter(|cost| cost.is_finite() && *cost >= 0.0)
}

/// Accounting needed to enforce a configured budget was not available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetUnavailable {
    #[error("token usage is unavailable; the token budget cannot be checked")]
    Tokens,
    #[error("cost is unavailable; the cost budget cannot be checked")]
    Cost,
}

/// A configured budget is not a valid finite ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetInvalid {
    #[error("max_cost_usd must be finite and nonnegative")]
    Cost,
}

/// A proven cap breach is distinct from unavailable accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetStop {
    #[error("budget exceeded: {0:?}")]
    Exceeded(BudgetExceeded),
    #[error(transparent)]
    UsageUnavailable(#[from] BudgetUnavailable),
    #[error(transparent)]
    InvalidConfiguration(#[from] BudgetInvalid),
}

impl From<BudgetExceeded> for BudgetStop {
    fn from(cap: BudgetExceeded) -> Self {
        Self::Exceeded(cap)
    }
}

/// Which cap was hit; surfaced as `terminated_by` when the loop exits early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetExceeded {
    Steps,
    Tokens,
    Wall,
    Cost,
    Proposals,
    CheckerCalls,
}

impl AgentBudget {
    /// Stop if a ceiling was met or the accounting needed to check it is
    /// unavailable. The loop checks before spending more.
    pub fn check(&self, used: &BudgetUsage) -> Result<(), BudgetStop> {
        self.validate()?;
        if used.steps >= self.max_steps {
            return Err(BudgetExceeded::Steps.into());
        }
        self.check_tokens(used)?;
        if used.wall_secs >= self.max_wall_secs {
            return Err(BudgetExceeded::Wall.into());
        }
        self.check_cost(used)?;
        if used.proposals >= self.max_proposals {
            return Err(BudgetExceeded::Proposals.into());
        }
        if used.checker_calls >= self.max_checker_calls {
            return Err(BudgetExceeded::CheckerCalls.into());
        }
        Ok(())
    }

    /// Check between LLM calls within a single transition or proposal batch.
    pub(crate) fn check_llm_usage(&self, used: &BudgetUsage) -> Result<(), BudgetStop> {
        self.validate()?;
        self.check_tokens(used)?;
        self.check_cost(used)
    }

    pub(crate) fn check_llm_call(&self, used: &BudgetUsage) -> Result<(), BudgetStop> {
        self.check_llm_usage(used)?;
        if used.wall_secs >= self.max_wall_secs {
            return Err(BudgetExceeded::Wall.into());
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), BudgetStop> {
        if self
            .max_cost_usd
            .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
        {
            return Err(BudgetInvalid::Cost.into());
        }
        Ok(())
    }

    fn check_tokens(&self, used: &BudgetUsage) -> Result<(), BudgetStop> {
        let tokens = used.tokens.ok_or(BudgetUnavailable::Tokens)?;
        if tokens >= self.max_tokens {
            return Err(BudgetExceeded::Tokens.into());
        }
        Ok(())
    }

    fn check_cost(&self, used: &BudgetUsage) -> Result<(), BudgetStop> {
        if let Some(max) = self.max_cost_usd {
            let cost = valid_cost(used.cost_usd).ok_or(BudgetUnavailable::Cost)?;
            if cost >= max {
                return Err(BudgetExceeded::Cost.into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_budget_is_ok() {
        let b = AgentBudget::default();
        assert!(b.check(&BudgetUsage::default()).is_ok());
        let used = BudgetUsage {
            steps: 10,
            tokens: Some(5_000),
            wall_secs: 30,
            cost_usd: Some(0.0),
            ..Default::default()
        };
        assert!(b.check(&used).is_ok());
    }

    #[test]
    fn each_cap_trips_independently() {
        let b = AgentBudget {
            max_steps: 5,
            max_tokens: 100,
            max_wall_secs: 60,
            max_cost_usd: Some(1.0),
            max_proposals: 7,
            max_checker_calls: 9,
        };
        assert_eq!(
            b.check(&BudgetUsage {
                steps: 5,
                ..Default::default()
            }),
            Err(BudgetExceeded::Steps.into())
        );
        assert_eq!(
            b.check(&BudgetUsage {
                tokens: Some(100),
                ..Default::default()
            }),
            Err(BudgetExceeded::Tokens.into())
        );
        assert_eq!(
            b.check(&BudgetUsage {
                wall_secs: 60,
                ..Default::default()
            }),
            Err(BudgetExceeded::Wall.into())
        );
        assert_eq!(
            b.check(&BudgetUsage {
                cost_usd: Some(1.5),
                ..Default::default()
            }),
            Err(BudgetExceeded::Cost.into())
        );
        assert_eq!(
            b.check(&BudgetUsage {
                proposals: 7,
                ..Default::default()
            }),
            Err(BudgetExceeded::Proposals.into())
        );
        assert_eq!(
            b.check(&BudgetUsage {
                checker_calls: 9,
                ..Default::default()
            }),
            Err(BudgetExceeded::CheckerCalls.into())
        );
    }

    #[test]
    fn cost_is_unbounded_when_unset() {
        let b = AgentBudget {
            max_cost_usd: None,
            ..Default::default()
        };
        let used = BudgetUsage {
            cost_usd: Some(1_000_000.0),
            ..Default::default()
        };
        assert!(b.check(&used).is_ok(), "no cost cap -> cost never trips");
    }

    #[test]
    fn steps_checked_before_other_caps() {
        // All caps breached at once -> Steps reported first (deterministic order).
        let b = AgentBudget {
            max_steps: 1,
            max_tokens: 1,
            max_wall_secs: 1,
            max_cost_usd: Some(0.0),
            max_proposals: 1,
            max_checker_calls: 1,
        };
        let used = BudgetUsage {
            steps: 9,
            tokens: Some(9),
            wall_secs: 9,
            cost_usd: Some(9.0),
            proposals: 9,
            checker_calls: 9,
        };
        assert_eq!(b.check(&used), Err(BudgetExceeded::Steps.into()));
    }

    #[test]
    fn missing_usage_is_not_a_measured_zero_and_does_not_recover() {
        let mut used = BudgetUsage::default();
        used.accrue(Some(citadel_llm::TokenUsage {
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: Some(0.0),
        }));
        assert_eq!(used.tokens, Some(0));
        assert_eq!(used.cost_usd, Some(0.0));
        assert!(AgentBudget::default().check(&used).is_ok());
        used.accrue(None);
        used.accrue(Some(citadel_llm::TokenUsage {
            input_tokens: 10,
            output_tokens: 2,
            cost_usd: Some(0.25),
        }));
        assert_eq!(used.tokens, None);
        assert_eq!(used.cost_usd, None);
        assert_eq!(
            AgentBudget::default().check(&used),
            Err(BudgetStop::UsageUnavailable(BudgetUnavailable::Tokens))
        );
    }

    #[test]
    fn unpriced_or_invalid_cost_preserves_tokens_but_cannot_satisfy_a_cost_cap() {
        for cost in [None, Some(-0.1), Some(f64::NAN), Some(f64::INFINITY)] {
            let mut used = BudgetUsage::default();
            used.accrue(Some(citadel_llm::TokenUsage {
                input_tokens: 8,
                output_tokens: 3,
                cost_usd: cost,
            }));
            assert_eq!(used.tokens, Some(11));
            assert_eq!(used.cost_usd, None);
            assert!(AgentBudget::default().check(&used).is_ok());
            let capped = AgentBudget {
                max_cost_usd: Some(1.0),
                ..Default::default()
            };
            assert_eq!(
                capped.check(&used),
                Err(BudgetStop::UsageUnavailable(BudgetUnavailable::Cost))
            );
            used.accrue(Some(citadel_llm::TokenUsage {
                input_tokens: 1,
                output_tokens: 1,
                cost_usd: Some(0.5),
            }));
            assert_eq!(
                used.cost_usd, None,
                "a partial price never restores a complete total"
            );
        }
    }

    #[test]
    fn aggregate_overflow_is_unavailable_instead_of_wrapping_or_infinity() {
        let mut used = BudgetUsage {
            tokens: Some(u64::MAX),
            cost_usd: Some(f64::MAX),
            ..Default::default()
        };
        used.accrue(Some(citadel_llm::TokenUsage {
            input_tokens: 1,
            output_tokens: 0,
            cost_usd: Some(f64::MAX),
        }));
        assert_eq!(used.tokens, None);
        assert_eq!(used.cost_usd, None);
    }

    #[test]
    fn invalid_cost_limits_are_distinct_from_zero_cost_exhaustion() {
        for max in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let budget = AgentBudget {
                max_cost_usd: Some(max),
                ..Default::default()
            };
            assert_eq!(
                budget.check(&BudgetUsage::default()),
                Err(BudgetStop::InvalidConfiguration(BudgetInvalid::Cost))
            );
        }
        let zero = AgentBudget {
            max_cost_usd: Some(0.0),
            ..Default::default()
        };
        assert_eq!(
            zero.check(&BudgetUsage::default()),
            Err(BudgetStop::Exceeded(BudgetExceeded::Cost))
        );
    }
}
