//! Hard budget caps for the agent loop.
//!
//! [`AgentBudget`] is the ceilings, [`BudgetUsage`] the running tally; `check` is
//! pure and the loop calls it before every transition, exiting through Done on breach.

#[derive(Debug, Clone, Copy)]
pub struct AgentBudget {
    pub max_steps: u32,
    pub max_tokens: u64,
    pub max_wall_secs: u64,
    /// Only enforced for backends that report cost (`TokenUsage::cost_usd`).
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
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BudgetUsage {
    pub steps: u32,
    pub tokens: u64,
    pub wall_secs: u64,
    pub cost_usd: f64,
    pub proposals: u32,
    pub checker_calls: u32,
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
    /// `Err(cap)` if continuing would meet or exceed any ceiling. Checked
    /// before each step, so a breach stops the loop without spending more.
    pub fn check(&self, used: &BudgetUsage) -> Result<(), BudgetExceeded> {
        if used.steps >= self.max_steps {
            return Err(BudgetExceeded::Steps);
        }
        if used.tokens >= self.max_tokens {
            return Err(BudgetExceeded::Tokens);
        }
        if used.wall_secs >= self.max_wall_secs {
            return Err(BudgetExceeded::Wall);
        }
        if let Some(max) = self.max_cost_usd {
            if used.cost_usd >= max {
                return Err(BudgetExceeded::Cost);
            }
        }
        if used.proposals >= self.max_proposals {
            return Err(BudgetExceeded::Proposals);
        }
        if used.checker_calls >= self.max_checker_calls {
            return Err(BudgetExceeded::CheckerCalls);
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
            tokens: 5_000,
            wall_secs: 30,
            cost_usd: 0.0,
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
            Err(BudgetExceeded::Steps)
        );
        assert_eq!(
            b.check(&BudgetUsage {
                tokens: 100,
                ..Default::default()
            }),
            Err(BudgetExceeded::Tokens)
        );
        assert_eq!(
            b.check(&BudgetUsage {
                wall_secs: 60,
                ..Default::default()
            }),
            Err(BudgetExceeded::Wall)
        );
        assert_eq!(
            b.check(&BudgetUsage {
                cost_usd: 1.5,
                ..Default::default()
            }),
            Err(BudgetExceeded::Cost)
        );
        assert_eq!(
            b.check(&BudgetUsage {
                proposals: 7,
                ..Default::default()
            }),
            Err(BudgetExceeded::Proposals)
        );
        assert_eq!(
            b.check(&BudgetUsage {
                checker_calls: 9,
                ..Default::default()
            }),
            Err(BudgetExceeded::CheckerCalls)
        );
    }

    #[test]
    fn cost_is_unbounded_when_unset() {
        let b = AgentBudget {
            max_cost_usd: None,
            ..Default::default()
        };
        let used = BudgetUsage {
            cost_usd: 1_000_000.0,
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
            tokens: 9,
            wall_secs: 9,
            cost_usd: 9.0,
            proposals: 9,
            checker_calls: 9,
        };
        assert_eq!(b.check(&used), Err(BudgetExceeded::Steps));
    }
}
