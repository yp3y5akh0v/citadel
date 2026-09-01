//! Request-scoped limits for memory reads.

use std::cell::RefCell;

use citadel_sql::ReadBudget;
use serde_json::Value;

use crate::{MemError, MemoryEngine, MemoryMaintenance, Result};

thread_local! {
    static MATERIALIZED_CONTENT_BUDGETS: RefCell<Vec<ReadBudget>> = const { RefCell::new(Vec::new()) };
    static RETURNED_CONTENT_BUDGETS: RefCell<Vec<ReadBudget>> = const { RefCell::new(Vec::new()) };
}

/// Limits applied to one logical memory operation.
///
/// Storage materialization is admitted before an overflow value is allocated.
/// Returned-content accounting is separate so plaintext retained in an
/// encrypted region's ANN cache cannot bypass the limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryReadLimits {
    pub max_value_bytes: usize,
    pub max_materialized_bytes: usize,
    pub max_returned_bytes: usize,
}

impl MemoryReadLimits {
    pub const fn new(
        max_value_bytes: usize,
        max_materialized_bytes: usize,
        max_returned_bytes: usize,
    ) -> Self {
        Self {
            max_value_bytes,
            max_materialized_bytes,
            max_returned_bytes,
        }
    }
}

impl MemoryEngine {
    /// Run one logical operation with storage and returned-content limits.
    ///
    /// The scope is synchronous and thread-local. Nested scopes restore the
    /// outer limits on every exit, including panic unwinding.
    pub fn with_read_limits<T>(
        &self,
        limits: MemoryReadLimits,
        operation: impl FnOnce(&Self) -> T,
    ) -> T {
        with_read_limits_scope(limits, || operation(self))
    }
}

impl MemoryMaintenance {
    /// Run one logical model-free operation with storage and returned-content limits.
    ///
    /// The scope is synchronous and thread-local. Nested scopes restore the
    /// outer limits on every exit, including panic unwinding.
    pub fn with_read_limits<T>(
        &self,
        limits: MemoryReadLimits,
        operation: impl FnOnce(&Self) -> T,
    ) -> T {
        with_read_limits_scope(limits, || operation(self))
    }
}

fn with_read_limits_scope<T>(limits: MemoryReadLimits, operation: impl FnOnce() -> T) -> T {
    let materialized = ReadBudget::new(limits.max_value_bytes, limits.max_materialized_bytes);
    let returned = ReadBudget::new(limits.max_value_bytes, limits.max_returned_bytes);
    with_content_budget(BudgetKind::Materialized, &materialized, || {
        with_content_budget(BudgetKind::Returned, &returned, || {
            citadel_sql::with_read_budget(&materialized, operation)
        })
    })
}

#[derive(Clone, Copy)]
enum BudgetKind {
    Materialized,
    Returned,
}

fn with_content_budget<T>(
    kind: BudgetKind,
    budget: &ReadBudget,
    operation: impl FnOnce() -> T,
) -> T {
    struct Guard(BudgetKind);

    impl Drop for Guard {
        fn drop(&mut self) {
            let pop = |budgets: &RefCell<Vec<ReadBudget>>| {
                budgets
                    .borrow_mut()
                    .pop()
                    .expect("memory read-limit scope stack remains balanced");
            };
            match self.0 {
                BudgetKind::Materialized => MATERIALIZED_CONTENT_BUDGETS.with(pop),
                BudgetKind::Returned => RETURNED_CONTENT_BUDGETS.with(pop),
            }
        }
    }

    let push = |budgets: &RefCell<Vec<ReadBudget>>| budgets.borrow_mut().push(budget.clone());
    match kind {
        BudgetKind::Materialized => MATERIALIZED_CONTENT_BUDGETS.with(push),
        BudgetKind::Returned => RETURNED_CONTENT_BUDGETS.with(push),
    }
    let _guard = Guard(kind);
    operation()
}

fn current_content_budget(kind: BudgetKind) -> Option<ReadBudget> {
    let current = |budgets: &RefCell<Vec<ReadBudget>>| budgets.borrow().last().cloned();
    match kind {
        BudgetKind::Materialized => MATERIALIZED_CONTENT_BUDGETS.with(current),
        BudgetKind::Returned => RETURNED_CONTENT_BUDGETS.with(current),
    }
}

fn map_budget_error(error: citadel_core::Error) -> MemError {
    match error {
        citadel_core::Error::ReadBudgetExceeded {
            size,
            max_value,
            remaining,
        } => MemError::ReadLimitExceeded {
            size,
            max_value,
            remaining,
        },
        error => MemError::Core(error),
    }
}

/// Account for an atom as it is materialized or cloned from a decrypted cache.
pub(crate) fn charge_atom_content(kind: &str, text: &str, payload: &Value) -> Result<()> {
    charge_atom_against(BudgetKind::Materialized, kind, text, payload)
}

/// Admit already-sized caller-controlled content into the shared
/// materialization budget.
pub(crate) fn charge_materialized_bytes(size: usize) -> Result<()> {
    charge_bytes_against(BudgetKind::Materialized, size)
}

/// Run a storage query under a mandatory cap while respecting a stricter
/// surrounding memory-read scope. The caller still charges owned plaintext
/// through [`charge_materialized_bytes`] after the query returns.
pub(crate) fn with_storage_read_cap<T>(
    max_value_bytes: usize,
    max_total_bytes: usize,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    struct Settlement {
        outer: Option<ReadBudget>,
        child: ReadBudget,
        initial_remaining: usize,
    }

    impl Settlement {
        fn settle(&mut self) -> Result<()> {
            let Some(outer) = self.outer.take() else {
                return Ok(());
            };
            let spent = self
                .initial_remaining
                .saturating_sub(self.child.remaining());
            outer
                .record_aggregate_spend(spent)
                .map_err(map_budget_error)
        }
    }

    impl Drop for Settlement {
        fn drop(&mut self) {
            let _ = self.settle();
        }
    }

    let outer = current_content_budget(BudgetKind::Materialized);
    let (max_value_bytes, max_total_bytes) =
        outer
            .as_ref()
            .map_or((max_value_bytes, max_total_bytes), |outer| {
                let remaining = outer.remaining();
                (
                    max_value_bytes.min(outer.max_value()).min(remaining),
                    max_total_bytes.min(remaining),
                )
            });
    let mut settlement = Settlement {
        outer,
        child: ReadBudget::new(max_value_bytes, max_total_bytes),
        initial_remaining: max_total_bytes,
    };
    let result = citadel_sql::with_read_budget(&settlement.child, operation);
    settlement.settle()?;
    result
}

/// Account for one atom that survived ranking and will leave the engine.
pub(crate) fn charge_returned_atom_content(kind: &str, text: &str, payload: &Value) -> Result<()> {
    charge_atom_against(BudgetKind::Returned, kind, text, payload)
}

fn charge_atom_against(
    kind: BudgetKind,
    atom_kind: &str,
    text: &str,
    payload: &Value,
) -> Result<()> {
    let Some(budget) = current_content_budget(kind) else {
        return Ok(());
    };
    let stop_after = budget.max_value().min(budget.remaining());
    let size = atom_content_bytes(atom_kind, text, payload, stop_after);
    budget.try_charge(size).map_err(map_budget_error)
}

pub(crate) fn atom_content_bytes(
    kind: &str,
    text: &str,
    payload: &Value,
    stop_after: usize,
) -> usize {
    kind.len()
        .saturating_add(text.len())
        .saturating_add(json_memory_bytes(payload, stop_after))
}

/// Account for an edge's caller-authored evidence.
pub(crate) fn charge_edge_evidence(evidence: Option<&Value>) -> Result<()> {
    charge_evidence_against(BudgetKind::Materialized, evidence)
}

/// Account for caller-authored edge evidence that will leave the engine.
pub(crate) fn charge_returned_edge_evidence(evidence: Option<&Value>) -> Result<()> {
    charge_evidence_against(BudgetKind::Returned, evidence)
}

fn charge_evidence_against(kind: BudgetKind, evidence: Option<&Value>) -> Result<()> {
    let Some(budget) = current_content_budget(kind) else {
        return Ok(());
    };
    let stop_after = budget.max_value().min(budget.remaining());
    let size = evidence.map_or(0, |value| json_memory_bytes(value, stop_after));
    budget.try_charge(size).map_err(map_budget_error)
}

/// Account for other caller-controlled text that will leave the engine.
pub(crate) fn charge_returned_bytes(size: usize) -> Result<()> {
    charge_bytes_against(BudgetKind::Returned, size)
}

fn charge_bytes_against(kind: BudgetKind, size: usize) -> Result<()> {
    let Some(budget) = current_content_budget(kind) else {
        return Ok(());
    };
    budget.try_charge(size).map_err(map_budget_error)
}

/// Iterative estimate of the owned JSON memory that a caller receives. Node
/// overhead makes deeply nested or very wide values hit the bound before this
/// traversal's own stack can become large.
fn json_memory_bytes(root: &Value, stop_after: usize) -> usize {
    const NODE_OVERHEAD: usize = 16;

    let mut total = 0usize;
    let mut stack = vec![root];
    while let Some(value) = stack.pop() {
        total = total.saturating_add(NODE_OVERHEAD);
        match value {
            Value::String(text) => total = total.saturating_add(text.len()),
            Value::Array(values) => {
                total = total.saturating_add(values.len());
                if total > stop_after {
                    return total;
                }
                stack.extend(values);
            }
            Value::Object(fields) => {
                total = total.saturating_add(fields.len());
                for (key, value) in fields {
                    total = total.saturating_add(key.len());
                    if total > stop_after {
                        return total;
                    }
                    stack.push(value);
                }
            }
            Value::Number(number) => total = total.saturating_add(number.to_string().len()),
            Value::Bool(_) => total = total.saturating_add(5),
            Value::Null => total = total.saturating_add(4),
        }
        if total > stop_after {
            return total;
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use citadel::{Database, DatabaseBuilder};
    use citadel_sql::Connection;

    use super::*;

    #[test]
    fn nested_scopes_restore_the_outer_content_budget() {
        let outer = ReadBudget::new(64, 32);
        let inner = ReadBudget::new(64, 64);

        with_content_budget(BudgetKind::Materialized, &outer, || {
            charge_atom_content("a", "b", &Value::Null).unwrap();
            let after_first = outer.remaining();
            with_content_budget(BudgetKind::Materialized, &inner, || {
                charge_atom_content("inner", "value", &Value::Null).unwrap();
            });
            assert_eq!(outer.remaining(), after_first);
            assert!(matches!(
                charge_atom_content("too", "large", &Value::Null),
                Err(MemError::ReadLimitExceeded { .. })
            ));
        });
    }

    #[test]
    fn content_scope_is_removed_after_unwind() {
        let budget = ReadBudget::new(1, 1);
        let _ = std::panic::catch_unwind(|| {
            with_content_budget(BudgetKind::Materialized, &budget, || panic!("probe"));
        });
        charge_atom_content("unbounded", "after", &Value::Null).unwrap();
    }

    #[test]
    fn wide_json_is_rejected_without_growing_an_unbounded_walk_stack() {
        let payload = Value::Array(vec![Value::Null; 10_000]);
        let budget = ReadBudget::new(128, 128);
        with_content_budget(BudgetKind::Materialized, &budget, || {
            assert!(matches!(
                charge_atom_content("fact", "text", &payload),
                Err(MemError::ReadLimitExceeded { .. })
            ));
        });
    }

    #[test]
    fn mandatory_storage_cap_debits_the_surrounding_materialization_budget() {
        let db: Arc<Database> = Arc::new(
            DatabaseBuilder::new("ignored.db")
                .passphrase(b"budget-probe")
                .create_in_memory()
                .unwrap(),
        );
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE budget_probe (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        let payload = "x".repeat(16 * 1024);
        conn.execute_params(
            "INSERT INTO budget_probe VALUES (1, $1)",
            &[citadel_sql::Value::Text(payload.into())],
        )
        .unwrap();

        let outer = ReadBudget::new(64 * 1024, 64 * 1024);
        with_content_budget(BudgetKind::Materialized, &outer, || {
            let before = outer.remaining();
            let result = with_storage_read_cap(64 * 1024, 64 * 1024, || {
                Ok(conn.query("SELECT payload FROM budget_probe")?)
            })
            .unwrap();
            assert_eq!(result.rows.len(), 1);
            assert!(
                outer.remaining() < before,
                "child storage reads must debit the surrounding budget"
            );
        });
    }

    #[test]
    fn failed_storage_reads_still_debit_the_surrounding_budget() {
        let db: Arc<Database> = Arc::new(
            DatabaseBuilder::new("ignored.db")
                .passphrase(b"budget-probe")
                .create_in_memory()
                .unwrap(),
        );
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE budget_probe (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        conn.execute_params(
            "INSERT INTO budget_probe VALUES (1, $1)",
            &[citadel_sql::Value::Text("x".repeat(16 * 1024).into())],
        )
        .unwrap();

        let outer = ReadBudget::new(64 * 1024, 64 * 1024);
        with_content_budget(BudgetKind::Materialized, &outer, || {
            let before = outer.remaining();
            let error = with_storage_read_cap(64 * 1024, 64 * 1024, || {
                let result = conn.query("SELECT payload FROM budget_probe")?;
                assert_eq!(result.rows.len(), 1);
                Err::<(), _>(MemError::Invalid("probe".into()))
            })
            .unwrap_err();
            assert!(matches!(error, MemError::Invalid(message) if message == "probe"));
            assert!(outer.remaining() < before);
        });
    }

    #[test]
    fn unwinding_storage_reads_still_debit_the_surrounding_budget() {
        let db: Arc<Database> = Arc::new(
            DatabaseBuilder::new("ignored.db")
                .passphrase(b"budget-probe")
                .create_in_memory()
                .unwrap(),
        );
        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE budget_probe (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        conn.execute_params(
            "INSERT INTO budget_probe VALUES (1, $1)",
            &[citadel_sql::Value::Text("x".repeat(16 * 1024).into())],
        )
        .unwrap();

        let outer = ReadBudget::new(64 * 1024, 64 * 1024);
        with_content_budget(BudgetKind::Materialized, &outer, || {
            let before = outer.remaining();
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_storage_read_cap::<()>(64 * 1024, 64 * 1024, || {
                    let result = conn.query("SELECT payload FROM budget_probe")?;
                    assert_eq!(result.rows.len(), 1);
                    panic!("probe");
                })
                .unwrap();
            }));
            assert!(panic.is_err());
            assert!(outer.remaining() < before);
        });
    }
}
