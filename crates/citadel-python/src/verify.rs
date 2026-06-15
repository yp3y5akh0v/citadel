//! Verifier bridge: Python object adapted to citadel-ai's `Verifier`.

use std::sync::Arc;

use citadel_ai::{
    CheckerAttestation, ScoredOutcome, Verifier, VerifyError, VerifyKind, VerifyOutcome,
    VerifyRequest,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

use crate::dict_item;
use crate::graph::goal_to_py;
use crate::llm::tool_call_to_py;

// ---- conversion ------------------------------------------------------------

fn verify_kind_str(kind: VerifyKind) -> &'static str {
    match kind {
        VerifyKind::Constraint => "constraint",
        VerifyKind::Acceptance => "acceptance",
        VerifyKind::Rank => "rank",
    }
}

/// Render the verifier's request context as a dict.
fn verify_request_to_py(py: Python<'_>, req: &VerifyRequest<'_>) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item("kind", verify_kind_str(req.kind))?;
    d.set_item("goal", goal_to_py(py, req.goal)?)?;
    let tool_calls = req
        .tool_calls
        .iter()
        .map(|c| tool_call_to_py(py, c))
        .collect::<PyResult<Vec<_>>>()?;
    d.set_item("tool_calls", tool_calls)?;
    d.set_item("evidence", req.evidence.to_vec())?;
    d.into_py_any(py)
}

fn verify_outcome_from_py(obj: &Bound<'_, PyAny>) -> PyResult<VerifyOutcome> {
    if let Ok(satisfied) = obj.extract::<bool>() {
        return Ok(VerifyOutcome {
            satisfied,
            reason: String::new(),
        });
    }
    let d = obj
        .extract::<Bound<'_, PyDict>>()
        .map_err(|_| PyValueError::new_err("verify() must return a bool or {satisfied, reason}"))?;
    Ok(VerifyOutcome {
        satisfied: dict_item(&d, "satisfied")?
            .ok_or_else(|| PyValueError::new_err("verify() result missing 'satisfied'"))?
            .extract()?,
        reason: dict_item(&d, "reason")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
    })
}

fn scored_outcome_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ScoredOutcome> {
    let d = obj.extract::<Bound<'_, PyDict>>().map_err(|_| {
        PyValueError::new_err("score() must return {satisfied, score, reason?, cell?, terminal?}")
    })?;
    Ok(ScoredOutcome {
        satisfied: dict_item(&d, "satisfied")?
            .ok_or_else(|| PyValueError::new_err("score() result missing 'satisfied'"))?
            .extract()?,
        score: dict_item(&d, "score")?
            .ok_or_else(|| PyValueError::new_err("score() result missing 'score'"))?
            .extract()?,
        reason: dict_item(&d, "reason")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
        cell: dict_item(&d, "cell")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_default(),
        terminal: dict_item(&d, "terminal")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(false),
    })
}

// ---- the bridge ------------------------------------------------------------

/// Adapts a Python verifier to [`Verifier`]. With `checker_id`+`checker_version`
/// it is a mint-eligible checker, else a critic.
struct PyVerifierBridge {
    callable: Py<PyAny>,
    attestation: Option<CheckerAttestation>,
}

impl PyVerifierBridge {
    fn from_object(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            attestation: read_attestation(obj)?,
            callable: obj.clone().unbind(),
        })
    }
}

fn read_attestation(obj: &Bound<'_, PyAny>) -> PyResult<Option<CheckerAttestation>> {
    let id = obj.getattr("checker_id").ok().filter(|v| !v.is_none());
    let version = obj.getattr("checker_version").ok().filter(|v| !v.is_none());
    match (id, version) {
        (Some(id), Some(version)) => Ok(Some(CheckerAttestation::new(
            id.extract::<String>()?,
            version.extract::<String>()?,
        ))),
        _ => Ok(None),
    }
}

fn pyerr_to_verify(e: PyErr) -> VerifyError {
    VerifyError::Failed(e.to_string())
}

impl Verifier for PyVerifierBridge {
    fn verify(&self, req: &VerifyRequest<'_>) -> Result<VerifyOutcome, VerifyError> {
        Python::attach(|py| {
            let d = verify_request_to_py(py, req).map_err(pyerr_to_verify)?;
            let out = self
                .callable
                .bind(py)
                .call_method1("verify", (d,))
                .map_err(pyerr_to_verify)?;
            verify_outcome_from_py(&out).map_err(pyerr_to_verify)
        })
    }

    fn score(&self, req: &VerifyRequest<'_>) -> Result<ScoredOutcome, VerifyError> {
        Python::attach(|py| {
            let obj = self.callable.bind(py);
            let d = verify_request_to_py(py, req).map_err(pyerr_to_verify)?;
            if let Ok(method) = obj.getattr("score") {
                if !method.is_none() {
                    let out = method.call1((d,)).map_err(pyerr_to_verify)?;
                    return scored_outcome_from_py(&out).map_err(pyerr_to_verify);
                }
            }
            // No custom score(): derive a 1.0/0.0 scale from verify (the trait default).
            let out = obj.call_method1("verify", (d,)).map_err(pyerr_to_verify)?;
            let o = verify_outcome_from_py(&out).map_err(pyerr_to_verify)?;
            Ok(ScoredOutcome {
                score: if o.satisfied { 1.0 } else { 0.0 },
                satisfied: o.satisfied,
                reason: o.reason,
                cell: String::new(),
                terminal: false,
            })
        })
    }

    fn attestation(&self) -> Option<CheckerAttestation> {
        self.attestation.clone()
    }

    fn cross_check(&self, req: &VerifyRequest<'_>) -> Result<bool, VerifyError> {
        Python::attach(|py| {
            let obj = self.callable.bind(py);
            match obj.getattr("cross_check") {
                Ok(method) if !method.is_none() => {
                    let d = verify_request_to_py(py, req).map_err(pyerr_to_verify)?;
                    method
                        .call1((d,))
                        .map_err(pyerr_to_verify)?
                        .extract::<bool>()
                        .map_err(pyerr_to_verify)
                }
                _ => Ok(true),
            }
        })
    }
}

/// Adapt a Python verifier object for the agent's `verifier` config.
pub(crate) fn build_verifier(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Verifier>> {
    Ok(Arc::new(PyVerifierBridge::from_object(obj)?))
}
