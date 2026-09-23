//! Completion receipts and errors for the native benchmark harnesses.

use citadel_llm::{FinishReason, LlmError, TokenUsage};
use citadel_mem::MemError;

use crate::core::agentic::ExtractionParseError;
use crate::core::eval::{CompletionCallAudit, CompletionFinish, JudgeOutcome};

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderStage {
    Extraction,
    ExtractionValidation,
    FinalAnswer,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CompletedReaderCall {
    pub call: CompletionCallAudit,
    pub finish_reason: CompletionFinish,
}

#[derive(Debug, thiserror::Error)]
#[error("reader {stage:?} failed: {source}")]
pub struct ReaderFailure {
    pub stage: ReaderStage,
    pub completed_calls: Vec<CompletedReaderCall>,
    #[source]
    pub source: Box<BenchError>,
}

#[derive(Debug, thiserror::Error)]
#[error("completion failed: {source}")]
pub struct CompletionFailure {
    /// The request was handed to the completion path; usage is absent on failure.
    pub call: CompletionCallAudit,
    #[source]
    pub source: Box<BenchError>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "benchmark", rename_all = "snake_case")]
pub enum QuestionIdentity {
    Locomo {
        sample_id: String,
        qa_index: usize,
    },
    #[serde(rename = "longmemeval")]
    LongMemEval {
        question_id: String,
    },
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionStage {
    Reader,
    Judge,
    Observer,
}

#[derive(Debug, serde::Serialize)]
pub struct UsageAccounting {
    /// Response usage reported by LLMClient, whose API does not expose upstream field presence.
    pub observed_input_tokens: u64,
    pub observed_output_tokens: u64,
    pub unknown_usage_attempts: u64,
    pub estimated_cost_usd: Option<f64>,
}

pub(crate) fn sum_costs(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    let valid = |cost: &f64| cost.is_finite() && *cost >= 0.0;
    left.filter(valid)
        .zip(right.filter(valid))
        .map(|(left, right)| left + right)
        .filter(valid)
}

impl UsageAccounting {
    pub fn from_calls<'a>(calls: impl IntoIterator<Item = &'a CompletionCallAudit>) -> Self {
        let mut out = Self {
            observed_input_tokens: 0,
            observed_output_tokens: 0,
            unknown_usage_attempts: 0,
            estimated_cost_usd: Some(0.0),
        };
        for call in calls {
            if let Some(usage) = call.usage {
                out.observed_input_tokens = out
                    .observed_input_tokens
                    .saturating_add(u64::from(usage.input_tokens));
                out.observed_output_tokens = out
                    .observed_output_tokens
                    .saturating_add(u64::from(usage.output_tokens));
            }
            out.unknown_usage_attempts = out
                .unknown_usage_attempts
                .saturating_add(call.unknown_usage_attempts());
            out.estimated_cost_usd = sum_costs(out.estimated_cost_usd, call.estimated_cost_usd());
        }
        out
    }
}

#[derive(Debug)]
pub struct QuestionFailure {
    pub identity: QuestionIdentity,
    pub stage: QuestionStage,
    pub calls: Vec<CompletionCallAudit>,
    pub source: Box<BenchError>,
    /// Failure-observer diagnostics never replace the original failure.
    pub observer_errors: Vec<String>,
    pub completed_output: Option<Box<CompletedOutput>>,
}

#[derive(Debug, serde::Serialize)]
pub struct CompletedOutput {
    pub answer: String,
    pub judge: Option<JudgeOutcome>,
}

/// Completed questions retained when another worker interrupts the run.
#[derive(Debug)]
pub struct QuestionCompletion {
    pub identity: QuestionIdentity,
    pub calls: Vec<CompletionCallAudit>,
    pub output: CompletedOutput,
}

impl QuestionCompletion {
    pub fn write_json_line(&self, writer: &mut impl std::io::Write) -> Result<()> {
        write_json_line(self, writer)
    }
}

impl serde::Serialize for QuestionCompletion {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut fields = serializer.serialize_struct("QuestionCompletion", 5)?;
        fields.serialize_field("event", "completed")?;
        fields.serialize_field("identity", &self.identity)?;
        fields.serialize_field("calls", &self.calls)?;
        fields.serialize_field("accounting", &UsageAccounting::from_calls(&self.calls))?;
        fields.serialize_field("completed_output", &self.output)?;
        fields.end()
    }
}

#[derive(Debug)]
pub struct QuestionBatchFailure {
    pub failures: Vec<QuestionFailure>,
    pub completed: Vec<QuestionCompletion>,
}

impl serde::Serialize for QuestionBatchFailure {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut fields = serializer.serialize_struct("QuestionBatchFailure", 3)?;
        fields.serialize_field("failures", &self.failures)?;
        fields.serialize_field("completed", &self.completed)?;
        fields.serialize_field("accounting", &self.accounting())?;
        fields.end()
    }
}

impl QuestionBatchFailure {
    pub fn accounting(&self) -> UsageAccounting {
        UsageAccounting::from_calls(
            self.failures
                .iter()
                .flat_map(|failure| &failure.calls)
                .chain(self.completed.iter().flat_map(|completed| &completed.calls)),
        )
    }
}

fn write_json_line(record: &impl serde::Serialize, writer: &mut impl std::io::Write) -> Result<()> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    writer.write_all(&line)?;
    writer.flush()?;
    Ok(())
}

impl QuestionFailure {
    pub fn new(identity: QuestionIdentity, stage: QuestionStage, source: BenchError) -> Self {
        let calls = source.completion_calls();
        Self {
            identity,
            stage,
            calls,
            source: Box::new(source),
            observer_errors: Vec::new(),
            completed_output: None,
        }
    }

    pub fn prepend_calls(mut self, prior: impl IntoIterator<Item = CompletionCallAudit>) -> Self {
        self.calls.splice(0..0, prior);
        self
    }

    pub fn accounting(&self) -> UsageAccounting {
        UsageAccounting::from_calls(&self.calls)
    }

    pub fn with_completed_output(mut self, answer: String, judge: Option<JudgeOutcome>) -> Self {
        self.completed_output = Some(Box::new(CompletedOutput { answer, judge }));
        self
    }

    /// Persist one complete failure receipt before the runner returns its error.
    pub fn write_json_line(&self, writer: &mut impl std::io::Write) -> Result<()> {
        write_json_line(self, writer)
    }
}

impl serde::Serialize for QuestionFailure {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut fields = serializer.serialize_struct("QuestionFailure", 9)?;
        fields.serialize_field("event", "failed")?;
        fields.serialize_field("identity", &self.identity)?;
        fields.serialize_field("stage", &self.stage)?;
        fields.serialize_field("error", &self.source.to_string())?;
        fields.serialize_field("failure_detail", &self.source.failure_detail())?;
        fields.serialize_field("calls", &self.calls)?;
        fields.serialize_field("accounting", &self.accounting())?;
        fields.serialize_field("observer_errors", &self.observer_errors)?;
        fields.serialize_field("completed_output", &self.completed_output)?;
        fields.end()
    }
}

/// An observer sees successful results and failures through one ordered callback.
/// Journal events contain the complete state for their question identity. If a
/// journal flush fails after writing a completion, a later failure event replaces
/// that question's state; it must not be added to the completed event's usage.
/// Identical request hashes can describe distinct billable logical calls. Count
/// attempts within each question/call, not globally by request hash. A malformed
/// or incomplete journal is an I/O failure, not a complete accounting record.
/// The returned batch retains receipts even when journal I/O cannot preserve them.
pub enum QuestionEvent<'a, T> {
    Completed(&'a T),
    Failed(&'a QuestionFailure),
}

pub type QuestionObserver<'callback, T> =
    dyn for<'event> FnMut(QuestionEvent<'event, T>) -> Result<()> + Send + 'callback;

pub(crate) fn observe_failure<T>(
    mut failure: QuestionFailure,
    observer: &std::sync::Mutex<&mut QuestionObserver<'_, T>>,
    failures: &std::sync::Mutex<Vec<QuestionFailure>>,
) {
    let notification = {
        let mut observer = observer.lock().expect("observer poisoned");
        (*observer)(QuestionEvent::Failed(&failure))
    }; // Release the observer mutex before taking the failures mutex.
    if let Err(error) = notification {
        failure.observer_errors.push(error.to_string());
    }
    failures.lock().expect("failures poisoned").push(failure);
}

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("dataset error: {0}")]
    Dataset(String),
    #[error("{} failed and {} completed benchmark question(s) retained", .0.failures.len(), .0.completed.len())]
    Questions(Box<QuestionBatchFailure>),
    #[error(transparent)]
    Reader(#[from] Box<ReaderFailure>),
    #[error(transparent)]
    Completion(#[from] Box<CompletionFailure>),
    #[error("invalid extraction completion ({finish_reason:?}): {reason}")]
    InvalidExtractionCompletion {
        reason: &'static str,
        response: String,
        finish_reason: FinishReason,
    },
    #[error("invalid extraction: {source}")]
    InvalidExtraction {
        response: String,
        #[source]
        source: ExtractionParseError,
    },
    #[error("invalid judge response ({finish_reason:?}): {reason}")]
    InvalidJudgeResponse {
        reason: &'static str,
        response: String,
        finish_reason: FinishReason,
        usage: TokenUsage,
        call: Option<Box<CompletionCallAudit>>,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Mem(#[from] MemError),
    #[error(transparent)]
    Db(#[from] citadel::Error),
    #[error(transparent)]
    Llm(#[from] LlmError),
}

impl BenchError {
    fn failure_detail(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Self::Reader(failure) => {
                json!({"stage": failure.stage, "cause": failure.source.failure_detail()})
            }
            Self::Completion(failure) => failure.source.failure_detail(),
            Self::InvalidJudgeResponse {
                response,
                finish_reason,
                ..
            }
            | Self::InvalidExtractionCompletion {
                response,
                finish_reason,
                ..
            } => {
                json!({"response": response, "finish_reason": CompletionFinish::from(*finish_reason)})
            }
            Self::InvalidExtraction { response, .. } => json!({"response": response}),
            _ => serde_json::Value::Null,
        }
    }

    pub(crate) fn with_completion_call(mut self, call: CompletionCallAudit) -> Self {
        if let Self::InvalidJudgeResponse { call: receipt, .. } = &mut self {
            *receipt = Some(Box::new(call));
        }
        self
    }

    fn completion_calls(&self) -> Vec<CompletionCallAudit> {
        match self {
            Self::Reader(failure) => failure
                .completed_calls
                .iter()
                .map(|r| r.call.clone())
                .chain(failure.source.completion_calls())
                .collect(),
            Self::Completion(failure) => vec![failure.call.clone()],
            Self::InvalidJudgeResponse {
                call: Some(call), ..
            } => vec![call.as_ref().clone()],
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonfinite_cost_totals_are_unknown_explicitly() {
        assert_eq!(sum_costs(Some(f64::MAX), Some(f64::MAX)), None);
        assert_eq!(sum_costs(Some(f64::NAN), Some(0.0)), None);
        assert_eq!(sum_costs(Some(-1.0), Some(0.0)), None);
        assert_eq!(sum_costs(Some(-1.0), Some(2.0)), None);
        assert_eq!(sum_costs(Some(0.03), Some(0.02)), Some(0.05));
    }

    #[test]
    fn failure_jsonl_preserves_exact_error_text_and_flush_errors() {
        let failure = QuestionFailure::new(
            QuestionIdentity::LongMemEval {
                question_id: "fixture".into(),
            },
            QuestionStage::Reader,
            BenchError::Dataset("line one\nline two – café".into()),
        );
        let mut bytes = Vec::new();
        failure.write_json_line(&mut bytes).unwrap();
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
        let decoded: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded["error"], "dataset error: line one\nline two – café");
        struct Broken {
            flush_only: bool,
        }
        impl std::io::Write for Broken {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.flush_only {
                    Ok(bytes.len())
                } else {
                    Err(std::io::Error::other("write failed"))
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::other("flush failed"))
            }
        }
        for flush_only in [false, true] {
            let error = failure
                .write_json_line(&mut Broken { flush_only })
                .unwrap_err();
            assert!(error.to_string().contains(if flush_only {
                "flush failed"
            } else {
                "write failed"
            }));
        }
        assert_eq!(serde_json::to_value(&failure).unwrap(), decoded);
    }
}
