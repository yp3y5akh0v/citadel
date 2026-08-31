//! Bounded, cancellable newline-delimited JSON-RPC transport for MCP stdio.

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use citadel::CancelToken;
use citadel_mem::{MemoryEngine, MemoryReadLimits};
use serde_json::{json, Value};

use crate::protocol::{
    error_response, error_response_with_data, parse_message, INTERNAL_ERROR, INVALID_PARAMS,
    INVALID_REQUEST, PARSE_ERROR,
};

use super::{request_key, RequestKey};

/// Matches the official TypeScript SDK's default stdio receive buffer.
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 10 * 1024 * 1024;
/// Refill rate for tool invocations. Discovery, list, and resource methods are exempt.
pub(crate) const DEFAULT_TOOL_RATE_LIMIT_PER_MINUTE: u32 = 120;

const MAX_TOOL_BURST: u32 = 20;
const MAX_PENDING_REQUESTS: usize = 16;
const MAX_PENDING_BYTES: usize = 32 * 1024 * 1024;
const MAX_OUTBOUND_MESSAGES: usize = 1;
const MIN_FRAME_BYTES: usize = 512;
const MAX_SERIALIZED_REQUEST_ID_BYTES: usize = 256;
const TOOL_RATE_LIMITED: i64 = -31999;
const SERVER_BUSY: i64 = -31998;
const MINIMAL_OVERSIZE_ERROR: &[u8] = br#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"response exceeds stdio frame limit"}}"#;

#[derive(Clone, Copy)]
pub(super) struct Options {
    pub max_frame_bytes: usize,
    pub tool_rate_limit_per_minute: u32,
    pub allow_protected_memory_erasure: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            tool_rate_limit_per_minute: DEFAULT_TOOL_RATE_LIMIT_PER_MINUTE,
            allow_protected_memory_erasure: false,
        }
    }
}

struct ExecutionContext {
    mem: Arc<MemoryEngine>,
    region: String,
    options: Options,
}

pub(super) fn serve(mem: Arc<MemoryEngine>, region: &str, options: Options) -> io::Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    // `Stdout` is movable to the scoped worker; the shared writer still makes each
    // JSON-RPC line and flush one indivisible transport operation.
    let mut writer = io::stdout();
    serve_io(
        &mut reader,
        &mut writer,
        mem,
        region,
        options,
        &|mem, region, request, allow_erasure, _cancel| {
            super::dispatch_with_policy(mem, region, request, allow_erasure)
        },
    )
}

#[derive(Debug, PartialEq, Eq)]
enum Frame {
    Eof,
    Line(Vec<u8>),
    Oversized,
}

/// Read one newline-delimited frame without ever retaining more than `max_bytes`.
/// Once the limit is crossed, fail immediately instead of draining an attacker-
/// controlled unterminated line; the transport closes on [`Frame::Oversized`].
fn read_frame(reader: &mut impl BufRead, max_bytes: usize) -> io::Result<Frame> {
    let mut line = Vec::with_capacity(max_bytes.min(8 * 1024));

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(Frame::Eof)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stdio frame ended before newline delimiter",
                ))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.unwrap_or(available.len());
        match line.len().checked_add(take) {
            Some(total) if total <= max_bytes => line.extend_from_slice(&available[..take]),
            _ => return Ok(Frame::Oversized),
        }
        let consumed = take + usize::from(newline.is_some());
        reader.consume(consumed);

        if newline.is_some() {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(Frame::Line(line));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionEra {
    Opening,
    Probe,
    LegacyInitializing,
    LegacyAwaitingInitialized,
    Legacy,
    Modern,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestDisposition {
    Dispatch,
    LegacyInitialize,
}

fn carries_modern_envelope_claim(message: &Value) -> bool {
    message
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
        .is_some_and(|meta| {
            meta.contains_key(super::PROTOCOL_VERSION_META)
                || meta.contains_key(super::CLIENT_INFO_META)
                || meta.contains_key(super::CLIENT_CAPABILITIES_META)
        })
}

/// Pin one stdio process to a protocol era. A valid `server/discover` opens a
/// probe window but deliberately permits the legacy fallback until another
/// valid modern request commits the connection.
fn enforce_connection_era(
    state: &mut ConnectionEra,
    message: &Value,
) -> Result<RequestDisposition, Value> {
    let Some(request) = message.as_object() else {
        return Ok(RequestDisposition::Dispatch);
    };
    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || request
            .get("params")
            .is_some_and(|params| !params.is_object())
    {
        return Ok(RequestDisposition::Dispatch);
    }
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return Ok(RequestDisposition::Dispatch);
    };
    let Some(id) = request.get("id").filter(|id| request_key(id).is_some()) else {
        return Ok(RequestDisposition::Dispatch);
    };

    let carries_modern_claim = carries_modern_envelope_claim(message);
    match *state {
        ConnectionEra::Legacy if carries_modern_claim => {
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "modern request metadata is not valid on a legacy stdio connection",
            ));
        }
        ConnectionEra::Modern if !carries_modern_claim => {
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "modern stdio requests require per-request protocol metadata",
            ));
        }
        ConnectionEra::Legacy if method == "initialize" => {
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "legacy initialization has already completed",
            ));
        }
        ConnectionEra::Legacy | ConnectionEra::Modern => return Ok(RequestDisposition::Dispatch),
        ConnectionEra::LegacyInitializing => {
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "legacy initialization response is still pending",
            ));
        }
        ConnectionEra::LegacyAwaitingInitialized => {
            if !carries_modern_claim && method == "ping" {
                return Ok(RequestDisposition::Dispatch);
            }
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "legacy initialization is incomplete: send notifications/initialized first",
            ));
        }
        ConnectionEra::Opening | ConnectionEra::Probe => {}
    }

    // The router remains the source of truth for envelope validation. Invalid
    // or unsupported claims receive its normal per-request error and do not pin.
    let Ok(era) = super::request_era(message, method, id) else {
        return Ok(RequestDisposition::Dispatch);
    };
    match era {
        super::ProtocolEra::Legacy if method == "initialize" => {
            if let Err(message) = super::legacy_initialize_version(message) {
                return Err(error_response(id.clone(), INVALID_PARAMS, message));
            }
            *state = ConnectionEra::LegacyInitializing;
            return Ok(RequestDisposition::LegacyInitialize);
        }
        super::ProtocolEra::Legacy => {
            return Err(error_response(
                id.clone(),
                INVALID_REQUEST,
                "legacy stdio connections must initialize before other requests",
            ));
        }
        super::ProtocolEra::Modern if method == "server/discover" => {
            *state = ConnectionEra::Probe;
        }
        super::ProtocolEra::Modern => *state = ConnectionEra::Modern,
    }
    Ok(RequestDisposition::Dispatch)
}

fn is_initialized_notification(message: &Value) -> bool {
    let Some(notification) = message.as_object() else {
        return false;
    };
    !notification.contains_key("id")
        && notification.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && notification.get("method").and_then(Value::as_str) == Some("notifications/initialized")
        && notification.get("params").is_none_or(|params| {
            params
                .as_object()
                .is_some_and(|params| params.get("_meta").is_none_or(Value::is_object))
        })
}

fn observe_initialized_notification(state: &mut ConnectionEra, message: &Value) -> bool {
    if !is_initialized_notification(message) {
        return false;
    }
    if *state == ConnectionEra::LegacyAwaitingInitialized {
        *state = ConnectionEra::Legacy;
    }
    true
}

fn request_id_within_limit(value: &Value) -> bool {
    request_key(value).is_some()
        && serialize_bounded(value, MAX_SERIALIZED_REQUEST_ID_BYTES).is_some()
}

const REQUEST_PENDING: u8 = 0;
const REQUEST_COMPLETED: u8 = 1;
const REQUEST_CANCELLED: u8 = 2;

struct RequestControl {
    token: CancelToken,
    state: AtomicU8,
    cancellable: bool,
}

impl RequestControl {
    fn new(cancellable: bool) -> Self {
        Self {
            token: CancelToken::new(),
            state: AtomicU8::new(REQUEST_PENDING),
            cancellable,
        }
    }

    /// Cancellation and completion contend for the same transition. Whichever
    /// wins determines whether a response may be written.
    fn cancel(&self) -> bool {
        if !self.cancellable {
            return false;
        }
        if self
            .state
            .compare_exchange(
                REQUEST_PENDING,
                REQUEST_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.token.cancel();
            true
        } else {
            false
        }
    }

    fn complete(&self) -> bool {
        self.state
            .compare_exchange(
                REQUEST_PENDING,
                REQUEST_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == REQUEST_CANCELLED
    }
}

#[derive(Default)]
struct RequestRegistry {
    requests: Mutex<HashMap<RequestKey, Arc<RequestControl>>>,
}

impl RequestRegistry {
    fn lock(&self) -> MutexGuard<'_, HashMap<RequestKey, Arc<RequestControl>>> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, key: RequestKey, cancellable: bool) -> Option<Arc<RequestControl>> {
        let mut requests = self.lock();
        if requests.contains_key(&key) {
            return None;
        }
        let control = Arc::new(RequestControl::new(cancellable));
        requests.insert(key, Arc::clone(&control));
        Some(control)
    }

    fn cancel(&self, key: &RequestKey) -> bool {
        let control = self.lock().get(key).cloned();
        control.is_some_and(|control| control.cancel())
    }

    fn cancel_all(&self) {
        let controls: Vec<_> = self.lock().values().cloned().collect();
        for control in controls {
            control.cancel();
        }
    }

    fn remove(&self, key: &RequestKey, control: &Arc<RequestControl>) {
        let mut requests = self.lock();
        if requests
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, control))
        {
            requests.remove(key);
        }
    }
}

struct RegisteredRequest {
    key: RequestKey,
    control: Arc<RequestControl>,
}

struct Job {
    request: Value,
    registered: Option<RegisteredRequest>,
    initialization_completion: Option<SyncSender<bool>>,
    _pending_bytes: PendingByteReservation,
}

struct Outbound {
    frame: Vec<u8>,
    registered: Option<RegisteredRequest>,
    initialization_completion: Option<(SyncSender<bool>, bool)>,
}

enum CompletionDisposition {
    Respond(Option<RegisteredRequest>),
    Cancelled,
}

/// Mark request execution complete before its response enters the transport.
///
/// The registration stays installed until the writer consumes the response, so
/// a client cannot reuse an id while its earlier response is still queued. Once
/// completion wins, a late cancellation is a no-op even under stdout
/// backpressure. If cancellation already won, remove the registration and
/// suppress the response.
fn linearize_completion(
    registry: &RequestRegistry,
    registered: Option<RegisteredRequest>,
) -> CompletionDisposition {
    match registered {
        Some(registered) if registered.control.complete() => {
            CompletionDisposition::Respond(Some(registered))
        }
        Some(registered) => {
            registry.remove(&registered.key, &registered.control);
            CompletionDisposition::Cancelled
        }
        None => CompletionDisposition::Respond(None),
    }
}

impl Outbound {
    fn new(response: Value, registered: Option<RegisteredRequest>, max_frame_bytes: usize) -> Self {
        let id = response.get("id").cloned().unwrap_or(Value::Null);
        let frame = serialize_bounded(&response, max_frame_bytes).unwrap_or_else(|| {
            let fallback = error_response_with_data(
                id,
                INTERNAL_ERROR,
                "response exceeds stdio frame limit",
                Some(json!({
                    "maxFrameBytes": max_frame_bytes,
                    "hint": "narrow the query or lower its limit",
                })),
            );
            serialize_bounded(&fallback, max_frame_bytes)
                .or_else(|| {
                    serialize_bounded(
                        &error_response(
                            Value::Null,
                            INTERNAL_ERROR,
                            "response exceeds stdio frame limit",
                        ),
                        max_frame_bytes,
                    )
                })
                .unwrap_or_else(|| {
                    debug_assert!(MINIMAL_OVERSIZE_ERROR.len() <= max_frame_bytes);
                    MINIMAL_OVERSIZE_ERROR.to_vec()
                })
        });
        Self {
            frame,
            registered,
            initialization_completion: None,
        }
    }

    fn with_initialization_completion(
        mut self,
        completion: Option<SyncSender<bool>>,
        succeeded: bool,
    ) -> Self {
        self.initialization_completion = completion.map(|completion| (completion, succeeded));
        self
    }
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next =
            self.bytes.len().checked_add(bytes.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "response is too large")
            })?;
        if next > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response is too large",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_bounded(response: &Value, max_frame_bytes: usize) -> Option<Vec<u8>> {
    let mut buffer = BoundedBuffer {
        bytes: Vec::with_capacity(max_frame_bytes.min(8 * 1024)),
        limit: max_frame_bytes,
    };
    serde_json::to_writer(&mut buffer, response).ok()?;
    Some(buffer.bytes)
}

struct PendingByteBudget {
    limit: usize,
    used: Mutex<usize>,
}

impl PendingByteBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Mutex::new(0),
        })
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<PendingByteReservation> {
        let mut used = self
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = used.checked_add(bytes)?;
        if next > self.limit {
            return None;
        }
        *used = next;
        Some(PendingByteReservation {
            budget: Arc::clone(self),
            bytes,
        })
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        *self
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct PendingByteReservation {
    budget: Arc<PendingByteBudget>,
    bytes: usize,
}

impl Drop for PendingByteReservation {
    fn drop(&mut self) {
        let mut used = self
            .budget
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(*used >= self.bytes);
        *used = used.saturating_sub(self.bytes);
    }
}

struct TokenBucket {
    limit_per_minute: u32,
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(limit_per_minute: u32, now: Instant) -> Self {
        let capacity = f64::from(limit_per_minute.min(MAX_TOOL_BURST));
        Self {
            limit_per_minute,
            capacity,
            tokens: capacity,
            refill_per_second: f64::from(limit_per_minute) / 60.0,
            last_refill: now,
        }
    }

    fn acquire(&mut self, now: Instant) -> Result<(), u64> {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.refill_per_second).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }

        let seconds = (1.0 - self.tokens) / self.refill_per_second;
        let retry = Duration::from_secs_f64(seconds.max(0.0));
        Err(retry.as_millis().max(1) as u64)
    }
}

fn is_cancellation_notification(message: &Value) -> Option<RequestKey> {
    let request = message.as_object()?;
    if request.contains_key("id")
        || request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || request.get("method").and_then(Value::as_str) != Some("notifications/cancelled")
    {
        return None;
    }
    let params = request.get("params")?.as_object()?;
    if params
        .get("reason")
        .is_some_and(|reason| !reason.is_string())
    {
        return None;
    }
    request_key(params.get("requestId")?)
}

fn response_id(request: &Value) -> Value {
    request
        .get("id")
        .filter(|id| request_key(id).is_some())
        .cloned()
        .unwrap_or(Value::Null)
}

fn write_response(writer: &mut impl Write, frame: &[u8]) -> io::Result<()> {
    writer.write_all(frame)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn rate_limit_response(id: Value, bucket: &TokenBucket, retry_after_ms: u64) -> Value {
    error_response_with_data(
        id,
        TOOL_RATE_LIMITED,
        "tool invocation rate limit exceeded",
        Some(json!({
            "limitPerMinute": bucket.limit_per_minute,
            "burst": bucket.capacity as u32,
            "retryAfterMs": retry_after_ms,
        })),
    )
}

fn server_busy_response(id: Value, reason: &str) -> Value {
    error_response_with_data(
        id,
        SERVER_BUSY,
        "server is busy",
        Some(json!({
            "reason": reason,
            "maxPendingRequests": MAX_PENDING_REQUESTS,
            "maxPendingBytes": MAX_PENDING_BYTES,
        })),
    )
}

fn worker_loop<D>(
    receiver: Receiver<Job>,
    outbound: SyncSender<Outbound>,
    registry: Arc<RequestRegistry>,
    shutdown: Arc<AtomicBool>,
    execution: ExecutionContext,
    dispatch: &D,
) -> io::Result<()>
where
    D: Fn(&MemoryEngine, &str, &Value, bool, &CancelToken) -> Option<Value> + Sync,
{
    let ExecutionContext {
        mem,
        region,
        options,
    } = execution;
    let mut limiter = TokenBucket::new(options.tool_rate_limit_per_minute, Instant::now());
    while let Ok(job) = receiver.recv() {
        if job
            .registered
            .as_ref()
            .is_some_and(|registered| registered.control.is_cancelled())
        {
            let registered = job.registered.expect("checked as present");
            registry.remove(&registered.key, &registered.control);
            continue;
        }

        let id = response_id(&job.request);
        let is_tool_call = job.registered.is_some()
            && job.request.get("method").and_then(Value::as_str) == Some("tools/call");
        let response = if is_tool_call {
            match limiter.acquire(Instant::now()) {
                Ok(()) => execute_job(&mem, &region, &job, options, dispatch, id),
                Err(retry_after_ms) => Some(rate_limit_response(id, &limiter, retry_after_ms)),
            }
        } else {
            execute_job(&mem, &region, &job, options, dispatch, id)
        };

        if let Some(response) = response {
            let initialization_succeeded = response.get("result").is_some();
            let registered = match linearize_completion(&registry, job.registered) {
                CompletionDisposition::Respond(registered) => registered,
                CompletionDisposition::Cancelled => {
                    if let Some(completion) = job.initialization_completion {
                        let _ = completion.send(false);
                    }
                    continue;
                }
            };
            if !send_outbound_blocking(
                &outbound,
                &registry,
                &shutdown,
                Outbound::new(response, registered, options.max_frame_bytes)
                    .with_initialization_completion(
                        job.initialization_completion,
                        initialization_succeeded,
                    ),
            ) {
                break;
            }
        } else if let Some(registered) = job.registered {
            registered.control.complete();
            registry.remove(&registered.key, &registered.control);
            if let Some(completion) = job.initialization_completion {
                let _ = completion.send(false);
            }
        } else if let Some(completion) = job.initialization_completion {
            let _ = completion.send(false);
        }
    }
    Ok(())
}

fn writer_loop(
    receiver: Receiver<Outbound>,
    writer: &mut impl Write,
    registry: Arc<RequestRegistry>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    while let Ok(mut outbound) = receiver.recv() {
        let initialization_completion = outbound.initialization_completion.take();
        let registered = outbound.registered.take();
        let write_result = write_response(writer, &outbound.frame);
        if let Some(registered) = registered {
            registry.remove(&registered.key, &registered.control);
        }
        if write_result.is_err() {
            if let Some((completion, _)) = initialization_completion {
                let _ = completion.send(false);
            }
            shutdown.store(true, Ordering::Release);
            registry.cancel_all();
            return Ok(());
        }
        if let Some((completion, succeeded)) = initialization_completion {
            let _ = completion.send(succeeded);
        }
    }
    Ok(())
}

#[cfg(test)]
fn worker_loop_with_writer<W, D>(
    receiver: Receiver<Job>,
    writer: &mut W,
    registry: Arc<RequestRegistry>,
    shutdown: Arc<AtomicBool>,
    execution: ExecutionContext,
    dispatch: &D,
) -> io::Result<()>
where
    W: Write + Send,
    D: Fn(&MemoryEngine, &str, &Value, bool, &CancelToken) -> Option<Value> + Sync,
{
    let (outbound, receiver_outbound) = mpsc::sync_channel(MAX_OUTBOUND_MESSAGES);
    std::thread::scope(|scope| {
        let output = {
            let registry = Arc::clone(&registry);
            let shutdown = Arc::clone(&shutdown);
            scope.spawn(move || writer_loop(receiver_outbound, writer, registry, shutdown))
        };
        let worker = worker_loop(receiver, outbound, registry, shutdown, execution, dispatch);
        let output = output
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("test writer panicked")));
        worker.and(output)
    })
}

fn send_outbound(
    sender: &SyncSender<Outbound>,
    registry: &RequestRegistry,
    shutdown: &AtomicBool,
    outbound: Outbound,
) -> bool {
    match sender.try_send(outbound) {
        Ok(()) => true,
        Err(TrySendError::Disconnected(outbound) | TrySendError::Full(outbound)) => {
            if let Some(registered) = outbound.registered {
                registered.control.cancel();
                registry.remove(&registered.key, &registered.control);
            }
            shutdown.store(true, Ordering::Release);
            registry.cancel_all();
            false
        }
    }
}

/// The execution worker may wait for the single outbound slot: the independent
/// reader remains free to deliver cancellation while stdout is backpressured.
fn send_outbound_blocking(
    sender: &SyncSender<Outbound>,
    registry: &RequestRegistry,
    shutdown: &AtomicBool,
    outbound: Outbound,
) -> bool {
    match sender.send(outbound) {
        Ok(()) => true,
        Err(mpsc::SendError(outbound)) => {
            if let Some(registered) = outbound.registered {
                registered.control.cancel();
                registry.remove(&registered.key, &registered.control);
            }
            shutdown.store(true, Ordering::Release);
            registry.cancel_all();
            false
        }
    }
}

fn execute_job<D>(
    mem: &MemoryEngine,
    region: &str,
    job: &Job,
    options: Options,
    dispatch: &D,
    id: Value,
) -> Option<Value>
where
    D: Fn(&MemoryEngine, &str, &Value, bool, &CancelToken) -> Option<Value> + Sync,
{
    let token = job
        .registered
        .as_ref()
        .map(|registered| registered.control.token.clone())
        .unwrap_or_default();
    match catch_unwind(AssertUnwindSafe(|| {
        mem.with_cancel_token(token.clone(), |mem| {
            mem.with_read_limits(memory_read_limits(options.max_frame_bytes), |mem| {
                dispatch(
                    mem,
                    region,
                    &job.request,
                    options.allow_protected_memory_erasure,
                    &token,
                )
            })
        })
    })) {
        Ok(response) => response,
        Err(_) => {
            eprintln!("citadeldb-mcp: request handler panicked");
            Some(error_response(id, INTERNAL_ERROR, "internal server error"))
        }
    }
}

fn memory_read_limits(max_frame_bytes: usize) -> MemoryReadLimits {
    // Reserve space for JSON structure and escaping. Serialization still
    // enforces the exact frame bound after the memory engine returns.
    let caller_content = (max_frame_bytes / 2).max(1);
    MemoryReadLimits::new(
        caller_content,
        max_frame_bytes.saturating_mul(16).max(caller_content),
        caller_content,
    )
}

fn enqueue(
    sender: &SyncSender<Job>,
    outbound: &SyncSender<Outbound>,
    registry: &RequestRegistry,
    shutdown: &AtomicBool,
    max_frame_bytes: usize,
    job: Job,
) -> io::Result<bool> {
    match sender.try_send(job) {
        Ok(()) => Ok(true),
        Err(TrySendError::Disconnected(_)) => Ok(false),
        Err(TrySendError::Full(job)) => {
            let id = response_id(&job.request);
            let response = server_busy_response(id, "request queue is full");
            let registered = match linearize_completion(registry, job.registered) {
                CompletionDisposition::Respond(registered) => registered,
                CompletionDisposition::Cancelled => {
                    if let Some(completion) = job.initialization_completion {
                        let _ = completion.send(false);
                    }
                    return Ok(true);
                }
            };
            Ok(send_outbound(
                outbound,
                registry,
                shutdown,
                Outbound::new(response, registered, max_frame_bytes)
                    .with_initialization_completion(job.initialization_completion, false),
            ))
        }
    }
}

fn reader_loop(
    reader: &mut impl BufRead,
    sender: &SyncSender<Job>,
    outbound: &SyncSender<Outbound>,
    registry: &RequestRegistry,
    shutdown: &AtomicBool,
    pending_bytes: &Arc<PendingByteBudget>,
    max_frame_bytes: usize,
) -> io::Result<()> {
    let mut connection_era = ConnectionEra::Opening;
    while !shutdown.load(Ordering::Acquire) {
        let frame = read_frame(reader, max_frame_bytes)?;
        let bytes = match frame {
            Frame::Eof => return Ok(()),
            Frame::Oversized => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stdio frame exceeds {max_frame_bytes} bytes"),
                ));
            }
            Frame::Line(bytes) => bytes,
        };
        let Ok(line) = std::str::from_utf8(&bytes) else {
            let response = error_response(Value::Null, PARSE_ERROR, "parse error: invalid JSON");
            if !send_outbound(
                outbound,
                registry,
                shutdown,
                Outbound::new(response, None, max_frame_bytes),
            ) {
                return Ok(());
            }
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        let message = match parse_message(line) {
            Ok(message) => message,
            Err(response) => {
                if !send_outbound(
                    outbound,
                    registry,
                    shutdown,
                    Outbound::new(response, None, max_frame_bytes),
                ) {
                    return Ok(());
                }
                continue;
            }
        };

        if message
            .get("id")
            .is_some_and(|id| request_key(id).is_some() && !request_id_within_limit(id))
        {
            let message = format!(
                "invalid request: serialized id exceeds {MAX_SERIALIZED_REQUEST_ID_BYTES} bytes"
            );
            let response = error_response(Value::Null, INVALID_REQUEST, &message);
            if !send_outbound(
                outbound,
                registry,
                shutdown,
                Outbound::new(response, None, max_frame_bytes),
            ) {
                return Ok(());
            }
            continue;
        }

        if message.get("id").is_none()
            && message.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
        {
            if let Some(key) = is_cancellation_notification(&message) {
                registry.cancel(&key);
            }
            continue;
        }

        if observe_initialized_notification(&mut connection_era, &message) {
            continue;
        }

        // Valid notifications never execute tools and never receive responses.
        // Cancellation was handled above because it alone mutates transport state.
        if message.get("id").is_none()
            && message.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
            && message.get("method").is_some_and(Value::is_string)
            && message
                .get("params")
                .is_none_or(|params| params.is_object())
        {
            continue;
        }

        let previous_era = connection_era;
        let disposition = match enforce_connection_era(&mut connection_era, &message) {
            Ok(disposition) => disposition,
            Err(response) => {
                if !send_outbound(
                    outbound,
                    registry,
                    shutdown,
                    Outbound::new(response, None, max_frame_bytes),
                ) {
                    return Ok(());
                }
                continue;
            }
        };

        let registered = if let Some(key) = message.get("id").and_then(request_key) {
            let cancellable = message.get("method").and_then(Value::as_str) != Some("initialize");
            let Some(control) = registry.register(key.clone(), cancellable) else {
                connection_era = previous_era;
                let response = error_response(
                    Value::Null,
                    INVALID_REQUEST,
                    "invalid request: duplicate in-flight id",
                );
                if !send_outbound(
                    outbound,
                    registry,
                    shutdown,
                    Outbound::new(response, None, max_frame_bytes),
                ) {
                    return Ok(());
                }
                continue;
            };
            Some(RegisteredRequest { key, control })
        } else {
            None
        };
        let Some(pending_bytes) = pending_bytes.reserve(bytes.len()) else {
            connection_era = previous_era;
            let response =
                server_busy_response(response_id(&message), "pending byte budget exceeded");
            let registered = match linearize_completion(registry, registered) {
                CompletionDisposition::Respond(registered) => registered,
                CompletionDisposition::Cancelled => continue,
            };
            if !send_outbound(
                outbound,
                registry,
                shutdown,
                Outbound::new(response, registered, max_frame_bytes),
            ) {
                return Ok(());
            }
            continue;
        };
        let (initialization_completion, initialization_result) =
            if disposition == RequestDisposition::LegacyInitialize {
                let (completion, result) = mpsc::sync_channel(1);
                (Some(completion), Some(result))
            } else {
                (None, None)
            };
        if !enqueue(
            sender,
            outbound,
            registry,
            shutdown,
            max_frame_bytes,
            Job {
                request: message,
                registered,
                initialization_completion,
                _pending_bytes: pending_bytes,
            },
        )? {
            return Ok(());
        }
        if let Some(initialization_result) = initialization_result {
            match initialization_result.recv() {
                Ok(true) => connection_era = ConnectionEra::LegacyAwaitingInitialized,
                Ok(false) => connection_era = previous_era,
                Err(_) => return Ok(()),
            }
        }
    }
    Ok(())
}

fn serve_io<R, W, D>(
    reader: &mut R,
    writer: &mut W,
    mem: Arc<MemoryEngine>,
    region: &str,
    options: Options,
    dispatch: &D,
) -> io::Result<()>
where
    R: BufRead,
    W: Write + Send,
    D: Fn(&MemoryEngine, &str, &Value, bool, &CancelToken) -> Option<Value> + Sync,
{
    if options.max_frame_bytes < MIN_FRAME_BYTES || options.tool_rate_limit_per_minute == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "stdio frame limit must be at least 512 bytes and tool rate limit must be non-zero",
        ));
    }

    let registry = Arc::new(RequestRegistry::default());
    let shutdown = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::sync_channel(MAX_PENDING_REQUESTS);
    let (outbound, outbound_receiver) = mpsc::sync_channel(MAX_OUTBOUND_MESSAGES);
    let pending_bytes = PendingByteBudget::new(MAX_PENDING_BYTES);
    let region = region.to_owned();

    std::thread::scope(|scope| {
        let worker = {
            let registry = Arc::clone(&registry);
            let shutdown = Arc::clone(&shutdown);
            let worker_outbound = outbound.clone();
            scope.spawn(move || {
                worker_loop(
                    receiver,
                    worker_outbound,
                    registry,
                    shutdown,
                    ExecutionContext {
                        mem,
                        region,
                        options,
                    },
                    dispatch,
                )
            })
        };
        let output = {
            let registry = Arc::clone(&registry);
            let shutdown = Arc::clone(&shutdown);
            scope.spawn(move || writer_loop(outbound_receiver, writer, registry, shutdown))
        };

        let read_result = reader_loop(
            reader,
            &sender,
            &outbound,
            &registry,
            &shutdown,
            &pending_bytes,
            options.max_frame_bytes,
        );
        registry.cancel_all();
        drop(sender);
        drop(outbound);

        let worker_result = worker
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("stdio worker panicked")));
        let output_result = output
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("stdio writer panicked")));
        read_result.and(worker_result).and(output_result)
    })
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor, Read};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Condvar, Mutex};

    use citadel::{Argon2Profile, DatabaseBuilder};
    use citadel_mem::{AtomInput, MockEmbedder};

    use super::*;

    fn engine() -> (tempfile::TempDir, Arc<MemoryEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("stdio.cdl"))
            .passphrase(b"stdio-tests")
            .argon2_profile(Argon2Profile::Iot)
            .create()
            .unwrap();
        let mem = Arc::new(MemoryEngine::open(Arc::new(db)).unwrap());
        mem.create_region("r", Arc::new(MockEmbedder::new(8)))
            .unwrap();
        (dir, mem)
    }

    fn test_options(rate: u32) -> Options {
        Options {
            max_frame_bytes: 1024,
            tool_rate_limit_per_minute: rate,
            allow_protected_memory_erasure: false,
        }
    }

    fn registered_job(registry: &RequestRegistry, id: Value, method: &str) -> Job {
        let key = request_key(&id).unwrap();
        let control = registry
            .register(key.clone(), method != "initialize")
            .unwrap();
        let request = json!({"jsonrpc": "2.0", "id": id, "method": method});
        let bytes = request.to_string().len();
        Job {
            request,
            registered: Some(RegisteredRequest { key, control }),
            initialization_completion: None,
            _pending_bytes: PendingByteBudget::new(bytes).reserve(bytes).unwrap(),
        }
    }

    fn responses(output: &[u8]) -> Vec<Value> {
        std::str::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn default_frame_limit_matches_the_official_typescript_sdk() {
        assert_eq!(DEFAULT_MAX_FRAME_BYTES, 10 * 1024 * 1024);
    }

    #[test]
    fn application_error_codes_are_distinct_and_outside_the_reserved_range() {
        assert_eq!(TOOL_RATE_LIMITED, -31999);
        assert_eq!(SERVER_BUSY, -31998);
        assert_ne!(TOOL_RATE_LIMITED, SERVER_BUSY);
    }

    #[test]
    fn pending_byte_reservations_release_when_jobs_drop() {
        let budget = PendingByteBudget::new(32);
        let job = Job {
            request: json!({}),
            registered: None,
            initialization_completion: None,
            _pending_bytes: budget.reserve(24).unwrap(),
        };
        assert_eq!(budget.used(), 24);
        assert!(budget.reserve(9).is_none());
        drop(job);
        assert_eq!(budget.used(), 0);
        assert!(budget.reserve(32).is_some());
    }

    #[test]
    fn rejected_byte_reservations_never_accumulate() {
        let budget = PendingByteBudget::new(32);
        for _ in 0..100 {
            assert!(budget.reserve(33).is_none());
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn bounded_reader_accepts_the_limit_and_fails_closed_after_overflow() {
        let mut exact = BufReader::with_capacity(2, Cursor::new(b"12345678\n"));
        assert_eq!(
            read_frame(&mut exact, 8).unwrap(),
            Frame::Line(b"12345678".to_vec())
        );
        assert_eq!(read_frame(&mut exact, 8).unwrap(), Frame::Eof);

        let mut overflow =
            BufReader::with_capacity(3, Cursor::new(b"123456789012345\n{\"ok\":true}\n"));
        assert_eq!(read_frame(&mut overflow, 8).unwrap(), Frame::Oversized);
    }

    #[test]
    fn bounded_reader_rejects_an_unterminated_oversized_frame() {
        let mut reader = BufReader::with_capacity(2, Cursor::new(b"123456789"));
        assert_eq!(read_frame(&mut reader, 8).unwrap(), Frame::Oversized);
    }

    #[test]
    fn bounded_reader_rejects_a_partial_frame_at_eof() {
        let mut reader = BufReader::with_capacity(2, Cursor::new(b"{}"));
        let error = read_frame(&mut reader, 8).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn cancellation_parser_accepts_request_ids_and_ignores_malformed_notifications() {
        for id in [
            json!(0),
            json!(-1),
            json!(-2.0),
            json!(1.0),
            json!(u64::MAX),
            json!(""),
            json!("call-1"),
        ] {
            let message = json!({
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": id.clone(), "reason": "stop"}
            });
            assert_eq!(
                is_cancellation_notification(&message),
                request_key(&id),
                "{id}"
            );
        }

        for message in [
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":true}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":-2.5}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1.5}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9_007_199_254_740_992.0}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"reason":7}}),
            json!({"jsonrpc":"1.0","method":"notifications/cancelled","params":{"requestId":1}}),
            json!({"jsonrpc":"2.0","id":9,"method":"notifications/cancelled","params":{"requestId":1}}),
        ] {
            assert!(
                is_cancellation_notification(&message).is_none(),
                "{message}"
            );
        }
        assert_eq!(request_key(&json!(1.0)), request_key(&json!(1)));
        assert!(request_id_within_limit(&json!(1.0)));
        assert!(!request_id_within_limit(&json!(1.5)));
        assert!(!request_id_within_limit(&json!(9_007_199_254_740_992.0)));
    }

    fn modern_request(id: i64, method: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        })
    }

    fn legacy_initialize(id: i64) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"}
            }
        })
    }

    #[test]
    fn stdio_probe_allows_fallback_then_pins_one_era() {
        let mut era = ConnectionEra::Opening;
        assert_eq!(
            enforce_connection_era(&mut era, &modern_request(1, "server/discover")).unwrap(),
            RequestDisposition::Dispatch
        );
        assert_eq!(era, ConnectionEra::Probe);

        assert_eq!(
            enforce_connection_era(&mut era, &legacy_initialize(2)).unwrap(),
            RequestDisposition::LegacyInitialize
        );
        assert_eq!(era, ConnectionEra::LegacyInitializing);
        era = ConnectionEra::LegacyAwaitingInitialized;
        assert!(observe_initialized_notification(
            &mut era,
            &json!({
                "jsonrpc":"2.0",
                "method":"notifications/initialized",
                "params":{"_meta":{"client":"ready"},"x-client-extension":true}
            })
        ));
        assert_eq!(era, ConnectionEra::Legacy);
        assert_eq!(
            enforce_connection_era(&mut era, &json!({"jsonrpc":"2.0","id":3,"method":"ping"}))
                .unwrap(),
            RequestDisposition::Dispatch
        );
        let duplicate = enforce_connection_era(&mut era, &legacy_initialize(3)).unwrap_err();
        assert_eq!(duplicate["error"]["code"], INVALID_REQUEST);
        assert_eq!(era, ConnectionEra::Legacy);
        let mismatch =
            enforce_connection_era(&mut era, &modern_request(4, "tools/list")).unwrap_err();
        assert_eq!(mismatch["error"]["code"], INVALID_REQUEST);
        assert_eq!(era, ConnectionEra::Legacy);
    }

    #[test]
    fn legacy_requests_cannot_run_before_a_successful_handshake() {
        let mut era = ConnectionEra::Opening;
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/list"});
        let response = enforce_connection_era(&mut era, &request).unwrap_err();
        assert_eq!(response["error"]["code"], INVALID_REQUEST);
        assert_eq!(era, ConnectionEra::Opening);

        let mut invalid = legacy_initialize(2);
        invalid["params"]
            .as_object_mut()
            .unwrap()
            .remove("clientInfo");
        let response = enforce_connection_era(&mut era, &invalid).unwrap_err();
        assert_eq!(response["error"]["code"], INVALID_PARAMS);
        assert_eq!(era, ConnectionEra::Opening);
    }

    #[test]
    fn stdio_legacy_handshake_gates_execution_until_initialized() {
        let (_dir, mem) = engine();
        let input = format!(
            "{}\n{}\n",
            legacy_initialize(1),
            json!({
                "jsonrpc":"2.0",
                "method":"notifications/initialized",
                "params":{"_meta":{"client":"ready"}}
            })
        );
        let mut reader = Cursor::new(input.into_bytes());
        let mut output = Vec::new();
        serve_io(
            &mut reader,
            &mut output,
            mem,
            "r",
            Options {
                max_frame_bytes: 16 * 1024,
                ..test_options(120)
            },
            &|mem, region, request, allow_erasure, _cancel| {
                super::super::dispatch_with_policy(mem, region, request, allow_erasure)
            },
        )
        .unwrap();

        let output = responses(&output);
        assert_eq!(output.len(), 1, "{output:?}");
        assert_eq!(output[0]["id"], 1);
        assert_eq!(output[0]["result"]["protocolVersion"], "2025-11-25");
    }

    #[test]
    fn stdio_rejects_pre_initialize_work_without_dispatching_it() {
        let (_dir, mem) = engine();
        let mut reader = Cursor::new(
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"mem_forget\",\"arguments\":{\"ids\":[1]}}}\n"
                .to_vec(),
        );
        let calls = AtomicUsize::new(0);
        let mut output = Vec::new();
        serve_io(
            &mut reader,
            &mut output,
            mem,
            "r",
            test_options(120),
            &|_, _, _, _, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(json!({"jsonrpc":"2.0","id":1,"result":{}}))
            },
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["error"]["code"], INVALID_REQUEST);
    }

    #[test]
    fn stdio_rejects_a_fractional_request_id_without_registering_it() {
        let (_dir, mem) = engine();
        let mut reader =
            Cursor::new(b"{\"jsonrpc\":\"2.0\",\"id\":1.5,\"method\":\"ping\"}\n".to_vec());
        let mut output = Vec::new();
        serve_io(
            &mut reader,
            &mut output,
            mem,
            "r",
            test_options(120),
            &|mem, region, request, allow_erasure, _cancel| {
                super::super::dispatch_with_policy(mem, region, request, allow_erasure)
            },
        )
        .unwrap();

        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert!(output[0]["id"].is_null());
        assert_eq!(output[0]["error"]["code"], INVALID_REQUEST);
    }

    #[test]
    fn stdio_modern_commit_rejects_a_late_legacy_handshake() {
        let mut era = ConnectionEra::Opening;
        enforce_connection_era(&mut era, &modern_request(1, "server/discover")).unwrap();
        enforce_connection_era(&mut era, &modern_request(2, "tools/list")).unwrap();
        assert_eq!(era, ConnectionEra::Modern);

        let mismatch = enforce_connection_era(&mut era, &legacy_initialize(3)).unwrap_err();
        assert_eq!(mismatch["error"]["code"], INVALID_REQUEST);
        assert_eq!(era, ConnectionEra::Modern);
    }

    #[test]
    fn duplicate_ids_never_replace_tokens_and_ids_are_reusable_after_cleanup() {
        let registry = RequestRegistry::default();
        let numeric = RequestKey::Integer("1".to_owned());
        let first = registry.register(numeric.clone(), true).unwrap();
        assert!(registry.register(numeric.clone(), true).is_none());
        assert!(registry
            .register(RequestKey::String("1".to_string()), true)
            .is_some());
        assert!(registry.cancel(&numeric));
        assert!(first.token.is_cancelled());
        assert!(
            !registry.cancel(&numeric),
            "duplicate cancellation is a no-op"
        );
        registry.remove(&numeric, &first);
        assert!(registry.register(numeric, true).is_some());
    }

    #[test]
    fn completion_and_cancellation_have_one_atomic_winner() {
        let completion_wins = RequestControl::new(true);
        assert!(completion_wins.complete());
        assert!(!completion_wins.cancel());
        assert!(!completion_wins.token.is_cancelled());

        let cancellation_wins = RequestControl::new(true);
        assert!(cancellation_wins.cancel());
        assert!(!cancellation_wins.complete());
        assert!(cancellation_wins.token.is_cancelled());

        let initialize = RequestControl::new(false);
        assert!(!initialize.cancel());
        assert!(!initialize.token.is_cancelled());
        assert!(initialize.complete());
    }

    #[test]
    fn handler_completion_survives_outbound_backpressure_and_late_cancellation() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(1);
        let (outbound, receiver_outbound) = mpsc::sync_channel(1);
        let (handler_returned, observed_handler_return) = mpsc::sync_channel(1);

        // Fill the only outbound slot so the worker cannot publish the durable
        // result until this test explicitly releases the transport.
        outbound
            .send(Outbound::new(
                json!({"jsonrpc":"2.0","id":"blocker","result":{}}),
                None,
                1024,
            ))
            .unwrap();

        let job = registered_job(&registry, json!(1), "tools/call");
        let control = Arc::clone(&job.registered.as_ref().unwrap().control);
        let dispatch = |mem: &MemoryEngine, _: &str, request: &Value, _: bool, _: &CancelToken| {
            let atom_id = mem
                .remember("r", AtomInput::new("fact", "durable before transport"))
                .unwrap();
            handler_returned.send(atom_id).unwrap();
            Some(json!({
                "jsonrpc":"2.0",
                "id": request["id"],
                "result": {"atom_id": atom_id}
            }))
        };

        let (state_after_handler, late_cancellation_won, atom_id, response) =
            std::thread::scope(|scope| {
                let worker = {
                    let registry = Arc::clone(&registry);
                    let shutdown = Arc::clone(&shutdown);
                    let mem = Arc::clone(&mem);
                    scope.spawn(move || {
                        worker_loop(
                            receiver,
                            outbound,
                            registry,
                            shutdown,
                            ExecutionContext {
                                mem,
                                region: "r".to_string(),
                                options: test_options(120),
                            },
                            &dispatch,
                        )
                    })
                };

                sender.send(job).unwrap();
                let atom_id = observed_handler_return
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap();

                let deadline = Instant::now() + Duration::from_secs(2);
                while control.state.load(Ordering::Acquire) == REQUEST_PENDING
                    && Instant::now() < deadline
                {
                    std::thread::yield_now();
                }
                let state_after_handler = control.state.load(Ordering::Acquire);
                let late_cancellation_won = registry.cancel(&RequestKey::Integer("1".to_owned()));

                let blocker = receiver_outbound.recv().unwrap();
                assert_eq!(
                    serde_json::from_slice::<Value>(&blocker.frame).unwrap()["id"],
                    "blocker"
                );
                let response = receiver_outbound
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap();
                let registered = response.registered.as_ref().unwrap();
                registry.remove(&registered.key, &registered.control);
                let response: Value = serde_json::from_slice(&response.frame).unwrap();

                drop(sender);
                worker.join().unwrap().unwrap();
                (
                    state_after_handler,
                    late_cancellation_won,
                    atom_id,
                    response,
                )
            });
        assert_eq!(
            state_after_handler, REQUEST_COMPLETED,
            "handler return did not linearize completion before transport"
        );
        assert!(
            !late_cancellation_won,
            "late cancellation suppressed a completed durable write"
        );
        assert_eq!(mem.count("r", "fact").unwrap(), 1);
        assert!(mem.fetch_one("r", atom_id).unwrap().is_some());
        assert_eq!(response["result"]["atom_id"], atom_id);
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn queue_full_busy_response_completes_and_releases_the_request() {
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(1);
        let (outbound, receiver_outbound) = mpsc::sync_channel(1);
        let filler_budget = PendingByteBudget::new(1);
        sender
            .send(Job {
                request: json!({}),
                registered: None,
                initialization_completion: None,
                _pending_bytes: filler_budget.reserve(1).unwrap(),
            })
            .unwrap();

        let job = registered_job(&registry, json!(1), "tools/call");
        let control = Arc::clone(&job.registered.as_ref().unwrap().control);
        assert!(enqueue(&sender, &outbound, &registry, &shutdown, 1024, job,).unwrap());
        assert_eq!(control.state.load(Ordering::Acquire), REQUEST_COMPLETED);
        assert!(!registry.cancel(&RequestKey::Integer("1".to_owned())));

        drop(outbound);
        let mut output = Vec::new();
        writer_loop(
            receiver_outbound,
            &mut output,
            Arc::clone(&registry),
            Arc::clone(&shutdown),
        )
        .unwrap();
        drop(sender);
        drop(receiver.recv().unwrap());

        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["id"], 1);
        assert_eq!(output[0]["error"]["code"], SERVER_BUSY);
        assert_eq!(
            output[0]["error"]["data"]["reason"],
            "request queue is full"
        );
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn pending_byte_busy_response_completes_and_releases_the_request() {
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let pending_bytes = PendingByteBudget::new(0);
        let (sender, _receiver) = mpsc::sync_channel(1);
        let (outbound, receiver_outbound) = mpsc::sync_channel(1);
        let input = format!("{}\n", modern_request(1, "server/discover"));
        let mut reader = Cursor::new(input.into_bytes());

        reader_loop(
            &mut reader,
            &sender,
            &outbound,
            &registry,
            &shutdown,
            &pending_bytes,
            1024,
        )
        .unwrap();
        let key = RequestKey::Integer("1".to_owned());
        let control = registry.lock().get(&key).cloned().unwrap();
        assert_eq!(control.state.load(Ordering::Acquire), REQUEST_COMPLETED);
        assert!(!registry.cancel(&key));

        drop(outbound);
        let mut output = Vec::new();
        writer_loop(
            receiver_outbound,
            &mut output,
            Arc::clone(&registry),
            Arc::clone(&shutdown),
        )
        .unwrap();

        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["id"], 1);
        assert_eq!(output[0]["error"]["code"], SERVER_BUSY);
        assert_eq!(
            output[0]["error"]["data"]["reason"],
            "pending byte budget exceeded"
        );
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn token_bucket_has_a_bounded_burst_and_exact_refill() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(DEFAULT_TOOL_RATE_LIMIT_PER_MINUTE, start);
        assert_eq!(bucket.capacity as u32, MAX_TOOL_BURST);
        for _ in 0..MAX_TOOL_BURST {
            assert_eq!(bucket.acquire(start), Ok(()));
        }
        assert_eq!(bucket.acquire(start), Err(500));
        assert_eq!(bucket.acquire(start + Duration::from_millis(500)), Ok(()));
    }

    #[test]
    fn a_running_request_can_be_cancelled_and_the_next_request_still_runs() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(4);
        let (started_sender, started_receiver) = mpsc::sync_channel(0);
        let calls = AtomicUsize::new(0);
        let dispatch =
            |_: &MemoryEngine, _: &str, request: &Value, _: bool, token: &CancelToken| {
                calls.fetch_add(1, Ordering::Relaxed);
                if request["id"] == json!(1) {
                    started_sender.send(()).unwrap();
                    while !token.is_cancelled() {
                        std::thread::yield_now();
                    }
                }
                Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{}}))
            };
        let mut output = Vec::new();
        std::thread::scope(|scope| {
            let output = &mut output;
            let worker = {
                let registry = Arc::clone(&registry);
                let shutdown = Arc::clone(&shutdown);
                let mem = Arc::clone(&mem);
                scope.spawn(move || {
                    worker_loop_with_writer(
                        receiver,
                        output,
                        registry,
                        shutdown,
                        ExecutionContext {
                            mem,
                            region: "r".to_string(),
                            options: test_options(120),
                        },
                        &dispatch,
                    )
                })
            };

            sender
                .send(registered_job(&registry, json!(1), "test/block"))
                .unwrap();
            started_receiver.recv().unwrap();
            assert!(registry.cancel(&RequestKey::Integer("1".to_owned())));
            sender
                .send(registered_job(&registry, json!(2), "test/next"))
                .unwrap();
            drop(sender);
            worker.join().unwrap().unwrap();
        });

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["id"], 2);
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn queued_cancellation_skips_dispatch_without_affecting_the_running_request() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(4);
        let (started_sender, started_receiver) = mpsc::sync_channel(0);
        let release = Arc::new(AtomicBool::new(false));
        let calls = AtomicUsize::new(0);
        let dispatch = |_: &MemoryEngine, _: &str, request: &Value, _: bool, _: &CancelToken| {
            calls.fetch_add(1, Ordering::Relaxed);
            if request["id"] == json!(1) {
                started_sender.send(()).unwrap();
                while !release.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
            }
            Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{}}))
        };
        let mut output = Vec::new();
        std::thread::scope(|scope| {
            let output = &mut output;
            let worker = {
                let registry = Arc::clone(&registry);
                let shutdown = Arc::clone(&shutdown);
                let mem = Arc::clone(&mem);
                scope.spawn(move || {
                    worker_loop_with_writer(
                        receiver,
                        output,
                        registry,
                        shutdown,
                        ExecutionContext {
                            mem,
                            region: "r".to_string(),
                            options: test_options(120),
                        },
                        &dispatch,
                    )
                })
            };

            sender
                .send(registered_job(&registry, json!(1), "test/block"))
                .unwrap();
            started_receiver.recv().unwrap();
            sender
                .send(registered_job(&registry, json!(2), "test/queued"))
                .unwrap();
            assert!(registry.cancel(&RequestKey::Integer("2".to_owned())));
            release.store(true, Ordering::Release);
            drop(sender);
            worker.join().unwrap().unwrap();
        });

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let output = responses(&output);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["id"], 1);
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn handler_panic_is_sanitized_and_does_not_poison_the_next_request() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(4);
        let dispatch = |_: &MemoryEngine, _: &str, request: &Value, _: bool, _: &CancelToken| {
            if request["id"] == json!(1) {
                panic!("sensitive panic detail")
            }
            Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{}}))
        };
        let mut output = Vec::new();
        std::thread::scope(|scope| {
            let output = &mut output;
            let worker = {
                let registry = Arc::clone(&registry);
                let shutdown = Arc::clone(&shutdown);
                let mem = Arc::clone(&mem);
                scope.spawn(move || {
                    worker_loop_with_writer(
                        receiver,
                        output,
                        registry,
                        shutdown,
                        ExecutionContext {
                            mem,
                            region: "r".to_string(),
                            options: test_options(120),
                        },
                        &dispatch,
                    )
                })
            };
            sender
                .send(registered_job(&registry, json!(1), "test/panic"))
                .unwrap();
            sender
                .send(registered_job(&registry, json!(2), "test/next"))
                .unwrap();
            drop(sender);
            worker.join().unwrap().unwrap();
        });

        let output = responses(&output);
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["error"]["code"], INTERNAL_ERROR);
        assert_eq!(output[0]["error"]["message"], "internal server error");
        assert!(!output[0].to_string().contains("sensitive"));
        assert_eq!(output[1]["id"], 2);
        assert!(output[1].get("result").is_some());
        assert!(registry.lock().is_empty());
    }

    #[test]
    fn oversized_response_becomes_a_bounded_error_and_the_next_request_runs() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(4);
        let dispatch = |_: &MemoryEngine, _: &str, request: &Value, _: bool, _: &CancelToken| {
            let result = if request["id"] == json!(1) {
                json!({"text": "x".repeat(4096)})
            } else {
                json!({"ok": true})
            };
            Some(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        };
        let mut output = Vec::new();
        let mut options = test_options(120);
        options.max_frame_bytes = MIN_FRAME_BYTES;
        std::thread::scope(|scope| {
            let output = &mut output;
            let worker = {
                let registry = Arc::clone(&registry);
                let shutdown = Arc::clone(&shutdown);
                let mem = Arc::clone(&mem);
                scope.spawn(move || {
                    worker_loop_with_writer(
                        receiver,
                        output,
                        registry,
                        shutdown,
                        ExecutionContext {
                            mem,
                            region: "r".to_string(),
                            options,
                        },
                        &dispatch,
                    )
                })
            };
            sender
                .send(registered_job(&registry, json!(1), "test/large"))
                .unwrap();
            sender
                .send(registered_job(&registry, json!(2), "test/next"))
                .unwrap();
            drop(sender);
            worker.join().unwrap().unwrap();
        });

        assert!(output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .all(|line| line.len() <= MIN_FRAME_BYTES));
        let output = responses(&output);
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["id"], 1);
        assert_eq!(output[0]["error"]["code"], INTERNAL_ERROR);
        assert_eq!(
            output[0]["error"]["message"],
            "response exceeds stdio frame limit"
        );
        assert_eq!(output[0]["error"]["data"]["maxFrameBytes"], MIN_FRAME_BYTES);
        assert_eq!(output[1]["id"], 2);
        assert!(output[1].get("result").is_some());
    }

    #[test]
    fn memory_reads_fail_inside_the_engine_before_the_transport_fallback() {
        let (_dir, mem) = engine();
        let atom = mem
            .remember("r", AtomInput::new("fact", "x".repeat(4096)))
            .unwrap();
        let registry = RequestRegistry::default();
        let mut job = registered_job(&registry, json!(1), "tools/call");
        job.request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "mem_fetch",
                "arguments": {"limit": 1}
            }
        });
        let options = test_options(120);

        let response = execute_job(
            &mem,
            "r",
            &job,
            options,
            &|mem, region, request, allow_erasure, _cancel| {
                super::super::dispatch_with_policy(mem, region, request, allow_erasure)
            },
            json!(1),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("memory read limit exceeded"));

        let outbound = Outbound::new(response, job.registered, options.max_frame_bytes);
        assert!(outbound.frame.len() <= options.max_frame_bytes);
        let response: Value = serde_json::from_slice(&outbound.frame).unwrap();
        assert_ne!(
            response["error"]["message"],
            "response exceeds stdio frame limit"
        );

        let mut resource_job = registered_job(&registry, json!(2), "resources/read");
        resource_job.request = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "resources/read",
            "params": {"uri": format!("memory://atom/{atom}")}
        });
        let response = execute_job(
            &mem,
            "r",
            &resource_job,
            options,
            &|mem, region, request, allow_erasure, _cancel| {
                super::super::dispatch_with_policy(mem, region, request, allow_erasure)
            },
            json!(2),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], INTERNAL_ERROR);
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("memory read limit exceeded"));
        let outbound = Outbound::new(response, resource_job.registered, options.max_frame_bytes);
        assert!(outbound.frame.len() <= options.max_frame_bytes);
    }

    #[test]
    fn oversized_response_with_an_unbounded_id_never_panics_or_exceeds_the_frame() {
        let admitted = json!("x".repeat(MAX_SERIALIZED_REQUEST_ID_BYTES - 2));
        assert!(request_id_within_limit(&admitted));
        let correlated = Outbound::new(
            json!({
                "jsonrpc": "2.0",
                "id": admitted,
                "result": {"text": "x".repeat(4096)}
            }),
            None,
            MIN_FRAME_BYTES,
        );
        let correlated: Value = serde_json::from_slice(&correlated.frame).unwrap();
        assert_eq!(
            correlated["id"],
            "x".repeat(MAX_SERIALIZED_REQUEST_ID_BYTES - 2)
        );
        assert_eq!(correlated["error"]["code"], INTERNAL_ERROR);

        let unbounded = json!("x".repeat(MIN_FRAME_BYTES));
        assert!(!request_id_within_limit(&unbounded));
        let fallback = Outbound::new(
            json!({
                "jsonrpc": "2.0",
                "id": unbounded,
                "result": {"text": "x".repeat(4096)}
            }),
            None,
            MIN_FRAME_BYTES,
        );
        assert!(fallback.frame.len() <= MIN_FRAME_BYTES);
        let fallback: Value = serde_json::from_slice(&fallback.frame).unwrap();
        assert!(fallback["id"].is_null());
        assert_eq!(fallback["error"]["code"], INTERNAL_ERROR);
    }

    #[test]
    fn rate_limit_applies_only_to_tool_invocations() {
        let (_dir, mem) = engine();
        let registry = Arc::new(RequestRegistry::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(4);
        let dispatch = |_: &MemoryEngine, _: &str, request: &Value, _: bool, _: &CancelToken| {
            Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{}}))
        };
        let mut output = Vec::new();
        std::thread::scope(|scope| {
            let output = &mut output;
            let worker = {
                let registry = Arc::clone(&registry);
                let shutdown = Arc::clone(&shutdown);
                let mem = Arc::clone(&mem);
                scope.spawn(move || {
                    worker_loop_with_writer(
                        receiver,
                        output,
                        registry,
                        shutdown,
                        ExecutionContext {
                            mem,
                            region: "r".to_string(),
                            options: test_options(1),
                        },
                        &dispatch,
                    )
                })
            };
            sender
                .send(registered_job(&registry, json!(1), "tools/call"))
                .unwrap();
            sender
                .send(registered_job(&registry, json!(2), "resources/list"))
                .unwrap();
            sender
                .send(registered_job(&registry, json!(3), "tools/call"))
                .unwrap();
            drop(sender);
            worker.join().unwrap().unwrap();
        });

        let output = responses(&output);
        assert_eq!(output.len(), 3);
        assert!(output[0].get("result").is_some());
        assert!(output[1].get("result").is_some());
        assert_eq!(output[2]["error"]["code"], TOOL_RATE_LIMITED);
        assert_eq!(output[2]["error"]["data"]["limitPerMinute"], 1);
        assert_eq!(output[2]["error"]["data"]["burst"], 1);
        assert!(output[2]["error"]["data"]["retryAfterMs"].as_u64().unwrap() > 0);
        assert!(registry.lock().is_empty());
    }

    struct EofAfterStart {
        bytes: Vec<u8>,
        offset: usize,
        started: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Read for EofAfterStart {
        fn read(&mut self, target: &mut [u8]) -> io::Result<usize> {
            let available = self.fill_buf()?;
            let count = available.len().min(target.len());
            target[..count].copy_from_slice(&available[..count]);
            self.consume(count);
            Ok(count)
        }
    }

    impl BufRead for EofAfterStart {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            if self.offset < self.bytes.len() {
                return Ok(&self.bytes[self.offset..]);
            }
            let (started, ready) = &*self.started;
            let mut started = started.lock().unwrap();
            while !*started {
                started = ready.wait(started).unwrap();
            }
            Ok(&[])
        }

        fn consume(&mut self, amount: usize) {
            self.offset = (self.offset + amount).min(self.bytes.len());
        }
    }

    #[test]
    fn eof_cancels_active_work_and_suppresses_its_response() {
        let (_dir, mem) = engine();
        let started = Arc::new((Mutex::new(false), Condvar::new()));
        let mut reader = EofAfterStart {
            bytes: br#"{"jsonrpc":"2.0","id":1,"method":"test/block","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}
"#
            .to_vec(),
            offset: 0,
            started: Arc::clone(&started),
        };
        let dispatch =
            |_: &MemoryEngine, _: &str, request: &Value, _: bool, token: &CancelToken| {
                let (started, ready) = &*started;
                *started.lock().unwrap() = true;
                ready.notify_one();
                while !token.is_cancelled() {
                    std::thread::yield_now();
                }
                Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{}}))
            };
        let mut output = Vec::new();
        serve_io(
            &mut reader,
            &mut output,
            mem,
            "r",
            test_options(120),
            &dispatch,
        )
        .unwrap();
        assert!(output.is_empty());
    }
}
