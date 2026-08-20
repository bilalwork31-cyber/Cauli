use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

fn d_v() -> u32 {
    1
}

/// Highest envelope protocol version this worker understands (PROTOCOL.md
/// section 2). The protocol does not define forward compatibility for a
/// higher `v`, so the conservative reading applies: accept the current
/// version, reject anything newer rather than guess at a shape this build
/// has never seen.
pub const PROTOCOL_VERSION: u32 = 1;
fn d_queue() -> String {
    "default".to_string()
}
fn d_kind() -> String {
    "io".to_string()
}
fn d_max_retries() -> u32 {
    3
}
fn d_backoff_base() -> u64 {
    500
}
fn d_backoff_factor() -> f64 {
    2.0
}
fn d_backoff_max() -> u64 {
    60_000
}
fn d_true() -> bool {
    true
}
fn d_timeout() -> u64 {
    300_000
}

/// Ceiling accepted for `timeout_ms` at parse (the default above is already
/// well under it). The recovery loop's `required_idle_ms = max(visibility
/// timeout, timeout_ms + grace)` (worker/src/loops.rs) means a `timeout_ms`
/// near u64::MAX makes a stuck entry effectively unreclaimable rather than
/// merely slow to reclaim, so an extreme value is clamped here instead of
/// left to saturating arithmetic further down the pipeline.
const MAX_TIMEOUT_MS: u64 = 86_400_000; // 24h

/// Core of the `deserialize_flexible_*` family below: a JSON integer is
/// accepted as is; a JSON float is accepted only when it is an exact whole
/// number in u64 range, since PROTOCOL.md invites any compliant JSON codec
/// and several emit large integers in exponent form (`1.7e12`). A fractional
/// value (`1.5`) is malformed, not rounded: silently truncating it would
/// change what the caller asked for, which is why this cannot be simplified
/// into a plain `as u64` cast.
fn value_to_u64(v: &Value) -> Result<u64, String> {
    let n = match v {
        Value::Number(n) => n,
        other => return Err(format!("invalid type: {other}, expected a number")),
    };
    if let Some(u) = n.as_u64() {
        return Ok(u);
    }
    match n.as_f64() {
        Some(f) if f.is_finite() && f.fract() == 0.0 && f >= 0.0 && f < u64::MAX as f64 => {
            Ok(f as u64)
        }
        _ => Err(format!("number {n} is not a valid whole number in range")),
    }
}

fn deserialize_flexible_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    value_to_u64(&Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

fn deserialize_flexible_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let n = value_to_u64(&Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)?;
    u32::try_from(n).map_err(|_| serde::de::Error::custom(format!("{n} out of range for u32")))
}

fn deserialize_flexible_opt_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(v) => value_to_u64(&v).map(Some).map_err(serde::de::Error::custom),
    }
}

/// `timeout_ms` gets its own wrapper on top of `deserialize_flexible_u64`: an
/// extremely large value is clamped to `MAX_TIMEOUT_MS` here rather than
/// rejected, since it plausibly means the caller wants an effectively
/// unbounded timeout. Zero is also nonsense (PROTOCOL §4.6: a zero timeout
/// elapses before any attempt can possibly finish) but is deliberately NOT
/// rejected here. dispatch.rs checks it after a full, successful parse
/// instead, the same way it already checks `v` against `PROTOCOL_VERSION`,
/// so an otherwise valid id is still recoverable and a caller blocked in
/// `AsyncResult.get()` gets a real "Malformed" answer rather than hanging.
/// Failing the parse itself, as args/kwargs and the other flexible numeric
/// fields do, would take that id down with it, same as any other
/// unparseable envelope.
fn deserialize_timeout_ms<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(deserialize_flexible_u64(deserializer)?.min(MAX_TIMEOUT_MS))
}

/// Task envelope, PROTOCOL §2. Unknown fields are preserved across
/// deserialize -> mutate -> serialize (retry re-enqueue) via the flattened map.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    #[serde(default = "d_v", deserialize_with = "deserialize_flexible_u32")]
    pub v: u32,
    pub id: String,
    pub task: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub kwargs: Value,
    #[serde(default = "d_queue")]
    pub queue: String,
    #[serde(default = "d_kind")]
    pub kind: String,
    #[serde(default, deserialize_with = "deserialize_flexible_u32")]
    pub retries: u32,
    #[serde(
        default = "d_max_retries",
        deserialize_with = "deserialize_flexible_u32"
    )]
    pub max_retries: u32,
    #[serde(
        default = "d_backoff_base",
        deserialize_with = "deserialize_flexible_u64"
    )]
    pub backoff_base_ms: u64,
    #[serde(default = "d_backoff_factor")]
    pub backoff_factor: f64,
    #[serde(
        default = "d_backoff_max",
        deserialize_with = "deserialize_flexible_u64"
    )]
    pub backoff_max_ms: u64,
    #[serde(default = "d_true")]
    pub jitter: bool,
    #[serde(default = "d_timeout", deserialize_with = "deserialize_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default, deserialize_with = "deserialize_flexible_opt_u64")]
    pub soft_timeout_ms: Option<u64>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "d_true")]
    pub store_result: bool,
    #[serde(default, deserialize_with = "deserialize_flexible_u64")]
    pub enqueued_at: u64,
    #[serde(default)]
    pub not_before: Option<f64>,
    /// PROTOCOL §9.1. Absolute epoch-ms deadline past which this task is no
    /// longer worth running: the worker discards it at dispatch instead of
    /// executing it. Null means "no deadline". Deliberately absolute (not a
    /// duration) so it survives a retry, a delayed-zset hop and a crash
    /// redelivery unchanged, and so it means the same thing on a broker that
    /// has no delayed-delivery primitive of its own.
    #[serde(default, deserialize_with = "deserialize_flexible_opt_u64")]
    pub expires_at: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Envelope {
    // `args_json()` / `kwargs_json()` are gone: nothing serializes task
    // arguments to JSON text any more. Both execution paths hand the parsed
    // values to `pyjson::json_to_py`, and the cpu path serializes borrowed
    // values straight onto the wire.

    /// args as a BORROWED `Value` ("null" tolerated -> a shared empty array).
    /// Callers that serialize (the cpu child request) go straight from the
    /// parsed tree to the wire: no string round-trip and, unlike the previous
    /// `args_value()`, no clone of the tree either.
    pub fn args_ref(&self) -> &Value {
        static EMPTY_ARRAY: std::sync::LazyLock<Value> =
            std::sync::LazyLock::new(|| Value::Array(Vec::new()));
        match &self.args {
            Value::Null => &EMPTY_ARRAY,
            v => v,
        }
    }

    /// kwargs as a BORROWED `Value` ("null" tolerated -> a shared empty
    /// object). See `args_ref`.
    pub fn kwargs_ref(&self) -> &Value {
        static EMPTY_OBJECT: std::sync::LazyLock<Value> =
            std::sync::LazyLock::new(|| Value::Object(serde_json::Map::new()));
        match &self.kwargs {
            Value::Null => &EMPTY_OBJECT,
            v => v,
        }
    }

    /// Effective async timeout seconds, §4.6:
    /// `min(soft_timeout_ms or timeout_ms, timeout_ms) / 1000`.
    pub fn effective_async_timeout_s(&self) -> f64 {
        let soft = self.soft_timeout_ms.unwrap_or(self.timeout_ms);
        (soft.min(self.timeout_ms) as f64) / 1000.0
    }

    /// PROTOCOL §9.1/§9.2: the instant past which this entry must not run,
    /// combining the envelope's own `expires_at` with the queue's configured
    /// max age. The EARLIER of the two wins: a queue TTL is an operator-side
    /// ceiling on how long anything may sit there, so a per-call `expires`
    /// cannot be used to exceed it, and a queue TTL cannot extend a per-call
    /// `expires` either.
    ///
    /// `queue_ttl_ms` is applied only when `enqueued_at` is present (a
    /// crafted or ancient envelope with `enqueued_at == 0` would otherwise be
    /// judged expired by ~55 years and dropped). `saturating_add` keeps a
    /// hostile `enqueued_at` near u64::MAX from wrapping into a past deadline.
    pub fn expiry_deadline_ms(&self, queue_ttl_ms: Option<u64>) -> Option<u64> {
        let by_ttl = match (queue_ttl_ms, self.enqueued_at) {
            (Some(ttl), enq) if enq > 0 => Some(enq.saturating_add(ttl)),
            _ => None,
        };
        match (self.expires_at, by_ttl) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        }
    }
}

/// PROTOCOL §8 `error.origin`: cauli machinery synthesized the error object.
pub const ORIGIN_WORKER: &str = "worker";

/// Error object, PROTOCOL §8.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ErrorJson {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub traceback: String,
    /// §8 `origin`: `"worker"` when cauli machinery synthesized this object,
    /// `"task"` when an exception propagated out of user code. Empty means
    /// unknown, which happens only for an executor response written before
    /// the field existed; it is then omitted from the wire rather than sent
    /// as `""`, since the empty string is not one of the defined values.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
}

impl ErrorJson {
    /// Every Rust side construction is cauli machinery minting an error for
    /// a task that raised nothing, so origin is `"worker"` by construction
    /// and there is no call site that has to remember to set it. The one
    /// error object Rust does NOT mint, the one decoded from an executor
    /// response, carries the origin Python put on it.
    pub fn new(type_: &str, message: impl Into<String>) -> Self {
        ErrorJson {
            type_: type_.to_string(),
            message: message.into(),
            traceback: String::new(),
            origin: ORIGIN_WORKER.to_string(),
        }
    }
}

/// Result key value builders, PROTOCOL §8.
/// Borrowed result envelopes.
///
/// `serde_json::json!({... "result": result ...})` deep-clones `result` into a
/// fresh `Value` tree and then serializes that tree, so a task returning a
/// large structure paid for a full copy of it on the way out. Serializing a
/// borrowing struct writes the same bytes straight from the original value.
#[derive(serde::Serialize)]
struct SuccessResult<'a> {
    status: &'static str,
    result: &'a Value,
    error: (), // serializes as null
    finished_at: u64,
}

#[derive(serde::Serialize)]
struct FailureResult<'a> {
    status: &'static str,
    result: (), // serializes as null
    error: &'a ErrorJson,
    finished_at: u64,
}

pub fn result_success(result: &Value, finished_at: u64) -> String {
    serde_json::to_string(&SuccessResult {
        status: "success",
        result,
        error: (),
        finished_at,
    })
    .expect("result envelope is always serializable")
}

pub fn result_failure(error: &ErrorJson, finished_at: u64) -> String {
    serde_json::to_string(&FailureResult {
        status: "failure",
        result: (),
        error,
        finished_at,
    })
    .expect("result envelope is always serializable")
}

/// §4.5 suppression. `claimant_id` names the task that holds the key, and it
/// is the only thread a suppressed caller has: a claim is never released, so
/// a resubmission after the claimant was dead lettered is suppressed too, and
/// without the id there is no way to discover that the work never succeeded.
/// Null only in the race where the key expired between the failed claim and
/// the read of its holder.
pub fn result_duplicate(finished_at: u64, claimant_id: &str) -> String {
    serde_json::json!({
        "status": "duplicate",
        "result": null,
        "error": null,
        "claimant_id": (!claimant_id.is_empty()).then_some(claimant_id),
        "finished_at": finished_at,
    })
    .to_string()
}

/// §9.1: a task discarded unrun past its deadline. Its own status rather than
/// a `"failure"`, because it is not one — the task never ran, so there is no
/// exception, no traceback and nothing to retry. The error object is carried
/// anyway so a client that only knows `failure`/`success` still gets a usable
/// `type`/`message` out of it.
pub fn result_expired(deadline_ms: u64, finished_at: u64) -> String {
    serde_json::json!({
        "status": "expired",
        "result": null,
        "error": {
            "type": "Expired",
            "message": format!(
                "task expired at {deadline_ms} (picked up at {finished_at}, \
                 {}ms late) and was discarded without running",
                finished_at.saturating_sub(deadline_ms)
            ),
            "traceback": "",
            "origin": ORIGIN_WORKER,
        },
        "finished_at": finished_at,
    })
    .to_string()
}

/// Truncate `s` to at most `max_bytes` bytes, backing off to the nearest
/// preceding UTF-8 char boundary so multibyte input (audit H4 — executor
/// garbage, oversize envelopes) can never panic on a byte-index slice.
pub fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// PROTOCOL §4.4: redelivery limit = max(3, max_retries + 1) computed per
/// envelope; 3 if the envelope is unreadable.
pub fn redelivery_limit(env: Option<&Envelope>) -> u64 {
    match env {
        Some(e) => std::cmp::max(3, e.max_retries as u64 + 1),
        None => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"{
        "v": 1,
        "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "task": "myapp.tasks.send_email",
        "args": [1, "x"],
        "kwargs": {"k": true},
        "queue": "emails",
        "kind": "io",
        "retries": 2,
        "max_retries": 5,
        "backoff_base_ms": 250,
        "backoff_factor": 1.5,
        "backoff_max_ms": 30000,
        "jitter": false,
        "timeout_ms": 12000,
        "soft_timeout_ms": 6000,
        "idempotency_key": "idk-1",
        "store_result": false,
        "enqueued_at": 1700000000000,
        "not_before": 1700000005000.5,
        "expires_at": 1700000060000,
        "trace_id": "unknown-field-must-survive",
        "meta": {"nested": [1, 2]}
    }"#;

    #[test]
    fn roundtrip_preserves_known_and_unknown_fields() {
        let e: Envelope = serde_json::from_str(FULL).unwrap();
        assert_eq!(e.v, 1);
        assert_eq!(e.id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(e.task, "myapp.tasks.send_email");
        assert_eq!(e.queue, "emails");
        assert_eq!(e.retries, 2);
        assert_eq!(e.max_retries, 5);
        assert_eq!(e.backoff_base_ms, 250);
        assert_eq!(e.backoff_factor, 1.5);
        assert_eq!(e.backoff_max_ms, 30_000);
        assert!(!e.jitter);
        assert_eq!(e.timeout_ms, 12_000);
        assert_eq!(e.soft_timeout_ms, Some(6_000));
        assert_eq!(e.idempotency_key.as_deref(), Some("idk-1"));
        assert!(!e.store_result);
        assert_eq!(e.enqueued_at, 1_700_000_000_000);
        assert_eq!(e.not_before, Some(1_700_000_005_000.5));
        assert_eq!(
            e.extra.get("trace_id").unwrap(),
            &Value::String("unknown-field-must-survive".into())
        );

        let mut e2 = e.clone();
        e2.retries += 1;
        let s = serde_json::to_string(&e2).unwrap();
        let back: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(back["retries"], 3);
        assert_eq!(back["trace_id"], "unknown-field-must-survive");
        assert_eq!(back["meta"]["nested"][1], 2);
        assert_eq!(back["kwargs"]["k"], true);
    }

    #[test]
    fn nulls_and_missing_fields_tolerated() {
        let s = r#"{"id":"abc","task":"t","soft_timeout_ms":null,"idempotency_key":null,"not_before":null}"#;
        let e: Envelope = serde_json::from_str(s).unwrap();
        assert_eq!(e.v, 1);
        assert_eq!(e.queue, "default");
        assert_eq!(e.kind, "io");
        assert_eq!(e.retries, 0);
        assert_eq!(e.max_retries, 3);
        assert_eq!(e.backoff_base_ms, 500);
        assert_eq!(e.backoff_factor, 2.0);
        assert_eq!(e.backoff_max_ms, 60_000);
        assert!(e.jitter);
        assert_eq!(e.timeout_ms, 300_000);
        assert_eq!(e.soft_timeout_ms, None);
        assert_eq!(e.idempotency_key, None);
        assert!(e.store_result);
        assert_eq!(e.not_before, None);
        // null args/kwargs normalize to an empty array/object rather than
        // reaching a task as None.
        assert_eq!(e.args_ref(), &Value::Array(vec![]));
        assert_eq!(e.kwargs_ref(), &Value::Object(serde_json::Map::new()));
    }

    #[test]
    fn malformed_rejected() {
        assert!(serde_json::from_str::<Envelope>("{not json").is_err());
        assert!(serde_json::from_str::<Envelope>(r#"{"task":"t"}"#).is_err()); // no id
        assert!(serde_json::from_str::<Envelope>(r#"{"id":"x"}"#).is_err()); // no task
    }

    #[test]
    fn effective_async_timeout() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.timeout_ms = 10_000;
        assert_eq!(e.effective_async_timeout_s(), 10.0);
        e.soft_timeout_ms = Some(4_000);
        assert_eq!(e.effective_async_timeout_s(), 4.0);
        e.soft_timeout_ms = Some(50_000); // soft > hard: hard wins
        assert_eq!(e.effective_async_timeout_s(), 10.0);
    }

    /// §9.1/§9.2: the deadline is the EARLIER of the envelope's own
    /// `expires_at` and `enqueued_at + queue_ttl`, in both directions.
    #[test]
    fn expiry_deadline_takes_the_earlier_of_expires_and_queue_ttl() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.enqueued_at = 1_000_000;

        // Neither set: no deadline at all.
        assert_eq!(e.expiry_deadline_ms(None), None);

        // Only the envelope's own expiry.
        e.expires_at = Some(1_060_000);
        assert_eq!(e.expiry_deadline_ms(None), Some(1_060_000));

        // Only the queue TTL.
        e.expires_at = None;
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_030_000));

        // Both: queue TTL is the tighter one and wins (a per-call `expires`
        // cannot be used to sit in a TTL-bounded queue longer than allowed).
        e.expires_at = Some(1_060_000);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_030_000));

        // Both: the envelope's own expiry is tighter and wins.
        e.expires_at = Some(1_010_000);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_010_000));
    }

    /// A queue TTL needs a real `enqueued_at` to mean anything. Without this
    /// guard an envelope missing the field (0) would be "expired" by decades
    /// and every such entry would be silently discarded.
    #[test]
    fn queue_ttl_ignored_without_enqueued_at() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        assert_eq!(e.enqueued_at, 0);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), None);
        // ... but an explicit expires_at still applies.
        e.expires_at = Some(7);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(7));
    }

    /// H3-style overflow guard: a hostile `enqueued_at` near u64::MAX must not
    /// wrap the queue-TTL deadline into the past (which would expire every
    /// task on that queue).
    #[test]
    fn queue_ttl_deadline_saturates_instead_of_wrapping() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.enqueued_at = u64::MAX;
        assert_eq!(e.expiry_deadline_ms(Some(60_000)), Some(u64::MAX));
    }

    #[test]
    fn expires_at_roundtrips_and_defaults_to_null() {
        let e: Envelope = serde_json::from_str(FULL).unwrap();
        assert_eq!(e.expires_at, Some(1_700_000_060_000));
        let back: Value = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(back["expires_at"], 1_700_000_060_000u64);

        // Absent in an older client's envelope: null, never a bogus 0 (which
        // would read as "expired in 1970" and discard the task).
        let old: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        assert_eq!(old.expires_at, None);
    }

    #[test]
    fn expired_result_json_shape() {
        let v: Value = serde_json::from_str(&result_expired(100, 350)).unwrap();
        assert_eq!(v["status"], "expired");
        assert_eq!(v["result"], Value::Null);
        assert_eq!(v["error"]["type"], "Expired");
        assert_eq!(v["error"]["origin"], ORIGIN_WORKER);
        assert!(v["error"]["message"].as_str().unwrap().contains("250ms"));
        assert_eq!(v["finished_at"], 350);
    }

    #[test]
    fn redelivery_limit_rules() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.max_retries = 0;
        assert_eq!(redelivery_limit(Some(&e)), 3); // max(3, 1)
        e.max_retries = 2;
        assert_eq!(redelivery_limit(Some(&e)), 3); // max(3, 3)
        e.max_retries = 5;
        assert_eq!(redelivery_limit(Some(&e)), 6); // max(3, 6)
        assert_eq!(redelivery_limit(None), 3); // unreadable envelope
    }

    #[test]
    fn result_json_shapes() {
        let ok = result_success(&serde_json::json!({"a": 1}), 123);
        let v: Value = serde_json::from_str(&ok).unwrap();
        assert_eq!(v["status"], "success");
        assert_eq!(v["result"]["a"], 1);
        assert_eq!(v["error"], Value::Null);
        assert_eq!(v["finished_at"], 123);

        let err = ErrorJson::new("ValueError", "boom");
        let f = result_failure(&err, 5);
        let v: Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["status"], "failure");
        assert_eq!(v["result"], Value::Null);
        assert_eq!(v["error"]["type"], "ValueError");
        assert_eq!(v["error"]["message"], "boom");
        assert_eq!(v["error"]["origin"], ORIGIN_WORKER);

        let d = result_duplicate(9, "0123456789abcdef0123456789abcdef");
        let v: Value = serde_json::from_str(&d).unwrap();
        assert_eq!(v["status"], "duplicate");
        assert_eq!(v["result"], Value::Null);
        assert_eq!(v["error"], Value::Null);
    }

    /// A suppressed caller gets nothing back but this result, so the id of
    /// the task that holds the key has to travel in it: the claim is never
    /// released, and after the claimant is dead lettered the only way to
    /// find out the work never succeeded is `cauli:result:{claimant_id}`.
    #[test]
    fn duplicate_result_carries_the_claimant_id() {
        let claimant = "0123456789abcdef0123456789abcdef";
        let v: Value = serde_json::from_str(&result_duplicate(9, claimant)).unwrap();
        assert_eq!(v["claimant_id"], claimant);
        assert_eq!(v["finished_at"], 9);

        // Unknown holder (the key expired mid claim) reports null rather
        // than an empty string a caller would have to special case.
        let v: Value = serde_json::from_str(&result_duplicate(9, "")).unwrap();
        assert_eq!(v["claimant_id"], Value::Null);
    }

    /// H4 regression: a naive `&s[..512]` panics when byte 512 lands inside a
    /// multibyte char. Build a string where that is guaranteed (3-byte chars
    /// don't divide 512 evenly) and confirm safe_truncate never panics and
    /// never returns a string longer than the cap.
    #[test]
    fn safe_truncate_never_panics_on_multibyte_boundary() {
        let s = "€".repeat(200); // 600 bytes, 3 bytes/char; 512 is mid-char
        let t = safe_truncate(&s, 512);
        assert!(t.len() <= 512);
        assert!(s.starts_with(t));

        let short = "hello";
        assert_eq!(safe_truncate(short, 512), "hello");
    }

    // Bug: a wrongly typed args/kwargs must be rejected before execution,
    // never reach fn(*args, **kwargs). The shape gate itself lives in
    // dispatch.rs (args_kwargs_shape_ok), not here: see that module's
    // tests for the array/object/null cases. Deserialize still accepts
    // any JSON type for these two fields, unchanged.

    /// Regression coverage requested alongside the fix above: a legitimate
    /// envelope with args/kwargs absent, and one with them present but
    /// empty, must both still normalize to an empty array/object.
    #[test]
    fn args_kwargs_absent_or_empty_still_normalize_correctly() {
        let absent: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        assert_eq!(absent.args_ref(), &Value::Array(vec![]));
        assert_eq!(absent.kwargs_ref(), &Value::Object(serde_json::Map::new()));

        let empty: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","args":[],"kwargs":{}}"#).unwrap();
        assert_eq!(empty.args_ref(), &Value::Array(vec![]));
        assert_eq!(empty.kwargs_ref(), &Value::Object(serde_json::Map::new()));
    }

    // Bug: timeout_ms 0 guarantees a hard timeout on every attempt.
    // Rejected in dispatch.rs (after a successful parse, like the `v`
    // version check), not here: see this file's doc comment on
    // deserialize_timeout_ms for why.

    #[test]
    fn timeout_ms_zero_parses_here_unclamped() {
        let e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":0}"#).unwrap();
        assert_eq!(e.timeout_ms, 0);
    }

    /// A value near u64::MAX must not make the recovery loop's
    /// required_idle_ms effectively infinite (worker/src/loops.rs); it is
    /// clamped rather than rejected, since it plausibly means "never time
    /// out" rather than a malformed request. The clamp value (86_400_000 =
    /// 24h) is hardcoded rather than referencing the implementation's own
    /// constant, so a change to that constant fails this test instead of
    /// trivially passing it.
    #[test]
    fn timeout_ms_huge_value_clamped_not_rejected() {
        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":18446744073709551615}"#)
                .unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);

        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":999999999999}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);

        // Just at and just under the ceiling: passed through unclamped.
        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":86400000}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);
        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":86399999}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_399_999);
    }

    // Bug: integers in exponent or float form rejected outright.

    #[test]
    fn integral_float_accepted_for_integer_fields() {
        let e: Envelope = serde_json::from_str(
            r#"{"id":"a","task":"t","timeout_ms":300000.0,"enqueued_at":1.7e12,
                "retries":2.0,"max_retries":5.0,"backoff_base_ms":250.0,"backoff_max_ms":30000.0}"#,
        )
        .unwrap();
        assert_eq!(e.timeout_ms, 300_000);
        assert_eq!(e.enqueued_at, 1_700_000_000_000);
        assert_eq!(e.retries, 2);
        assert_eq!(e.max_retries, 5);
        assert_eq!(e.backoff_base_ms, 250);
        assert_eq!(e.backoff_max_ms, 30_000);
    }

    #[test]
    fn integral_float_accepted_for_optional_integer_fields() {
        let e: Envelope = serde_json::from_str(
            r#"{"id":"a","task":"t","soft_timeout_ms":6000.0,"expires_at":1.7e12}"#,
        )
        .unwrap();
        assert_eq!(e.soft_timeout_ms, Some(6_000));
        assert_eq!(e.expires_at, Some(1_700_000_000_000));

        let e: Envelope = serde_json::from_str(
            r#"{"id":"a","task":"t","soft_timeout_ms":null,"expires_at":null}"#,
        )
        .unwrap();
        assert_eq!(e.soft_timeout_ms, None);
        assert_eq!(e.expires_at, None);
    }

    /// The whole risk of the fix above: a fractional value must still be
    /// rejected, not rounded; NaN, infinity and out of range floats must be
    /// rejected too, not saturated into a plausible looking integer.
    #[test]
    fn fractional_and_non_finite_and_out_of_range_floats_rejected() {
        // fractional: must not be silently rounded
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","timeout_ms":1.5}"#).is_err()
        );
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1.1}"#).is_err()
        );

        // negative: out of range for an unsigned field, integer or float form
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","timeout_ms":-5}"#).is_err()
        );
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":-5.0}"#)
                .is_err()
        );

        // infinity: a plain JSON number token can overflow f64 to infinity
        // with no "Infinity" keyword involved, so this must reach and fail
        // the finiteness check rather than being merely unreachable.
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1e400}"#)
                .is_err()
        );

        // huge but finite float: out of u64 range
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1e30}"#)
                .is_err()
        );

        // out of range for u32 specifically (v is u32)
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","v":4294967296.0}"#).is_err()
        );
        assert!(serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","v":1.0}"#).is_ok());
    }
}
