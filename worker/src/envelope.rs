use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

fn d_v() -> u32 {
    1
}
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

/// Task envelope, PROTOCOL §2. Unknown fields are preserved across
/// deserialize -> mutate -> serialize (retry re-enqueue) via the flattened map.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    #[serde(default = "d_v")]
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
    #[serde(default)]
    pub retries: u32,
    #[serde(default = "d_max_retries")]
    pub max_retries: u32,
    #[serde(default = "d_backoff_base")]
    pub backoff_base_ms: u64,
    #[serde(default = "d_backoff_factor")]
    pub backoff_factor: f64,
    #[serde(default = "d_backoff_max")]
    pub backoff_max_ms: u64,
    #[serde(default = "d_true")]
    pub jitter: bool,
    #[serde(default = "d_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub soft_timeout_ms: Option<u64>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "d_true")]
    pub store_result: bool,
    #[serde(default)]
    pub enqueued_at: u64,
    #[serde(default)]
    pub not_before: Option<f64>,
    /// PROTOCOL §9.1. Absolute epoch-ms deadline past which this task is no
    /// longer worth running: the worker discards it at dispatch instead of
    /// executing it. Null means "no deadline". Deliberately absolute (not a
    /// duration) so it survives a retry, a delayed-zset hop and a crash
    /// redelivery unchanged, and so it means the same thing on a broker that
    /// has no delayed-delivery primitive of its own.
    #[serde(default)]
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

/// Error object, PROTOCOL §8.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ErrorJson {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub traceback: String,
}

impl ErrorJson {
    pub fn new(type_: &str, message: impl Into<String>) -> Self {
        ErrorJson {
            type_: type_.to_string(),
            message: message.into(),
            traceback: String::new(),
        }
    }
}

/// Result key value builders, PROTOCOL §8.
pub fn result_success(result: &Value, finished_at: u64) -> String {
    serde_json::json!({
        "status": "success",
        "result": result,
        "error": null,
        "finished_at": finished_at,
    })
    .to_string()
}

pub fn result_failure(error: &ErrorJson, finished_at: u64) -> String {
    serde_json::json!({
        "status": "failure",
        "result": null,
        "error": error,
        "finished_at": finished_at,
    })
    .to_string()
}

pub fn result_duplicate(finished_at: u64) -> String {
    serde_json::json!({
        "status": "duplicate",
        "result": null,
        "error": null,
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

        let d = result_duplicate(9);
        let v: Value = serde_json::from_str(&d).unwrap();
        assert_eq!(v["status"], "duplicate");
        assert_eq!(v["result"], Value::Null);
        assert_eq!(v["error"], Value::Null);
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
}
