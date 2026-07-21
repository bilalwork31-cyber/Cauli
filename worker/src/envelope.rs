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
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Envelope {
    /// args as a JSON array string ("null" tolerated -> "[]").
    pub fn args_json(&self) -> String {
        match &self.args {
            Value::Null => "[]".to_string(),
            v => v.to_string(),
        }
    }

    /// kwargs as a JSON object string ("null" tolerated -> "{}").
    pub fn kwargs_json(&self) -> String {
        match &self.kwargs {
            Value::Null => "{}".to_string(),
            v => v.to_string(),
        }
    }

    /// args as a `Value` directly ("null" tolerated -> `[]`). Used by callers
    /// (the cpu child request) that build a `Value` anyway, to avoid a
    /// pointless string round-trip (serialize to JSON text, then reparse it
    /// right back into a `Value` — see audit nit on `exec.rs::run_cpu_task`).
    pub fn args_value(&self) -> Value {
        match &self.args {
            Value::Null => Value::Array(Vec::new()),
            v => v.clone(),
        }
    }

    /// kwargs as a `Value` directly ("null" tolerated -> `{}`). See `args_value`.
    pub fn kwargs_value(&self) -> Value {
        match &self.kwargs {
            Value::Null => Value::Object(serde_json::Map::new()),
            v => v.clone(),
        }
    }

    /// Effective async timeout seconds, §4.6:
    /// `min(soft_timeout_ms or timeout_ms, timeout_ms) / 1000`.
    pub fn effective_async_timeout_s(&self) -> f64 {
        let soft = self.soft_timeout_ms.unwrap_or(self.timeout_ms);
        (soft.min(self.timeout_ms) as f64) / 1000.0
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
        assert_eq!(e.args_json(), "[]");
        assert_eq!(e.kwargs_json(), "{}");
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
