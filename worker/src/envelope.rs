use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::LazyLock;

fn d_v() -> u32 {
    1
}

/// Highest envelope `v` understood (PROTOCOL §2). Newer is rejected, since
/// the protocol defines no forward compatibility.
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

/// `timeout_ms` clamp. Recovery waits `max(visibility, timeout_ms + grace)`
/// (loops.rs), so a value near u64::MAX would make a stuck entry
/// unreclaimable.
const MAX_TIMEOUT_MS: u64 = 86_400_000; // 24h

/// Integers as is; floats only when an exact whole number in u64 range,
/// since some JSON codecs emit `1.7e12`. `1.5` is malformed, not truncated,
/// so this cannot become a plain `as u64` cast.
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

/// Huge values clamp to `MAX_TIMEOUT_MS` (they mean "unbounded"). Zero is
/// rejected later, in dispatch.rs after a full parse, so the id survives and
/// a caller in `AsyncResult.get()` gets "Malformed" instead of hanging.
fn deserialize_timeout_ms<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(deserialize_flexible_u64(deserializer)?.min(MAX_TIMEOUT_MS))
}

/// Task envelope, PROTOCOL §2. Unknown fields survive a retry re-enqueue
/// via `extra`. `args` and `kwargs` stay the caller's JSON text and go back
/// out verbatim; absent and `null` both read as `None`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    #[serde(default = "d_v", deserialize_with = "deserialize_flexible_u32")]
    pub v: u32,
    pub id: String,
    pub task: String,
    #[serde(default)]
    pub args: Option<Box<RawValue>>,
    #[serde(default)]
    pub kwargs: Option<Box<RawValue>>,
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
    /// PROTOCOL §9.1 epoch ms deadline, discarded unrun past it. Absolute so
    /// it survives retries, delayed hops and redelivery unchanged.
    #[serde(default, deserialize_with = "deserialize_flexible_opt_u64")]
    pub expires_at: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn raw_literal(json: &str) -> Box<RawValue> {
    RawValue::from_string(json.to_owned()).expect("a JSON literal")
}

impl Envelope {
    /// args as the caller's JSON text, absent or null read as `[]`.
    pub fn args_raw(&self) -> &RawValue {
        static EMPTY_ARRAY: LazyLock<Box<RawValue>> = LazyLock::new(|| raw_literal("[]"));
        self.args.as_deref().unwrap_or(&EMPTY_ARRAY)
    }

    /// kwargs as the caller's JSON text, absent or null read as `{}`.
    pub fn kwargs_raw(&self) -> &RawValue {
        static EMPTY_OBJECT: LazyLock<Box<RawValue>> = LazyLock::new(|| raw_literal("{}"));
        self.kwargs.as_deref().unwrap_or(&EMPTY_OBJECT)
    }

    /// §4.6 async deadlines `(soft_s, hard_s)`. `soft_s` is `Some` only when
    /// set and strictly before hard. Never collapse to `min(soft, hard)`:
    /// SoftTimeLimitExceeded could not fire and the soft budget would become
    /// the whole budget.
    pub fn async_timeouts_s(&self) -> (Option<f64>, f64) {
        let hard_s = (self.timeout_ms as f64) / 1000.0;
        let soft_s = match self.soft_timeout_ms {
            Some(soft) if soft > 0 && soft < self.timeout_ms => Some((soft as f64) / 1000.0),
            _ => None,
        };
        (soft_s, hard_s)
    }

    /// PROTOCOL §9.1/§9.2 deadline: the earlier of `expires_at` and
    /// `enqueued_at + queue_ttl_ms`. The TTL applies only when `enqueued_at`
    /// is set (0 would read as expired decades ago); saturating so a hostile
    /// `enqueued_at` cannot wrap into the past.
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
    /// §8 `origin`: `"worker"` or `"task"`. Empty (an executor that does not
    /// send it) is omitted from the wire, since `""` is not a defined value.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
}

impl ErrorJson {
    /// Every Rust minted error is cauli machinery, so origin is `"worker"`.
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
#[derive(serde::Serialize)]
struct FailureResult<'a> {
    status: &'static str,
    result: (), // serializes as null
    error: &'a ErrorJson,
    finished_at: u64,
}

/// `result_json` is JSON text the executor already encoded, embedded as is.
pub fn result_success(result_json: &str, finished_at: u64) -> String {
    format!(
        r#"{{"status":"success","result":{result_json},"error":null,"finished_at":{finished_at}}}"#
    )
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

/// §4.5 suppression. A claim is never released, so `claimant_id` is the
/// caller's only way to learn the holder was dead lettered. Null only if the
/// key expired between the failed claim and the read.
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

/// §9.1 discarded unrun: its own status, not a failure. The error object is
/// still carried for clients that only know `failure`/`success`.
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

/// At most `max_bytes`, backed off to a char boundary so untrusted multibyte
/// input cannot panic the slice.
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
        assert_eq!(e.args_raw().get(), "[]");
        assert_eq!(e.kwargs_raw().get(), "{}");
    }

    #[test]
    fn malformed_rejected() {
        assert!(serde_json::from_str::<Envelope>("{not json").is_err());
        assert!(serde_json::from_str::<Envelope>(r#"{"task":"t"}"#).is_err()); // no id
        assert!(serde_json::from_str::<Envelope>(r#"{"id":"x"}"#).is_err()); // no task
    }

    #[test]
    fn async_timeouts_keep_soft_and_hard_apart() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.timeout_ms = 10_000;
        assert_eq!(e.async_timeouts_s(), (None, 10.0));

        e.soft_timeout_ms = Some(4_000);
        assert_eq!(e.async_timeouts_s(), (Some(4.0), 10.0));

        e.soft_timeout_ms = Some(50_000);
        assert_eq!(e.async_timeouts_s(), (None, 10.0));
        e.soft_timeout_ms = Some(10_000);
        assert_eq!(e.async_timeouts_s(), (None, 10.0));
        e.soft_timeout_ms = Some(0);
        assert_eq!(e.async_timeouts_s(), (None, 10.0));
    }

    #[test]
    fn expiry_deadline_takes_the_earlier_of_expires_and_queue_ttl() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        e.enqueued_at = 1_000_000;

        assert_eq!(e.expiry_deadline_ms(None), None);

        e.expires_at = Some(1_060_000);
        assert_eq!(e.expiry_deadline_ms(None), Some(1_060_000));

        e.expires_at = None;
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_030_000));

        e.expires_at = Some(1_060_000);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_030_000));

        e.expires_at = Some(1_010_000);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(1_010_000));
    }

    #[test]
    fn queue_ttl_ignored_without_enqueued_at() {
        let mut e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        assert_eq!(e.enqueued_at, 0);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), None);
        e.expires_at = Some(7);
        assert_eq!(e.expiry_deadline_ms(Some(30_000)), Some(7));
    }

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
        let ok = result_success(r#"{"a":1}"#, 123);
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

    #[test]
    fn duplicate_result_carries_the_claimant_id() {
        let claimant = "0123456789abcdef0123456789abcdef";
        let v: Value = serde_json::from_str(&result_duplicate(9, claimant)).unwrap();
        assert_eq!(v["claimant_id"], claimant);
        assert_eq!(v["finished_at"], 9);

        let v: Value = serde_json::from_str(&result_duplicate(9, "")).unwrap();
        assert_eq!(v["claimant_id"], Value::Null);
    }

    #[test]
    fn safe_truncate_never_panics_on_multibyte_boundary() {
        let s = "€".repeat(200); // 600 bytes, 3 bytes/char; 512 is mid-char
        let t = safe_truncate(&s, 512);
        assert!(t.len() <= 512);
        assert!(s.starts_with(t));

        let short = "hello";
        assert_eq!(safe_truncate(short, 512), "hello");
    }

    #[test]
    fn args_kwargs_absent_or_empty_still_normalize_correctly() {
        let absent: Envelope = serde_json::from_str(r#"{"id":"a","task":"t"}"#).unwrap();
        assert_eq!(absent.args_raw().get(), "[]");
        assert_eq!(absent.kwargs_raw().get(), "{}");

        let empty: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","args":[],"kwargs":{}}"#).unwrap();
        assert_eq!(empty.args_raw().get(), "[]");
        assert_eq!(empty.kwargs_raw().get(), "{}");
    }

    /// Zero is rejected in dispatch.rs, not at parse (deserialize_timeout_ms).
    #[test]
    fn timeout_ms_zero_parses_here_unclamped() {
        let e: Envelope = serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":0}"#).unwrap();
        assert_eq!(e.timeout_ms, 0);
    }

    /// The 24h clamp is hardcoded so a change to MAX_TIMEOUT_MS fails here.
    #[test]
    fn timeout_ms_huge_value_clamped_not_rejected() {
        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":18446744073709551615}"#)
                .unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);

        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":999999999999}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);

        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":86400000}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_400_000);
        let e: Envelope =
            serde_json::from_str(r#"{"id":"a","task":"t","timeout_ms":86399999}"#).unwrap();
        assert_eq!(e.timeout_ms, 86_399_999);
    }

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

    #[test]
    fn fractional_and_non_finite_and_out_of_range_floats_rejected() {
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","timeout_ms":1.5}"#).is_err()
        );
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1.1}"#).is_err()
        );

        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","timeout_ms":-5}"#).is_err()
        );
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":-5.0}"#)
                .is_err()
        );

        // A plain number token can overflow f64 to infinity.
        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1e400}"#)
                .is_err()
        );

        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","enqueued_at":1e30}"#)
                .is_err()
        );

        assert!(
            serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","v":4294967296.0}"#).is_err()
        );
        assert!(serde_json::from_str::<Envelope>(r#"{"id":"a","task":"t","v":1.0}"#).is_ok());
    }

    /// Key order, integers past 64 bits, `-0.0`, escapes and whitespace all
    /// survive a retry re-enqueue.
    #[test]
    fn retry_reserialize_copies_args_and_kwargs_verbatim() {
        let args = r#"[{"zeta":1,"alpha":2}, 338958331192819208857724424333372550912, -0.0, "caf\u00e9 \ud83d\ude00", [[[]]]]"#;
        let kwargs = r#"{"z": 1e400, "a": "\u2028"}"#;
        let raw = format!(
            r#"{{"id":"a","task":"t","args":{args},"kwargs":{kwargs},"meta":{{"z":123456789012345678901234567890,"a":2}}}}"#
        );
        let mut e: Envelope = serde_json::from_str(&raw).unwrap();
        assert_eq!(e.args_raw().get(), args);
        assert_eq!(e.kwargs_raw().get(), kwargs);
        e.retries += 1;
        let back = serde_json::to_string(&e).unwrap();
        assert!(
            back.contains(&format!(r#""args":{args},"kwargs":{kwargs},"#)),
            "{back}"
        );
        assert!(
            back.contains(r#""meta":{"z":123456789012345678901234567890,"a":2}"#),
            "{back}"
        );
        let again: Envelope = serde_json::from_str(&back).unwrap();
        assert_eq!(again.args_raw().get(), args);
        assert_eq!(again.retries, 1);
    }

    #[test]
    fn null_or_absent_args_reserialize_as_null() {
        for raw in [
            r#"{"id":"a","task":"t"}"#,
            r#"{"id":"a","task":"t","args":null,"kwargs":null}"#,
        ] {
            let e: Envelope = serde_json::from_str(raw).unwrap();
            let back: Value = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
            assert_eq!(back["args"], Value::Null);
            assert_eq!(back["kwargs"], Value::Null);
        }
    }
}
