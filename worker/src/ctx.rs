//! Shared worker context + executor outcome model.

use crate::broker::AckBufs;
use crate::cli::Args;
use crate::cpu::CpuPool;
use crate::envelope::ErrorJson;
use crate::pyrt::{PyRuntime, SyncPool, TaskSpec};
use crate::stats::Counters;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Semaphore};

pub struct Ctx {
    pub args: Args,
    pub registry: HashMap<String, TaskSpec>,
    /// Write side connection (results, mover, idemp, recovery, trim).
    pub redis: ConnectionManager,
    /// §4.1 completion buffers, one flusher per queue (see `broker::AckBufs`).
    pub acks: AckBufs,
    pub counters: Arc<Counters>,
    pub io_sem: Arc<Semaphore>,
    /// Total permits of `io_sem`: the fetch loop needs capacity, not the free
    /// count, to tell "all slots busy" from "fewer slots than queues".
    pub io_concurrency: usize,
    /// Stream entries this process holds, per queue, keyed by parsed
    /// `(ms, seq)` so tracking allocates nothing. §4.4 recovery must never
    /// reclaim its own parked entry, or after `redelivery_limit` reclaims it
    /// is dead lettered as `RedeliveryLimitExceeded` without ever running.
    pub inflight_entries: HashMap<String, Mutex<HashSet<(u64, u64)>>>,
    pub sync_pool: SyncPool,
    pub pyrt: Arc<PyRuntime>,
    /// Started by the first cpu task or `--eager-cpu`. `cpu_cfg` None (no cpu
    /// task registered) yields the permanently closed `disabled()` pool.
    pub cpu: tokio::sync::OnceCell<CpuPool>,
    pub cpu_cfg: Option<crate::cpu::StartCfg>,
    pub result_ttl: u64,
    pub idemp_ttl: u64,
    /// PROTOCOL §9.2 per-queue max age in MILLISECONDS, `"*"` = fallback.
    pub queue_ttl_ms: HashMap<String, u64>,
    pub queues: Vec<String>,
    pub consumer: String,
    pub shutdown: watch::Receiver<bool>,
}

/// PROTOCOL §9.2 wildcard key: the TTL used for any queue without its own entry.
pub const QUEUE_TTL_WILDCARD: &str = "*";

/// `"<ms>-<seq>"` into its halves; `None` means the entry is not tracked.
pub fn parse_stream_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// Holds one entry in `Ctx::inflight_entries`, removed on drop including a
/// panicking unwind: a leaked entry would be permanently unreclaimable.
pub struct InflightEntry<'a> {
    set: Option<&'a Mutex<HashSet<(u64, u64)>>>,
    id: (u64, u64),
}

impl<'a> InflightEntry<'a> {
    /// An unknown queue or an id not shaped `<ms>-<seq>` yields an inert guard.
    pub fn track(ctx: &'a Ctx, queue: &str, stream_id: &str) -> Self {
        let parsed = parse_stream_id(stream_id);
        match (ctx.inflight_entries.get(queue), parsed) {
            (Some(set), Some(id)) => {
                set.lock().expect("inflight set poisoned").insert(id);
                InflightEntry { set: Some(set), id }
            }
            _ => InflightEntry {
                set: None,
                id: (0, 0),
            },
        }
    }
}

impl Drop for InflightEntry<'_> {
    fn drop(&mut self) {
        if let Some(set) = self.set {
            set.lock().expect("inflight set poisoned").remove(&self.id);
        }
    }
}

impl Ctx {
    pub fn shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn cpu_pool(&self) -> &CpuPool {
        self.cpu
            .get_or_init(|| async {
                match &self.cpu_cfg {
                    Some(cfg) => {
                        tracing::info!(
                            "first cpu task: starting cpu pool now ({} children)",
                            cfg.workers
                        );
                        crate::cpu::start(cfg.clone(), self.counters.clone()).await
                    }
                    None => crate::cpu::disabled(),
                }
            })
            .await
    }

    /// Dispatch tasks blocked on a full cpu backlog; 0 before the pool starts.
    pub fn cpu_overflow(&self) -> usize {
        self.cpu
            .get()
            .map_or(0, |p| p.overflow.load(Ordering::SeqCst))
    }

    /// `cpu_overflow()` plus one log line per zero/nonzero edge, shared by
    /// both admission gates. Stats reads use `cpu_overflow()`, which has no
    /// side effect.
    pub fn cpu_backlog(&self) -> usize {
        let n = self.cpu_overflow();
        // Redis anchored clock: an NTP step would distort the duration.
        self.counters.note_cpu_backlog(n, crate::clock::now_ms());
        n
    }

    /// Summed child RSS in MB. The pid list is cloned so the lock is never
    /// held across /proc reads.
    pub fn cpu_rss_mb(&self) -> u64 {
        self.cpu.get().map_or(0, |pool| {
            let pids = pool.child_pids.lock().unwrap().clone();
            crate::stats::cpu_rss_mb(&pids)
        })
    }

    /// True while this process holds `stream_id`; §4.4 recovery skips it.
    pub fn holds_entry(&self, queue: &str, stream_id: &str) -> bool {
        match (self.inflight_entries.get(queue), parse_stream_id(stream_id)) {
            (Some(set), Some(id)) => set.lock().expect("inflight set poisoned").contains(&id),
            _ => false,
        }
    }

    /// Max age in ms: exact match, then `"*"`, else unbounded.
    pub fn queue_ttl_ms(&self, queue: &str) -> Option<u64> {
        self.queue_ttl_ms
            .get(queue)
            .or_else(|| self.queue_ttl_ms.get(QUEUE_TTL_WILDCARD))
            .copied()
    }
}

/// Decrements an inflight counter on drop, including a panicking unwind. A
/// leaked slot keeps `inflight_total` above 0, so every shutdown would burn
/// the full `--drain-timeout`.
pub struct DecrGuard<'a>(pub &'a std::sync::atomic::AtomicI64);

impl Drop for DecrGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn now_ms() -> u64 {
    now_ms_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH))
}

fn now_ms_from(since_epoch: Result<std::time::Duration, std::time::SystemTimeError>) -> u64 {
    static CLOCK_WARNED: std::sync::Once = std::sync::Once::new();
    since_epoch.map(|d| d.as_millis() as u64).unwrap_or_else(|_| {
        // 0 everywhere is self consistent, so degrade and warn once.
        CLOCK_WARNED.call_once(|| {
            tracing::warn!(
                "system clock reads before the unix epoch: now_ms is returning 0 until it passes 1970"
            );
        });
        0
    })
}

/// Executor completion, normalized from shim / cpu-child response JSON.
pub enum Outcome {
    /// The task's return value as the JSON text the executor encoded.
    Success(String),
    ForceRetry {
        countdown: Option<f64>,
        err: ErrorJson,
    },
    Failure {
        err: ErrorJson,
        retryable: bool,
    },
}

#[derive(Deserialize)]
struct PyResp {
    ok: bool,
    #[serde(default)]
    result: Option<Box<RawValue>>,
    #[serde(default)]
    error: Option<ErrorJson>,
    #[serde(default)]
    retry: bool,
    #[serde(default)]
    countdown: Option<f64>,
    #[serde(default)]
    retryable: Option<bool>,
}

/// Parse an executor response line. With `from_cpu`, an error type named
/// exactly "Retry" forces a retry (§4.2/§5.1). "SerializationError" is never
/// retryable.
pub fn parse_pyresp(s: &str, from_cpu: bool) -> Outcome {
    let resp: PyResp = match serde_json::from_str(s) {
        Ok(r) => r,
        Err(e) => {
            return Outcome::Failure {
                err: ErrorJson::new(
                    "WorkerShimError",
                    // `s` is untrusted: a byte slice could split a char and panic.
                    format!(
                        "unparseable executor response ({e}): {}",
                        crate::envelope::safe_truncate(s, 512)
                    ),
                ),
                retryable: true,
            };
        }
    };
    if resp.ok {
        return Outcome::Success(resp.result.map_or_else(
            || "null".to_owned(),
            |raw| String::from(Box::<str>::from(raw)),
        ));
    }
    let err = resp.error.unwrap_or_else(|| {
        ErrorJson::new("UnknownError", "executor reported failure without error")
    });
    if resp.retry || (from_cpu && err.type_ == "Retry") {
        return Outcome::ForceRetry {
            countdown: resp.countdown,
            err,
        };
    }
    let retryable = resp.retryable.unwrap_or(err.type_ != "SerializationError");
    Outcome::Failure { err, retryable }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_ttl_lookup_prefers_exact_over_wildcard() {
        let lookup = |map: &HashMap<String, u64>, q: &str| -> Option<u64> {
            map.get(q).or_else(|| map.get(QUEUE_TTL_WILDCARD)).copied()
        };
        let mut map = HashMap::new();
        assert_eq!(lookup(&map, "default"), None);
        map.insert(QUEUE_TTL_WILDCARD.to_string(), 60_000);
        assert_eq!(lookup(&map, "default"), Some(60_000));
        assert_eq!(lookup(&map, "bulk"), Some(60_000));
        map.insert("bulk".to_string(), 5_000);
        assert_eq!(lookup(&map, "bulk"), Some(5_000));
        assert_eq!(lookup(&map, "default"), Some(60_000));
    }

    #[test]
    fn now_ms_returns_a_plausible_current_epoch() {
        let ms = now_ms();
        assert!(ms > 1_700_000_000_000, "now_ms looks wrong: {ms}");
    }

    #[test]
    fn now_ms_from_pre_epoch_error_degrades_to_zero_not_panic() {
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let err = std::time::SystemTime::now().duration_since(later);
        assert!(err.is_err());
        assert_eq!(now_ms_from(err), 0);
        let err2 = std::time::SystemTime::now().duration_since(later);
        assert_eq!(now_ms_from(err2), 0);
    }

    #[test]
    fn parses_success() {
        match parse_pyresp(
            r#"{"ok":true,"result":{"z":1, "a":18446744073709551616}}"#,
            false,
        ) {
            Outcome::Success(v) => assert_eq!(v, r#"{"z":1, "a":18446744073709551616}"#),
            _ => panic!("expected success"),
        }
        for line in [r#"{"ok":true,"result":null}"#, r#"{"ok":true}"#] {
            match parse_pyresp(line, true) {
                Outcome::Success(v) => assert_eq!(v, "null"),
                _ => panic!("expected success"),
            }
        }
    }

    #[test]
    fn parses_forced_retry_and_cpu_mapping() {
        match parse_pyresp(
            r#"{"ok":false,"retry":true,"countdown":1.5,"error":{"type":"Retry","message":"m"}}"#,
            false,
        ) {
            Outcome::ForceRetry { countdown, .. } => assert_eq!(countdown, Some(1.5)),
            _ => panic!("expected forced retry"),
        }
        match parse_pyresp(
            r#"{"ok":false,"error":{"type":"Retry","message":"m"}}"#,
            true,
        ) {
            Outcome::ForceRetry { countdown, .. } => assert_eq!(countdown, None),
            _ => panic!("expected cpu forced retry"),
        }
    }

    #[test]
    fn serialization_error_not_retryable() {
        for from_cpu in [false, true] {
            match parse_pyresp(
                r#"{"ok":false,"error":{"type":"SerializationError","message":"m"}}"#,
                from_cpu,
            ) {
                Outcome::Failure { retryable, .. } => assert!(!retryable),
                _ => panic!("expected failure"),
            }
        }
    }

    #[test]
    fn explicit_retryable_false_wins() {
        match parse_pyresp(
            r#"{"ok":false,"retryable":false,"error":{"type":"Unregistered","message":"m"}}"#,
            false,
        ) {
            Outcome::Failure { retryable, .. } => assert!(!retryable),
            _ => panic!("expected failure"),
        }
    }

    #[test]
    fn garbage_is_retryable_shim_error() {
        match parse_pyresp("not json", true) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "WorkerShimError");
                assert!(retryable);
            }
            _ => panic!("expected failure"),
        }
    }

    /// §8 `origin` belongs to whoever minted the error; the parser never classifies.
    #[test]
    fn origin_is_carried_through_from_the_executor_response() {
        match parse_pyresp(
            r#"{"ok":false,"error":{"type":"ValueError","message":"m","origin":"task"}}"#,
            true,
        ) {
            Outcome::Failure { err, .. } => assert_eq!(err.origin, "task"),
            _ => panic!("expected failure"),
        }
        match parse_pyresp(
            r#"{"ok":false,"error":{"type":"UnregisteredTask","message":"m","origin":"worker"}}"#,
            false,
        ) {
            Outcome::Failure { err, .. } => assert_eq!(err.origin, "worker"),
            _ => panic!("expected failure"),
        }
    }

    #[test]
    fn absent_origin_stays_unknown_and_worker_minted_errors_say_worker() {
        match parse_pyresp(
            r#"{"ok":false,"error":{"type":"ValueError","message":"m"}}"#,
            true,
        ) {
            Outcome::Failure { err, .. } => assert_eq!(err.origin, ""),
            _ => panic!("expected failure"),
        }
        match parse_pyresp("not json", false) {
            Outcome::Failure { err, .. } => {
                assert_eq!(err.type_, "WorkerShimError");
                assert_eq!(err.origin, "worker");
            }
            _ => panic!("expected failure"),
        }
        match parse_pyresp(r#"{"ok":false}"#, false) {
            Outcome::Failure { err, .. } => {
                assert_eq!(err.type_, "UnknownError");
                assert_eq!(err.origin, "worker");
            }
            _ => panic!("expected failure"),
        }
    }

    #[test]
    fn garbage_with_multibyte_chars_does_not_panic() {
        let garbage = "€".repeat(200); // 600 bytes, none ASCII, not valid JSON
        match parse_pyresp(&garbage, false) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "WorkerShimError");
                assert!(retryable);
                assert!(err.message.len() <= garbage.len() + 64);
            }
            _ => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn decr_guard_runs_on_panic_unwind() {
        use std::sync::atomic::AtomicI64;
        let counter = std::sync::Arc::new(AtomicI64::new(0));
        counter.fetch_add(1, Ordering::SeqCst);
        let c = counter.clone();
        let handle = tokio::spawn(async move {
            let _guard = DecrGuard(&c);
            panic!("simulated dispatch panic");
        });
        assert!(handle.await.is_err(), "task should have panicked");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "guard must decrement even on panic"
        );
    }
}
