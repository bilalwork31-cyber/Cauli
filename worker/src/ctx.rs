//! Shared worker context + executor outcome model.

use crate::cli::Args;
use crate::cpu::CpuPool;
use crate::envelope::ErrorJson;
use crate::pyrt::{PyRuntime, SyncPool, TaskSpec};
use crate::stats::Counters;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Semaphore};

pub struct Ctx {
    pub args: Args,
    pub registry: HashMap<String, TaskSpec>,
    /// Write-side connection (results, acks, mover, idemp, recovery). Clone per use.
    pub redis: ConnectionManager,
    pub counters: Arc<Counters>,
    pub io_sem: Arc<Semaphore>,
    /// Total permits `io_sem` was built with. The fetch loop needs the
    /// CAPACITY, not just the free count, to tell "every slot is busy" from
    /// "this deployment has fewer slots than it has queues" (see
    /// `loops::fetch_loop`).
    pub io_concurrency: usize,
    /// Stream entries this process currently holds in flight, per queue.
    /// One pre-built set per queue (queues are fixed at startup), keyed by
    /// the parsed `(ms, seq)` halves of the redis stream id so tracking an
    /// entry costs no allocation on the dispatch path.
    ///
    /// The section 4.4 recovery loop consults this before reclaiming: an
    /// entry THIS worker is still holding must never be XCLAIMed by it,
    /// however long it has been parked. Without it a cpu entry waiting for a
    /// child (or any entry waiting for an execution slot) crosses the idle
    /// threshold, gets reclaimed by its own worker, and after
    /// `redelivery_limit` reclaims is dead lettered as
    /// `RedeliveryLimitExceeded` having executed zero instructions.
    pub inflight_entries: HashMap<String, Mutex<HashSet<(u64, u64)>>>,
    pub sync_pool: SyncPool,
    pub pyrt: Arc<PyRuntime>,
    /// Lazily started cpu pool: empty until the first cpu task (or startup
    /// with `--eager-cpu`). `cpu_cfg` is None when no cpu task is registered,
    /// in which case first use gets the permanently closed `disabled()` pool.
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

/// Split a redis stream id (`"<ms>-<seq>"`) into its two numeric halves.
/// `None` for anything that is not that shape, in which case the caller
/// falls back to not tracking the entry, i.e. to the previous behavior.
pub fn parse_stream_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// Marks one stream entry as in flight in `Ctx::inflight_entries` for as
/// long as it is held, and removes it on drop -- including a panicking
/// unwind, exactly like `DecrGuard`. A leaked entry here would be
/// permanently unreclaimable, so the removal must not depend on the happy
/// path being taken.
pub struct InflightEntry<'a> {
    set: Option<&'a Mutex<HashSet<(u64, u64)>>>,
    id: (u64, u64),
}

impl<'a> InflightEntry<'a> {
    /// Records `stream_id` under `queue`. A queue this worker does not
    /// consume, or an id that is not `<ms>-<seq>`, yields an inert guard.
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

    /// The cpu pool, started on first use. Concurrent callers during the
    /// startup window all wait on the same init (OnceCell); after it, this is
    /// a lock-free read.
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

    /// Dispatch tasks currently blocked on a full cpu backlog; 0 while the
    /// pool has not started (nothing can be blocked on it yet).
    pub fn cpu_overflow(&self) -> usize {
        self.cpu
            .get()
            .map_or(0, |p| p.overflow.load(Ordering::SeqCst))
    }

    /// Same value as `cpu_overflow()`, plus a side effect: logs the
    /// zero/nonzero transition (`Counters::note_cpu_backlog`) so the fetch
    /// loop and the recovery loop's admission gate, which both poll this on
    /// every tick they are blocked, share one log line per edge instead of
    /// each logging independently. Use this at admission gate checks; use
    /// the plain `cpu_overflow()` where only the current depth is wanted
    /// (the periodic stats line), so that read stays side effect free.
    pub fn cpu_backlog(&self) -> usize {
        let n = self.cpu_overflow();
        // The redis anchored clock, like every other instant the worker
        // stamps: both ends of this duration are local reads, so it is self
        // consistent either way, but an NTP step would still distort the
        // backlog duration it reports. See clock.rs.
        self.counters.note_cpu_backlog(n, crate::clock::now_ms());
        n
    }

    /// Summed RSS of the cpu pool's live children in MB; 0 while the pool
    /// has not started. The pid list is cloned out from under the lock
    /// before any /proc read, so a stats tick never holds it across io.
    pub fn cpu_rss_mb(&self) -> u64 {
        self.cpu.get().map_or(0, |pool| {
            let pids = pool.child_pids.lock().unwrap().clone();
            crate::stats::cpu_rss_mb(&pids)
        })
    }

    /// True while this process still holds `stream_id` on `queue`: fetched
    /// or reclaimed, and not yet finished. The section 4.4 recovery loop
    /// uses it to refuse to reclaim its own live work.
    pub fn holds_entry(&self, queue: &str, stream_id: &str) -> bool {
        match (self.inflight_entries.get(queue), parse_stream_id(stream_id)) {
            (Some(set), Some(id)) => set.lock().expect("inflight set poisoned").contains(&id),
            _ => false,
        }
    }

    /// Configured max age for `queue`, in ms: an exact match wins over the
    /// `"*"` fallback, and no entry at all means unbounded.
    pub fn queue_ttl_ms(&self, queue: &str) -> Option<u64> {
        self.queue_ttl_ms
            .get(queue)
            .or_else(|| self.queue_ttl_ms.get(QUEUE_TTL_WILDCARD))
            .copied()
    }
}

/// Decrements an `AtomicI64` inflight counter on drop, including when the
/// scope is left by a panicking unwind. Without this (MEM-3 / H3), a panic
/// between a counter's increment and its manual decrement leaks the slot
/// forever, so `inflight_total` never reaches 0 and every shutdown burns the
/// full `--drain-timeout`. Borrows the counter for the guarded scope only.
pub struct DecrGuard<'a>(pub &'a std::sync::atomic::AtomicI64);

impl Drop for DecrGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn now_ms() -> u64 {
    now_ms_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH))
}

/// Split out from `now_ms` so the pre epoch branch is unit testable without
/// touching the real system clock: a test can hand in an `Err` built from
/// `duration_since` on two `SystemTime`s in the wrong order.
fn now_ms_from(since_epoch: Result<std::time::Duration, std::time::SystemTimeError>) -> u64 {
    static CLOCK_WARNED: std::sync::Once = std::sync::Once::new();
    since_epoch.map(|d| d.as_millis() as u64).unwrap_or_else(|_| {
        // System clock reads before the unix epoch. Every now_ms() call
        // collapses to 0 until the clock passes 1970, which is self
        // consistent (all fire_at/expiry comparisons still agree with each
        // other), so this degrades rather than corrupts anything; but it was
        // previously silent. Warn once so an operator with a misconfigured
        // clock can actually find out.
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
    Success(Value),
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
    result: Value,
    #[serde(default)]
    error: Option<ErrorJson>,
    #[serde(default)]
    retry: bool,
    #[serde(default)]
    countdown: Option<f64>,
    #[serde(default)]
    retryable: Option<bool>,
}

/// Parse an executor response line. `from_cpu` enables the cpu-child mapping:
/// an error type name of exactly "Retry" forces a retry the same as the io
/// path (§4.2/§5.1 — the shim and `_exec.py` both duck-type on class name);
/// countdown, when present in the response JSON, is honored for cpu tasks
/// too (the pipe protocol carries it — see §5.1). "SerializationError" is
/// treated as non-retryable regardless of path.
pub fn parse_pyresp(s: &str, from_cpu: bool) -> Outcome {
    let resp: PyResp = match serde_json::from_str(s) {
        Ok(r) => r,
        Err(e) => {
            return Outcome::Failure {
                err: ErrorJson::new(
                    "WorkerShimError",
                    // char-boundary safe: H4, `s` is executor-controlled garbage
                    // and a naive byte-index slice can panic on multibyte input.
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
        return Outcome::Success(resp.result);
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

    /// §9.2: an exact queue entry beats the `"*"` fallback; with neither, the
    /// queue is unbounded.
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
        // Regression guard for the now_ms_from split: a normal clock must
        // still produce real epoch millis, not accidentally always 0.
        let ms = now_ms();
        assert!(ms > 1_700_000_000_000, "now_ms looks wrong: {ms}");
    }

    #[test]
    fn now_ms_from_pre_epoch_error_degrades_to_zero_not_panic() {
        // Builds a real SystemTimeError without touching the actual clock:
        // duration_since(later) is Err when the receiver is earlier than the
        // argument, which is exactly the pre epoch shape now_ms() hits.
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let err = std::time::SystemTime::now().duration_since(later);
        assert!(err.is_err());
        assert_eq!(now_ms_from(err), 0);
        // The warn-once guard must not panic or change behavior on a second hit.
        let err2 = std::time::SystemTime::now().duration_since(later);
        assert_eq!(now_ms_from(err2), 0);
    }

    #[test]
    fn parses_success() {
        match parse_pyresp(r#"{"ok":true,"result":{"a":1}}"#, false) {
            Outcome::Success(v) => assert_eq!(v["a"], 1),
            _ => panic!("expected success"),
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
        // cpu child: type name Retry alone forces retry with computed backoff
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

    /// §8 `origin` is decided by whoever minted the error, so the parser has
    /// to carry the executor's value through untouched rather than classify
    /// anything itself.
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

    /// A cpu child predating the field sends no origin at all. That must
    /// decode as unknown (empty, and so omitted from the result document)
    /// rather than be guessed at, while every error the worker mints itself
    /// is "worker" by construction.
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

    /// H4 regression: multibyte garbage that isn't valid JSON must not panic
    /// on the byte-512 truncation of the error message.
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

    /// MEM-3 regression: a panic inside a DecrGuard-guarded scope must still
    /// decrement the counter (drop runs on unwind), so a panicking dispatch
    /// never leaks an inflight slot and shutdown drain is never stuck.
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
