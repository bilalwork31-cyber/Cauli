//! Shared worker context + executor outcome model.

use crate::cli::Args;
use crate::cpu::CpuPool;
use crate::envelope::ErrorJson;
use crate::pyrt::{PyRuntime, SyncPool, TaskSpec};
use crate::stats::Counters;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};

pub struct Ctx {
    pub args: Args,
    pub registry: HashMap<String, TaskSpec>,
    /// Write-side connection (results, acks, mover, idemp, recovery). Clone per use.
    pub redis: ConnectionManager,
    pub counters: Arc<Counters>,
    pub io_sem: Arc<Semaphore>,
    pub sync_pool: SyncPool,
    pub pyrt: Arc<PyRuntime>,
    pub cpu: CpuPool,
    pub result_ttl: u64,
    pub idemp_ttl: u64,
    pub queues: Vec<String>,
    pub consumer: String,
    pub shutdown: watch::Receiver<bool>,
}

impl Ctx {
    pub fn shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Executor completion, normalized from shim / cpu-child response JSON.
pub enum Outcome {
    Success(Value),
    ForceRetry { countdown: Option<f64>, err: ErrorJson },
    Failure { err: ErrorJson, retryable: bool },
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

/// Parse an executor response line. `from_cpu` enables the cpu-child mapping
/// (§5.1 children cannot carry retry/retryable flags): error type "Retry" =>
/// forced retry (countdown unavailable over the pipe => None => computed
/// backoff), "SerializationError" => non-retryable.
pub fn parse_pyresp(s: &str, from_cpu: bool) -> Outcome {
    let resp: PyResp = match serde_json::from_str(s) {
        Ok(r) => r,
        Err(e) => {
            return Outcome::Failure {
                err: ErrorJson::new(
                    "WorkerShimError",
                    format!("unparseable executor response ({e}): {}", &s[..s.len().min(512)]),
                ),
                retryable: true,
            }
        }
    };
    if resp.ok {
        return Outcome::Success(resp.result);
    }
    let err = resp
        .error
        .unwrap_or_else(|| ErrorJson::new("UnknownError", "executor reported failure without error"));
    if resp.retry || (from_cpu && err.type_ == "Retry") {
        return Outcome::ForceRetry {
            countdown: resp.countdown,
            err,
        };
    }
    let retryable = resp
        .retryable
        .unwrap_or(err.type_ != "SerializationError");
    Outcome::Failure { err, retryable }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        match parse_pyresp(r#"{"ok":false,"error":{"type":"Retry","message":"m"}}"#, true) {
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
}
