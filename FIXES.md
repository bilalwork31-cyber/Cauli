# Fixes applied against AUDIT.md (commit f7e8352)

One line per finding: what was done, or why it was intentionally skipped (Low/Nit only).

## Critical

- **C1** (idempotency + retry silently disables retries): Fixed. `broker::idemp_claim` is now a
  single Lua script (`SET NX`, else `GET` + compare) returning `Fresh` / `MineAgain` /
  `Duplicate`; `dispatch::process` proceeds on `Fresh` or `MineAgain` (the task's own earlier
  claim), only treating a genuinely different task id as a duplicate. `idemp_key` values are
  now folded through a deterministic FNV-1a hash before use as the redis key (also closes part
  of M1). PROTOCOL.md §4.5 rewritten to document the new semantics, including that
  crash-redelivered claimed tasks now re-execute instead of always resolving `"duplicate"`.
  Regression: `worker/tests/e2e.rs` (idempotency_key + `fx.flaky` fails-once-then-succeeds ->
  `"success"`), `itest/test_integration.py::test_idempotency_key_allows_retry`.

## High

- **H1** (visibility_timeout < task timeout_ms causes duplicate concurrent execution): Fixed.
  `loops::recovery_loop` now peeks each XPENDING candidate's envelope (non-destructive `XRANGE`
  via `broker::peek_entry`) and only reclaims once idle >= `max(visibility_timeout_ms,
  envelope.timeout_ms + grace)`; `--visibility-timeout` is now a floor, not the sole threshold.
  `main.rs` warns loudly at startup if any registered task's `timeout_ms >=
  visibility_timeout*1000`. PROTOCOL.md §4.4 rewritten. Regression:
  `h1_visibility_floor_does_not_reclaim_long_task` in `worker/tests/e2e_lifecycle.rs` (a task
  sleeping past the visibility floor with a large timeout_ms executes exactly once, verified via
  a counter file).
- **H2** (sync-pool thread loss + zombie execution): Fixed. `pyrt::SyncPool`'s job queue is now
  bounded (capacity = `--io-concurrency`) instead of unbounded; a queued job whose dispatcher
  already gave up (oneshot receiver dropped -> `resp.is_closed()`) is skipped instead of run late;
  a hard timeout (`exec::run_sync_task`) calls `report_hard_timeout()`, which spawns a
  replacement thread immediately so capacity is restored. Live/abandoned thread counts exported
  in the stats line (`sync_live`, `sync_abandoned`). Regression:
  `h2_sync_pool_survives_hard_timeout_abandonment` in `worker/tests/e2e_lifecycle.rs` (with
  `--io-threads 1`, a new task completes quickly after an abandonment, and the abandoned task's
  already-recorded failure result is not later overwritten).
- **H3** (crafted-envelope integer overflow): Fixed. `saturating_add` used everywhere an
  envelope-derived duration/timestamp is added to another value: the async backstop
  (`exec::run_async_task`, `env.timeout_ms.saturating_add(BACKSTOP_GRACE_MS)`), the retry
  fire-at score (`dispatch::schedule_retry`, `now_ms().saturating_add(d_ms)`), and the H1 reclaim
  threshold (`loops::recovery_loop`). Regression: `worker/tests/e2e.rs` sends
  `timeout_ms: 18446744073709551615` (u64::MAX) on a 3s async task and asserts it succeeds
  (not a spurious near-zero timeout, which is what the old wrapping-add produced in release
  builds).
- **H4** (UTF-8 byte-slice panic on executor garbage): Fixed. Added `envelope::safe_truncate`
  (backs off to the nearest preceding char boundary) and used it in `ctx::parse_pyresp`'s
  unparseable-response error message (previously `&s[..s.len().min(512)]`). Regression: unit
  tests `envelope::tests::safe_truncate_never_panics_on_multibyte_boundary` and
  `ctx::tests::garbage_with_multibyte_chars_does_not_panic` (a 600-byte all-multibyte garbage
  string, byte 512 is guaranteed mid-character).
- **Inflight accounting panic safety** (part of H3/MEM-3): Added `ctx::DecrGuard`, a drop guard
  that decrements an `AtomicI64` counter even on panic unwind. Used for `inflight_total`
  (`dispatch::spawn_dispatch`) and `inflight_io` (`exec::run_sync_task`, `run_async_task`),
  replacing manual `fetch_sub` calls that a panic could skip. `exec.rs`'s
  `.expect("spawn_blocking panicked")` (flagged in the audit's code-quality pass) is now mapped
  to a `WorkerShimError` failure instead of propagating a worker-task panic. Regression:
  `ctx::tests::decr_guard_runs_on_panic_unwind` (a tokio task panics inside a guarded scope;
  asserts the counter is back to 0).

## Medium

- **M1** (unvalidated `id`/`idempotency_key`): Fixed. `dispatch::valid_task_id` rejects any
  envelope `id` not matching `[a-z0-9]{32}` -> DLQ `"malformed"`. `idempotency_key` is folded
  through the FNV-1a hash (see C1) before becoming part of a redis key, which bounds its length
  and neutralizes cluster hash-tag / charset injection regardless of input. Client-side
  validation was not added: the client already only ever produces conformant 32-hex ids
  (`uuid4().hex`), and the worker-side hash makes `idempotency_key` safe for any input, so a
  client-side check would be pure defense in depth with no bug it closes. Regression: e2e crafted
  non-hex id -> DLQ malformed; unit test `broker::tests::idemp_key_is_deterministic_and_bounded`.
- **M2** (no envelope size limit): Fixed. New `--max-envelope-bytes` flag (default 1 MiB,
  `cli.rs`); `dispatch::process` rejects an oversize raw payload before `serde_json::from_str`
  ever sees it, storing only a 4KiB truncated preview in the DLQ entry. Regression: e2e sends a
  ~2MB envelope, asserts DLQ `"malformed"` and that the stored `e` field is truncated.
- **M3** (soft-timeout injection races): Fixed the "landed inside a later task" sub-case: a
  per-thread generation counter (`shim.py` `_thread_gen`) fences `_inject_soft` so a stale
  deadline can only fire if the thread is still on the generation that armed it. The narrower
  residual race (deadline fires between `fn()` returning and the `finally` block running,
  flipping a success into `SoftTimeLimitExceeded`) is inherent to `PyThreadState_SetAsyncExc` and
  cannot be closed by the generation counter (same generation); documented in PROTOCOL.md §4.6
  as an explicit, accepted limitation rather than silently left unstated.
- **M4** (credentials in logs/repr): Fixed both languages. `main.rs::redact_redis_url` masks
  `user:password@` before every log line that includes the redis URL (startup log, bad-url
  error, connect error). `py/rupy/app.py::_redact_redis_url` does the same for `Rupy.__repr__`.
  Traceback/result disclosure note added explicitly to PROTOCOL.md §8 and README's new
  "Semantics & limits" section. Regression: Rust unit tests `tests::redacts_userinfo` /
  `leaves_urls_without_userinfo_alone`; py `test_options.py::test_repr_redacts_credentials`.
- **M5** (`RUPY_EXEC_CMD` prod-reachable override): Fixed. Gated behind
  `#[cfg(any(test, feature = "test-hooks"))]` in `cpu.rs`; a plain `cargo build --release` has
  no code path reading that env var at all. `worker/Cargo.toml` adds a non-default `test-hooks`
  feature; the e2e suites now require `cargo test --features test-hooks` (documented in
  PROTOCOL.md §10 and `cpu.rs`'s module doc). Also warns loudly at startup when active, per the
  audit's "at minimum" fallback.
- **M6** (inconsistent Retry recognition): Fixed. `py/rupy/_exec.py` now uses the same duck-typed
  rule as `shim.py` (`type(exc).__name__ == "Retry" and hasattr(exc, "countdown")`) instead of
  `isinstance(exc, rupy.exceptions.Retry)`, so cpu and io tasks agree regardless of which
  `Retry`-named class raised it. PROTOCOL.md §4.2 rewritten to state the rule the code actually
  implements. Regression: `py/tests/test_exec.py::test_retry_recognized_by_duck_type_not_isinstance`
  (a `Retry`-named class that does NOT subclass `rupy.exceptions.Retry`).
- **M7** (protocol/doc drift on cpu Retry countdown): Fixed. PROTOCOL.md §5.1 now documents the
  `{"ok": false, "retry": true, "countdown": ...}` response shape explicitly (it already worked
  in code; only the docs lied). `ctx.rs`'s stale comment ("children cannot carry retry/retryable
  flags... countdown unavailable over the pipe") corrected. `ARCHITECTURE.md` limitation #2
  ("cpu Retry countdown... lost over the pipe") deleted since it was false.
- **M8** (CLI values accepted without floors): Fixed. `--batch` and `--visibility-timeout` are
  validated in `main.rs` after parsing (exit 1 if 0); `--drain-timeout` intentionally left
  unrestricted (0 = instant abandon is a legitimate, documented choice, not a storm-duplication
  risk). `io_loops`/`io_threads`/`io_concurrency` were already floored at the usage site.
  Regression: `m8_cli_floors_reject_zero` in `worker/tests/e2e_lifecycle.rs`.
- **MEM-1** (async `pending` map leaks on Rust-side backstop): Fixed. `PyRuntime::submit_async`
  now returns `(token, receiver)`; `exec::run_async_task` calls the new `PyRuntime::cancel(token)`
  when the backstop timeout fires, removing the pending-completion slot immediately instead of
  waiting (forever, if the loop is truly wedged) for the Python callback to do it.
  `PyRuntime::pending_len()` exported in the stats line as `pending_async`. Regression:
  `mem1_async_backstop_fires_cleanly` in `worker/tests/e2e_lifecycle.rs` (a coroutine that blocks
  its event-loop thread synchronously, so Python's own `wait_for` timeout can never fire, forcing
  the Rust backstop path). True long-run leak absence is a code-review claim (the fix removes the
  entry from the exact map identified as leaking), not something a short e2e test can observe.
- **MEM-4** (thread churn: one `threading.Timer` per soft-timeout task): Fixed. `shim.py` now
  runs one shared watchdog thread servicing a min-heap of `(deadline, tid, generation)`; sync
  tasks with `soft_timeout_ms` push a heap entry instead of spawning a `Timer`/OS thread. Safety
  (M3's generation fence) is unchanged since every popped entry still goes through
  `_inject_soft`'s generation check. Verified via the existing soft-timeout e2e/py test suites
  (behavior-preserving refactor); no new test added since the improvement is a throughput/thread-
  count property, not a new observable behavior.
- **MEM-5** (unbounded cpu-child stdout line buffering): **Skipped.** Low severity; the correct
  fix replaces `AsyncBufReadExt::lines()` with a hand-rolled chunked-read loop enforcing a byte
  cap across two hot-path call sites (ready-line and request/response reads) in `cpu.rs` — a
  materially bigger, regression-risky change than "cheap and safe" for a scenario that requires
  the operator's own task code (or a broken `rupy._exec`) to already be misbehaving.

## Low / Nit (fixed - cheap and safe)

- **L1** (`kill(0, SIGKILL)` footgun): `cpu::kill_children` now skips a tracked pid of 0
  explicitly.
- **L2** (non-atomic completion pipelines): Documented in PROTOCOL.md §4.1 — pipelined, not
  `MULTI`-wrapped, and why (Redis Cluster: a real transaction requires same-slot keys, which
  these pipelines don't guarantee).
- **L3** (idempotency fail-open only in a code comment): Documented explicitly in PROTOCOL.md
  §4.5.
- **L4** (`get()` can block forever by design): Docstring in `result.py::AsyncResult.get`
  updated to state this plainly and recommend an explicit `timeout`.
- **L5** (shim fallback `SoftTimeLimitExceeded` derives `BaseException`): Changed to `Exception`,
  matching the real class.
- **L6** (`Rupy._get_redis` not thread-safe): Double-checked locking added
  (`self._redis_lock`). Regression: `py/tests/test_options.py::test_get_redis_is_thread_safe`.
- **L7** (`sys.path` CWD injection): One-sentence caveat added to README's new "Semantics &
  limits" section and PROTOCOL.md §7.

## Code quality / dead code (all fixed)

- `dispatch::dlq_terminal`'s dead `_unused: Option<()>` parameter removed (and all 4 call sites).
- `ctx.rs`'s stale M7 comment fixed (see M7 above).
- `pyrt::TaskSpec` trimmed from 9 fields (8 behind `#[allow(dead_code)]`) to the 3 actually read
  (`kind`, `is_async`, `timeout_ms` — the last now used by the H1 startup check); shim.py's
  `load_app` JSON is unaffected (serde ignores the extra fields).
- `exec::run_cpu_task`'s `args_json()`-then-reparse round trip replaced with
  `Envelope::args_value()`/`kwargs_value()` (direct `Value`, no serialize-then-deserialize).
- `shim.py::_finish_value` no longer does a throwaway `json.dumps(rv)` probe; `_outcome_json`
  serializes the whole outcome exactly once, in every caller.
- `stats.rs`'s `.max(0)` clamp on `inflight_io`/`inflight_cpu` removed — printed raw, per the
  audit's "hides accounting bugs" note (also now backed by the DecrGuard fix, so it should never
  go negative in practice, but if it ever does, that's exactly the point of not hiding it).
- `main.rs` consumer name: dropped the hardcoded `:0` suffix; PROTOCOL.md §1 updated to
  `{hostname}:{pid}` (no unused `{n}`).
- `py/rupy/exceptions.py` (`TaskFailedError.__init__`) and `py/rupy/result.py`
  (`AsyncResult.__init__`) shadowed-builtin parameters (`type`, `traceback`, `id`) renamed to
  `type_`/`traceback_`/`task_id`; public attribute names (`.type`, `.traceback`, `.id`) unchanged
  (all call sites were already positional, so this is not a breaking change).
- `dispatch.rs`'s two `expect("envelope serialize")` calls: added a one-line comment explaining
  why they're infallible, per the audit's ask.
- Dependency review: `worker/Cargo.toml`'s `tokio` feature set trimmed from `"full"` to the
  ~7 features actually used (`rt-multi-thread`, `macros`, `sync`, `time`, `io-util`, `process`,
  `signal`) in both `[dependencies]` and `[dev-dependencies]`; `Cargo.lock` updated accordingly
  (drops `parking_lot`/`lock_api`/etc. that "full" pulled in unused). `cargo audit`: attempted,
  the advisory-db git fetch stalled on this machine exactly as the original audit noted — not a
  regression, a pre-existing environment limitation; still recommended for CI on a normal network.

## Protocol / docs drift (all fixed)

1. §5.1 schema vs `_exec.py`/ctx.rs/ARCHITECTURE.md: fixed (see M7).
2. §4.2 "matches `rupy.Retry`" vs name-based duck typing: fixed (see M6), §4.2 now states the
   actual duck-typed rule.
3. §4.2 retry promise vs §4.5 claim-at-start reality: fixed (see C1), §4.5 now states the
   "mine again" rule explicitly.
4. §4.1/§4.2 "one pipeline" implying atomicity: fixed (see L2).
5. §7 consumer-name `{n}` vs hardcoded `:0`: fixed (dropped `{n}` from the spec, matches code).
6. README "Redis >= 7.0" only in PROTOCOL: added to README's Quickstart section.

## Release mechanics

- `LICENSE-MIT` and `LICENSE-APACHE` added at repo root (dual MIT/Apache-2.0, copyright "the
  rupy contributors", 2026). `worker/Cargo.toml` and `py/pyproject.toml` both get
  `license = "MIT OR Apache-2.0"`; `pyproject.toml` also gets `authors`, `keywords`,
  `classifiers` (the broader "sparse metadata" nit -- `repository`, full `categories`, MSRV --
  is intentionally left out: it needs a real repo URL and a deliberate MSRV policy decision that
  belong to the project owner, not something to invent here). README gets a "## License" section
  ("Licensed under either of Apache-2.0 or MIT at your option").
- PROTOCOL.md preamble rewritten (dropped "This document is the LAW" / "built by separate
  people" / "flag the deviation loudly" agent-coordination phrasing) and §10 rewritten from
  machine-specific facts (Windows path, WSL distro, username, machine specs) into a generic
  "Building and testing" section with portable commands.
- **PyPI name conflict** (`rupy` already published by an unrelated package) and the package
  **rename entirely**: explicitly **out of scope** per this task's instructions ("Do NOT rename
  the package anywhere") and per separate direction that the project will be renamed (to
  "cauli") in a follow-up step after these commits land. No renaming was done anywhere in this
  change set.
- **CI, CONTRIBUTING.md, CHANGELOG.md, SECURITY.md, CODE_OF_CONDUCT.md**: not added — outside
  the file scope given for this task (new root files were limited to the two LICENSE files).

## Formatting

- `cargo fmt` run once across the whole `worker/` crate after all the above; `cargo fmt --check`
  is clean.
- `ruff format` run once across `py/`, `worker/src/shim.py`, `itest/` after all the above;
  `ruff format --check` is clean. `ruff check` is clean (zero findings, same as the original
  audit).
- `cargo clippy --all-targets` (both with and without `--features test-hooks`): zero warnings.
