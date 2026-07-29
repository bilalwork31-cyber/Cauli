# rupy pre-release audit (commit f7e8352)

Scope: `worker/src/**` (Rust + embedded `shim.py`), `py/rupy/**`, `py/pyproject.toml`,
`worker/Cargo.toml`, `PROTOCOL.md`, `README.md`, `worker/ARCHITECTURE.md`.
`itest/` and `bench/` used as usage references and empirical memory evidence only.

---

## Executive summary

**Ship-blocker verdict: YES — do not open source at this commit.** The runtime core is in
genuinely good shape (clean module layout, honest architecture doc, real e2e coverage, no
linear memory leak signal in a 10-minute 445k-task sustained run), but there is one
correctness bug that silently breaks a headline feature, a set of crafted-envelope panic
paths, and two release-mechanics blockers (no LICENSE, PyPI name is taken).

Top 5 issues:

1. **[C1] `idempotency_key` silently disables retries and masks failures.** The guard is
   claimed at execution start keyed only on existence, so the task's *own* retry finds its
   own claim and resolves as `"duplicate"` — the task never re-executes and the client sees
   `None` instead of the failure. Retry + idempotency cannot be used together today.
2. **[H1] Default visibility timeout (60s) is smaller than the default task timeout (300s).**
   Any task that runs longer than 60s is re-claimed by the recovery loop — including by the
   *same still-running worker* — and executed concurrently a second time, with default settings.
3. **[H3/H4] Crafted envelopes can panic worker tasks**: `env.timeout_ms + 2_000` overflows
   on `timeout_ms: u64::MAX` (panic in debug, near-zero timeout in release), and the
   executor-response error path slices a `String` at byte 512, which panics on a UTF-8
   boundary. Panics also leak `inflight_total`, so shutdown then always burns the full
   drain timeout.
4. **[H2] Sync pool threads lost to hard timeouts are never replaced**, and abandoned jobs
   still parked in the unbounded queue execute anyway later ("zombie execution"). A slow
   drip of wedged tasks permanently destroys sync-io capacity on a worker meant to run for
   months.
5. **[R-OSS] Release blockers: no LICENSE file anywhere, and the PyPI name `rupy` is
   already taken** (v0.6, "Random useful python stuff", last release 2022-12) — the
   documented `pip install rupy` installs someone else's package. Also `PROTOCOL.md` §10
   leaks internal machine details and reads as AI-agent coordination copy.

Memory verdict: **no leak signal** at the 10-minute scale (see §1 evidence); the risks are
the structural unbounded-growth paths listed below, not a steady-state drip.

---

## 1. Memory leak audit

### Empirical evidence (bench data, sustained runs)

- `bench/campaign/results/C_rupy.json` — 445,180-task, ~10-minute campaign. Stack RSS
  (harness sampler, 622 samples): 216 MB at t=29s → 225.4 MB at t=598s. Tail slope decays
  monotonically: 0.33 MB/min (last 300s) → 0.23 (last 150s) → **0.018 MB/min in the final
  minute** (flat at 225.4 MB for the last ~80s). Asymptotic, allocator-arena-shaped — not a
  linear leak.
- `bench/campaign/results/logs/C_rupy.worker.log` — the worker process's own `rss_mb`
  stats: 42 idle → 178 first-load → 188 at end (24,781 tasks through this worker). +1 MB
  over the final ~3.5 minutes, still decelerating.
- `bench/results/S4d_rupy_io_idle.json` / `S4e_rupy_cpu_idle.json`: idle RSS flat (0.00 /
  0.05 MB/min).
- Caveat: 10 minutes is not "months". Recommend a 24h mixed-load soak (with retries,
  timeouts, and cpu-child kills in the mix — the benchmark exercises the happy path almost
  exclusively: `failed=0 retried=0 dlq=0` throughout C_rupy) before making longevity claims.

### Structural findings

**MEM-1 (Medium): async `pending` map entries leak when the Rust backstop fires.**
`worker/src/pyrt.rs:53` holds `pending: HashMap<u64, oneshot::Sender<String>>`; the only
removal paths are the Python completion callback (`pyrt.rs:111`) and a submit error
(`pyrt.rs:195`). `worker/src/exec.rs:71-78` enforces a Rust-side backstop
(`timeout_ms + 2s`); when it fires the receiver is dropped but the map entry stays until
the Python callback eventually runs. If an event-loop thread is wedged (async task doing
blocking work), that is *never*, and the Python side also retains the pending coroutine,
its args/kwargs strings, and the `asyncio.wait_for` machinery. Every backstop firing
against a wedged loop leaks one entry + one coroutine, forever.
*Fix:* remove the token from `pending` in the `Err(_)` branch of `run_async_task` (give
`submit_async` a token handle / `cancel(token)` method), and export `pending.len()` in the
stats line so a wedge is observable.

**MEM-2 (High, doubles as robustness — see H2): sync pool loses threads permanently and
queues zombie jobs.** `worker/src/exec.rs:35-49` abandons the oneshot on hard timeout, but:
(a) the stuck OS thread (`pyrt.rs:230-243`) is never replaced — the pool shrinks by one for
the process lifetime; (b) the crossbeam channel (`pyrt.rs:226`) is unbounded and jobs whose
dispatcher already timed out are still executed when a thread eventually frees up, burning
capacity on results nobody will read; (c) with all threads wedged, the channel grows by
~`io_concurrency` jobs per `timeout_ms` period, indefinitely (each queued job also pins its
`SyncJob` strings — cloned args/kwargs — in memory).
*Fix:* in the pool loop, `continue` when `job.resp.is_closed()` (kills zombie execution
and bounds the queue in practice); count abandoned threads and spawn a replacement thread
when one is detected (the abandoned thread will die whenever the task returns —
`rx.recv()` will fail once the pool is dropped, or gate on a generation counter); export
live/abandoned thread counts in stats.

**MEM-3 (Low): panics leak `inflight_total` and inflate counters.**
`worker/src/dispatch.rs:16-19` does `fetch_sub` *after* `process().await` inside the
spawned task; any panic in the dispatch path (see H3/H4 — reachable from crafted input)
skips the decrement. `inflight_total` then never reaches 0 and every shutdown waits the
full `--drain-timeout` (`main.rs:156-165`). `inflight_io` has the same skew
(`exec.rs:26/50`).
*Fix:* decrement via a drop guard (or wrap `process` in `AssertUnwindSafe(...).catch_unwind()`).

**MEM-4 (Medium, perf): one OS thread per sync task with a soft timeout.**
`worker/src/shim.py:174-177` builds a `threading.Timer` per `run_sync` call — a full
thread spawn/teardown per task. At benchmark rates (hundreds of tasks/s) that is thousands
of thread creations per second if apps set `soft_timeout`. Not a leak (cancelled timers
exit), but heavy churn on a hot path.
*Fix:* one shared watchdog thread with a heap of (deadline, tid, generation).

**MEM-5 (Low): unbounded child stdout line buffering.** `worker/src/cpu.rs:147,197` reads
child responses with `lines()`; a buggy/hostile child (or a huge legitimate result) can
emit an arbitrarily long line that BufReader accumulates entirely in RAM.
*Fix:* cap the line length (e.g. `.take(limit)` on the reader) and treat overflow as
`WorkerLost`.

**Clean paths verified:** completion callback closure only holds the `pending` Arc
(`pyrt.rs:104-117`); tokens are `AtomicU64` (no reuse in practice); `_arun`
(`shim.py:229-265`) always invokes the callback under a defensive `except BaseException`
so the map is drained on every Python-visible completion path; sync-pool oneshots are
consumed or dropped, never parked; the fetch loop's semaphore gate (`loops.rs:21`) means
dispatch-task backlog is bounded by ~`io_concurrency + batch`; cpu backlog is genuinely
bounded at `2 * cpu_workers` with the overflow flag pausing fetch (`cpu.rs:73`,
`exec.rs:101-114`, `loops.rs:21`); cpu children are `wait()`ed on every respawn path (no
zombies; `cpu.rs:166-171, 179-184, 219-221`) and carry `PR_SET_PDEATHSIG`; redis
connections are two `ConnectionManager`s total, cheaply cloned handles; `mover/recovery/
stats` loops reuse a single `tokio::time::interval` each (no timer accumulation);
`py/rupy/_exec.py` has no per-request caches — the loop allocates and drops per line;
client `AsyncResult` polling holds no per-poll state.

---

## 2. Robustness / hardening findings

Ranked. Envelope contents treated as unvalidated input throughout.

### Critical

**C1: `idempotency_key` + retry = silent failure masking.**
`worker/src/dispatch.rs:37-57` claims `rupy:idemp:{key}` with `SET NX` at execution start
(`broker.rs:73-88`), storing the task id — but never compares the existing value to the
current envelope's id. On a retryable failure, `schedule_retry` re-enqueues the *same*
task id (`dispatch.rs:104-129`); the retry then finds its own claim, takes the
`Ok(false)` branch, and finishes as `"duplicate"` (counted as `ok`). Consequences: (1)
retries never execute for any task with an idempotency key — PROTOCOL §4.2's promise is
unreachable; (2) the transient failure is never stored (the retry path writes no result,
§4.2 step 4), so the final visible state is `duplicate` / `get() → None`: the failure is
invisible to the caller. PROTOCOL §4.5 documents the *crash* case ("redelivered claimed
tasks resolve as duplicate") but nowhere says "keys disable retries"; §4.2 says the
opposite. The itest suite only covers the happy dedup path
(`itest/test_integration.py:98-112`), so this was never caught.
*Fix:* treat `existing value == env.id` as "my claim, proceed" (single Lua: SET NX, else
GET and compare), which also makes crash-redelivery re-execute correctly for at-least-once
semantics; or DEL the key (compare-and-delete) when scheduling a retry. Add an itest:
idempotency_key + task that fails once then succeeds.

### High

**H1: recovery re-executes still-running tasks with default settings.**
Default `--visibility-timeout` is 60s (`cli.rs:40-41`) while the default envelope/task
`timeout_ms` is 300,000 (`envelope.rs:29-31`, `task.py:37`). The recovery loop
(`loops.rs:70-130`) XPENDINGs entries idle > 60s and XCLAIMs them — XPENDING does not
exclude the worker's own consumer, so a single worker running a legitimate 2-minute task
will claim *its own in-flight entry* at ~60-90s and dispatch it a second time,
concurrently, in the same process. Multi-worker deployments duplicate across workers. The
duplicate also inflates `delivery_count`, so a healthy long task can eventually be DLQ'd
as `redelivery_limit` while still succeeding. At-least-once semantics technically permit
this, but defaults that guarantee duplication for any task >60s are a production trap.
*Fix (layered):* (a) at startup, error or loudly warn if any registered task's
`timeout_ms` ≥ `visibility_timeout * 1000`; (b) skip XPENDING entries whose consumer is
`ctx.consumer` and whose id is currently in an in-flight set; (c) longer term, heartbeat
in-flight entries (periodic `XCLAIM` on own entries resets idle). Document the invariant
"visibility_timeout must exceed your longest task" in README/PROTOCOL.

**H2: sync-pool thread abandonment (capacity death spiral + zombie execution).**
See MEM-2. Robustness angle: a dependency outage that wedges 64 tasks (socket read, no TCP
timeout, C extension ignoring `SetAsyncExc`) permanently zeroes sync-io capacity; every
subsequent sync task then hard-times-out after queueing, while the queue grows without
bound and abandoned jobs re-execute side effects later at unpredictable times.
ARCHITECTURE.md honestly documents the per-slot loss but not the "never recovered" or
"still executes later" parts.

**H3: crafted-envelope integer overflow.** `worker/src/exec.rs:71`:
`timeout(Duration::from_millis(env.timeout_ms + 2_000), rx)` — an envelope with
`"timeout_ms": 18446744073709551615` (valid u64 JSON) panics the dispatch task in debug
builds and wraps to a ~2s backstop in release (spurious TimeoutError + retry churn).
Similarly `dispatch.rs:122` `now_ms() + d_ms` with attacker-chosen `backoff_max_ms` near
`u64::MAX` wraps `fire_at` to a tiny score, making the delayed entry fire immediately (hot
retry loop until max_retries).
*Fix:* `saturating_add` in both places; consider clamping envelope-supplied `timeout_ms`,
`backoff_*` to sane ceilings at parse time.

**H4: UTF-8 byte-slice panic on executor garbage.** `worker/src/ctx.rs:78`:
`&s[..s.len().min(512)]` panics if byte 512 is not a char boundary. `s` is any
line a cpu child wrote (a mixed-version `rupy._exec`, a `RUPY_EXEC_CMD` stand-in, or a
child in a corrupted state can emit non-JSON with multibyte characters). Combined with
MEM-3, one bad response line poisons the drain accounting permanently.
*Fix:* truncate on a char boundary (`s.char_indices().take_while(|(i, _)| *i < 512)` or
`floor_char_boundary` when stabilized), or escape with `{:?}`.

### Medium

**M1: envelope `id` and `idempotency_key` are unvalidated key material.**
`dispatch.rs:27-29` only checks non-empty; `broker.rs:18-23` interpolates both into redis
keys. A crafted `id` (protocol says 32-hex, worker enforces nothing) can: collide with /
overwrite another task's `rupy:result:{id}` (SET is last-writer-wins), embed `{hash-tags}`
that change cluster slot routing, or be megabytes long (key-size DoS; also breaks
`CLUSTER` multi-key pipelines like SET+XACK in cluster mode). Same for `idempotency_key`
(`rupy:idemp:{key}`), which is app-author-controlled by design. The *client* validates
queue names but neither validates ids/keys, and the *worker* validates only `--queues`
(`main.rs:67-72` — correctly; envelope `queue`/`task` are never interpolated into keys,
verified).
*Fix:* worker-side, reject envelopes whose `id` fails `[a-z0-9]{32}` (→ DLQ `malformed`);
cap `idempotency_key` length (e.g. 512 bytes) and either restrict charset or hash it
(`sha256` hex) into the key. Client-side, validate at `apply_async`.

**M2: no size limits on args/kwargs/envelope.** `Envelope.args/kwargs` are parsed into
`serde_json::Value` (2-10x memory amplification), then re-serialized per execution class;
with `--batch 16` and 256 in-flight, multi-MB payloads multiply. Nothing rejects a 100 MB
envelope.
*Fix:* configurable max envelope size checked before `serde_json::from_str`
(oversize → DLQ `malformed`); document a payload-size recommendation ("pass references,
not blobs").

**M3: soft-timeout injection races (PyThreadState_SetAsyncExc).**
`worker/src/shim.py:160-188`: (a) if the timer fires after `fn()` returns but before
`timer.cancel()`, the exception surfaces inside `_finish_value`/`json.dumps` and converts
a *successful* execution into a `SoftTimeLimitExceeded` failure → retry → duplicate side
effects; (b) narrower: if the timer thread stalls between `timer.cancel()` and the
`_set_async_exc(tid, None)` clear (`shim.py:184-187`), the injection can land after the
clear and detonate inside the *next task* running on that pool thread. Thread-id reuse is
not an issue (pool threads are long-lived), but task-slot reuse is.
*Fix:* per-thread generation counter: `_inject_soft(tid, gen)` no-ops unless
`_current_gen[tid] == gen`; bump the generation in the `finally` before clearing. Document
the residual "success flipped to timeout" window as inherent to async-exc.

**M4: credentials leak into logs and repr.** `main.rs:73-79` logs the full redis URL at
info level; `main.rs:98,105` echo it in connection errors; `py/rupy/app.py:158-162`
includes it in `__repr__`. `redis://user:password@host/0` is a common shape; logs and
tracebacks (repr) will contain the password.
*Fix:* redact userinfo before logging (`redis://***@host/0`) in both languages. Related
disclosure note: full task tracebacks are stored in `rupy:result:*` and DLQ entries —
documented in PROTOCOL §8, but README should say it plainly (tracebacks often contain
secrets/PII; anyone with redis read access sees them).

**M5: `RUPY_EXEC_CMD` replaces the cpu child binary from the environment.**
`cpu.rs:48-54`, whitespace-split, used verbatim. It is honestly documented as a test hook
(ARCHITECTURE.md), and an attacker who sets the worker's env has won anyway — but for a
public release it is an unnecessary prod-reachable override that security reviewers will
flag.
*Fix:* gate behind `#[cfg(any(test, feature = "test-hooks"))]` (e2e builds with the
feature), or at minimum `warn!` loudly at startup when set.

**M6: `Retry` recognition is inconsistent and name-based.** PROTOCOL §4.2 says the worker
matches "the exception class exposed as `rupy.Retry`". Reality: the io shim duck-types on
class *name* + `.countdown` (`shim.py:60-62`); the cpu child matches the real class
(`_exec.py:101`); the Rust cpu mapping *also* force-retries any error whose type string is
"Retry" (`ctx.rs:90`) — but then with `countdown: None`. Any user exception named `Retry`
changes control flow, differently per execution class.
*Fix:* pick one rule (module+name match is the practical embedded-interpreter version),
implement it in both shim and `_exec`, and make PROTOCOL say what the code does.

**M7: three-way drift on cpu Retry countdown (drift kills trust).**
`py/rupy/_exec.py:101-109` sends `"retry": true, "countdown": <float>` over the pipe;
`ctx.rs:71-95` honors `resp.retry`/`resp.countdown` for cpu responses too — so countdown
*works*. But `ctx.rs:67-70`'s comment says "children cannot carry retry/retryable flags
... countdown unavailable over the pipe => None", ARCHITECTURE.md "Known limitations" #2
claims "countdown lost over the pipe", and PROTOCOL §5.1's response schema has no
`retry`/`countdown` fields at all. Three documents, three stories, none matching the code.
*Fix:* add the fields to §5.1, delete limitation #2, fix the ctx.rs comment.

**M8: CLI values accepted without floors.** `--batch 0` → `COUNT 0` (unlimited XREADGROUP
fetch); `--visibility-timeout 0` → recovery claims *everything currently executing* every
500ms, storm-duplicating all in-flight work; `--drain-timeout 0` → instant abandon.
`io_loops/io_threads/io_concurrency` are already floored with `.max(1)`.
*Fix:* clap `value_parser(RangeInclusive)` floors: batch ≥ 1, visibility_timeout ≥ 5, etc.

### Low

**L1: `kill(0, SIGKILL)` latent footgun.** `cpu.rs:143` `child.id().unwrap_or(0)`;
`kill_children` (`cpu.rs:95-102`) then does `libc::kill(0, SIGKILL)` if a 0 ever gets
tracked — which signals the worker's *entire process group* (self-SIGKILL, plus whatever
shares the group). Currently unreachable (id() is Some right after spawn), one refactor
away from a very bad day. Skip pid 0 explicitly.

**L2: non-atomic completion pipelines.** All finish paths use `redis::pipe()` without
MULTI (`broker.rs:91-190`). A connection drop mid-pipeline can e.g. ZADD a retry without
XACKing the original → both the retry copy and the recovered original run. Fine under
at-least-once, but PROTOCOL §4.1/§4.2's "one pipeline" phrasing implies more atomicity
than exists — state it, or use `.atomic()`.

**L3: idempotency fail-open only documented in a code comment.** `dispatch.rs:52-56`:
redis error during claim → execute anyway. Reasonable choice; belongs in PROTOCOL §4.5.

**L4: client `get()` can block forever by design.** Malformed/unregistered/
redelivery-limit DLQ entries get no result key (ARCHITECTURE limitation #3), and
`store_result=False` tasks never have one; `AsyncResult.get()` without `timeout`
(`result.py:47-75`) then never returns. Docstring should warn; consider always passing a
timeout in examples.

**L5: shim fallback exception class mismatch.** `shim.py:35` stand-in
`SoftTimeLimitExceeded` derives `BaseException`; the real one (`exceptions.py:28`) derives
`Exception`. In the fallback case the injected exception isn't the class tasks catch and
escapes `except Exception`. Only reachable when the `rupy` package is absent from the
worker's interpreter (test-only in practice) — align the base class anyway.

**L6: `Rupy._get_redis` is not thread-safe** (`app.py:53-57`): two threads racing first
use can build two connection-pool clients; one leaks its pool. Use a lock or
`functools.cached_property`-style guard. Same pattern is fine in practice but cheap to fix.

**L7: `sys.path` injection of CWD.** `shim.py:96-98` and `_exec.py:58-60` prepend the
worker's CWD to `sys.path` (documented in PROTOCOL §7). Running `rupy-worker` in an
untrusted directory imports attacker-controlled modules. Standard Python-tool caveat;
worth one sentence in README ops notes.

**Verified non-issues (attack surface checks that passed):**
- Task dispatch can only reach registered names: worker routes via `ctx.registry`
  (in-memory map from `load_app`) and the shim looks up `_registry` — no dynamic import is
  ever driven by envelope contents (`dispatch.rs:31-34`, `shim.py:161,231`, `_exec.py:77`).
- Worker validates `--queues` (and the app default queue) against the same
  `[a-zA-Z0-9_.-]+` rule as the client before any key interpolation (`main.rs:62-72`,
  `cli.rs:60-64`); envelope `queue`/`task`/`kind` never reach key strings or the Lua mover.
- cpu children are spawned argv-style via `tokio::process::Command` — no shell anywhere
  (`cpu.rs:122-135`).
- Task writes to fd 1 in cpu children cannot corrupt the pipe protocol: `_exec.py:144-149`
  dup's the real stdout to a private fd and re-points fd 1 at stderr before the ready
  line. Verified correct ordering (dup before any protocol output).
- Malformed envelope JSON, missing `e` field, empty id/task, and unreadable recovery
  claims all route to DLQ without executing and without wedging the fetch loop
  (`dispatch.rs:22-34`, `loops.rs:100-118`); serde rejects NaN/Infinity and negative
  integers for u64 fields (malformed → DLQ). The client refuses NaN at enqueue
  (`app.py:29-32`).
- Signal handling: first SIGTERM/SIGINT flips a watch channel (no async-signal-unsafe
  work in handler context), second exits 130; children die via PDEATHSIG. Drain keeps
  mover/acks alive per §4.7 (`main.rs:148-168,171-187`). No race found beyond the
  MEM-3 counter caveat.

### Dependency review

`worker/Cargo.toml`: anyhow, async-channel 2, clap 4, crossbeam-channel 0.5, gethostname,
libc, pyo3 0.26, rand 0.9, redis 0.32, serde/serde_json, tokio 1 (`features = ["full"]` —
trim to the ~6 features actually used for compile time and surface), tracing. All
mainstream and current-generation. Lockfile check: crossbeam-channel 0.5.16 (past the
RUSTSEC-2025-0024 double-free fixed in 0.5.15), pyo3 0.26.0 (the RUSTSEC-2024-0392 buffer
overflow affected <0.24), redis 0.32.7, tokio 1.53.0, rand 0.9.5 — nothing known-bad
locked. Keep `Cargo.lock` committed and add `cargo audit` to CI. `py/`: single runtime
dep `redis>=5` — appropriately minimal.
Authoritative advisory scan: see §4 dynamic results.

---

## 3. Open source readiness

### Release blockers

- **No LICENSE file** (repo-wide, and no `license` field in either manifest — `cargo
  publish` will refuse outright). Recommend dual **MIT OR Apache-2.0** (Rust ecosystem
  norm, maximally adoptable for the Python side too): `LICENSE-MIT` + `LICENSE-APACHE` at
  root, `license = "MIT OR Apache-2.0"` in both manifests.
- **PyPI name `rupy` is taken**: <https://pypi.org/project/rupy/> — v0.6 "Random useful
  python stuff", last released 2022-12. `pip install rupy` (py/README.md:8) currently
  installs that package. PEP 541 name transfer is slow and uncertain. Rename the
  distribution (e.g. `rupy-tasks`, `rupyq`) or pick a new project name before any
  announcement; there is also a long-standing Java HTTP server called "rupy" competing for
  search results. crates.io: `rupy-worker` is **available** (404 as of 2026-07-20).
- **PROTOCOL.md must be rewritten for a public audience.** The preamble ("This document is
  the LAW", "built by separate people", "flag the deviation loudly in your final report")
  is agent-coordination copy, and **§10 leaks internal environment details** (Windows
  paths, WSL distro and username, machine specs, "internet OK"). A skeptical HN reader
  will screenshot §10. Keep §1-§9 (they are genuinely good spec writing), delete §10 into
  an internal doc, and rewrite the preamble as a normal versioned-protocol statement.

### Checklist

| Item | Status | Notes |
|---|---|---|
| LICENSE | **Missing** | blocker, see above |
| README accuracy | Good | all claimed flags exist in `cli.rs`; architecture diagram matches code; add a "semantics & limits" section (at-least-once, visibility-timeout invariant, sync hard-timeout limits, tracebacks-in-redis) |
| CONTRIBUTING.md | Missing | build matrix (WSL/Linux-only worker), test invocations (`cargo test` needs redis + python3-dev), style tools |
| CHANGELOG.md | Missing | start at 0.1.0, Keep-a-Changelog format |
| SECURITY.md | Missing | disclosure contact; state the trust model (redis is trusted infra; envelope payloads are not) |
| CODE_OF_CONDUCT.md | Missing | Contributor Covenant |
| CI | **Missing entirely** | suggested GitHub Actions matrix below |
| `worker/Cargo.toml` metadata | Sparse | missing `license`, `repository`, `readme`, `keywords`, `categories`, `rust-version` (MSRV); trim `tokio` features |
| `py/pyproject.toml` metadata | Sparse | missing `license`, `authors`, `urls`, `classifiers`, `keywords`; consider `dynamic = ["version"]` sourcing `__init__.__version__` (currently duplicated, both 0.1.0, consistent today) |
| Versioning | OK | 0.1.0 across Cargo.toml / pyproject / `__init__.py` |
| py typing | Good | `py.typed` shipped, public API fully annotated |
| Rustdoc / docstrings | Good | module-level docs everywhere; public fns documented; py docstrings say why |
| Tests | Good story | worker unit tests + 2 e2e binaries + cross-component itest + py unit tests; gaps: no test for C1 (idemp+retry), H1 (long task vs visibility), crafted-envelope fuzz cases |
| Bench reproducibility | OK | results JSONs untracked (gitignored) but RESULTS.md summaries are tracked; fine |

**Suggested CI (GitHub Actions):**
- `worker`: ubuntu-latest; matrix `{rust: [stable, MSRV], python: [3.12]}`; services:
  `redis:7`; steps: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test` (needs `python3-dev`), `cargo audit` (scheduled weekly too).
- `py`: ubuntu + windows + macos (client is cross-platform); matrix python 3.10-3.13;
  `ruff check`, `ruff format --check`, `pytest py/tests`.
- `itest`: ubuntu; build release worker, `pip install -e py`, redis service, `pytest itest`.

### Code quality, file by file (nits unless marked)

- `worker/src/dispatch.rs:156-164` — `dlq_terminal(..., _unused: Option<()>)`: dead
  parameter on a 7-arg function; every call site passes `None`. Delete it (it reads like a
  leftover from a refactor — exactly the kind of thing "AI slop" hunters grep for).
- `worker/src/ctx.rs:67-70` — comment factually wrong (see M7). Stale comments in a
  protocol-normalization function are a trust problem.
- `worker/src/pyrt.rs:24-40` — `TaskSpec` carries 9 `#[allow(dead_code)]` fields "kept for
  introspection" that nothing reads. Either read them (startup validation: e.g. warn when
  `timeout_ms > visibility_timeout`, which would fix H1's observability) or cut to
  `kind` + `is_async`.
- `worker/src/exec.rs:90-91` — `serde_json::from_str::<Value>(&env.args_json())` parses a
  string that was just serialized from `env.args` (a `Value`). Use `env.args` directly
  (normalize null → `[]` once); round-tripping JSON to build JSON invites "generated code"
  snark.
- `worker/src/shim.py:66-78` — `_finish_value` runs `json.dumps(rv)` to probe
  serializability, discards it, then the caller dumps the whole outcome again: double
  serialization of every successful sync/async result. Restructure to dump once and catch.
- `worker/src/stats.rs:25-26` — `.max(0)` clamps negative inflight counters in the stats
  line, hiding accounting bugs (MEM-3 would be visible without it). Print raw values.
- `worker/src/main.rs:127-131` — consumer name hardcodes `:0` suffix; protocol says
  `{hostname}:{pid}:{n}`. Harmless, but either use it or drop `{n}` from the spec.
- `py/rupy/exceptions.py:43-52` — parameter `type` shadows the builtin (and `traceback`
  shadows the module); `py/rupy/result.py:25` — parameter `id` shadows the builtin. Rename
  (`type_`/`task_id`) or add trailing underscores; linters will flag these on day one.
- `py/rupy/result.py:47` — `get()` is poll-based (50ms default). Fine for v1; docstring
  should say so and warn about the forever-block cases (L4).
- `py/rupy/app.py:53-57` — thread-safety (L6).
- `worker/ARCHITECTURE.md` — genuinely good, honest doc; fix limitation #2 (M7) and add
  the "abandoned sync threads are never replaced" caveat to limitation #1 (H2).
- Comment quality overall: strong — comments explain why (GIL discipline in `pyrt.rs:1-9`,
  `.pth` rationale in `shim.py:103-104`, fd-dup rationale in `_exec.py:8-15`). No
  model-talking-to-reviewer filler found in code. The offenders are PROTOCOL.md's preamble
  /§10 and the stale drift comments above.
- Naming consistency: good throughout (envelope/queue/kind vocabulary is uniform across
  Rust, Python, and docs).
- `unwrap()/expect()` inventory: startup-time uses are fine (`main.rs:84,173-174`);
  mutex `.unwrap()`s are conventional; the two `expect("envelope serialize")` in
  `dispatch.rs:123,141` are actually infallible (serde_json can't produce NaN from parsed
  JSON) but deserve a comment saying why; `exec.rs:69` `expect("spawn_blocking panicked")`
  converts a Python-side panic into a worker-task panic that then trips MEM-3 — map it to
  a `WorkerShimError` failure instead.
- Function length / module organization: good; largest function (`cpu.rs::child_loop`,
  ~110 lines) is a readable state machine; module map in ARCHITECTURE.md matches reality.

### Protocol / docs drift summary (each erodes trust independently)

1. §5.1 response schema vs `_exec.py` retry/countdown fields vs ctx.rs comment vs
   ARCHITECTURE limitation #2 (M7).
2. §4.2 "matches the class `rupy.Retry`" vs name-based duck typing (M6).
3. §4.2 retry promise vs §4.5 claim-at-start reality for idempotent tasks (C1) — the
   protocol never states that an idempotency key currently disables retries.
4. §4.1/§4.2 "one pipeline" implies atomicity the pipelines don't have (L2).
5. §7 consumer-name `{n}` vs hardcoded `:0` (nit).
6. README "Redis >= 7.0" appears only in PROTOCOL; surface it in README install notes.

---

## 4. Dynamic check results

Run in WSL Ubuntu-24.04 after the parallel benchmark released the machine
(`CARGO_TARGET_DIR=~/rupy-target`, ruff 0.15.22 in a fresh `~/rupy-audit-venv`).

- **`cargo fmt --check`: FAIL — 63 diff hunks across 10 files** (`src/backoff.rs`,
  `src/cpu.rs`, `src/ctx.rs`, `src/dispatch.rs`, `src/exec.rs`, `src/loops.rs`,
  `src/main.rs`, `src/pyrt.rs`, `tests/common/mod.rs`, `tests/e2e.rs`,
  `tests/e2e_lifecycle.rs`). All cosmetic (manual line-wrapping vs rustfmt's), but CI with
  `fmt --check` would fail on day one; run `cargo fmt` once before release.
- **`cargo clippy --all-targets`: CLEAN** — zero warnings on default lints. Genuinely good
  signal for the review-bait crowd.
- **`ruff check py/ worker/src/shim.py itest/`: CLEAN** — zero findings on default rules.
  (Note: default ruff does not include the shadowed-builtin lints behind the §3 naming
  nits; enabling `A` (flake8-builtins) would surface them.)
- **`ruff format --check`: FAIL — all 17 Python files would be reformatted.** Diffs are
  trivial (e.g. blank line after module docstring), i.e. consistent hand style that isn't
  ruff/black style. Either adopt `ruff format` once, or ship a `[tool.ruff]` config that
  matches the existing style; a formatter-check CI gate needs one or the other.
- **Short soak (throwaway redis :6395, release worker, fresh client venv):** 45s idle →
  150s sustained mixed load → 60s post-load idle. 62,176 tasks executed through one worker
  (58,290 ok; 1,943 failed → 1,943 retried → 1,943 DLQ'd — i.e. unlike the benchmark, this
  soak exercised the retry, DLQ, soft-timeout-timer, and async paths, not just the happy
  path). Worker RSS: **34 MB idle → 39 MB under load → 39 MB flat** for the entire
  post-load idle minute, then a clean SIGTERM drain ("drained cleanly", exit 0). No
  post-load growth at all; corroborates the §1 no-leak read. (cpu-child path not loaded in
  this soak; it is covered by the S2b/S4e bench data in §1.)
- **`cargo audit`: attempted, did not complete in the audit window** — the RustSec
  advisory-db git fetch was network-bound on this machine (three attempts, including a
  shallow clone, all stalled in transfer). Substituted with a manual review of
  `worker/Cargo.lock` against known advisories (see §2 dependency review): crossbeam-channel
  0.5.16 (RUSTSEC-2025-0024 fixed in 0.5.15 — clear), pyo3 0.26.0 (RUSTSEC-2024-0392
  affected <0.24 — clear), redis 0.32.7 / tokio 1.53.0 / rand 0.9.5 — no known advisories.
  **Action item: run `cargo audit` in CI on a machine with normal GitHub throughput before
  tagging a release.**
