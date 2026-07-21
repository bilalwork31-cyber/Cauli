# cauli protocol v1

This document is the versioned wire/behavior contract between the Rust worker (`worker/`) and
the Python package (`py/`): Redis key layout, envelope JSON, retry/timeout/idempotency
semantics, and the cpu-child pipe protocol. Implementations in either language must match it;
where this document and the code disagree, that is a bug (in one or the other), not a license
to improvise.

cauli is a Rust background worker runtime for Python task queues. One Rust OS process executes
many Python tasks concurrently:

- **io tasks** run inside ONE embedded CPython interpreter: `async def` tasks on embedded
  asyncio event loop thread(s); sync io tasks on a Python thread pool (the GIL is released
  during blocking I/O by CPython itself).
- **cpu tasks** run on a small pool of child Python processes (`python3 -m cauli._exec`),
  sized to CPU cores, fed over a line delimited JSON pipe protocol.

Broker and result backend: Redis >= 7.0. At least once delivery via Redis Streams consumer
groups. All timestamps are integer unix epoch **milliseconds** unless stated otherwise.

---

## 1. Redis key layout

All keys use prefix `cauli:`. `{queue}` is a queue name matching `[a-zA-Z0-9_.-]+`.

| Key | Type | Purpose |
|---|---|---|
| `cauli:q:{queue}` | Stream | Ready tasks. Each entry has exactly one field `e` whose value is the envelope JSON (UTF-8). |
| `cauli:delayed:{queue}` | ZSET | Delayed/retrying tasks. member = envelope JSON string, score = fire_at epoch ms. |
| `cauli:dlq:{queue}` | Stream | Dead letters. Fields: `e` = envelope JSON, `reason` = string, `error` = error JSON (see §8) or empty string. |
| `cauli:result:{task_id}` | String | Result JSON (see §8), `SET ... EX result_ttl`. |
| `cauli:idemp:{h}` | String | Idempotency guard. Value = task id that claimed it. `SET NX EX idemp_ttl`. |

`{h}` in the idempotency key is a deterministic hash of the app-supplied `idempotency_key`
(the worker uses FNV-1a 64-bit, hex-encoded), not the raw string. This bounds the key to a
fixed size and neutralizes cluster hash-tag injection (`{...}`) or other charset abuse from an
app-controlled string; it does not need to be cryptographic since idempotency keys are chosen
by the app author, not an adversary distinct from the app.

Consumer group: name `cauli`, created per queue stream with
`XGROUP CREATE cauli:q:{queue} cauli 0 MKSTREAM` (ignore BUSYGROUP error).
Consumer name: `{hostname}:{pid}` (any unique string is acceptable).

## 2. Envelope JSON

Produced by the Python client at enqueue time. Field names and types are exact.
Unknown fields must be preserved on re-enqueue (retries) if practical, otherwise dropped.

```json
{
  "v": 1,
  "id": "32 char lowercase hex (uuid4().hex)",
  "task": "registered task name, e.g. myapp.tasks.send_email",
  "args": [],
  "kwargs": {},
  "queue": "default",
  "kind": "io",
  "retries": 0,
  "max_retries": 3,
  "backoff_base_ms": 500,
  "backoff_factor": 2.0,
  "backoff_max_ms": 60000,
  "jitter": true,
  "timeout_ms": 300000,
  "soft_timeout_ms": null,
  "idempotency_key": null,
  "store_result": true,
  "enqueued_at": 0,
  "not_before": null
}
```

- `kind`: `"io"` or `"cpu"`. Worker-side registry wins if it disagrees (registry is authoritative).
- `retries`: attempts completed so far. 0 on first enqueue. The worker increments it when
  scheduling a retry.
- `timeout_ms`: hard timeout. `soft_timeout_ms`: null or int < timeout_ms.
- `idempotency_key`: null or string (see §1 for how the worker keys it).
- `not_before`: null normally. When the client enqueues with `countdown`, the client does NOT
  XADD; it ZADDs the envelope to `cauli:delayed:{queue}` with score = now + countdown*1000 and
  sets `not_before` to that score.

Envelope contents are treated as unvalidated input by the worker (they may be crafted, not just
client-produced). Two worker-side gates apply before an entry is ever executed:

- `id` must match `[a-z0-9]{32}` (32 lowercase hex, matching what the client always produces);
  anything else -> DLQ reason `"malformed"`, no retry. Without this, a crafted id could collide
  with / overwrite another task's `cauli:result:{id}` key.
- The raw `e` field must not exceed `--max-envelope-bytes` (default 1 MiB); oversize -> DLQ
  reason `"malformed"` (a truncated preview is stored, not the full oversize payload). This
  bounds the `serde_json::Value` memory amplification and processing cost of a hostile or
  simply oversized payload. Recommendation: pass references (ids, URLs, keys) in args/kwargs,
  not large blobs.

## 3. Client enqueue rules (Python package)

1. Build envelope. `enqueued_at` = now ms.
2. If countdown given: `ZADD cauli:delayed:{queue} score envelope_json`. Done.
3. Else: `XADD cauli:q:{queue} * e envelope_json`.
4. Return `AsyncResult(id)`.

No client-side idempotency check (the worker enforces it at execution time).

## 4. Worker delivery loop

- One consumer group read loop:
  `XREADGROUP GROUP cauli {consumer} COUNT {batch} BLOCK 1000 STREAMS cauli:q:{q1} cauli:q:{q2} ... > > ...`
  Batch default 16. Only fetch when free execution slots exist (per class admission below;
  a simple global gate on io slots is acceptable but do not let cpu backlog starve io fetch
  indefinitely: bound in-worker cpu backlog to `2 * cpu_workers` pending items).
- Parse envelope from field `e`. Malformed JSON: XACK + XADD to DLQ with reason
  `"malformed"` (best effort raw payload in `e`), continue.
- Unknown task name (not in registry): DLQ with reason `"unregistered"`, XACK. No retry.
- Route by registry kind (fallback envelope kind): io/async, io/sync, cpu.

### 4.1 Completion

- Success: if `store_result`: `SET cauli:result:{id} {result json} EX result_ttl`.
  Then `XACK cauli:q:{queue} cauli {stream_id}` and `XDEL cauli:q:{queue} {stream_id}`.
- Failure (Python exception, timeout, worker-side error): see retry policy.

All completion writes in this section (and §4.2, §4.4, §4.5) are issued as a single redis
`pipe()`, but WITHOUT `MULTI`/`EXEC` — they are pipelined, not atomic. This is deliberate:
wrapping multi-key pipelines (result key + stream + delayed zset + DLQ stream, potentially on
different hash slots) in a real transaction would break Redis Cluster, where all keys in a
`MULTI` must map to the same slot. A connection drop mid-pipeline can therefore apply some
commands and not others (e.g. a retry `ZADD` without the matching `XACK`) — under at-least-once
delivery this just means the original entry is later recovered and re-executed via §4.4, which
is within the documented semantics, not a new failure mode.

### 4.2 Retry policy

On failure with `retries < max_retries`:

1. `retries += 1` in the envelope.
2. attempt = new `retries` value (1-based).
   `d_ms = min(backoff_max_ms, backoff_base_ms * backoff_factor^(attempt-1))`.
   If `jitter`: `d_ms = uniform(0.5 * d_ms, d_ms)`.
3. `ZADD cauli:delayed:{queue} (now + d_ms) new_envelope_json`.
4. XACK + XDEL the delivered entry. Do NOT write a result key (task is still pending).

On failure with `retries >= max_retries` (final):
1. `XADD cauli:dlq:{queue} * e envelope_json reason "max_retries" error {error json}`.
2. If `store_result`: `SET cauli:result:{id} {failure result json} EX result_ttl`.
3. XACK + XDEL.

A task may raise `cauli.Retry(countdown=X)` to force a retry with an explicit delay
(still bounded by max_retries; the forced countdown replaces the computed backoff).
Recognition is duck-typed, identically across both Python execution paths (the embedded io
shim and the cpu child's `cauli._exec`) and the Rust cpu-response mapping: an exception whose
class name is exactly `"Retry"` AND exposes a `.countdown` attribute is treated as a forced
retry, regardless of which module defines that class (this lets an app or test fixture supply
its own duck-typed `Retry` without importing `cauli.Retry` — the embedded io shim in particular
cannot rely on `isinstance` since the worker's interpreter may not have `cauli` installed at
all). `.countdown` is read as a float seconds value (`None` → use the computed backoff).

### 4.3 Delayed mover

Every 250ms per queue, atomically move due members (Lua script, single EVAL):

```lua
-- KEYS[1]=cauli:delayed:{queue}  KEYS[2]=cauli:q:{queue}  ARGV[1]=now_ms  ARGV[2]=limit
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, e in ipairs(due) do
  redis.call('ZREM', KEYS[1], e)
  redis.call('XADD', KEYS[2], '*', 'e', e)
end
return #due
```

limit = 128. Both the worker AND the Python client ship this mover (worker runs it always;
client does not run it — single source: worker only. The client merely ZADDs).

### 4.4 Crash recovery / redelivery

Every `visibility_timeout / 2` (visibility_timeout default 60s, CLI flag), per queue:

1. `XPENDING cauli:q:{queue} cauli IDLE {visibility_timeout_ms} - + {batch}` (extended form,
   returns entry id, consumer, idle ms, delivery_count). `visibility_timeout` is a FLOOR here,
   not itself the reclaim threshold for every task — see the per-envelope check below.
2. For each candidate entry, peek its envelope (`XRANGE` by exact id — read-only, does not
   touch the PEL) and compute `required_idle_ms = max(visibility_timeout_ms, envelope.timeout_ms
   + grace_ms)` (grace_ms = 2000; if the envelope cannot be parsed, `required_idle_ms` falls
   back to `visibility_timeout_ms` alone). If the entry's idle time is below
   `required_idle_ms`, skip it this tick: it is still within its own timeout budget, i.e.
   legitimately still running, not stuck.
   **Invariant this protects:** the default visibility_timeout (60s) is smaller than the
   default task timeout_ms (300s); without the per-envelope check, ANY task running longer
   than 60s — including one still in flight on the SAME worker that delivered it, since
   XPENDING does not exclude the consumer's own entries — would be reclaimed and executed a
   second time, concurrently, with default settings. The worker also warns loudly at startup
   if any registered task's `timeout_ms >= visibility_timeout * 1000`.
3. Otherwise (idle >= required_idle_ms): if `delivery_count > redelivery_limit` (default
   `max(3, max_retries+1)`, computed per envelope after claim; use 3 if envelope unreadable):
   claim it (`XCLAIM ... JUSTID` acceptable), DLQ with reason `"redelivery_limit"`, XACK+XDEL.
4. Else `XCLAIM cauli:q:{queue} cauli {consumer} {visibility_timeout_ms} {id}` and execute it
   normally (same code path as a fresh delivery; do not increment `retries` for a claim).

This makes a SIGKILLed worker's in flight tasks run again elsewhere: at least once semantics.
Operators: size `--visibility-timeout` to exceed your longest task's `timeout_ms`; the
per-envelope check above is a safety net against duplicate concurrent execution, not a reason
to ignore the invariant (a too-low visibility_timeout still means redelivery is slower than it
needs to be for tasks that legitimately crash).

### 4.5 Idempotency guard

At execution start, if `idempotency_key` is not null (`{h}` = the hashed key per §1):

- Atomically (single Lua script): `SET cauli:idemp:{h} {task_id} NX EX idemp_ttl`; if that fails
  because the key already exists, `GET` the existing value and compare it to `task_id`.
- If the SET succeeded (fresh claim), OR the existing value equals THIS task's own `id`
  (**"mine again"** — see below) → execute normally.
- If the existing value is a DIFFERENT task id → do NOT execute. If `store_result`: write
  result JSON with status `"duplicate"` (result null). XACK + XDEL. This is dedup within
  idemp_ttl, best effort by design (at-least-once broker).

**"Mine again"** covers two cases that both reuse the SAME task `id`: a scheduled retry
(§4.2 re-enqueues the same id after incrementing `retries`) and a crash-redelivered claim
(§4.4 re-executes without incrementing `retries`). In both cases the idempotency key is
already held by this exact task id from a previous attempt, and the guard now lets it proceed
— the retry/redelivery genuinely re-executes the task, and a subsequent success or failure is
recorded normally. **This is a correctness fix, not a docs update alone:** the guard used to be
claimed at execution start keyed only on existence (no comparison), so a task's own retry
would find its own claim and resolve as `"duplicate"` every time — retries and
`idempotency_key` were silently incompatible, and the retried failure was never visible to the
caller (no result was written on the failed attempt, and the eventual `"duplicate"` gave
`get() -> None`). Only a task id DIFFERENT from the claimant is a genuine duplicate.
Consequence of the fix: a crash-redelivered claimed task now re-executes (at-least-once
semantics apply to it same as any other task) rather than always resolving as `"duplicate"` —
if the original attempt's side effects had already taken effect before the crash, they can
happen again; this is the same at-least-once trade-off every other task already has.

Fail-open: a redis error during the claim itself (not a "key exists" result — an actual
connection/command error) does NOT block execution. At-least-once semantics already accept
duplicate execution as possible, so refusing to run the task because the dedup check failed
would be strictly worse than running it; the worker logs a warning and proceeds.

### 4.6 Timeouts

- async io task: wrapped in `asyncio.wait_for(coro, effective_s)` where
  `effective_s = min(soft_timeout_ms or timeout_ms, timeout_ms) / 1000`. Timeout → failure
  (retryable) with error type `"TimeoutError"`. The Rust-side backstop around the completion
  channel is `timeout_ms + grace` (grace = 2000ms) using saturating arithmetic, so a
  crafted/huge `timeout_ms` (e.g. `u64::MAX`) cannot wrap into a near-zero spurious timeout.
- sync io task (thread): soft timeout only, via `PyThreadState_SetAsyncExc` injecting
  `cauli.SoftTimeLimitExceeded` after `soft_timeout_ms` (if set). A per-thread generation
  counter fences a timer that fires after the task already finished, so a stale injection
  cannot land inside a LATER task on the same pool thread. One residual, inherent-to-async-exc
  race remains: if the timer fires after the task function returns but before its `finally`
  block runs, the injected exception can surface while building the result and flip a
  successful execution into a `SoftTimeLimitExceeded` failure. Hard timeout for threads cannot
  kill the thread: after `timeout_ms` the worker marks the task failed (retry path) and
  abandons the thread result (logs a warning), and spawns a replacement pool thread so
  sync-io capacity is restored immediately rather than shrinking permanently. If the job was
  still queued (not yet dequeued) when its own hard timeout fired, a worker thread skips
  running it instead of executing it late with no one listening ("zombie execution").
- cpu task: soft timeout enforced inside the child via SIGALRM raising
  `cauli.SoftTimeLimitExceeded`; hard timeout enforced by the worker: SIGKILL the child,
  respawn it, mark failure (retryable) with error type `"TimeoutError"`.

### 4.7 Graceful shutdown

On SIGTERM or SIGINT: stop fetching new work; keep the delayed mover and acks running; wait
up to `--drain-timeout` (default 30s) for in flight tasks; then exit 0. Unfinished tasks stay
pending in the consumer group and are recovered via §4.4 by the next worker. Second signal:
exit immediately (code 130).

## 5. Execution classes

- `--io-loops N` (default 1): threads each running an asyncio event loop for async tasks.
- `--io-threads N` (default 64): Python thread pool for sync io tasks.
- `--io-concurrency N` (default 256): max in flight io tasks total (semaphore, admission gate).
- `--cpu-workers N` (default = cores): child processes for cpu tasks.

### 5.1 cpu child protocol (`python3 -m cauli._exec`)

Spawn: `{python} -m cauli._exec --app {module:attr}`. Child imports the app, prints exactly one
ready line on stdout: `{"ready": true, "pid": 1234}\n`, then reads requests line by line from
stdin and answers one line per request on stdout. stderr is passthrough logging.

Request:  `{"id": "...", "task": "...", "args": [...], "kwargs": {...}, "soft_timeout_ms": null}\n`
Response: `{"id": "...", "ok": true, "result": <json>}\n`
      or: `{"id": "...", "ok": false, "error": {"type": "...", "message": "...", "traceback": "..."}}\n`
      or: `{"id": "...", "ok": false, "retry": true, "countdown": <float|null>, "error": {...}}\n`

The third shape is a forced retry (a task raised `cauli.Retry`, recognized per §4.2's duck-typed
rule): `countdown` DOES cross the pipe (a float seconds value, or `null` to use the computed
backoff) — it is not lost or unavailable here, despite what an earlier draft of this document
and worker/ARCHITECTURE.md once claimed.

One request in flight per child. Non JSON serializable result → treated as error
`{"type": "SerializationError", ...}`. Child must never crash on task exceptions; it reports
them. Worker kills/respawns child on hard timeout or child death (child death = task failure,
retryable, error type `"WorkerLost"`).

## 6. Python public API (`py/`)

Package name `cauli`, pure Python, py>=3.10, only hard dependency: `redis>=5`.

```python
from cauli import Cauli, Retry, SoftTimeLimitExceeded, TaskFailedError

app = Cauli(
    redis_url="redis://localhost:6379/0",   # or env CAULI_REDIS_URL; default shown
    default_queue="default",
    result_ttl=3600,      # seconds
    idemp_ttl=86400,      # seconds
)

@app.task(
    name=None,            # default: f"{fn.__module__}.{fn.__qualname__}"
    kind=None,            # None → "io"; "cpu" must be explicit
    queue=None,           # default: app.default_queue
    max_retries=3,
    timeout=300.0,        # seconds → timeout_ms
    soft_timeout=None,    # seconds or None
    backoff_base=0.5,     # seconds
    backoff_factor=2.0,
    backoff_max=60.0,     # seconds
    jitter=True,
    store_result=True,
)
def send_email(to: str): ...

r = send_email.delay("a@b.com")                      # AsyncResult
r = send_email.apply_async(args=(), kwargs={}, countdown=None, queue=None,
                           idempotency_key=None)     # AsyncResult
r.id                    # task id (hex str)
r.status()              # "pending" | "success" | "failure" | "duplicate"
r.get(timeout=None, poll_interval=0.05)
    # blocks until result key exists; returns result value on success;
    # raises TaskFailedError(type, message, traceback) on failure;
    # returns None with .duplicate == True semantics for duplicate (get returns None);
    # raises TimeoutError if timeout expires while still pending.
```

Worker introspection contract (Rust reads these exact attributes via the embedded interpreter):

- `app._tasks` → `dict[str, TaskDef]`
- `TaskDef` attributes: `name: str`, `fn: callable`, `is_async: bool`,
  `kind: str` ("io"|"cpu"), `queue: str|None`, `max_retries: int`,
  `timeout_ms: int`, `soft_timeout_ms: int|None`, `backoff_base_ms: int`,
  `backoff_factor: float`, `backoff_max_ms: int`, `jitter: bool`, `store_result: bool`
- `app.redis_url: str`, `app.default_queue: str`, `app.result_ttl: int`, `app.idemp_ttl: int`
- Exceptions: `cauli.Retry(countdown: float|None)` (attribute `.countdown`),
  `cauli.SoftTimeLimitExceeded`.

Decorated task objects also stay directly callable (`send_email("a@b.com")` runs the function
inline, no queue) so the same code is testable without a broker.

## 7. Worker CLI (`cauli-worker` binary)

```
cauli-worker --app myproj.tasks:app [--queues default,emails] [--redis-url URL]
            [--io-loops 1] [--io-threads 64] [--io-concurrency 256] [--cpu-workers N]
            [--batch 16] [--visibility-timeout 60] [--max-envelope-bytes 1048576]
            [--drain-timeout 30] [--python python3] [--stats-interval 10] [--log-level info]
```

- `--app`: `module:attr`. The worker adds CWD to `sys.path`, imports module, reads attr. Note:
  this means running `cauli-worker` in an untrusted working directory can import
  attacker-controlled modules — a standard Python-tooling caveat, not specific to cauli.
- `--redis-url` precedence: CLI > env `CAULI_REDIS_URL` > `app.redis_url`.
- `--queues` default: `app.default_queue`.
- `--batch` and `--visibility-timeout` must be >= 1 (exit 1 at startup otherwise): `--batch 0`
  would mean "unlimited" to Redis's `XREADGROUP COUNT`, and `--visibility-timeout 0` would make
  the recovery loop (§4.4) reclaim every currently-executing task on nearly every tick.
- `--max-envelope-bytes` (default 1 MiB): see §2.
- `--stats-interval`: seconds between one line stats logs:
  `stats: fetched=N ok=N failed=N retried=N dlq=N inflight_io=N inflight_cpu=N rss_mb=N
  sync_live=N sync_abandoned=N pending_async=N`. `sync_live` is the sync-io thread pool's
  current thread count (initial + any replacements spawned per §4.6); `sync_abandoned` is the
  cumulative count of hard-timeout abandonments that triggered a replacement. `pending_async`
  is the number of async tasks currently awaiting a completion callback from the embedded event
  loop(s); a value that only grows over time signals a wedged event-loop thread (one that never
  yields back to asyncio, so its `asyncio.wait_for` timeout can never even fire).
- Exit codes: 0 graceful, 1 fatal config/startup error, 130 forced.

## 8. Result / error JSON

Result key value (`cauli:result:{id}`):

```json
{"status": "success", "result": <json>, "error": null, "finished_at": 123}
{"status": "failure", "result": null, "error": {"type": "ValueError", "message": "...", "traceback": "..."}, "finished_at": 123}
{"status": "duplicate", "result": null, "error": null, "finished_at": 123}
```

`traceback` may be truncated to 8KB. Task return values must be JSON serializable; a non
serializable success value is a failure with type `"SerializationError"` (no retry — treat as
final failure regardless of retries left).

Full tracebacks (and results) are stored in plaintext in `cauli:result:*` and DLQ stream
entries. If a task's exception message or arguments embed secrets or PII, anyone with read
access to the Redis instance can see them — this is a property of the trust model (Redis is
trusted infra; task payloads/results are not automatically scrubbed), not a bug.

## 9. Non-goals for v1

Chains/chords/canvas, cron/beat scheduling, priorities, rate limits, task events bus,
multi broker (Redis only), result backends other than Redis, Windows worker support
(worker targets Linux; client is cross platform).

## 10. Building and testing

The worker (`worker/`) targets Linux; build and test it on Linux or WSL. The Python client
(`py/`) is cross-platform.

Requirements: a current stable Rust toolchain, Python >= 3.10 with development headers
(`python3-dev` / `python3-devel`) since the worker embeds CPython via pyo3, and a local Redis
server (>= 7.0) for tests — never point tests at a shared/production Redis; use a throwaway
instance on its own port (e.g. `redis-server --port <port> --save '' --appendonly no
--daemonize yes`) and shut it down afterward.

```bash
# worker (Rust)
cd worker
cargo build --release
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo test --release --bin cauli-worker       # unit tests only
cargo test --release --features test-hooks   # unit tests + both e2e suites
                                              # (e2e needs the CAULI_EXEC_CMD test hook; see cpu.rs)

# python client
cd py
pip install -e '.[dev]'
ruff format --check .
ruff check .
pytest -q

# cross-component integration (real client + real worker binary + real cpu children)
cd itest
pip install -e ../py
pytest -q test_integration.py   # needs the release worker binary built above on PATH
                                 # or set CAULI_WORKER_BIN=/path/to/cauli-worker
```

Building on a slow or network filesystem: set `CARGO_TARGET_DIR` to a local path before running
cargo commands to avoid compiling through a slow mount.
