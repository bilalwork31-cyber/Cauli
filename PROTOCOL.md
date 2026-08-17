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
  sized to CPU cores, fed over a line delimited JSON protocol. By default the children are
  forked from ONE preloaded, `gc.freeze()`-d fork-server parent (§5.1) so respawns are cheap
  and the warmed import image is shared copy-on-write.

Broker and result backend: Redis >= 7.0. At least once delivery via Redis Streams consumer
groups. All timestamps are integer unix epoch **milliseconds** unless stated otherwise.

---

## 1. Redis key layout

All keys use prefix `cauli:`. `{queue}` is a queue name matching `[a-zA-Z0-9_.-]+`.

| Key | Type | Purpose |
|---|---|---|
| `cauli:q:{queue}` | Stream | Ready tasks. Each entry has exactly one field `e` whose value is the envelope JSON (UTF-8). |
| `cauli:delayed:{queue}` | ZSET | Delayed/retrying tasks. member = envelope JSON string, score = fire_at epoch ms. |
| `cauli:dlq:{queue}` | Stream | Dead letters. Fields: `e` = envelope JSON, `reason` = string, `error` = error JSON (see §8) or empty string. Capped at 1000 entries (approximate XADD MAXLEN); see below. |
| `cauli:result:{task_id}` | String | Result JSON (see §8), `SET ... EX result_ttl`. |
| `cauli:idemp:{h}` | String | Idempotency guard. Value = task id that claimed it. `SET NX EX idemp_ttl`. |
| `cauli:beat:*` | (several) | Periodic scheduler state. Owned by `cauli-beat`, not by the worker; full layout in §10.1. |

`{h}` in the idempotency key is a deterministic hash of the app-supplied `idempotency_key`
(the worker uses FNV-1a 64-bit, hex-encoded), not the raw string. This bounds the key to a
fixed size and neutralizes cluster hash-tag injection (`{...}`) or other charset abuse from an
app-controlled string; it does not need to be cryptographic since idempotency keys are chosen
by the app author, not an adversary distinct from the app.

Every `cauli:dlq:{queue}` XADD (§4, §4.2, §4.4, §9.1) carries `MAXLEN ~ 1000`, so the stream
never grows past roughly that size regardless of how long the worker has been running. Without
a bound, a long lived worker under a sustained trickle of failures would grow the stream forever
until Redis runs out of memory, taking down every queue in the deployment, not just the failing
one. The tradeoff is deliberate: past the cap, the OLDEST dead letters are dropped to make room
for new ones, so a worker that has been failing for a long time keeps only its most recent 1000
(approximately, `~` trims to whole internal stream nodes so it can overshoot slightly) dead
letters per queue. Losing old dead letters beats an out of memory event that stops every queue.

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
  "not_before": null,
  "expires_at": null
}
```

- `v`: protocol version, currently always `1`. The worker accepts a `v` at or below its own
  supported version and rejects anything higher, since no forward compatibility is defined for
  a version this build predates -> DLQ reason `"malformed"` (§8) rather than guessing at an
  unknown shape.
- `kind`: `"io"` or `"cpu"`. Worker-side registry wins if it disagrees (registry is authoritative).
- `retries`: attempts completed so far. 0 on first enqueue. The worker increments it when
  scheduling a retry.
- `timeout_ms`: hard timeout. `soft_timeout_ms`: null or int < timeout_ms.
- `idempotency_key`: null or string (see §1 for how the worker keys it).
- `not_before`: null normally; otherwise the absolute epoch-ms instant before which the task
  must not run. Set from either `countdown` (relative seconds → `now + countdown*1000`) or
  `eta` (an absolute, timezone-AWARE datetime → its epoch ms); the two are mutually exclusive.
  When `not_before` is in the future the client does NOT XADD: it ZADDs the envelope to
  `cauli:delayed:{queue}` with score = `not_before`. When it is already in the past the client
  XADDs normally (a past eta means "due now") but still records the requested instant here, so
  the field stays a faithful audit trail of what was asked for.
- `expires_at`: null or the absolute epoch-ms instant past which the task is no longer worth
  running. Enforced by the worker at dispatch — see §9.1. Deliberately absolute rather than a
  duration: it survives retries, the delayed-zset hop and crash redelivery unchanged, and it
  means the same thing on a broker with no delayed-delivery primitive of its own.

Two further OPTIONAL fields appear on envelopes published by the periodic scheduler (§10).
They are informational provenance; the worker preserves them (unknown fields round-trip) but
attaches no behavior to them:

- `beat_name`: the schedule entry that produced this task.
- `beat_slot`: the schedule slot (epoch ms) this firing represents. Distinct per firing, so it
  is the field to group by when auditing "did every slot fire exactly once".

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

1. Resolve the queue (§9.3 routing precedence).
2. Build envelope. `enqueued_at` = now ms; `not_before` from `countdown`/`eta`; `expires_at`
   from `expires`, or from the queue's TTL when no explicit `expires` was given (§9.2).
3. If `not_before` is in the future: `ZADD cauli:delayed:{queue} not_before envelope_json`. Done.
4. Else: `XADD cauli:q:{queue} * e envelope_json`.
5. Return `AsyncResult(id)`.

No client-side idempotency check (the worker enforces it at execution time), and no client-side
expiry check either: a task whose `expires_at` has already passed at enqueue time is still
enqueued and is dropped by the worker at dispatch, so expiry has exactly ONE enforcement point
and one set of semantics (§9.1).

The wire format is plain JSON; clients MAY produce/parse it with any compliant JSON codec.
The bundled client uses msgspec (a required dependency, not an optional accelerator) and
additionally validates the object tree against the JSON type set before encoding, because
msgspec on its own would encode `NaN`/`Infinity` as `null`, accept `set` and `bytes`, and
coerce non-`str` dict keys — none of which this protocol defines.

## 4. Worker delivery loop

- One consumer group read loop:
  `XREADGROUP GROUP cauli {consumer} COUNT {batch} BLOCK 1000 STREAMS cauli:q:{q1} cauli:q:{q2} ... > > ...`
  Batch default 16. Only fetch when free execution slots exist (per class admission below;
  a simple global gate on io slots is acceptable but do not let cpu backlog starve io fetch
  indefinitely: bound in-worker cpu backlog to twice the cpu pool's in-flight capacity,
  i.e. `2 * cpu_workers * cpu_child_threads` pending items).
- Parse envelope from field `e`. Malformed JSON: XACK + XADD to DLQ with reason
  `"malformed"` (best effort raw payload in `e`), continue. A result key is written too when the
  id can be recovered from the entry (§8).
- Unknown task name (not in registry): DLQ with reason `"unregistered"`, XACK. No retry. A
  result key is written too (§8): the id is always recoverable here, since the envelope parsed.
- Expired (past `expires_at` or the queue's TTL): DLQ with reason `"expired"`, XACK. No retry,
  no execution. Checked BEFORE the §4.5 idempotency claim — see §9.1.
- Route by registry kind (fallback envelope kind): io/async, io/sync, cpu.

**Queue order is dispatch order within a batch.** `XREADGROUP` returns one entries array per
stream, in the order the keys were given, and the worker iterates them in that order. So with
`--queues high,default,bulk`, everything read from `high` in a given batch is dispatched before
anything from `default`. This is the only prioritization cauli offers and it cannot starve
anything: `COUNT` applies per stream, so every listed queue contributes to every batch. See
§9.4 for why there are no priority LEVELS.

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

1. `XPENDING cauli:q:{queue} cauli IDLE {visibility_timeout_ms} {start} + {count}` (extended
   form, returns entry id, consumer, idle ms, delivery_count). `visibility_timeout` is a FLOOR
   here, not itself the reclaim threshold for every task — see the per-envelope check below.
   Each tick must drain the ENTIRE eligible backlog, not one fixed-size page: page through the
   PEL with `start` = `-` for the first page and the exclusive form `({last_id}` afterwards
   (the worker uses pages of 128; reusing the fetch `--batch` here was a bug — after a
   `kill -9` with a few hundred tasks in flight, reclaim trickled back at `batch` entries per
   half-visibility-period while the worker sat otherwise idle). The exclusive cursor also
   guarantees termination: entries skipped by the per-envelope check below are not re-read
   within the same tick. Implementations should pipeline the per-page peeks and claims (steps
   2-4) rather than issuing one round trip per entry, and should gate page fetching on the
   same admission criteria as §4 fetch (free io capacity, cpu backlog not overflowing) so a
   huge reclaimed backlog queues in Redis rather than in worker memory.
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
   claim it (`XCLAIM ... JUSTID` acceptable), DLQ with reason `"redelivery_limit"`, XACK+XDEL. A
   result key is written too when the id is recoverable (§8).
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
- cpu task: soft timeout enforced inside the child: SIGALRM raising
  `cauli.SoftTimeLimitExceeded` when the child executes single threaded
  (`--cpu-child-threads 1`, and always in stdio fallback mode); with M > 1 worker threads a
  shared watchdog thread injects it per request via `PyThreadState_SetAsyncExc` (SIGALRM only
  ever fires in a process's main thread), with the same generation fencing and the same
  residual injection race as the sync-io path above. Hard timeout enforced by the worker:
  SIGKILL the child, replace it (a replacement fork in fork-server mode, a fresh spawn in
  stdio mode), mark failure (retryable) with error type `"TimeoutError"`.

### 4.7 Graceful shutdown

On SIGTERM or SIGINT: stop fetching new work; keep the delayed mover and acks running; wait
up to `--drain-timeout` (default 30s) for in flight tasks; then exit 0. Unfinished tasks stay
pending in the consumer group and are recovered via §4.4 by the next worker. Second signal:
exit immediately (code 130).

### 4.8 Task lifecycle hooks

The app object may expose three optional attributes, each a list of zero-argument callables
(the worker duck-reads them with `getattr(..., default empty)` like every other §6 attribute;
the Python client registers them via `app.before_task(fn)`, `app.after_task(fn)`,
`app.process_init(fn)`, all usable as decorators):

- `_before_task_hooks` — run immediately before EVERY task executes, in the same
  thread/process that is about to run it, on every execution path: the sync io thread pool,
  the embedded asyncio loop threads, and the cpu children (both fork-server and stdio modes).
  They run BEFORE the soft timeout is armed, so hook time is not charged against the task's
  soft budget and a soft-timeout injection can never land inside a hook.
- `_after_task_hooks` — run after EVERY task finishes, on every outcome path (success, task
  exception, forced retry, soft timeout), after the soft-timeout disarm. A sync task's result
  is reported only after its after-hooks return.
- `_process_init_hooks` — run once per cauli-managed Python process, after the app import and
  before any task can execute: in the worker's embedded interpreter (at `load_app`), in the
  fork-server parent (BEFORE the first fork, so a resource opened as an import side effect —
  e.g. a DB connection from a module-level query — is closed before any child can inherit its
  fd), in each forked child (right after fork), and in each stdio child (after import).

Hooks do NOT run for entries that never execute: malformed, unregistered, duplicate-resolved
(§4.5), or redelivery-limit DLQ entries.

Error handling: a hook that raises is reported on stderr and skipped — it never fails the
task and never prevents the remaining hooks from running (`Exception` is caught;
`SystemExit`/`KeyboardInterrupt` propagate). Registration order is call order. The worker
holds references to the app's live hook LISTS, so hooks appended after startup are honored.

On the asyncio path hooks run on the event loop thread; a hook may return an awaitable and
the worker awaits it there (the sync-pool and cpu paths ignore a returned awaitable). Keep
loop-thread hooks fast — they run between tasks on a shared event loop.

Purpose: per-task resource lifecycle for frameworks WITHOUT cauli core depending on any
framework. The bundled `cauli.contrib.django` registers Django's `close_old_connections`
before/after every task (Celery-fixup parity: `CONN_MAX_AGE` honored, stale connections
replaced after a DB restart when `CONN_HEALTH_CHECKS` is on) and `connections.close_all` as a
process-init hook.

Related sync-pool invariant: each sync io pool thread keeps ONE persistent CPython thread
state for its whole lifetime. `threading.local` storage therefore survives across tasks on
that thread — Django's per-thread connection cache (and anything else built on thread-locals)
behaves exactly as it does under any long-lived thread. (Prior to this being pinned down, the
embedded runtime created and destroyed a thread state per task, silently wiping thread-local
state between tasks; treat that as a bug class, not a knob.)

## 5. Execution classes

- `--io-loops N` (default 1): threads each running an asyncio event loop for async tasks.
- `--io-threads N` (default 64): Python thread pool for sync io tasks.
- `--io-concurrency N` (default 256): max in flight io tasks total (semaphore, admission gate).
- `--cpu-workers N` (default = cores): child processes for cpu tasks.
- `--cpu-child-threads M` (default 1): worker threads per cpu child (fork-server mode); each
  child accepts up to M requests in flight.

### 5.1 cpu child protocol (`python3 -m cauli._exec`)

Both modes below speak the same line delimited JSON request/response shapes; only the
transport and the number of requests in flight differ.

Request:  `{"id": "...", "task": "...", "args": [...], "kwargs": {...}, "soft_timeout_ms": null}\n`
Response: `{"id": "...", "ok": true, "result": <json>}\n`
      or: `{"id": "...", "ok": false, "error": {"type": "...", "message": "...", "traceback": "..."}}\n`
      or: `{"id": "...", "ok": false, "retry": true, "countdown": <float|null>, "error": {...}}\n`

`id` is a worker chosen correlation string, echoed verbatim in the response (the worker uses
`{envelope id}.{sequence}`; the child attaches no meaning to it). The third response shape is
a forced retry (a task raised `cauli.Retry`, recognized per §4.2's duck-typed rule):
`countdown` DOES cross the pipe (a float seconds value, or `null` to use the computed
backoff) — it is not lost or unavailable here, despite what an earlier draft of this document
and worker/ARCHITECTURE.md once claimed.

A non JSON serializable result → error `{"type": "SerializationError", ...}`. The child must
never crash on task exceptions; it reports them. Child death mid-request = task failure,
retryable, error type `"WorkerLost"`. Children run the §4.8 before/after task hooks around
every request they execute, and the §4.8 process-init hooks per that section's timing rules.

#### Fork-server mode (default)

The worker binds a unix stream socket listener inside a private, `0700`-permission directory
under the system temp dir (so no other local uid can even reach the socket) and spawns ONE
parent process per pool. Every accepted connection is additionally checked against the
worker's own uid via `SO_PEERCRED` before it is trusted as a real forked child — the kernel-
reported peer pid is used for tracking/kill decisions in preference to the client-claimed one.
The socket file and its containing directory are removed on every worker exit path (clean
drain, forced double-signal exit, and fork-server startup failure after a successful bind).

`{python} -m cauli._exec --app {module:attr} --fork-server --connect {socket_path}
--child-threads {M}`

- The PARENT imports the app once, runs `gc.collect()` then `gc.freeze()` (the warmed import
  image moves to the permanent GC generation, so children fork copy-on-write and their —
  default, enabled — GC never scans or dirties frozen objects), prints exactly one line
  `{"server": true, "pid": P}\n` on stdout, then serves its stdin/stdout control channel:
  request `{"cmd": "fork"}\n` → `fork()` → reply `{"forked": <child_pid>}\n` (or
  `{"error": "..."}\n` for a failed fork / unrecognized command). The parent reaps children
  (SIGCHLD), exits 0 on stdin EOF, and sets `PR_SET_PDEATHSIG` so it dies with the worker.
  App state created at import time is shared by ALL children of a parent; import-time side
  effects that must not survive a fork (open connections, background threads) are the app
  author's responsibility, the standard preload/fork caveat.
- Each forked CHILD re-arms `PR_SET_PDEATHSIG` (fork clears it), connects to
  `{socket_path}`, sends the ready line `{"ready": true, "pid": N, "concurrency": M}\n` over
  that connection, then speaks the request/response protocol on it. Up to the advertised
  concurrency requests may be in flight per child; responses may arrive out of order and are
  matched by `id`. Soft timeout inside the child: SIGALRM when M = 1; a watchdog thread +
  `PyThreadState_SetAsyncExc` per request when M > 1 (§4.6). Socket EOF → child exits 0.
- The worker maintains `--cpu-workers` serving children by requesting forks (a replacement
  is requested whenever a child dies or is killed — respawns are cheap: no re-import). Hard
  timeout of any in flight request: SIGKILL the child by pid, fail that request as
  `"TimeoutError"`, fail the child's other in flight requests as `"WorkerLost"` (retryable),
  request a replacement fork. A dead child's pid is never SIGKILLed a second time (it may
  already be reused by an unrelated process once the fork-server parent reaps it via
  SIGCHLD); a child that stops draining its socket without dying (write stalls past a bounded
  budget) is treated as gone and killed the same as a hard timeout. A fork request is retried
  until it succeeds instead of being dropped: a parseable fork refusal from a HEALTHY parent
  (e.g. transient EAGAIN/ENOMEM) retries with backoff and never touches the parent process; a
  genuine control-channel failure respawns the parent (its children died with it via
  PDEATHSIG) and then retries the SAME fork request against the fresh parent.
- stderr of the parent and of every child is passthrough logging.

#### Stdio mode (fallback)

Entered with `--no-fork-server`, or automatically when fork-server startup fails (listener
bind failure, parent spawn failure, or no `{"server": ...}` line within the handshake
timeout).

Spawn per child: `{python} -m cauli._exec --app {module:attr}`. Each child imports the app
itself, prints exactly one ready line on stdout: `{"ready": true, "pid": 1234}\n`, then reads
requests line by line from stdin and answers one line per request on stdout, ONE request in
flight per child. stderr is passthrough logging. Soft timeout via SIGALRM. Worker
kills/respawns the child on hard timeout or child death (each respawn re-imports the app).

## 6. Python public API (`py/`)

Package name `cauli`, pure Python, py>=3.10, only hard dependency: `redis>=5`.

```python
from cauli import Cauli, Retry, SoftTimeLimitExceeded, TaskFailedError, crontab, interval

app = Cauli(
    redis_url="redis://localhost:6379/0",   # or env CAULI_REDIS_URL; default shown
    default_queue="default",
    result_ttl=3600,      # seconds
    idemp_ttl=86400,      # seconds
    task_routes=None,     # §9.3: {glob: queue | {"queue": ...} | callable}, ordered
    queue_ttl=None,       # §9.2: seconds (all queues) or {queue: seconds}, "*" = fallback
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
                           idempotency_key=None,
                           eta=None,        # absolute, timezone-AWARE datetime
                           expires=None)    # seconds, or an aware datetime
r.id                    # task id (hex str)
r.status()              # "pending" | "success" | "failure" | "duplicate" | "expired"
r.get(timeout=None, poll_interval=0.05)
    # blocks until result key exists; returns result value on success;
    # raises TaskFailedError(type, message, traceback) on failure;
    # returns None with .duplicate == True semantics for duplicate (get returns None);
    # sets .expired = True and raises TaskFailedError(type="Expired") for expired;
    # raises TimeoutError if timeout expires while still pending.
```

`eta` and `countdown` are mutually exclusive (ValueError if both). **A naive datetime is
rejected** for `eta` and for a datetime `expires`: there is no correct default. Celery's
`enable_utc` reinterprets naive datetimes as UTC, which silently shifts every eta by the local
offset for anyone passing `datetime.now()`; assuming local time instead is just as wrong on a
server whose TZ differs from the developer's laptop. Attach a timezone explicitly. An `eta` in
the past is not an error — it means "due now" — and is published straight to the stream.

Periodic schedules (§10). Declaring one does not talk to Redis; `cauli-beat` syncs it:

```python
from cauli import crontab, interval

app.add_periodic_task(
    "nightly-report",                  # stable entry name (the Redis field key)
    report_task,                       # a TaskDef, or a task name string
    crontab(minute=0, hour=3, timezone="Europe/Berlin"),
    args=(), kwargs=None,
    queue=None,          # §9.3 precedence rule 1
    expires=None,        # seconds -> the published envelope's expires_at (§9.1)
    idempotent=False,    # adds idempotency_key = "beat:{name}:{slot}"
    enabled=True,
    on_missed="fire_once",   # or "skip", with max_lateness (§10.4)
    max_lateness=None,       # seconds
)                                       # -> ScheduleEntry

interval(30.0)                                  # every 30 seconds
crontab(minute="*/5", hour="9-17", day_of_week="mon-fri", timezone="UTC")
```

Lifecycle hook registration (§4.8), each usable as a decorator and returning `fn` unchanged:

```python
app.before_task(fn)     # fn() before every task, in the task's thread/process
app.after_task(fn)      # fn() after every task, all outcome paths
app.process_init(fn)    # fn() once per cauli-managed process, before any task
```

Django-only enqueue helpers on every task object (lazy Django import; RuntimeError without
Django installed — cauli core takes no Django dependency):

```python
send_email.delay_on_commit(*args, **kwargs)          # -> None
send_email.apply_async_on_commit(args=(), kwargs=None, countdown=None,
                                 queue=None, idempotency_key=None,
                                 using=None)          # -> None
```

Both defer the enqueue to `django.db.transaction.on_commit`: the task is published only if
the surrounding transaction commits, never on rollback (the footgun prevented: `delay()`
inside `atomic()` publishes immediately, so the worker can execute against a row that is not
committed yet — or that never will be). Outside an atomic block the enqueue happens
immediately. Return `None`, not an AsyncResult: no task id exists until commit time. The
opt-in `cauli.contrib.django` module additionally provides `django_app()` (settings-driven
config: `CAULI_REDIS_URL`, `CAULI_DEFAULT_QUEUE`, `CAULI_RESULT_TTL`, `CAULI_IDEMP_TTL`;
registers the §4.8 DB connection hooks), `install_db_hooks(app)` and
`autodiscover_tasks(app)`.

Worker introspection contract (Rust reads these exact attributes via the embedded interpreter):

- `app._tasks` → `dict[str, TaskDef]`
- `TaskDef` attributes: `name: str`, `fn: callable`, `is_async: bool`,
  `kind: str` ("io"|"cpu"), `queue: str|None`, `max_retries: int`,
  `timeout_ms: int`, `soft_timeout_ms: int|None`, `backoff_base_ms: int`,
  `backoff_factor: float`, `backoff_max_ms: int`, `jitter: bool`, `store_result: bool`
- `app.redis_url: str`, `app.default_queue: str`, `app.result_ttl: int`, `app.idemp_ttl: int`
- `app.queue_ttl: dict[str, float]` (§9.2, seconds; key `"*"` = fallback; absent == no TTLs)
- Optional lifecycle hook lists (§4.8): `app._before_task_hooks`,
  `app._after_task_hooks`, `app._process_init_hooks` → `list[callable]` (absent == empty)
- Exceptions: `cauli.Retry(countdown: float|None)` (attribute `.countdown`),
  `cauli.SoftTimeLimitExceeded`.

Decorated task objects also stay directly callable (`send_email("a@b.com")` runs the function
inline, no queue) so the same code is testable without a broker.

## 7. Worker CLI (`cauli-worker` binary)

```
cauli-worker --app myproj.tasks:app [--queues default,emails] [--redis-url URL]
             # queue ORDER is dispatch order within a fetch batch (§4, §9.4)
            [--io-loops 1] [--io-threads 64] [--io-concurrency 256] [--cpu-workers N]
            [--cpu-child-threads 1] [--no-fork-server]
            [--batch 16] [--visibility-timeout 60] [--max-envelope-bytes 1048576]
            [--drain-timeout 30] [--python python3] [--stats-interval 10] [--log-level info]
```

- `--app`: `module:attr`. The worker adds CWD to `sys.path`, imports module, reads attr. Note:
  this means running `cauli-worker` in an untrusted working directory can import
  attacker-controlled modules — a standard Python-tooling caveat, not specific to cauli.
- `--redis-url` precedence: CLI > env `CAULI_REDIS_URL` > `app.redis_url`.
- `--queues` default: `app.default_queue`.
- `--batch`, `--visibility-timeout` and `--max-envelope-bytes` must be >= 1 (exit 1 at startup
  otherwise): `--batch 0` would mean "unlimited" to Redis's `XREADGROUP COUNT`,
  `--visibility-timeout 0` would make the recovery loop (§4.4) reclaim every currently-executing
  task on nearly every tick, and `--max-envelope-bytes 0` would dead letter every single message
  as oversize.
- `--max-envelope-bytes` (default 1 MiB): see §2.
- `--cpu-child-threads` (default 1): per-child request concurrency, `--no-fork-server`:
  force the stdio child mode — both per §5.1.
- `--stats-interval`: seconds between one line stats logs:
  `stats: fetched=N ok=N failed=N retried=N dlq=N expired=N inflight_io=N inflight_cpu=N
  rss_mb=N sync_live=N sync_abandoned=N pending_async=N async_rejected=N cpu_backlog=N`.
  `expired` (§9.1) counts entries
  discarded unrun past their deadline; they are counted in `dlq` too, but broken out because a
  rising `expired` means the queue cannot keep up, which is a different alert from a rising
  `failed`. `sync_live` is the sync-io thread pool's
  current thread count (initial + any replacements spawned per §4.6); `sync_abandoned` is the
  cumulative count of hard-timeout abandonments that triggered a replacement. `pending_async`
  is the number of async tasks currently awaiting a completion callback from the embedded event
  loop(s); a value that only grows over time signals a wedged event-loop thread (one that never
  yields back to asyncio, so its `asyncio.wait_for` timeout can never even fire). `async_rejected`
  is the cumulative count of submissions the shim's own per loop queue has rejected past its cap,
  the field that actually moves during that same wedge once `pending_async` alone would not show
  it, since a wedged loop never runs the completion callback that clears that bookkeeping.
  `cpu_backlog` is the live depth of dispatch tasks parked on a full cpu backlog channel (§4,
  §5.1: bound to twice `cpu_workers` times `cpu_child_threads`); a nonzero reading means the
  fetch loop has paused fetching for every lane, not just cpu, because it cannot know an entry's
  lane before parsing it. The transition is logged too: a warning the moment `cpu_backlog` first
  goes above zero, and a matching one with the total paused duration when it returns to zero.
- Exit codes: 0 graceful, 1 fatal config/startup error, 130 forced.

### 7.1 Scheduler CLI (`cauli-beat`, Python entry point)

```
cauli-beat --app myproj.tasks:app [--redis-url URL] [--lock-ttl 30] [--max-interval 5]
           [--instance-id ID] [--no-lock] [--once] [--log-level info]
```

Equivalent to `python -m cauli.beat`. Rationale for it being a separate entry point rather
than a `cauli-worker` flag: §10.7.

- `--app`, `--redis-url`: same meaning and precedence as the worker's.
- `--lock-ttl` (default 30s): the scheduler lease. Directly bounds worst-case scheduling
  lateness after a leader crash (failover happens between `2/3 * lock_ttl` and `lock_ttl`, per
  §10.5). Lower it for tighter schedules, raise it if a busy box makes refreshes unreliable.
- `--max-interval` (default 5s): longest sleep between ticks; also how quickly a schedule
  edited in Redis is picked up. Beat otherwise sleeps exactly until the next slot.
- `--instance-id`: lease holder id (default `host:pid:rand`). Useful in tests and in logs.
- `--no-lock`: tick without taking the lease. Still exactly-once — that is the CAS's job, not
  the lease's (§10.5) — but every replica does the polling work. Debugging aid.
- `--once`: run a single tick and exit, for driving beat from system cron instead of running a
  daemon. Still takes the lease (and releases it), so `--once` on several hosts is safe.
- SIGTERM/SIGINT: release the lease and exit 0. Exit 1 on a fatal config/import error.

## 8. Result / error JSON

Result key value (`cauli:result:{id}`):

```json
{"status": "success", "result": <json>, "error": null, "finished_at": 123}
{"status": "failure", "result": null, "error": {"type": "ValueError", "message": "...", "traceback": "..."}, "finished_at": 123}
{"status": "duplicate", "result": null, "error": null, "finished_at": 123}
{"status": "expired", "result": null, "error": {"type": "Expired", "message": "...", "traceback": ""}, "finished_at": 123}
```

`"expired"` (§9.1) is its own status rather than a `"failure"`: the task never ran, so there is
no exception, no traceback and nothing to retry — reporting it as a failure would put a
fabricated error in front of the caller. The error object is populated anyway (`type` =
`"Expired"`, message naming the deadline and how late the pickup was) so a client that only
knows success/failure still gets something usable, and `AsyncResult.get()` raises
`TaskFailedError(type="Expired")` rather than returning a silent `None`.

A terminal dead letter that never executed at all (malformed envelope, unregistered task, or
redelivery limit exceeded; §4/§4.4) also writes a `"failure"` result reusing this same shape,
whenever the task id could be recovered from the entry (present and matching the §2 charset
gate). `error.type` names why: `"Malformed"`, `"UnregisteredTask"` or
`"RedeliveryLimitExceeded"`. Without this, `AsyncResult.get()` called with no timeout would
block forever on a result key that would never be written. Where the id cannot be recovered
(the envelope is not valid JSON, or its `id` fails the charset gate) there is nothing to key a
result on, so none is written and the caller's own timeout, or lack of one, governs as before.

`traceback` may be truncated to 8KB. Task return values must be JSON serializable; a non
serializable success value is a failure with type `"SerializationError"` (no retry — treat as
final failure regardless of retries left).

Full tracebacks (and results) are stored in plaintext in `cauli:result:*` and DLQ stream
entries. If a task's exception message or arguments embed secrets or PII, anyone with read
access to the Redis instance can see them — this is a property of the trust model (Redis is
trusted infra; task payloads/results are not automatically scrubbed), not a bug.

## 9. Scheduling controls: expiry, queue TTL, routing, priorities

### 9.1 Task expiration

`expires_at` (§2) is an absolute epoch-ms deadline. A task whose deadline has passed when a
worker picks it up is **discarded instead of executed**.

**Where it is enforced: at dispatch, and only at dispatch.** Concretely, in `process()` after
the envelope parses and the task name resolves, and BEFORE the §4.5 idempotency claim.

The alternatives were considered and rejected:

- *At enqueue.* The client cannot know how long the entry will actually wait, so this would
  only catch the degenerate already-expired case and would need a second check anyway.
  Two checks means two sets of semantics to keep in agreement.
- *In the delayed mover (§4.3).* It only sees delayed entries, not the backlogged ready ones
  that are the actual motivation for queue TTLs, and it would have to `cjson.decode` every
  moved envelope inside the Lua script to find the field.
- *In the fetch loop.* It has not parsed the envelope yet.

Dispatch is the one point every path converges on — fresh delivery, mover hand-off, scheduled
retry, §4.4 crash reclaim — so one check covers all of them. It also needs nothing from the
broker, which is what lets a future SQS or RabbitMQ backend inherit expiry unchanged.

Before the idempotency claim, because an expired task must not burn the idempotency key and
lock out a later, still-valid task carrying the same one.

**Observable outcome**, both of these:

1. `XADD cauli:dlq:{queue} * e {envelope} reason "expired" error ""` — so expired work is
   auditable and replayable, not silently vanished.
2. If `store_result`: a result key with status `"expired"` (§8), so a caller blocked in
   `get()` is told what happened instead of waiting out its timeout.

Then XACK + XDEL. No retry (a retry would expire identically), and no lifecycle hooks run
(§4.8: hooks run only for entries that execute). The `expired` counter in the stats line
(§7) is broken out from `dlq` because "work is being thrown away because the queue cannot keep
up" is a different operational signal from "work is failing".

### 9.2 Queue TTL

`app.queue_ttl` is a mapping `{queue_name: seconds}` where the key `"*"` is the fallback for
any queue without its own entry (the Python constructor also accepts a bare number, normalized
to `{"*": n}`). It bounds how long an entry may sit in a queue before it stops being worth
running.

It is applied in two places, which is deliberate and is not double enforcement:

- **Client, at enqueue**: when the caller gave no explicit `expires`, `expires_at` is stamped as
  `enqueued_at + ttl`. This makes the deadline visible in the envelope and works on any broker.
- **Worker, at dispatch**: the effective deadline is
  `min(envelope.expires_at, enqueued_at + queue_ttl_ms)`. The EARLIER wins in both directions,
  so a per-call `expires` cannot be used to sit in a TTL-bounded queue longer than the operator
  allows, and the TTL cannot extend a shorter per-call `expires` either.

The worker-side half is what makes the setting effective for envelopes produced before the TTL
existed, or by a client that did not read the same config. It is skipped when `enqueued_at` is
0 (absent): without that guard every such envelope would look ~55 years overdue and be
discarded. `enqueued_at + ttl` uses saturating arithmetic so a hostile `enqueued_at` cannot
wrap the deadline into the past.

The worker reads the map through the same duck-typed §6 introspection as everything else, so an
app object predating this feature simply reports no TTLs.

### 9.3 Queue routing rules

`app.task_routes` is an ORDERED list of `(glob, destination)` rules applied at enqueue time.
The Python constructor accepts a mapping `{glob: dest}` (insertion ordered), a sequence of
pairs, or a sequence mixing pairs with bare callables. A destination is a queue name, a
`{"queue": ...}` mapping, or a callable `(task_name, args, kwargs) -> str | dict | None`; a
callable returning `None` means "no opinion", so a router can fall through to the next rule.
`glob` is matched case-sensitively against the FULL task name (`fnmatch`), and the first
matching rule wins.

**Queue precedence, highest first:**

1. per-call `queue=` on `apply_async` (or a beat entry's own `queue`)
2. `app.task_routes`
3. the task's own decorator `queue=`
4. `app.default_queue`

Routes sit ABOVE the decorator queue on purpose: the point of app-level routing is that an
operator re-routes a task without editing the code that declared it. They sit BELOW a per-call
`queue=` because that is explicit runtime intent at the call site. (This is the same order
Celery uses, so the mental model ports.)

Routing is a CLIENT-side concern: it decides where the envelope is published, and the envelope
records the outcome in its `queue` field. The worker does not re-route; it consumes the queues
it was told to consume. That keeps routing broker-agnostic — the same rules produce an SQS
queue name or a RabbitMQ routing key without the worker learning anything new.

### 9.4 Priorities: not supported, by decision

**cauli does not implement priority levels. Use separate queues.**

Redis Streams have no priority. Every "priority" on Streams is emulation — typically N
sub-streams per logical queue, drained in weighted order — and that emulation is a bad trade
here:

- It multiplies the per-queue machinery by N: N consumer groups, N PELs for the §4.4 recovery
  loop to page through, N delayed zsets for the mover, N DLQs. The recovery loop's
  full-backlog drain and its per-envelope idle check are the most subtle code in the worker;
  fanning them out N-fold to emulate a feature the broker does not have is a poor exchange.
- It breaks the single blocking `XREADGROUP`. The current fetch loop issues ONE
  `XREADGROUP ... BLOCK 1000` across all queues; weighted draining needs either several
  non-blocking reads per pass (a busy poll) or a scheduler that decides which key set to block
  on (which is where starvation bugs live). The brief was explicit that the fetch loop's
  admission and backpressure behavior must not silently degrade, and this would degrade it.
- A subtly starving implementation is worse than none. Weighted draining starves low priority
  work exactly when the system is busiest — the moment you least want to discover it.
- It would be the wrong abstraction to bake into the envelope. The planned brokers disagree
  sharply: **SQS has no priorities at all**; **RabbitMQ has native `x-max-priority` queues**
  where a numeric priority is a real broker feature. A Redis-shaped N-sub-queue emulation
  encoded in the envelope would have to be ignored on SQS and bypassed on RabbitMQ.

What to do instead, in increasing order of isolation:

1. **One worker, ordered queues.** `--queues high,default,bulk`: within each fetch batch,
   entries from earlier-listed queues are dispatched first (§4), and no queue can starve
   because `COUNT` applies per stream. Good enough for "usually go first".
2. **Separate worker fleets.** Run a fleet on `--queues high` and another on `--queues bulk`.
   This is the only configuration that gives real isolation — dedicated capacity, not just
   dedicated ordering — and it is what a priority queue is usually being asked to approximate.

§9.3 routing is what makes both of these an operator change rather than a code change: point a
pattern at `high` and the task moves fleets without a deploy.

If a future RabbitMQ backend lands, a numeric `priority` envelope field mapping to
`x-max-priority` is a clean addition at that point, with an honest "ignored on Redis and SQS".
It is not being invented now on the strength of one broker that cannot implement it.

## 10. Periodic scheduling (`cauli-beat`)

`cauli-beat` publishes tasks on interval and crontab schedules. It is a separate process; the
worker knows nothing about it and needs no changes to run tasks it publishes (a beat-published
envelope is an ordinary §2 envelope).

**Multiple replicas are safe by default.** This is the design's whole point and the reason it
does not follow Celery, whose beat persists last-run times to a local `shelve` file with NO
locking — running two gives you every scheduled task twice, which is why its own docs tell you
to ensure only one is running and why the ecosystem routes around it with `celery-redbeat`.

### 10.1 Redis key layout

| Key | Type | Owner | Purpose |
|---|---|---|---|
| `cauli:beat:schedule` | Hash | operator / app | field = entry name, value = entry JSON (§10.2). The DEFINITION: what to run and when. |
| `cauli:beat:due` | ZSET | beat | member = entry name, score = next fire slot (epoch ms). The scheduler's only mutable state. |
| `cauli:beat:rev` | Hash | beat | entry name → schedule fingerprint. Detects an edited schedule so its slot is reseeded. |
| `cauli:beat:state` | Hash | beat | entry name → last-firing JSON: `last_slot`, `fired_at`, `lateness_ms`, `status` (`"fired"`/`"skipped"`), `task_id`, `next_slot`, `instance`. |
| `cauli:beat:runs` | Hash | beat | entry name → total run count (`HINCRBY`, exact). |
| `cauli:beat:lock` | String | beat | scheduler lease. Value = holder's instance id, `SET NX PX lock_ttl`. |

Definition and runtime state are separate keys on purpose: an admin UI (or an operator with
`redis-cli`) edits `cauli:beat:schedule` and nothing else, and can never corrupt the
scheduler's own bookkeeping by doing so. That split is what makes a Django-admin view over the
schedule an addition rather than a rewrite.

### 10.2 Schedule entry JSON

```json
{
  "name": "nightly-report",
  "task": "myapp.tasks.report",
  "args": [],
  "kwargs": {},
  "schedule": {"type": "interval", "every_ms": 60000},
  "queue": null,
  "expires": null,
  "idempotent": false,
  "enabled": true,
  "on_missed": "fire_once",
  "max_lateness": null,
  "source": "code"
}
```

`schedule` is one of:

```json
{"type": "interval", "every_ms": 60000}
{"type": "crontab", "minute": "0", "hour": "3", "day_of_month": "*",
 "month": "*", "day_of_week": "*", "timezone": "Europe/Berlin"}
```

- `name` is the entry's stable identity (it is the hash field key). Renaming creates a new
  entry with a fresh slot and orphans the old one.
- `queue` is the beat-entry equivalent of a per-call `queue=` and has the same precedence
  (§9.3 rule 1). With it null, `task_routes` and then the task's own queue apply.
- `expires` (seconds) becomes the published envelope's `expires_at` (§9.1). Recommended on
  anything time-sensitive: a report scheduled for 03:00 is usually not worth running at 09:00.
- `idempotent`: when true the published envelope carries
  `idempotency_key = "beat:{name}:{slot}"`. Belt and braces on top of the §10.5 CAS — off by
  default because it costs one `cauli:idemp:*` key per firing for `idemp_ttl` seconds.
- `on_missed` / `max_lateness`: §10.4.
- `source`: `"code"` for entries declared with `app.add_periodic_task`, anything else for
  entries created directly in Redis. Governs reconciliation (§10.3).

**Crontab semantics** follow `cron(8)`, not Celery:

- `day_of_month` and `day_of_week` are **OR'd** when both are restricted (a field counts as
  restricted unless it is exactly `*`). `0 3 1 * mon` fires on the 1st of the month AND on
  every Monday. Celery ANDs them. An expression written in crontab syntax should mean what
  crontab means.
- `day_of_week` is 0-6 with 0 = Sunday; 7 is accepted as Sunday too.
- Fields accept `*`, `*/step`, `a`, `a-b`, `a-b/step`, `a/step` (vixie: `a` to the top of the
  range by `step`), comma lists, and three-letter names for month and day-of-week.
- The timezone is an explicit IANA name, default `"UTC"`. There is no global "enable UTC"
  switch, because a schedule's timezone is a property of that schedule, not of the deployment.

**DST** falls out of one invariant rather than special cases: `next_after(slot)` must return an
instant strictly greater than `slot`.

- *Fall back* (a wall time occurs twice): the first occurrence fires. The second is not a later
  instant than one already consumed, so it cannot fire. A 01:30 daily job fires once that day.
- *Spring forward* (a wall time never occurs): `zoneinfo` resolves the nonexistent local time
  with the pre-transition offset, so a 02:30 job fires at the instant 02:30 standard time would
  have been — 03:30 by the new wall clock. It fires once; it is not dropped.
- *Ordering*. Wall clock order is instant order only while the offset holds still, so a day whose
  offset changes is scanned in full and answered with the EARLIEST instant after `slot`, not with
  the first wall time that matches. Without that rule a zone whose jump is wider than the gap
  between two scheduled hours loses a real slot: on `Antarctica/Troll` (+00 to +02) the
  nonexistent wall 02:30 resolves to 02:30Z while the real wall 03:30 resolves to 01:30Z, so
  answering with the first wall match would skip 01:30Z permanently. Taking the earliest instant
  is also what keeps the ordinary one hour case firing once, where a nonexistent 02:30 and a real
  03:30 are the same instant.

### 10.3 Reconciliation between code and Redis

Entries declared in code (`app.add_periodic_task(...)`) are upserted into
`cauli:beat:schedule` with `source: "code"`, at startup and again whenever an instance becomes
leader. A stored entry with `source == "code"` that no longer exists in code is DELETED, so
removing an `add_periodic_task` call actually unschedules it rather than leaving it firing
forever. Entries with any other source — created via the API or a future admin view — are never
touched by this reconciliation.

Reconciliation runs only while holding the lease, so during a rolling deploy the two code
versions do not both reap each other's entries at once. They can still briefly disagree about a
newly added or removed entry until the old replicas are gone; this converges, and is the
expected cost of declaring schedules in code.

### 10.4 The tick, and what happens after downtime

Per tick (the leader sleeps until the next slot, capped by `--max-interval` and by its own
lease refresh interval):

1. `now` = Redis `TIME`, **not** the local clock (§10.5).
2. Read all definitions (`HGETALL cauli:beat:schedule`) and all slots+revs in one round trip.
3. Seed any entry that has no slot, or whose stored rev no longer matches its definition, with
   `next_after(now)`. Seeding is a Lua CAS of its own so two replicas cannot seed twice.
   Disabled entries, and slots whose definition has vanished, have their slot removed.
4. `ZRANGEBYSCORE cauli:beat:due -inf now` → the due entries.
5. For each due entry at slot `S`: compute `S' = advance_past(S, now)`, build the envelope,
   then claim-and-publish atomically (§10.5).

**Missed ticks are coalesced, never replayed.** `advance_past(S, now)` returns the first slot
strictly after BOTH `S` and `now`. So an entry that was due 500 times while beat was down fires
**once** on recovery and then resumes its normal cadence. Replaying 500 firings is almost never
what anyone wants from a scheduler, and it is the failure mode that turns a brief outage into
an incident.

Every slot that gets dropped announces itself at WARNING, because lateness alone does not tell an
operator how much scheduled work never ran. A coalesced firing logs the count of slots it
swallowed next to its lateness; an entry that loses its slot (disabled, definition deleted, or
definition unreadable) logs which of those it was; and a tick that hits the cap of 500 due entries
logs that the remainder is deferred to the next tick.

Whether that single recovery firing happens at all is per entry and explicit:

| `on_missed` | `max_lateness` | Behavior for a slot that is `L` late |
|---|---|---|
| `"fire_once"` (default) | `null` (default) | Always fires, however late. |
| `"fire_once"` | `S` | Always fires (the setting has no effect without `"skip"`). |
| `"skip"` | `null` | Always fires (nothing is ever considered "missed"). |
| `"skip"` | `S` | Fires if `L <= S`; otherwise does NOT fire. |

In every row the slot is advanced past `now` regardless, so a suppressed firing does not leave
the entry stuck re-triggering. A suppressed firing is recorded in `cauli:beat:state` with
`status: "skipped"` and its `lateness_ms`, and logged at WARNING — dropping scheduled work is
only acceptable when it is announced.

Use `"skip"` + `max_lateness` for work whose value is tied to its slot (a 03:00 report, a
"send the daily digest" job). Leave the default for work that just needs to happen (a cleanup
sweep, a cache warm).

### 10.5 Exactly once per slot, across replicas

Two mechanisms, with two different jobs. Confusing them is the usual way this goes wrong.

**The lease is for efficiency, not safety.** `cauli:beat:lock` is `SET NX PX lock_ttl`, holding
the instance's id. The holder refreshes at `lock_ttl / 3` via a compare-and-refresh script
(`PEXPIRE` only if `GET` still equals me), so two consecutive failed refreshes still leave a
third of the lease in hand. A non-holder can neither refresh nor release it. Non-leaders poll
for it and do no scheduling work. If a refresh fails because someone else now holds it, the
instance steps down immediately.

A failed Redis call is NOT itself treated as losing the lease. The instance re-verifies with a
refresh on its next pass, which renews when the lease is still its own and steps it down when
it is not. Treating a blip as a loss would strand it: `SET NX` cannot reacquire a key that
already holds its own id, so the instance would sit as a standby waiting out a full `lock_ttl`
for a lease it never lost — one transient error costing a whole lease of scheduling.

**The compare-and-set is what makes firing exactly once.** Advancing an entry's slot from `S`
to `S'` and publishing the task happen inside ONE Lua script, which refuses unless the stored
score is still exactly `S`:

```lua
-- KEYS[1]=cauli:beat:due  KEYS[2]=cauli:beat:state  KEYS[3]=cauli:beat:runs
-- KEYS[4]=target key (cauli:q:{queue} or cauli:delayed:{queue})
-- ARGV: 1=name 2=expected_slot 3=next_slot 4=mode 5=envelope 6=delayed_score 7=state_json
local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
if cur == false then return 0 end
if tonumber(cur) ~= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
if ARGV[4] == 'stream' then
  redis.call('XADD', KEYS[4], '*', 'e', ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
elseif ARGV[4] == 'delayed' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[7])
return 1
```

(`mode = 'none'` advances the slot without publishing: the `on_missed: "skip"` path.)

Safety therefore does not depend on the lease at all. A lease can always be defeated — a
stop-the-world GC pause, a network partition, a Redis failover — so building the exactly-once
guarantee on it would be a bug waiting for a bad day. Two instances that BOTH believe they are
leader still produce exactly one firing per slot: they race the same CAS and one of them gets
0 back. The lease only stops every replica from doing redundant polling work.

**Why clock skew cannot break it.** Two inputs decide a firing, and neither is the replica's
clock:

- "Now" is `TIME` from Redis, so every replica compares against the same clock the slots are
  stored against. A replica minutes off does not fire early or late.
- The CAS compares the slot the caller **expected**, `S`, and never the value it proposes. That
  is what makes it a mutual exclusion: the winner's `ZADD` moves the score off `S`, so every
  other racer's `ZSCORE` stops matching and returns 0. Agreement on `S'` is not required, and
  does not hold in general: `advance_past` fast forwards past the present (section 10.4) and so
  does read "now", meaning two replicas that read `TIME` seconds apart genuinely propose
  different `S'` after an outage. Only the winner's value is written, so the schedule resumes on
  one phase rather than two. (`next_after` on its own is a pure function of the previous slot,
  which is worth having, but the safety argument does not rest on it.)

**Leader dies mid-tick.** Advance-and-publish is one script, so a slot is either
fired-and-advanced or neither — there is no window where a slot is consumed without a task
being published. A leader that dies partway through a tick leaves its remaining entries
un-advanced; the next leader finds them due and fires them, late but exactly once.

**Leader dies between ticks.** The lease expires and a standby acquires it. Because the holder
refreshes at `lock_ttl / 3`, the residual lease at the moment of death is between
`2/3 * lock_ttl` and `lock_ttl`, which bounds failover latency. Slots that fall inside that
window are coalesced into one firing on recovery (§10.4), not replayed. `--lock-ttl` is
therefore a direct dial on worst-case scheduling lateness after a crash.

Measured (3 replicas, one Redis, a 0.5s interval entry, leader SIGKILLed with no chance to
release its lease — `py/tests/test_beat_ha.py` covers the same ground as an assertion):

| `--lock-ttl` | lease takeover after SIGKILL | duplicate firings | slots re-fired across the handover |
|---|---|---|---|
| 2s | 1.99s | 0 | 0 |
| 5s | 5.01s | 0 | 0 |
| 15s | 10.03s | 0 | 0 |

The 15s row shows the `2/3 * lock_ttl` floor: the leader had refreshed ~5s before it died, so
only ~10s of lease remained. In all three runs the outage appears as a SINGLE coalesced firing,
not a replay of the ~4/10/20 slots that elapsed, and `cauli:beat:runs` matched the number of
envelopes published exactly.

**The guarantee is per Redis dataset.** Everything above holds for as long as the CAS write
survives, and Redis replication is asynchronous, so a failover promotes whatever the replica had
actually received. If the leader fires slot `S`, a worker consumes and executes that task, and
the master then dies before the write reaches the replica, the promoted node still has `S` in
`cauli:beat:due` and beat fires `S` a second time. That is a genuine duplicate run. It is a
property of the store, not of the CAS, which cannot defend a write the database has forgotten.
Sentinel gets the atomic script but no immunity from this; only a synchronously replicated store
would be immune.

`idempotent: true` does not reliably cover it. The key is `beat:{name}:{slot}`, so it is stable
across the second firing, but the worker writes its `cauli:idemp:*` guard (section 4.5) to the
same node inside the same window of lost writes, so a failover that loses the slot advance can
lose the guard with it. It narrows the window rather than closing it. Work that genuinely must
not run twice needs a deduplication check in a store whose durability you have chosen on purpose,
keyed on the envelope's `beat_slot`.

**Redis Cluster.** The claim script touches both `cauli:beat:*` and `cauli:q:{queue}`, which do
not hash to the same slot, so Cluster rejects it with CROSSSLOT. Beat detects this once, logs a
warning and degrades to CAS then publish. The CAS itself is unchanged, so the SLOT is still
claimed exactly once, but the publish is a separate command now and that costs atomicity in both
directions:

- a crash in the gap between the advance and the publish drops that firing entirely;
- a publish that lands but whose reply is lost, and which is then retried, publishes the envelope
  twice: same task id, two stream entries, one increment of `cauli:beat:runs`.

Whether the second can happen is a property of the client, not of beat. `redis.Redis` is built
with zero command retries by default, so it raises rather than retrying. `RedisCluster` defaults
to `cluster_error_retry_attempts=10` and does retry on connection errors, and Cluster is the only
thing that reaches this path at all. Treat scheduled tasks as at least once when running beat
against Cluster, and deduplicate on `beat_slot`. Standalone and Sentinel Redis get the atomic
path and neither failure mode.

### 10.6 What a non-Redis broker would have to provide

Beat talks to a `ScheduleStore` seam (`cauli/beat.py`), not to Redis commands. A backend needs:

1. **An atomic compare-and-set on a per-entry value, bundled with the publish.** Required for
   correctness; everything in §10.5 rests on it.
2. **A lease with a TTL that only its holder can refresh or release.** Required only for
   efficiency (see §10.5), so a backend that cannot offer one still works, at the cost of every
   replica ticking.

Neither is a queue primitive, which is the point: **SQS has neither** (the usual answer is an
external store — a DynamoDB conditional write for (1), the same for (2)), and **RabbitMQ has
neither natively either**. Beat's schedule store is expected to stay separable from whatever
carries the tasks. Note also that a broker's delay ceiling is a store concern rather than a
beat concern here: SQS caps `DelaySeconds` at 900s, so a `not_before` further out than 15
minutes needs a cauli-side delayed store on that backend regardless of beat.

### 10.7 Why a separate entrypoint rather than a worker flag

`cauli-beat` is a Python console script (`python -m cauli.beat` is equivalent), not a Rust
binary and not `cauli-worker --beat`:

- **The schedule model has to be Python.** Entries live in Redis so a Django-admin view can
  edit them; that view is Python and so is the client. A Rust scheduler would mean two
  implementations of crontab semantics and of the entry format, and they would drift.
- **Timezone and cron correctness are cheaper and safer in Python.** `zoneinfo` is stdlib and
  DST-aware; reimplementing the DST rules of §10.2 in Rust to schedule a handful of jobs a day
  buys nothing.
- **There is no throughput argument.** Beat does O(one tick per second) work. Rust's advantage
  in this codebase is per-task overhead on the hot path, and beat has no hot path.
- **Replica counts differ.** You want ~2 beat replicas for availability and N worker replicas
  for throughput. A `--beat` flag forces a bad choice: enable it everywhere (N replicas all
  contending for one lease, doing redundant polling) or designate special workers (whose
  scale-down now also kills the scheduler).
- **The worker's fetch/dispatch loop stays untouched**, which matters given how much of §4.4
  and §5.1 is load-bearing.

The cost, stated plainly: one more process to deploy. That is the same shape as `celery beat`,
so it is familiar, and `--once` exists for anyone who would rather drive a tick from system
cron than run a daemon.

## 11. Non-goals for v1

Chains/chords/canvas, priority levels (§9.4 — deliberate, not pending), rate limits, task
events bus, multi broker (Redis only), result backends other than Redis, Windows worker support
(worker targets Linux; client and `cauli-beat` are cross platform), and a schedule admin UI
(the Redis layout in §10.1 is designed for one; the UI itself is out of scope).

## 12. Building and testing

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

# python client (includes the scheduler and its concurrency suites)
cd py
pip install -e '.[dev]'
ruff format --check .
ruff check .
pytest -q
# tests/test_schedules.py     -- interval/crontab math, POSIX dom-dow OR, DST transitions
# tests/test_beat.py          -- seeding, missed-slot policy, reconciliation, and the
#                                claim CAS under real thread contention
# tests/test_beat_ha.py       -- TWO real `python -m cauli.beat` PROCESSES against one
#                                Redis: exactly one firing per slot, then SIGKILL the
#                                lease holder and prove the standby takes over. These
#                                spawn subprocesses and take ~13s; they are the only
#                                meaningful test of the §10.5 claims.

# cross-component integration (real client + real worker binary + real cpu children)
cd itest
pip install -e ../py
pytest -q                        # needs the release worker binary built above on PATH
                                 # or set CAULI_WORKER_BIN=/path/to/cauli-worker
# test_django.py additionally needs django + psycopg (pip install 'django>=4.2'
# 'psycopg[binary]') and the postgres server binaries (initdb/pg_ctl, e.g.
# apt install postgresql-16) — it stands up a throwaway user-owned postgres
# on port 54329 and a throwaway redis on 6395; it skips itself when either
# dependency is missing.
```

Building on a slow or network filesystem: set `CARGO_TARGET_DIR` to a local path before running
cargo commands to avoid compiling through a slow mount.

## 13. Distribution

Two artifacts per release, published together and versioned in lockstep.

| Package | Contents | Wheel tag |
|---|---|---|
| `cauli` | the pure-Python client, plus the `cauli-beat` entry point | `py3-none-any` |
| `cauli-worker` | the prebuilt Rust worker binary | `cp3XX-cp3XX-manylinux_2_28_{x86_64,aarch64}` |

### 13.1 Why the worker ships one wheel per CPython version

`cauli-worker` embeds CPython (pyo3 `auto-initialize`), so the built binary carries
`NEEDED: libpython3.X.so.1.0` and will not start against any other minor version. It is not a
portable static binary, and shipping it as one would only move the failure to exec time.

Wheel tags describe exactly that constraint, which is why the worker is distributed as a wheel
rather than a tarball: `cp312-cp312-manylinux_2_28_x86_64` means "needs CPython 3.12 on glibc
2.28 or newer", and pip refuses to install it anywhere else.

Installing into the app's virtualenv also settles which interpreter the worker embeds, by
construction rather than by configuration. pip places the binary in that venv's own `bin/`, the
loader resolves that venv's `libpython`, and `shim.py` reads `VIRTUAL_ENV` for site-packages
(§6). The three cannot disagree.

Requirements: Linux on x86_64 or aarch64, glibc 2.28 or newer, and a CPython configured with
`--enable-shared`. python.org builds, the Docker `python:*` images, Debian/Ubuntu/Fedora system
packages, conda and actions/setup-python all qualify; `pyenv` does not unless rebuilt with
`PYTHON_CONFIGURE_OPTS="--enable-shared"`. Anything outside that builds the worker from source,
which has no such constraints.

Raw binaries are also attached to each GitHub release, as
`cauli-worker-<version>-cp3XX-cp3XX-<platform>.tar.gz`, for deployments that are not a
virtualenv. The CPython version stays in the filename because for this binary it is not
optional information.

### 13.2 The two things a release is gated on

Neither package is uploaded until both hold, on every supported CPython version:

1. **The worker links libpython dynamically.** Asserted with `readelf -d` on the binary inside
   the built wheel. pyo3 links libpython statically when the build interpreter has no shared
   library, and a statically linked worker would carry its own interpreter, whose `sys.prefix`
   points at a path on the build machine. On a user's box that either fails to find a stdlib
   or, worse, starts an interpreter that cannot see their virtualenv and so cannot import
   their tasks. It is a silent-wrong-answer failure, so it is a build-time gate.
2. **The built wheels run the real integration suite.** They are installed into a clean
   virtualenv with `--no-index`, and `itest/test_integration.py` runs against that installed
   binary: real worker process, real cpu children, real Redis. A release that cannot run is
   worse than a late one.

### 13.3 Version lockstep

`cauli-worker` depends on `cauli==<same version>`. The two implement one wire contract, so at
0.x a mismatched pair is a bug rather than a convenience, and a pin turns it into a pip
resolution error instead of a protocol bug at runtime.

Four files carry the version: `worker/Cargo.toml` (which is also the worker wheel's version,
via maturin), the `cauli==` pin in `worker/pyproject.toml`, `py/pyproject.toml`, and
`py/cauli/__init__.py`. `scripts/check_versions.py` asserts they agree, and that they match the
git tag when given one. It runs on every push, not only at tag time, because that failure
otherwise surfaces at a user's `pip install`.

Releasing is pushing a `vX.Y.Z` tag. `.github/workflows/release.yml` does the rest and uploads
via PyPI trusted publishing, so no API token is stored in the repository.
