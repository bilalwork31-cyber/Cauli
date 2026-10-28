# cauli protocol v1

The contract between the Rust worker (`worker/`) and the Python package (`py/`):
Redis keys, envelope, delivery, retry, timeout and idempotency rules, the cpu
child protocol, results, the scheduler and packaging. Both implementations MUST
match it. A disagreement between this document and the code is a bug in one of
them. Design rationale lives in [docs/architecture.md](docs/architecture.md).

Broker and result store: Redis >= 7.0. Timestamps are integer unix epoch
milliseconds unless stated otherwise.

## 1. Redis key layout

Every key has the prefix `cauli:`. `{queue}` matches `[a-zA-Z0-9_.-]+`.

| Key | Type | Contents |
|---|---|---|
| `cauli:q:{queue}` | Stream | Ready tasks. Each entry has one field `e`, the envelope JSON (UTF-8) |
| `cauli:delayed:{queue}` | ZSET | Delayed and retrying tasks. Member = envelope JSON, score = fire time |
| `cauli:dlq:{queue}` | Stream | Dead letters. Fields `e` (envelope), `reason` (string), `error` (§8 error JSON or empty) |
| `cauli:result:{task_id}` | String | Result JSON (§8), `SET ... EX result_ttl` |
| `cauli:idemp:{h}` | String | Idempotency claim. Value = claiming task id (§4.5) |
| `cauli:beat:*` | several | Scheduler state, owned by `cauli-beat` (§10.1) |

- `{h}` is SHA-256 of the `idempotency_key`, truncated to 128 bits, as 32
  lowercase hex characters. The raw key never appears in a key name.
- Every DLQ `XADD` carries `MAXLEN ~ 1000` and is followed by `EXPIRE` of 7
  days, so a DLQ keeps roughly its newest 1000 entries and disappears 7 days
  after its last write. Older dead letters are dropped.
- Consumer group `cauli` per queue stream, created with
  `XGROUP CREATE cauli:q:{queue} cauli 0 MKSTREAM` (BUSYGROUP ignored).
  Consumer name `{hostname}:{pid}`.
- `XLEN cauli:q:{queue}` is backlog plus up to one flush window of completed
  entries (§4.1). It is not a backlog reading; `oldest_ms` (§7) is.

## 2. Envelope JSON

Produced by the client at enqueue. Field names and types are exact. Unknown
fields MUST be preserved on re-enqueue (retries).

```json
{
  "v": 1,
  "id": "32 lowercase hex characters, 128 random bits",
  "task": "myapp.tasks.send_email",
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

| Field | Rule |
|---|---|
| `v` | Protocol version, `1`. A `v` above the worker's own is dead lettered `"malformed"` |
| `id` | MUST match `[0-9a-f]{32}`, else dead lettered `"malformed"` without retry |
| `args` | JSON array or null (= `[]`). Any other type: `"malformed"` |
| `kwargs` | JSON object or null (= `{}`). Any other type: `"malformed"` |
| `kind` | `"io"` or `"cpu"`. The worker's registry wins over this field |
| `retries` | Failed executions so far. 0 at enqueue; the worker increments it per retry |
| `timeout_ms` | Hard timeout. MUST be > 0, else `"malformed"`. Above 24 hours it is clamped to 24 hours |
| `soft_timeout_ms` | null or int. Ignored unless `0 < soft_timeout_ms < timeout_ms` |
| `idempotency_key` | null or string (§4.5) |
| `enqueued_at` | Client clock at enqueue. 0 means absent |
| `not_before` | null, or the instant before which the task MUST NOT run. From `countdown` (`now + countdown * 1000`) or `eta` (an aware datetime); the two are exclusive. A past value is kept as given |
| `expires_at` | null, or the absolute instant past which the task is discarded unrun (§9.1). Survives retries, the delayed set and redelivery unchanged |

Envelopes published by `cauli-beat` also carry `beat_name` (the schedule entry)
and `beat_slot` (the slot in epoch ms, distinct per firing). The worker
preserves both and attaches no behaviour to them.

Integer fields (`v`, `retries`, `max_retries`, `backoff_base_ms`,
`backoff_max_ms`, `timeout_ms`, `soft_timeout_ms`, `enqueued_at`, `expires_at`)
accept a JSON integer, or a number with a decimal point or exponent whose value
is an exact whole number (`1.7e12`). A fractional value, NaN, an infinity or a
value outside the field's range is `"malformed"`, never rounded. The 24 hour
`timeout_ms` clamp is the only exception.

Worker gates, applied before execution, to every envelope as untrusted input:

- The raw `e` field MUST NOT exceed `--max-envelope-bytes` (default 1 MiB).
  Oversize is `"malformed"`; the DLQ stores a truncated preview.
- A producer MUST refuse to publish an envelope above its own limit
  (`Cauli(max_envelope_bytes=...)`, default 1 MiB), raising at the call site
  and writing nothing. Keep the producer limit at or below the worker's.
- For an entry dead lettered without executing, the worker recovers the task
  id from `e` so it can write a failure result (§8). An oversize entry is read
  only as its first 4096 bytes; if the id is not in them there is no result,
  and a caller in `get()` waits out its own timeout.
- Memory: `args` and `kwargs` are carried as the producer's JSON text. Size
  for `in flight io tasks x max-envelope-bytes x 10` in the worst case.

## 3. Client enqueue rules

1. Resolve the queue (§9.3).
2. Build the envelope: `enqueued_at` = now; `not_before` from `countdown` or
   `eta`; `expires_at` from `expires`, else from the queue TTL (§9.2).
3. If `not_before` is in the future: `ZADD cauli:delayed:{queue} not_before envelope`.
4. Else: `XADD cauli:q:{queue} * e envelope`.
5. Return `AsyncResult(id)`.

The client does not check idempotency, and does not drop an envelope already
past `expires_at`: it is published and discarded at dispatch. The one client
side expiry rule is the §9.2 refusal of a delay past the queue TTL.

The wire format is plain JSON and any compliant codec MAY produce it. A
producer MUST reject what JSON cannot express: NaN and infinities, non `str`
dict keys, `set`, `bytes` and any non JSON type. The bundled client uses
msgspec and validates the value tree first, because msgspec alone accepts all
of those.

## 4. Worker delivery loop

### Delivery guarantee

- Once Redis accepts an enqueue, a task is never lost silently: it runs to a
  recorded outcome or is dead lettered with a reason.
- Execution is at least once. Every internal failure (a torn completion
  pipeline, a worker crash, a script error, a failed idempotency claim)
  resolves toward running the task again.
- Work terminates: `max_retries` failed executions, each attempt redelivered
  after a crash at most `redelivery_limit` times (§4.4), then the DLQ.
- Worst case executions of one task: `(max_retries + 1) x (redelivery_limit + 1)`,
  20 with the defaults. Size quotas, rate limits and alerts on that product.
- `cauli-beat` fires each slot at most once (§10.5).

Everything above holds within one Redis dataset, and only while it persists:

- Redis MUST persist (RDB, AOF or both) and restore on restart. A Redis that
  comes back empty has lost every unacked entry, every delayed and retried
  task, and every idempotency claim; nothing redelivers them.
- `maxmemory-policy` MUST NOT evict cauli keys; use `noeviction`. `FLUSHALL`
  on a broker is data loss.
- Replication is asynchronous. A failover can forget an acknowledged enqueue
  (the one silent loss), an idempotency claim, or a beat slot advance (a slot
  fires twice). Sentinel has the same exposure.
- After a dataset reset the worker gets NOGROUP, logs an error starting
  `redis has no consumer group`, recreates the groups and resumes (§7).

What a user MUST do: write every task to tolerate running twice.
`idempotency_key` narrows the window and does not remove the obligation. Work
that must never repeat needs its own dedup check in the task, keyed on
something stable (`beat_slot` for scheduled work). Keep `--visibility-timeout`
above the longest task timeout, keep `--drain-timeout` above it too, and watch
the DLQ before its cap drops entries. The worker warns at startup when a
registered task's timeout exceeds `--drain-timeout`; it does not check
`--visibility-timeout`.

### Clocks and reference points

Absolute instants are Redis time. The worker samples `TIME` once at startup
(blocking), resamples every 15 to 30 seconds, and extrapolates between samples
on a monotonic clock. If `TIME` fails it keeps extrapolating from its last
anchor (or the local clock) and warns; it does not refuse to start. Durations
are monotonic and local. The client stamps `enqueued_at`, numeric `expires` and
`countdown` from its own clock; client hosts need NTP.

| Measurement | Clock | Zero point |
|---|---|---|
| delayed score (retry, countdown, eta) | Redis, via the worker's anchor | unix epoch |
| §9.1 expiry deadline | stamped by the client, compared on the anchor | unix epoch |
| result `finished_at`, dead letter stamps | Redis, via the anchor | unix epoch |
| `IDLE`, `delivery_count` (§4.4) | inside Redis | the entry's last delivery or claim |
| `timeout_ms`, `required_idle_ms`, `visibility_timeout` | monotonic, in the worker | start of the measurement |
| execution timeout backstop | monotonic, in the worker | when the executor takes its slot, not delivery |

### The read loop

- `XREADGROUP GROUP cauli {consumer} COUNT {n} BLOCK {block_ms} STREAMS cauli:q:{q1} ... > ...`
- `n = max(1, min(--batch, free io slots) / streams)` per stream, `--batch`
  default 64. A fetch waits until at least `min(streams, --io-concurrency)` io
  slots are free. A fetch MUST NOT take more entries than can start, because
  idle time counts from delivery.
- `block_ms = min(1000, --redis-timeout * 1000 / 2)`, at least 50.
- Fetch only while io slots are free and the cpu backlog is not full. The cpu
  backlog holds at most `2 * cpu_workers * cpu_child_threads` entries.
- Per entry, in order:

| Condition | Outcome |
|---|---|
| over `--max-envelope-bytes`, or not valid JSON, or a §2 gate fails | DLQ `"malformed"`, ack, failure result when the id is recoverable |
| task name not registered | DLQ `"unregistered"`, ack, failure result when the id is recoverable |
| past its deadline (§9.1) | DLQ `"expired"`, ack, `"expired"` result. Checked before §4.5 |
| otherwise | §4.5 claim, then execute on the registry's lane: async io, sync io, or cpu |

Queue order is dispatch order within a batch: entries from earlier `--queues`
are dispatched first. `COUNT` applies per stream, so no listed queue starves.
There are no priority levels (§9.4).

### 4.1 Completion

Each outcome's own writes:

| Outcome | Writes |
|---|---|
| success | `SET cauli:result:{id} ... EX result_ttl` if `store_result` |
| retry | `ZADD cauli:delayed:{queue}` (§4.2) |
| final failure | DLQ `XADD`, failure result if `store_result` |
| never executed (malformed, unregistered, redelivery limit, expired) | DLQ `XADD`, result per §8 |

Completions are buffered per queue. A flush fires at `--ack-batch`
completions (default 64) or when the oldest buffered completion is
`--ack-flush-ms` old (default 2 ms), and takes two round trips:

1. One pipeline of every buffered completion's own writes, each reply read.
2. One multi id `XACK`, then one multi id `XDEL`, naming only the entries
   whose own writes all succeeded. An entry with a refused write (for example
   OOM under `noeviction`) stays pending and is redelivered under §4.4.

Rules:

- Order is fixed: own writes, then `XACK`, then `XDEL`. A crash before the ack
  redelivers (and resolves under §4.5); between ack and delete it leaves
  orphans for the trim. Deleting before acking would strand entries in the
  PEL, where they could never be claimed or acked.
- No `MULTI`. The keys can live in different slots; a torn flush resolves
  toward re execution.
- A completion is reported (counters, drain) only after its flush is answered.
  A crash loses at most one unflushed buffer per queue to redelivery.

Trim: every 100 ms per queue, the worker computes a boundary below which every
entry is acked (the group's oldest pending id, or one past `last-delivered-id`
with an empty PEL, reading `XINFO GROUPS` before `XPENDING`) and runs
`XTRIM cauli:q:{queue} MINID {boundary}`. It removes only torn flush orphans,
never a pending or undelivered entry, and is safe with several workers. An
orphan behind a long running task waits for that task.

### 4.2 Retry policy

On a retryable failure with `retries < max_retries`:

1. `retries += 1`; `attempt = retries`.
2. `d = min(backoff_max_ms, backoff_base_ms * backoff_factor^(attempt - 1))`;
   if `jitter`, `d = uniform(0.5 * d, d)`. A `backoff_factor` below 1.0 counts
   as 1.0. `d`, a forced `countdown` included, is clamped to 30 days.
3. `ZADD cauli:delayed:{queue} (now + d) envelope`, in the flush before the
   ack. No result is written; the task is still pending.

On failure with `retries >= max_retries`: DLQ `XADD` with reason
`"max_retries"` and the error, failure result if `store_result`, ack.

A failure marked `retryable: false` takes the final path on its first attempt
with reason `"not_retryable"`, whatever retries remain. Only the worker's own
`SerializationError` is non retryable; every exception from user code is
retryable.

Forced retry: an exception whose class name is exactly `Retry` and that has a
`countdown` attribute is a forced retry, on every lane, whatever module defines
it. `countdown` is float seconds and replaces the computed backoff; `None` or a
non numeric value keeps the computed backoff. `max_retries` still bounds it.

### 4.3 Delayed mover

Every `--mover-interval` (default 50 ms), per queue, the worker runs:

```lua
-- KEYS[1]=cauli:delayed:{queue}  KEYS[2]=cauli:q:{queue}  ARGV[1]=now_ms  ARGV[2]=limit
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, e in ipairs(due) do
  redis.call('XADD', KEYS[2], '*', 'e', e)
  redis.call('ZREM', KEYS[1], e)
end
return #due
```

- `XADD` MUST come before `ZREM`: Lua does not roll back, so a failure part
  way may duplicate an entry, never lose it.
- `limit` = `--mover-limit` (default 128). Within one tick the script repeats
  while a queue returns a full page, up to 32 times per queue.
- `now_ms` is the Redis anchored clock.
- Only the worker runs the mover. The client only `ZADD`s.
- On Redis Cluster this script fails with CROSSSLOT on every call; the worker
  logs that by name. Cluster is unsupported (§10.5).

### 4.4 Crash recovery / redelivery

Every `max(visibility_timeout / 2, 500 ms)` (`--visibility-timeout`, default
60 s), per queue:

1. `XPENDING cauli:q:{queue} cauli IDLE {visibility_timeout_ms} {start} + {count}`,
   paging with `start = -` then `({last_id}` until the PEL is exhausted, with
   `count = min(free io slots, 128)`. Each tick drains the whole eligible
   backlog. Pages are pipelined and wait for free io slots and an empty cpu
   backlog.
2. Peek each candidate's envelope (`XRANGE` by id) and compute
   `required_idle_ms = max(visibility_timeout_ms, timeout_ms + 2000 + margin)`
   with `margin = max(redis_timeout_ms, 2000)`, 7 s over the task timeout by
   default; `visibility_timeout_ms` alone if the envelope does not parse. Skip
   the entry if its idle time is below that, or if this process still holds
   it.
3. Otherwise, if `delivery_count > redelivery_limit`, claim it, DLQ it with
   reason `"redelivery_limit"`, write a failure result when the id is
   recoverable, and ack. `redelivery_limit = max(3, max_retries + 1)`, or 3 if
   the envelope does not parse.
4. Else `XCLAIM` it to this consumer and execute it as a fresh delivery,
   without incrementing `retries`.

`retries` counts failed executions and travels in the envelope;
`delivery_count` counts crash redeliveries of one attempt and lives in the PEL,
so each retry starts with a fresh count. An attempt dead letters on the
delivery after `delivery_count` passes the limit.

Every 20 ticks the loop also deletes consumers idle for at least
`max(4 x visibility_timeout, 10 min)` that own no pending entries.

Idle time counts from delivery, the execution timer from when the executor
takes a slot. Fetching no more than the free io slots keeps that gap small on
the io lanes; on the cpu lane an entry waiting in the cpu backlog can be
reclaimed, which resets its idle time.

### 4.5 Idempotency guard

At execution start, when `idempotency_key` is not null, one Lua script:

1. `SET cauli:idemp:{h} {task_id} NX EX ttl` with
   `ttl = max(idemp_ttl, ceil((timeout_ms + 2000) / 1000))`.
2. If the key exists, `GET` it.

| Holder | Outcome |
|---|---|
| none (fresh claim) | execute |
| this task's own `id` (a retry or a crash redelivery) | `PEXPIRE` back to `ttl`, execute |
| a different task id | do not execute. If `store_result`, write status `"duplicate"` with `claimant_id`. Ack |

- A claim is never released, including after the claimant is dead lettered.
  A resubmission with the same key is suppressed for the rest of the TTL;
  read `cauli:result:{claimant_id}` or the DLQ for the real outcome.
- `claimant_id` is null only when the key expired between the `SET` and the
  `GET`.
- Fail open: a Redis error during the claim (not "key exists") executes the
  task and logs a warning.

### 4.6 Timeouts

| Lane | Soft timeout | Hard timeout |
|---|---|---|
| async io | the body runs as a Task, cancelled at the soft mark and reported as `SoftTimeLimitExceeded`; the body sees `asyncio.CancelledError` | `asyncio.wait_for`; a body that swallows the soft cancel is cancelled again at the hard mark, reported `TimeLimitExceeded`. Rust backstop at `timeout_ms + 2000`, saturating |
| sync io | `SoftTimeLimitExceeded` injected into the thread with `PyThreadState_SetAsyncExc` | `timeout_ms` from when a pool thread picks the job up: the task is failed (retryable) and its thread abandoned; a replacement starts, up to 4 x `--io-threads` threads in total, then a warning. A job that waits `timeout_ms` for a free thread fails as `WorkerLost` and is skipped, never run late |
| cpu | `SIGALRM` in the child when single threaded (`--cpu-child-threads 1`, and always in stdio mode); a watchdog thread with `PyThreadState_SetAsyncExc` when above 1 | the worker SIGKILLs the child, fails the request `TimeLimitExceeded` (retryable), and replaces the child |

- Both timeouts are failures and retryable.
- Injected soft timeouts are fenced by a per thread generation, so a late timer
  never lands in a later task. One race remains: a timer firing after the
  function returns but before its result is built can turn a success into a
  `SoftTimeLimitExceeded` failure.

### 4.7 Graceful shutdown

On SIGTERM or SIGINT: stop fetching; keep the mover, the flushers and the trim
running; wait up to `--drain-timeout` (default 30 s) for in flight tasks, each
counted until its flush is answered. Exit 0 when drained, 88 when tasks were
left; those stay pending and are recovered under §4.4. A second signal at
least 1 s after the first exits 130 immediately; one sooner is treated as the
same stop request.

### 4.8 Task lifecycle hooks

The app MAY expose three lists of zero argument callables, read with `getattr`
(absent means empty). The client fills them with `app.before_task(fn)`,
`app.after_task(fn)` and `app.process_init(fn)`.

| List | Runs |
|---|---|
| `_before_task_hooks` | before every task, in its thread or process, on every lane, before the soft timeout is armed |
| `_after_task_hooks` | after every task on every outcome, after the soft timeout is disarmed. A sync result is reported after these return |
| `_process_init_hooks` | once per cauli managed process, after the app import and before any task: the embedded interpreter, the fork server parent before its first fork, each forked child after the fork, each stdio child |

- Hooks do not run for entries that never execute: malformed, unregistered,
  duplicate, expired, redelivery limit.
- A hook raising `Exception` is reported on stderr and skipped; it never fails
  the task or stops later hooks. `SystemExit` and `KeyboardInterrupt`
  propagate.
- Registration order is call order. The worker holds the live lists, so hooks
  appended after startup run.
- On the async lane hooks run on the loop thread and a returned awaitable is
  awaited. The sync and cpu lanes ignore it.
- Each sync pool thread keeps one CPython thread state for life, so
  `threading.local` survives across tasks on that thread.

`cauli.contrib.django` registers `close_old_connections` before and after
every task and `connections.close_all` at process init.

## 5. Execution classes

Three lanes: async io tasks on `--io-loops` asyncio loops, sync io tasks on
`--io-threads` threads, both under one gate of `--io-concurrency` tasks; cpu
tasks on `--cpu-workers` child processes of `--cpu-child-threads` requests
each. Defaults and the `-c` derivation:
[docs/configuration.md](docs/configuration.md).

### 5.1 cpu child protocol (`python3 -m cauli._exec`)

Line delimited JSON, one object per line, in both modes:

```
request:  {"id": "...", "task": "...", "args": [...], "kwargs": {...}, "soft_timeout_ms": null}
response: {"id": "...", "ok": true, "result": <json>}
          {"id": "...", "ok": false, "error": {"type": "...", "message": "...", "traceback": "..."}}
          {"id": "...", "ok": false, "retry": true, "countdown": <float|null>, "error": {...}}
```

- `id` is a correlation string the worker chooses (`{envelope id}.{sequence}`),
  echoed verbatim.
- The third shape is a forced retry (§4.2).
- A result that is not JSON serializable is error type `SerializationError`.
- The child MUST NOT crash on a task exception; it reports it.
- Child death during a request fails it with `WorkerLost`, retryable.
- A response that is not JSON fails the request as `WorkerShimError`,
  retryable.
- Children run the §4.8 hooks.

#### Fork-server mode (default)

The worker binds a unix stream socket in a private `0700` directory under the
system temp directory, then starts one parent:

`{python} -m cauli._exec --app {module:attr} --fork-server --connect {socket_path} --child-threads {M}`

- The parent imports the app, runs `gc.collect()` and `gc.freeze()`, prints
  `{"server": true, "pid": P}`, and serves its stdin and stdout: request
  `{"cmd": "fork"}`, reply `{"forked": <pid>}` or `{"error": "..."}`. It reaps
  children, exits 0 on stdin EOF, and sets `PR_SET_PDEATHSIG`.
- Each child re arms `PR_SET_PDEATHSIG`, connects to the socket, sends
  `{"ready": true, "pid": N, "concurrency": M}`, then serves requests. Up to
  `M` requests are in flight; responses may arrive out of order and are matched
  by `id`. The worker clamps `M` to `--cpu-child-threads`. Socket EOF exits the
  child.
- The worker checks every connection's uid with `SO_PEERCRED` and tracks the
  kernel reported pid.
- The worker keeps `--cpu-workers` children by requesting forks. On a hard
  timeout it SIGKILLs the child, fails the timed out request
  `TimeLimitExceeded` and the child's other requests `WorkerLost`, and requests
  a replacement. A dead child's pid is never signalled again. A child that
  stops reading its socket past a bounded write timeout is killed the same way.
- A fork refusal from a healthy parent is retried with backoff. A broken
  control channel respawns the parent and retries the same fork.
- `--cpu-max-tasks-per-child` (default 10000, 0 disables) recycles a child
  after that many tasks; staged requests drain first.
- The socket and its directory are removed on every exit path.
- stderr of parent and children passes through to the worker log.

#### Stdio mode (fallback)

Used with `--no-fork-server`, or when fork server startup fails (bind, spawn,
or no `{"server": ...}` line within the handshake timeout).

`{python} -m cauli._exec --app {module:attr}` per child. The child imports the
app, prints `{"ready": true, "pid": N}`, then reads one request at a time from
stdin and answers on stdout. Soft timeout by `SIGALRM`. Hard timeout or death:
the worker kills and respawns the child, which re imports the app.

## 6. Python public API (`py/`)

Package `cauli`, Python >= 3.10, dependencies `redis>=5.0.1`, `msgspec>=0.18`,
`tzdata`, and `cauli-worker` behind the §13.4 marker.

```python
from cauli import Cauli, AsyncCauli, Retry, SoftTimeLimitExceeded, TaskFailedError, crontab, interval

app = Cauli(
    redis_url=None,          # None: env CAULI_REDIS_URL, else redis://localhost:6379/0
    default_queue="default",
    result_ttl=3600,         # seconds
    idemp_ttl=86400,         # seconds
    task_routes=None,        # §9.3
    queue_ttl=None,          # §9.2
    redis_client=None,       # a redis.Redis or a zero argument factory
    max_envelope_bytes=1048576,
)

@app.task(
    name=None,               # default f"{fn.__module__}.{fn.__qualname__}"
    kind=None,               # None means "io"
    queue=None,              # default app.default_queue
    max_retries=3,
    timeout=300.0,           # seconds
    soft_timeout=None,       # seconds
    backoff_base=0.5,        # seconds
    backoff_factor=2.0,
    backoff_max=60.0,        # seconds
    jitter=True,
    store_result=True,
)
def send_email(to: str): ...

r = send_email.delay("a@b.com")
r = send_email.apply_async(args=(), kwargs=None, countdown=None, queue=None,
                           idempotency_key=None, eta=None, expires=None)
r.id
r.status        # "pending" | "success" | "failure" | "duplicate" | "expired"
r.status()      # the same value, one GET either way
r.get(timeout=None, poll_interval=0.05)
r.duplicate     # True once a "duplicate" was read
r.claimant_id   # the claiming task's id, or None
r.claimant()    # AsyncResult for the claimant, or None

app.enqueue_many([task, (task, args), (task, args, kwargs, options)])
```

`delay` and `apply_async` raise `TypeError` when the arguments do not bind to
the task's signature. `result_ttl` and `idemp_ttl` MUST be > 0 (`ValueError`).
The worker never reads `redis_client`; it connects by URL (§7).

`get()` polls from 1 ms, doubling up to `poll_interval`. It returns the result
on success; raises `TaskFailedError(type, message,
traceback, origin)` on failure; returns `None` on duplicate; sets `.expired`
and raises `TaskFailedError(type="Expired")` on expired; raises the builtin
`TimeoutError` when its own timeout passes while pending.

Rules:

- `eta` and `countdown` are exclusive (`ValueError`). A naive datetime for
  `eta` or `expires` is rejected. A past `eta` publishes to the stream.
- `enqueue_many` validates and encodes every call before the first write,
  then sends one pipeline. It is not a transaction.
- A task object is directly callable and runs inline, with no broker.
- `AsyncCauli` takes the same options plus `async_redis_client`, and adds
  `adelay`, `aapply_async`, `aenqueue_many`, `aget` and `astatus`. Envelopes
  are identical on both paths.

Periodic schedules (§10), declared in code and synced by `cauli-beat`:

```python
app.add_periodic_task(
    "nightly-report",           # entry name
    report_task,                # a TaskDef or a task name
    crontab(minute=0, hour=3, timezone="Europe/Berlin"),
    args=(), kwargs=None,
    queue=None,                 # §9.3 rule 1
    expires=None,               # seconds
    idempotent=False,           # idempotency_key = "beat:{name}:{slot}"
    enabled=True,
    on_missed="fire_once",      # or "skip" (§10.4)
    max_lateness=None,          # seconds
)
interval(30.0)
crontab(minute="*/5", hour="9-17", day_of_week="mon-fri", timezone="UTC")
```

Hooks (§4.8): `app.before_task(fn)`, `app.after_task(fn)`,
`app.process_init(fn)`, each usable as a decorator and returning `fn`.

Django helpers on every task, importing Django lazily (`RuntimeError` without
it):

```python
send_email.delay_on_commit(*args, **kwargs)                       # -> None
send_email.apply_async_on_commit(args=(), kwargs=None, countdown=None,
                                 queue=None, idempotency_key=None,
                                 using=None, eta=None, expires=None)  # -> None
```

Both defer the publish to `transaction.on_commit` and publish nothing on
rollback; outside an atomic block they publish at once. Arguments are
validated at the call site. `cauli.contrib.django` provides `django_app()`,
`install_db_hooks(app)` and `autodiscover_tasks(app)`.

Worker introspection contract. The worker reads exactly these attributes:

- `app._tasks: dict[str, TaskDef]`
- `TaskDef`: `name: str`, `fn`, `is_async: bool`, `kind: str`, `queue: str | None`,
  `max_retries: int`, `timeout_ms: int`, `soft_timeout_ms: int | None`,
  `backoff_base_ms: int`, `backoff_factor: float`, `backoff_max_ms: int`,
  `jitter: bool`, `store_result: bool`
- `app.redis_url: str`, `app.default_queue: str`, `app.result_ttl: int`,
  `app.idemp_ttl: int`
- `app.queue_ttl: dict[str, float]`, seconds, `"*"` = fallback, absent = none
- `app._before_task_hooks`, `app._after_task_hooks`, `app._process_init_hooks`:
  `list[callable]`, absent = empty
- `cauli.Retry(countdown: float | None)`, `cauli.SoftTimeLimitExceeded`

## 7. Worker CLI (`cauli-worker` binary)

Flags and defaults: [docs/configuration.md](docs/configuration.md), or
`cauli-worker --help`.

- `--app module:attr`. The working directory is prepended to `sys.path`, so
  running in an untrusted directory can import untrusted modules.
- Redis URL precedence: `--redis-url` > env `CAULI_REDIS_URL` > `app.redis_url`.
  The URL is logged with credentials redacted.
- `--queues` defaults to `app.default_queue`.
- `--batch`, `--visibility-timeout` and `--max-envelope-bytes` MUST be >= 1;
  the worker exits 1 otherwise.
- Exit codes: 0 graceful, 1 config or startup error (including usage
  errors), 87 wedged event loop, 88 drain timeout with tasks left, 101 panic,
  130 forced. Under `--procs` above 1 the supervisor exits 1 if any process
  exited nonzero during shutdown.

**Stats line.** Every `--stats-interval` seconds, one line:

```
stats: pid=N host=S fetched=N ok=N failed=N retried=N dlq=N expired=N duplicate=N
       cpu_lost=N inflight_io=N inflight_cpu=N rss_mb=N sync_p50=N sync_p99=N async_p50=N
       async_p99=N cpu_p50=N cpu_p99=N oldest_ms=N cpu_rss_mb=N sync_live=N
       sync_abandoned=N async_rejected=N cpu_backlog=N loop_lag_ms=N
```

One physical line, wrapped here. The format is a stable contract:

- Space separated `key=value` pairs after `stats: `. `pid` and `host` lead.
- `host` is the only non numeric value: the hostname with every byte other
  than ASCII letters, digits, `.`, `-` and `_` replaced by `_`, or `unknown`.
- Every other value is a decimal integer. `inflight_io` and `inflight_cpu` can
  be negative (an accounting bug left visible).
- Counters are cumulative, gauges instantaneous, latency keys cover the
  interval just ended.
- A minor release MAY add a key. Renaming or removing one is a major version
  change.
- The line logged once at shutdown carries the keys up to and including
  `cpu_p99`.
- With `--procs` above 1, every process logs its own line.

| Key | Kind | Meaning |
|---|---|---|
| `fetched` | counter | entries read |
| `ok` | counter | successes |
| `failed` | counter | final failures |
| `retried` | counter | retries scheduled |
| `dlq` | counter | every DLQ write |
| `expired` | counter | the §9.1 subset of `dlq` |
| `duplicate` | counter | executions suppressed by §4.5 |
| `cpu_lost` | counter | cpu children that died mid task |
| `sync_abandoned` | counter | sync threads abandoned at a hard timeout |
| `async_rejected` | counter | async submissions rejected at the shim's per loop queue cap; nonzero means a wedged loop |
| `inflight_io`, `inflight_cpu` | gauge | tasks executing |
| `rss_mb` | gauge | this worker process only |
| `cpu_rss_mb` | gauge | summed over live cpu children |
| `oldest_ms` | gauge | age of the oldest outstanding entry: the older of the oldest pending entry and the oldest undelivered one, from stream ids. Keeps reporting while fetching is paused |
| `sync_live` | gauge | sync pool threads, replacements included |
| `cpu_backlog` | gauge | dispatches parked on a full cpu backlog. Nonzero pauses fetching for every lane; entering and leaving that state is logged |
| `loop_lag_ms` | gauge | the largest time any embedded loop took to run a callback stamped through `call_soon_threadsafe` |
| `sync_p50` ... `cpu_p99` | interval | per lane task latency in ms, from a 24 bucket log2 histogram, interpolated in the bucket. Resolution 2x |

**Event loop wedge, exit 87.** The worker stamps every loop every 5 seconds. A
loop is wedged when a stamp has gone unanswered for 15 seconds and a second
signal agrees: work outstanding on that loop with nothing completed over that
window, or `async_rejected` rising. The worker logs `wedged async event loop
confirmed` with the loop index and lag, and exits 87. In flight tasks are
redelivered under §4.4. Run the worker under a supervisor (systemd,
Kubernetes, `--procs`).

**Emptied broker.** On NOGROUP the worker logs an error starting
`redis has no consumer group`, naming what was lost (the PEL and the delayed
set), recreates the groups and resumes. The line appears once per reset.

### 7.1 Scheduler CLI (`cauli-beat`, Python entry point)

```
cauli-beat --app myproj.tasks:app [--redis-url URL] [--lock-ttl 30] [--max-interval 5]
           [--instance-id ID] [--no-lock] [--once] [--log-level info]
```

Same as `python -m cauli.beat`.

| Flag | Default | Meaning |
|---|---|---|
| `--app`, `--redis-url` | | as for the worker, same precedence |
| `--lock-ttl` | 30 | lease seconds. Failover after a leader crash takes between `2/3 * lock_ttl` and `lock_ttl` |
| `--max-interval` | 5 | longest sleep between ticks, and how fast an edited schedule is picked up |
| `--instance-id` | `host:pid:rand` | lease holder id |
| `--no-lock` | off | tick without the lease; still once per slot (§10.5) |
| `--once` | off | one tick, then exit. Takes and releases the lease |
| `--log-level` | `info` | |

SIGTERM or SIGINT releases the lease and exits 0. A fatal config or import
error exits 1, and so does `--redis-url` combined with an app that sets
`redis_client`. `--once` while another instance holds the lease does nothing
and exits 0.

## 8. Result / error JSON

`cauli:result:{id}`:

```json
{"status": "success", "result": <json>, "error": null, "finished_at": 123}
{"status": "failure", "result": null, "error": {"type": "ValueError", "message": "...", "traceback": "...", "origin": "task"}, "finished_at": 123}
{"status": "duplicate", "result": null, "error": null, "finished_at": 123, "claimant_id": "..."}
{"status": "expired", "result": null, "error": {"type": "Expired", "message": "...", "traceback": "", "origin": "worker"}, "finished_at": 123}
```

- Clients MUST ignore unknown fields in the document and in `error`.
- `expired` is its own status: the task never ran. Its error has type
  `Expired` and a message naming the deadline and the lateness.
- An entry dead lettered without executing (malformed, unregistered,
  redelivery limit) writes a `failure` result with `error.type` `Malformed`,
  `UnregisteredTask` or `RedeliveryLimitExceeded`, whenever its id is
  recoverable: readable from the entry (only its first 4096 bytes when it is
  oversize) and passing the §2 gate. Otherwise no result exists.
- `traceback` MAY be truncated to 8 KB.
- A result that is not JSON serializable is a failure with type
  `SerializationError`, not retried.
- `WorkerShimError`: a failure at the embedded shim boundary, retryable.
  `UnknownError`: an executor reported failure with no error object,
  retryable. `ProtocolError`: a cpu child could not decode a request line; it
  carries no id and is only logged.
- Results, tracebacks and DLQ entries are stored in plaintext. Redis is
  trusted.

### 8.1 `error.origin`

| Value | Meaning |
|---|---|
| `"worker"` | cauli synthesized the error; no user code raised |
| `"task"` | an exception propagated out of user code; `type` is its class name |
| `"client"` | synthesized by the client, never on the wire. Today only `InvalidResult`: a result document that is not JSON, not an object, or has no usable `status` |

- A propagated `SoftTimeLimitExceeded` has origin `"task"`.
- A document without `origin` comes from an older worker: treat it as
  unknown. `TaskFailedError.origin` is `None` then.

### 8.2 A worker enforced time limit is `"TimeLimitExceeded"`

| What happened | Surfaces as |
|---|---|
| the caller stopped waiting | builtin `TimeoutError` from `.get(timeout=)`, never written to Redis |
| the worker stopped the task at its limit | `TaskFailedError(type="TimeLimitExceeded", origin="worker")` |
| the task raised `TimeoutError` itself | `TaskFailedError(type="TimeoutError", origin="task")` |

On the async lane a `TimeoutError` raised before the task's deadline is the
task's own; one at or after the deadline is `TimeLimitExceeded`.

### 8.3 Did the task ever run?

| `error.type` | Ran? |
|---|---|
| `Malformed`, `UnregisteredTask`, `Expired` | never |
| `SerializationError` | ran to completion, result lost. On the io lanes a message naming the ARGUMENTS means it never ran |
| anything else, including every `origin: "task"` type | side effects unknown |

## 9. Scheduling controls: expiry, queue TTL, routing, priorities

### 9.1 Task expiration

A task whose deadline has passed when it reaches dispatch is discarded, not
executed. The check runs at dispatch only, after the task name resolves and
before the §4.5 claim, so it covers fresh delivery, the mover, retries and
reclaims, and an expired task never takes an idempotency key.

Outcome: `XADD cauli:dlq:{queue} * e {envelope} reason "expired" error ""`, an
`"expired"` result if `store_result`, ack. No retry, no hooks.

### 9.2 Queue TTL

`app.queue_ttl` maps queue name to seconds; `"*"` is the fallback. The
constructor also accepts a number, meaning `{"*": n}`.

- Client: when the call gave no `expires`, `expires_at = enqueued_at + ttl`.
- Worker: deadline = `min(expires_at, enqueued_at + ttl)`. The earlier wins
  both ways. Skipped when `enqueued_at` is 0. The addition saturates.
- The TTL is measured from enqueue, not from the due time. A client MUST
  refuse at enqueue when its computed fire time is later than the deadline it
  stamped (the Python client raises `ValueError`); otherwise the task waits
  out its delay and is discarded unrun.

### 9.3 Queue routing rules

`app.task_routes` is an ordered list of `(glob, destination)` rules, applied by
the client at enqueue. The constructor accepts a mapping (insertion order), a
sequence of pairs, or pairs mixed with callables. A destination is a queue
name, `{"queue": ...}`, or a callable `(task_name, args, kwargs) -> str | dict | None`;
`None` falls through to the next rule. Globs match the full task name with
`fnmatch`, case sensitive. The first match wins.

Queue precedence, highest first:

1. per call `queue=` (or a beat entry's `queue`)
2. `app.task_routes`
3. the decorator's `queue=`
4. `app.default_queue`

The worker never re routes; the envelope's `queue` records the decision.

### 9.4 Priorities: not supported, by decision

There are no priority levels. Use queue order within one worker
(`--queues high,default,bulk`, §4) or separate worker fleets per queue.

## 10. Periodic scheduling (`cauli-beat`)

`cauli-beat` publishes ordinary §2 envelopes on interval and crontab
schedules. The worker knows nothing about it. Several replicas are safe.

### 10.1 Redis key layout

| Key | Type | Owner | Contents |
|---|---|---|---|
| `cauli:beat:schedule` | Hash | operator or app | entry name to entry JSON (§10.2): the definition |
| `cauli:beat:due` | ZSET | beat | entry name, score = next slot (epoch ms) |
| `cauli:beat:rev` | Hash | beat | entry name to definition fingerprint; a change reseeds the slot |
| `cauli:beat:state` | Hash | beat | entry name to last firing: `last_slot`, `fired_at`, `lateness_ms`, `status` (`"fired"` or `"skipped"`), `task_id`, `next_slot`, `instance` |
| `cauli:beat:runs` | Hash | beat | entry name to run count (`HINCRBY`) |
| `cauli:beat:lock` | String | beat | lease: holder's instance id, `SET NX PX lock_ttl` |

Only `cauli:beat:schedule` is meant to be edited by hand or by an admin view.

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

`schedule` is `{"type": "interval", "every_ms": N}` or
`{"type": "crontab", "minute": "0", "hour": "3", "day_of_month": "*", "month": "*", "day_of_week": "*", "timezone": "Europe/Berlin"}`.

| Field | Rule |
|---|---|
| `name` | identity and hash field. Renaming creates a new entry and orphans the old one |
| `queue` | as a per call `queue=` (§9.3 rule 1); null lets routes and the task's queue apply |
| `expires` | seconds; becomes the envelope's `expires_at` |
| `idempotent` | true adds `idempotency_key = "beat:{name}:{slot}"`, one `cauli:idemp:*` key per firing |
| `on_missed`, `max_lateness` | §10.4 |
| `source` | `"code"` for `add_periodic_task` entries; anything else is never touched by reconciliation (§10.3) |

Crontab follows `cron(8)`:

- `day_of_month` and `day_of_week` are ORed when both are restricted (a field
  is restricted unless it is exactly `*`).
- `day_of_week` is 0 to 6, 0 = Sunday; 7 is also Sunday.
- Fields accept `*`, `*/step`, `a`, `a-b`, `a-b/step`, `a/step` (from `a` to
  the top of the range), comma lists, and three letter month and weekday
  names.
- `timezone` is an IANA name, default `"UTC"`.

DST: `next_after(slot)` returns the earliest instant strictly after `slot`.

- Fall back: a fixed wall time fires at its first occurrence only. A minute or
  hour field starting with `*` also fires in the repeated hour.
- Spring forward: a nonexistent wall time resolves with the pre transition
  offset (02:30 fires at what 02:30 standard time would be) and fires once.
- On a day whose offset changes, the whole day is scanned and the earliest
  instant wins, not the first matching wall time.

### 10.3 Reconciliation between code and Redis

At startup and on every lease acquisition, entries declared in code are
upserted with `source: "code"`, and stored `source: "code"` entries absent from
code are deleted. Entries with any other source are never touched. It runs only
under the lease.

### 10.4 The tick, and what happens after downtime

Per tick (the leader sleeps until the next slot, capped by `--max-interval` and
its lease refresh):

1. `now` = Redis `TIME`.
2. Read all definitions, slots and revs in one round trip.
3. Seed entries with no slot or a changed rev at `next_after(now)` (a Lua CAS).
   Remove slots of disabled or deleted entries.
4. `ZRANGEBYSCORE cauli:beat:due -inf now`, at most 500 entries per tick.
5. For each due entry at slot `S`: `S' = advance_past(S, now)`, build the
   envelope, claim and publish atomically (§10.5).

`advance_past(S, now)` is the first slot strictly after both `S` and `now`:
missed slots are coalesced into one firing, never replayed. Each drop is
logged at WARNING: a coalesced firing logs how many slots it swallowed, a
removed slot logs why, and a tick at the 500 cap logs the deferral.

| `on_missed` | `max_lateness` | A slot `L` late |
|---|---|---|
| `"fire_once"` (default) | any | fires |
| `"skip"` | `null` | fires |
| `"skip"` | `S` seconds | fires if `L <= S`, else skipped |

`L` is measured on the newest coalesced slot. The slot advances past `now` in
every case. A skip is recorded in `cauli:beat:state` with `status: "skipped"`
and logged at WARNING.

### 10.5 Exactly once per slot, across replicas

The lease is `SET cauli:beat:lock {id} NX PX lock_ttl`, refreshed every
`lock_ttl / 3` by a script that runs `PEXPIRE` only if the holder is still this
instance. Only the holder can refresh or release it. A refresh that finds
another holder steps down at once; a failed Redis call does not count as a
loss and is re verified on the next pass.

Firing is a compare and set on the slot, inside one script that also
publishes:

```lua
-- KEYS[1]=cauli:beat:due  KEYS[2]=cauli:beat:state  KEYS[3]=cauli:beat:runs
-- KEYS[4]=target key (cauli:q:{queue} or cauli:delayed:{queue})
-- ARGV: 1=name 2=expected_slot 3=next_slot 4=mode 5=envelope 6=delayed_score 7=state_json
local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
if cur == false then return 0 end
if tonumber(cur) ~= tonumber(ARGV[2]) then return 0 end
if ARGV[4] == 'stream' then
  redis.call('XADD', KEYS[4], '*', 'e', ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
elseif ARGV[4] == 'delayed' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[7])
return 1
```

- `mode = 'none'` advances without publishing (a skip).
- The publish MUST come before the slot advance: a failure part way can then
  only republish on a later tick, never consume a slot with nothing sent.
- Safety rests on the CAS, not the lease. Two instances that both think they
  lead race the same CAS; one gets 0. The comparison is on the expected slot
  `S`, so replicas need not agree on `S'`.
- "Now" is Redis `TIME`, so replica clock skew cannot fire early or late.
- A leader that dies mid tick leaves each slot either fired and advanced or
  untouched; the next leader fires the rest, late but once.
- After a leader dies, failover takes between `2/3 * lock_ttl` and `lock_ttl`;
  slots in that window coalesce into one firing.
- Per Redis dataset only: a failover that loses the slot advance fires `S`
  again (§4).

Redis Cluster is not supported. The worker's Redis client does not follow
MOVED, and the beat scripts span keys in different slots, so every invocation
fails with CROSSSLOT. Beat raises a named error on every tick and never fires.
Standalone and Sentinel are unaffected.

### 10.6 What a non-Redis broker would have to provide

Beat talks to a `ScheduleStore` (`cauli/beat.py`). A backend needs:

1. An atomic compare and set on a per entry value, bundled with the publish.
   Required for correctness.
2. A lease with a TTL that only its holder can refresh or release. Required
   only for efficiency.

### 10.7 Why a separate entrypoint rather than a worker flag

See [docs/architecture.md](docs/architecture.md#design-decisions). `cauli-beat`
is a Python console script; `python -m cauli.beat` is the same.

## 11. Non-goals for v1

Chains, groups, chords, canvas; priority levels (§9.4); rate limits; an events
bus; brokers and result backends other than Redis; Redis Cluster; a Windows or
macOS worker (the client and `cauli-beat` are cross platform); a schedule admin
UI (§10.1 is laid out for one).

## 12. Building and testing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## 13. Distribution

| Package | Contents | Wheel tag |
|---|---|---|
| `cauli` | the pure Python client and `cauli-beat` | `py3-none-any` |
| `cauli-worker` | the Rust worker binary | `cp3XX-cp3XX-manylinux_2_34_{x86_64,aarch64}`, plus `cp314t` |

`pip install cauli` installs both on a supported platform. Raw binaries are
also attached to each GitHub release as
`cauli-worker-<version>-cp3XX-cp3XX-<platform>.tar.gz`. A raw binary has no
launcher (§13.1): set `LD_LIBRARY_PATH` and `VIRTUAL_ENV` yourself.

### 13.1 Why the worker ships one wheel per CPython version

The binary links `libpython3.X.so.1.0` and runs only against that minor
version. Requirements: Linux on x86_64 or aarch64, glibc 2.35 or newer (the tag
says 2.34, the newest symbol version used), and a CPython built with
`--enable-shared` (python.org, Docker `python:*`, distro packages, uv,
`actions/setup-python`; pyenv only with `PYTHON_CONFIGURE_OPTS="--enable-shared"`;
conda is untested). A source build lifts the glibc floor but not Linux: the worker
calls `PR_SET_PDEATHSIG` unconditionally.

The wheel's `cauli-worker` is a launcher that adds the running interpreter's
libpython directory to `LD_LIBRARY_PATH`, sets `VIRTUAL_ENV` from `sys.prefix`
when unset, and execs `cauli-worker-bin`. The worker embeds the interpreter of
the venv it is installed in.

### 13.2 The two things a release is gated on

On every supported CPython version, before either package is uploaded:

1. The worker in the built wheel links libpython dynamically (`readelf -d`).
2. The built wheels, installed by file path into a clean venv, pass
   `itest/test_integration.py` against a real Redis.

### 13.3 Version lockstep

`cauli-worker` depends on `cauli==<version>` and `cauli` on
`cauli-worker==<version>` (behind the §13.4 marker). A mismatched pair fails to
resolve. Six places carry the version: `worker/Cargo.toml`, the `cauli==` pin
in `worker/pyproject.toml`, the `cauli-worker==` pin and `[project].version` in
`py/pyproject.toml`, `py/cauli/__init__.py`, and the README's Status section.
`scripts/check_versions.py` asserts they agree, and match the git tag when
given one, on every push. Pushing a `vX.Y.Z` tag runs
`.github/workflows/release.yml`, which publishes through PyPI trusted
publishing.

### 13.4 One install command, and the way out of it

```
cauli-worker==<version>; sys_platform == 'linux'
  and platform_python_implementation == 'CPython'
  and (platform_machine == 'x86_64' or platform_machine == 'aarch64')
  and python_version < '3.15'
```

- Off this marker the requirement disappears and the client installs alone.
  The `python_version` bound MUST be raised whenever the release matrix gains
  a version.
- musl (Alpine) and free threaded 3.13t satisfy the marker but have no wheel,
  so pip fails with `ResolutionImpossible`. Use the enqueue only install or a
  source build.
- Enqueue only install, for hosts that never run a worker:

```
pip install --no-deps cauli 'redis>=5.0.1' 'msgspec>=0.18' tzdata
```

Keep the names in that command in step with `[project].dependencies` in
`py/pyproject.toml`. `pip check` then reports the missing `cauli-worker`, which
is accurate.
