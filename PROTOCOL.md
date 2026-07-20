# rupy protocol v1

This document is the LAW. The Rust worker (`worker/`), the Python package (`py/`), and the
benchmark harness (`bench/`) are built by separate people against this spec. Do not deviate.
If something here is impossible to implement, implement the closest safe behavior and flag the
deviation loudly in your final report.

rupy is a Rust background worker runtime for Python task queues. One Rust OS process executes
many Python tasks concurrently:

- **io tasks** run inside ONE embedded CPython interpreter: `async def` tasks on embedded
  asyncio event loop thread(s); sync io tasks on a Python thread pool (the GIL is released
  during blocking I/O by CPython itself).
- **cpu tasks** run on a small pool of child Python processes (`python3 -m rupy._exec`),
  sized to CPU cores, fed over a line delimited JSON pipe protocol.

Broker and result backend: Redis >= 7.0. At least once delivery via Redis Streams consumer
groups. All timestamps are integer unix epoch **milliseconds** unless stated otherwise.

---

## 1. Redis key layout

All keys use prefix `rupy:`. `{queue}` is a queue name matching `[a-zA-Z0-9_.-]+`.

| Key | Type | Purpose |
|---|---|---|
| `rupy:q:{queue}` | Stream | Ready tasks. Each entry has exactly one field `e` whose value is the envelope JSON (UTF-8). |
| `rupy:delayed:{queue}` | ZSET | Delayed/retrying tasks. member = envelope JSON string, score = fire_at epoch ms. |
| `rupy:dlq:{queue}` | Stream | Dead letters. Fields: `e` = envelope JSON, `reason` = string, `error` = error JSON (see §8) or empty string. |
| `rupy:result:{task_id}` | String | Result JSON (see §8), `SET ... EX result_ttl`. |
| `rupy:idemp:{key}` | String | Idempotency guard. Value = task id that claimed it. `SET NX EX idemp_ttl`. |

Consumer group: name `rupy`, created per queue stream with
`XGROUP CREATE rupy:q:{queue} rupy 0 MKSTREAM` (ignore BUSYGROUP error).
Consumer name: `{hostname}:{pid}:{n}` (any unique string is acceptable).

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
- `idempotency_key`: null or string.
- `not_before`: null normally. When the client enqueues with `countdown`, the client does NOT
  XADD; it ZADDs the envelope to `rupy:delayed:{queue}` with score = now + countdown*1000 and
  sets `not_before` to that score.

## 3. Client enqueue rules (Python package)

1. Build envelope. `enqueued_at` = now ms.
2. If countdown given: `ZADD rupy:delayed:{queue} score envelope_json`. Done.
3. Else: `XADD rupy:q:{queue} * e envelope_json`.
4. Return `AsyncResult(id)`.

No client-side idempotency check (the worker enforces it at execution time).

## 4. Worker delivery loop

- One consumer group read loop:
  `XREADGROUP GROUP rupy {consumer} COUNT {batch} BLOCK 1000 STREAMS rupy:q:{q1} rupy:q:{q2} ... > > ...`
  Batch default 16. Only fetch when free execution slots exist (per class admission below;
  a simple global gate on io slots is acceptable but do not let cpu backlog starve io fetch
  indefinitely: bound in-worker cpu backlog to `2 * cpu_workers` pending items).
- Parse envelope from field `e`. Malformed JSON: XACK + XADD to DLQ with reason
  `"malformed"` (best effort raw payload in `e`), continue.
- Unknown task name (not in registry): DLQ with reason `"unregistered"`, XACK. No retry.
- Route by registry kind (fallback envelope kind): io/async, io/sync, cpu.

### 4.1 Completion

- Success: if `store_result`: `SET rupy:result:{id} {result json} EX result_ttl`.
  Then `XACK rupy:q:{queue} rupy {stream_id}` and `XDEL rupy:q:{queue} {stream_id}`.
- Failure (Python exception, timeout, worker-side error): see retry policy.

### 4.2 Retry policy

On failure with `retries < max_retries`:

1. `retries += 1` in the envelope.
2. attempt = new `retries` value (1-based).
   `d_ms = min(backoff_max_ms, backoff_base_ms * backoff_factor^(attempt-1))`.
   If `jitter`: `d_ms = uniform(0.5 * d_ms, d_ms)`.
3. `ZADD rupy:delayed:{queue} (now + d_ms) new_envelope_json`.
4. XACK + XDEL the delivered entry. Do NOT write a result key (task is still pending).

On failure with `retries >= max_retries` (final):
1. `XADD rupy:dlq:{queue} * e envelope_json reason "max_retries" error {error json}`.
2. If `store_result`: `SET rupy:result:{id} {failure result json} EX result_ttl`.
3. XACK + XDEL.

A task may raise `rupy.Retry(countdown=X)` to force a retry with an explicit delay
(still bounded by max_retries; the forced countdown replaces the computed backoff).
The worker recognizes it by exception type name `Retry` from module `rupy` (worker matches
on the exception class exposed as `rupy.Retry`) and reads its `.countdown` float seconds
attribute (may be None → use computed backoff).

### 4.3 Delayed mover

Every 250ms per queue, atomically move due members (Lua script, single EVAL):

```lua
-- KEYS[1]=rupy:delayed:{queue}  KEYS[2]=rupy:q:{queue}  ARGV[1]=now_ms  ARGV[2]=limit
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

1. `XPENDING rupy:q:{queue} rupy IDLE {visibility_timeout_ms} - + {batch}` (extended form,
   returns entry id, consumer, idle ms, delivery_count).
2. For each entry: if `delivery_count > redelivery_limit` (default `max(3, max_retries+1)`,
   computed per envelope after claim; use 3 if envelope unreadable):
   claim it (`XCLAIM ... JUSTID` acceptable), DLQ with reason `"redelivery_limit"`, XACK+XDEL.
3. Else `XCLAIM rupy:q:{queue} rupy {consumer} {visibility_timeout_ms} {id}` and execute it
   normally (same code path as a fresh delivery; do not increment `retries` for a claim).

This makes a SIGKILLed worker's in flight tasks run again elsewhere: at least once semantics.

### 4.5 Idempotency guard

At execution start, if `idempotency_key` is not null:

- `SET rupy:idemp:{key} {task_id} NX EX idemp_ttl`.
- If SET succeeded → execute normally.
- If SET failed (key exists) → do NOT execute. If `store_result`: write result JSON with
  status `"duplicate"` (result null). XACK + XDEL. This is dedup within idemp_ttl, best effort
  by design (at least once broker; the guard is claimed at start, so a crash mid-task will NOT
  re-execute a claimed key on redelivery — redelivered claimed tasks resolve as duplicate).

### 4.6 Timeouts

- async io task: wrapped in `asyncio.wait_for(coro, effective_s)` where
  `effective_s = min(soft_timeout_ms or timeout_ms, timeout_ms) / 1000`. Timeout → failure
  (retryable) with error type `"TimeoutError"`.
- sync io task (thread): soft timeout only, via `PyThreadState_SetAsyncExc` injecting
  `rupy.SoftTimeLimitExceeded` after `soft_timeout_ms` (if set). Hard timeout for threads
  cannot kill the thread: after `timeout_ms` the worker marks the task failed (retry path)
  and abandons the thread result (log a warning). Document this limitation.
- cpu task: soft timeout enforced inside the child via SIGALRM raising
  `rupy.SoftTimeLimitExceeded`; hard timeout enforced by the worker: SIGKILL the child,
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

### 5.1 cpu child protocol (`python3 -m rupy._exec`)

Spawn: `{python} -m rupy._exec --app {module:attr}`. Child imports the app, prints exactly one
ready line on stdout: `{"ready": true, "pid": 1234}\n`, then reads requests line by line from
stdin and answers one line per request on stdout. stderr is passthrough logging.

Request:  `{"id": "...", "task": "...", "args": [...], "kwargs": {...}, "soft_timeout_ms": null}\n`
Response: `{"id": "...", "ok": true, "result": <json>}\n`
      or: `{"id": "...", "ok": false, "error": {"type": "...", "message": "...", "traceback": "..."}}\n`

One request in flight per child. Non JSON serializable result → treated as error
`{"type": "SerializationError", ...}`. Child must never crash on task exceptions; it reports
them. Worker kills/respawns child on hard timeout or child death (child death = task failure,
retryable, error type `"WorkerLost"`).

## 6. Python public API (`py/`)

Package name `rupy`, pure Python, py>=3.10, only hard dependency: `redis>=5`.

```python
from rupy import Rupy, Retry, SoftTimeLimitExceeded, TaskFailedError

app = Rupy(
    redis_url="redis://localhost:6379/0",   # or env RUPY_REDIS_URL; default shown
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
- Exceptions: `rupy.Retry(countdown: float|None)` (attribute `.countdown`),
  `rupy.SoftTimeLimitExceeded`.

Decorated task objects also stay directly callable (`send_email("a@b.com")` runs the function
inline, no queue) so the same code is testable without a broker.

## 7. Worker CLI (`rupy-worker` binary)

```
rupy-worker --app myproj.tasks:app [--queues default,emails] [--redis-url URL]
            [--io-loops 1] [--io-threads 64] [--io-concurrency 256] [--cpu-workers N]
            [--batch 16] [--visibility-timeout 60] [--drain-timeout 30]
            [--python python3] [--stats-interval 10] [--log-level info]
```

- `--app`: `module:attr`. The worker adds CWD to `sys.path`, imports module, reads attr.
- `--redis-url` precedence: CLI > env `RUPY_REDIS_URL` > `app.redis_url`.
- `--queues` default: `app.default_queue`.
- `--stats-interval`: seconds between one line stats logs:
  `stats: fetched=N ok=N failed=N retried=N dlq=N inflight_io=N inflight_cpu=N rss_mb=N`.
- Exit codes: 0 graceful, 1 fatal config/startup error, 130 forced.

## 8. Result / error JSON

Result key value (`rupy:result:{id}`):

```json
{"status": "success", "result": <json>, "error": null, "finished_at": 123}
{"status": "failure", "result": null, "error": {"type": "ValueError", "message": "...", "traceback": "..."}, "finished_at": 123}
{"status": "duplicate", "result": null, "error": null, "finished_at": 123}
```

`traceback` may be truncated to 8KB. Task return values must be JSON serializable; a non
serializable success value is a failure with type `"SerializationError"` (no retry — treat as
final failure regardless of retries left).

## 9. Non-goals for v1

Chains/chords/canvas, cron/beat scheduling, priorities, rate limits, task events bus,
multi broker (Redis only), result backends other than Redis, Windows worker support
(worker targets Linux; client is cross platform).

## 10. Build/runtime environment facts (for all builders)

- Source lives on Windows at `D:\dev\projects\boring\rupy` = `/mnt/d/dev/projects/boring/rupy`
  inside WSL distro `Ubuntu-24.04` (the only distro; default user `blackdevil`, home
  `/home/blackdevil`). Run Linux commands from Windows as:
  `wsl.exe -d Ubuntu-24.04 -e bash -lc "<cmd>"`.
- Edit files ONLY with Windows-side tools on `D:\...`. Execute/build ONLY inside WSL.
- Rust 1.96, Python 3.12.3 (+python3-dev), Redis 7.0.15 at /usr/bin/redis-server, 6 cores,
  11GB RAM, cgroup v2 + systemd 255 available. Internet OK (crates.io + PyPI verified).
- ALWAYS set `CARGO_TARGET_DIR=/home/blackdevil/rupy-target` for cargo commands (building on
  /mnt/d is slow). Python venvs also go in WSL home, not /mnt/d.
- Do not use the system redis (may be shared). Start throwaway instances on your own port,
  e.g. `redis-server --port 63XX --save '' --appendonly no --daemonize yes` and shut them down.
