# cauli-worker architecture (as built)

One Rust OS process (`cauli-worker`) executes many Python tasks concurrently
against Redis Streams per PROTOCOL.md. Module map:

| File | Role |
|---|---|
| `src/main.rs` | Bootstrap: CLI, tracing, Python init, redis connect, spawns all loops, §4.7 drain, exit codes |
| `src/cli.rs` | clap CLI per §7 + queue name validation |
| `src/loops.rs` | fetch loop (XREADGROUP), delayed mover (§4.3), recovery (§4.4, full-backlog drain per tick), stats |
| `src/dispatch.rs` | per-entry pipeline: parse -> expiry (§9.1) -> idempotency (§4.5) -> route -> finish (§4.1/§4.2) |
| `src/exec.rs` | sync-thread / async-loop / cpu-child execution, timeouts (§4.6) |
| `src/ctx.rs` | shared context, executor response -> Outcome normalization |
| `src/pyrt.rs` | pyo3 glue: interpreter init, shim import, sync io thread pool, async submit bridge |
| `src/shim.py` | embedded Python shim (include_str!): app loading, run_sync, asyncio loops, soft-timeout watchdog, §4.8 lifecycle hooks |
| `src/cpu.rs` | §5.1 cpu pool: fork-server (unix socket, multiplexed) + stdio fallback, kill+respawn, `CAULI_EXEC_CMD` test hook |
| `src/broker.rs` | Redis key layout, group setup, mover Lua, pipelined completion writes |
| `src/envelope.rs` | §2 envelope (unknown-field preserving), §8 result/error JSON, redelivery limit, §9.1/§9.2 expiry deadline |
| `src/backoff.rs` | §4.2 backoff math (jitter after clamp) |
| `src/stats.rs` | counters + `stats:` line, rss_mb from /proc/self/status VmRSS |

## Threading model

- **tokio multi-thread runtime** owns all broker plumbing: one fetch loop doing
  `XREADGROUP ... BLOCK 1000` over all queues (dedicated ConnectionManager so
  BLOCK never stalls writes), pipelined ack/retry/DLQ writers, 250ms delayed
  mover (single EVAL per queue), XPENDING/XCLAIM recovery every
  visibility_timeout/2, stats loop, signal task. The recovery tick drains the
  ENTIRE eligible backlog, round-robin across queues in cursor-paged
  XPENDING pages of 128 (exclusive `(id` resume so entries skipped by the H1
  per-envelope idle check are not re-read within a tick), with the envelope
  peeks (XRANGE) and claims (XCLAIM) pipelined per page and page fetch gated
  on the same admission criteria as the fetch loop. It used to reuse the
  fetch `--batch` (16) as its per-tick cap — after a kill -9 with 200 tasks
  in flight, reclaim trickled at ~32/10s (~74s to recover work that re-runs
  in ~5s); the drain design recovers the same load in one tick
  (measured 74.0s -> 15.8s total, of which ~10s is the H1 idle threshold).
- **Embedded CPython** via pyo3 auto-initialize; `prepare_freethreaded_python()`
  at startup before the runtime. The GIL is NEVER held on tokio worker threads
  during broker I/O: sync tasks run on dedicated OS threads, async submission
  happens inside `spawn_blocking`, completions arrive from Python loop threads.
- **Shim** (`src/shim.py`, imported once via `PyModule::from_code`): the pyo3
  surface is strings-in/strings-out. `load_app` duck-types (getattr only) the
  app per §6 and honors VIRTUAL_ENV site-packages; `run_sync` captures returns/
  exceptions/Retry/SerializationError as outcome JSON; `start_loops(N)` spawns
  N daemon threads each running `loop.run_forever()`; `submit_async` uses
  `run_coroutine_threadsafe` + `asyncio.wait_for(coro, effective_s)` and pushes
  completion into a Rust `PyCFunction` closure -> tokio oneshot (no polling).
  Soft timeouts for sync tasks are serviced by ONE shared watchdog thread (a
  min-heap of (deadline, tid, generation)) rather than a `threading.Timer` per
  call, so hot-path soft-timeout usage does not spawn thousands of OS threads
  per second. §4.8 lifecycle hooks are duck-read off the app at `load_app`
  (live list references) and run around every task: before hooks pre-arm /
  after hooks post-disarm on the sync path, and on the loop thread for async
  tasks (awaitable-returning hooks are awaited); a raising hook is logged and
  skipped, never failing the task. The Rust side gives up waiting on an async completion after
  `timeout_ms + grace` and drops its pending-map slot (`PyRuntime::cancel`) so
  a wedged event-loop thread that never actually finishes the coroutine
  cannot leak that bookkeeping forever (the coroutine itself, if truly
  wedged on a synchronous blocking call, cannot be recovered from Rust --
  `pending_async` in the stats line makes a growing count of these visible).
- **Sync io pool** (`--io-threads`): Rust-spawned OS threads; each takes the
  GIL only around the shim `run_sync` call; CPython releases it during blocking
  I/O, so io parallelism is real. Each pool thread pins ONE persistent CPython
  thread state at spawn (`PyGILState_Ensure` + `PyEval_SaveThread`, never
  released): without it, every `Python::attach` created and destroyed a fresh
  thread state per task, silently wiping `threading.local` between tasks —
  Django's per-thread connection cache could never hit (a new DB connection
  per task, the old one orphaned until GC), and any thread-local pattern
  ported from Celery broke the same way. Soft timeout: shim watchdog `threading.Timer`
  injecting `SoftTimeLimitExceeded` via `PyThreadState_SetAsyncExc`, fenced by
  a per-thread generation counter so a timer that fires late (after its own
  task already finished) cannot land inside a later task on the same thread.
  The job queue is a bounded crossbeam channel (capacity = `--io-concurrency`);
  a queued job whose dispatcher already hard-timed-out is skipped rather than
  executed late ("zombie execution") when a thread reaches it, and a hard
  timeout spawns a replacement thread immediately so pool capacity is
  restored even though the wedged original thread can never be killed.
- **Cpu pool** (`--cpu-workers`, `--cpu-child-threads`): fork-server mode by
  default (§5.1). ONE parent (`{python} -m cauli._exec --app {spec}
  --fork-server --connect {sock} --child-threads {M}`) imports the app once,
  `gc.collect()` + `gc.freeze()`, then forks a child per `{"cmd":"fork"}`
  control line; children connect to the worker's tokio `UnixListener`, send
  `{"ready": true, "pid", "concurrency"}` and serve the line protocol on the
  socket. The listener lives inside a private `0700` directory and every
  accepted connection is checked against the worker's own uid via
  `SO_PEERCRED` (the kernel-reported peer pid, not the client-claimed one,
  is what gets tracked/killed) before it is trusted as a real child; a
  child's advertised concurrency is clamped to the configured
  `--cpu-child-threads` so it cannot defeat the backlog bound. Per child, one
  `serve_child` task multiplexes up to `concurrency` requests (pending map
  keyed by wire id, per-request hard-timeout deadlines via a single
  earliest-deadline sleep in the select loop; handing a request to the child
  is itself bounded by a write timeout, so a child that stops draining its
  socket without dying is detected instead of silently wedging the slot).
  `--cpu-prefetch` (default 4) stages that many extra requests in each child's
  socket buffer beyond the ones it executes, so a child that finishes a task
  reads its next one immediately instead of idling for a full round trip
  (socket write, tokio wakeup, select iteration, channel recv, socket write,
  child wakeup). A staged request's hard-timeout clock does NOT start until
  the request ahead of it completes, or a queued task could be declared timed
  out having never run. Worth 4.1x at 0.5ms tasks and nothing at 51ms
  (measured); the cost is that a child death fails everything
  staged behind it as retryable WorkerLost, and staged tasks wait out the
  queue ahead of them.
  Hard timeout: SIGKILL by pid, expired requests fail "TimeLimitExceeded", the
  rest "WorkerLost" (both retryable), then a replacement fork (cheap: no
  re-import). Child death (socket EOF): all in flight "WorkerLost" +
  replacement fork; that pid is never SIGKILLed (the fork-server parent has
  already reaped it via SIGCHLD, so the pid may already be reused). A fork
  request is retried until it succeeds: a parseable refusal from a healthy
  parent (e.g. transient EAGAIN/ENOMEM) retries with backoff and never
  touches the parent; a genuine control-channel failure respawns the parent
  (its children died via PDEATHSIG) and retries the same request against the
  fresh one, rather than dropping it for the requesting slot's 60s backstop
  to notice. Fork-server startup failure (bind/spawn/handshake) falls back
  to **stdio mode** — also forced by `--no-fork-server`: spawn per child, one
  in flight over stdin/stdout, SIGKILL + full respawn on hard timeout or
  death (the pre-fork-server behavior, preserved verbatim). The parent and
  children carry `PR_SET_PDEATHSIG=SIGKILL` (children re-arm it after fork
  since fork clears it) so a SIGKILLed worker cannot leak them; remaining
  executor pids are killed on exit paths (skipping any tracked pid <= 1,
  which would otherwise signal this process's own group) and the listener
  socket file and its private directory are removed on every exit path,
  including a forced double-signal exit and a bind-succeeded-but-handshake-
  failed startup abort.
  **Test hook:** env `CAULI_EXEC_CMD` (whitespace-split argv) replaces the child
  command verbatim (fork-server flags are appended to the override argv),
  used by e2e to run `tests/fixtures/fake_exec.py`, which implements both
  modes. Compiled in only under `cfg(test)` / the `test-hooks` cargo feature
  -- a normal `cargo build --release` has no code path that reads this env
  var.

## Expiry, queue TTL, routing, priorities

- **Expiry (§9.1) is enforced in exactly one place**: `dispatch::process`, after the
  envelope parses and the registry lookup succeeds, before the idempotency
  claim. Every path into execution converges there — fresh XREADGROUP
  delivery, delayed-mover hand-off, scheduled retry, §4.4 crash reclaim — so a
  single check covers all of them. Enqueue-time and mover-time checks were both
  rejected: the client cannot know how long an entry will wait (so it would
  need a second check anyway, and two checks means two semantics), and the
  mover only sees delayed entries, not the backlogged ready ones queue TTLs
  exist for, and would have to `cjson.decode` every moved envelope inside the
  Lua script. Placing it at dispatch also means it needs nothing from the
  broker, which is what lets a future SQS/RabbitMQ backend inherit it for free.
  Before the idempotency claim, so an expired task cannot burn the key and lock
  out a later valid task carrying the same one.
  Outcome: DLQ (`reason="expired"`) + a `"expired"` result key + XACK/XDEL, and
  the broken-out `expired` stats counter.
- **Queue TTL (§9.2)** arrives as `app.queue_ttl` through the same duck-typed
  shim `load_app` config as everything else (`{queue: seconds}`, `"*"` =
  fallback), converted to ms in `main.rs` and looked up by `Ctx::queue_ttl_ms`.
  `Envelope::expiry_deadline_ms` takes the EARLIER of it and the envelope's own
  `expires_at`, so neither can be used to defeat the other. It is skipped when
  `enqueued_at == 0` (an envelope without the field would otherwise look 55
  years overdue) and saturates on add.
- **Routing (§9.3) is entirely client side.** The worker consumes the queues it
  was told to and never re-routes; the envelope's `queue` field records where
  the client decided to publish. Nothing in this binary changed for it.
- **Priorities: not implemented, deliberately** (§9.4). The only prioritization
  is that `XREADGROUP` returns per-stream entry arrays in the order the keys
  were given and `fetch_loop` iterates them in that order, so earlier
  `--queues` entries dispatch first within a batch — and cannot starve later
  ones, since `COUNT` applies per stream. N weighted sub-queues would multiply
  the consumer groups, PELs (and so the §4.4 drain), movers and DLQs, and would
  replace the single `BLOCK 1000` XREADGROUP with either a busy poll or a
  hand-written key-set scheduler, which is where starvation bugs live. It would
  also be the wrong shape to carry forward: SQS has no priorities at all and
  RabbitMQ has native `x-max-priority`.

## Periodic scheduling

Not in this binary. `cauli-beat` is a Python entry point (`py/cauli/beat.py`,
PROTOCOL §10) and the worker is unaware of it: a beat-published envelope is an
ordinary §2 envelope arriving on an ordinary stream. The reasoning for keeping
it out of the worker (schedule model must be Python for a future admin view,
`zoneinfo` for DST correctness, no throughput argument, beat and worker replica
counts differ) is PROTOCOL §10.7.

## Admission / backpressure

Global io semaphore `--io-concurrency` gates io execution. Cpu backlog is a
bounded channel of `2 * cpu_workers * cpu_child_threads` (twice the pool's
in-flight capacity); a dispatch that finds it full parks on
`send().await` and raises an overflow flag. The fetch loop only issues
XREADGROUP when io permits exist AND overflow == 0. Io fetch can therefore
pause while a cpu flood drains, but never indefinitely: cpu children always
make progress (hard-timeout SIGKILL bounds every slot), so overflow clears in
bounded time. This satisfies "bound cpu backlog to 2*cpu_workers" without
letting it wedge io fetching forever.

## Completion writes (all pipelined, §4.1/§4.2)

- success: `SET result EX ttl` (if store_result) + XACK + XDEL
- retry: retries+=1, `ZADD delayed (now+d)` (unknown envelope fields preserved)
  + XACK + XDEL, d per §4.2 (jitter = uniform(0.5d, d) after the max clamp);
  `cauli.Retry.countdown` overrides d
- final failure: `XADD dlq (e, reason="max_retries", error)` + result + XACK+XDEL
- malformed / unregistered / redelivery_limit: DLQ (error field empty) + result
  (when the id is recoverable) + XACK+XDEL
- expired (§9.1): `XADD dlq (e, reason="expired")` + an `"expired"` result
  (when store_result) + XACK+XDEL; no retry, no lifecycle hooks

Counters: ok counts successes and duplicates; failed counts final failures;
retried counts scheduled retries; dlq counts every DLQ write; expired counts
the §9.1 subset of those (also included in dlq, broken out because "the queue
cannot keep up" is a different alert from "tasks are failing").

## Known limitations / deviations (flagged)

1. **Sync hard timeout cannot kill the thread** (§4.6, documented in protocol):
   the task is failed on the retry path and the thread result abandoned; the
   ORIGINAL OS thread stays occupied until the Python call returns (which may
   be never, e.g. a blocking call with no timeout, or a C extension ignoring
   the soft-timeout injection) -- but the pool's CAPACITY does not shrink: a
   replacement thread is spawned immediately, and if the original thread ever
   does return, it just resumes serving as extra headroom. A job still
   sitting in the queue when its own dispatcher already gave up is skipped
   rather than run late.
2. **No result key** is written for a `malformed` / `unregistered` /
   `redelivery_limit` DLQ entry when the task id itself cannot be recovered
   from the envelope (invalid JSON, or an id failing the §2 charset gate):
   there is nothing to key a result on. Otherwise a `"failure"` result is
   written alongside the DLQ entry (§4 / §4.4 / §8), so a client `get()`
   gets an answer instead of waiting on a key that would never exist.
3. **Non-JSON `-m cauli._exec` responses** are treated as retryable
   WorkerShimError failures rather than crashing the pool.
4. Usage errors print via clap but exit 1 (spec: 1 = fatal config error;
   clap's default 2 is overridden). `--help`/`--version` exit 0.
5. Second signal exits 130: the cpu pool is explicitly killed (same cleanup
   as the normal exit path, including the fork-server socket file) before
   the process exits; PDEATHSIG is the backstop if that step is ever skipped
   (e.g. a SIGKILLed worker), not the primary mechanism.

## Ops quickstart

```
cauli-worker --app myproj.tasks:app --queues default,emails \
    --redis-url redis://127.0.0.1:6379/0 --cpu-workers 4 --log-level info
```

Redis URL precedence: `--redis-url` > `CAULI_REDIS_URL` > `app.redis_url` (logged with
userinfo redacted, e.g. `redis://***@host/0` -- never logs a plaintext password).
Stats line every `--stats-interval`s:
`stats: fetched=N ok=N failed=N retried=N dlq=N expired=N inflight_io=N inflight_cpu=N
rss_mb=N sync_live=N sync_abandoned=N pending_async=N`.
Queue order in `--queues` is dispatch order within a fetch batch (§9.4); there
are no priority levels.
SIGTERM/SIGINT: stop fetching, drain up to `--drain-timeout`, exit 0; second
signal exits 130. SIGKILL is safe: pending entries are re-claimed by another
worker after `--visibility-timeout` (§4.4) -- but only once idle beyond
`max(visibility_timeout, task timeout_ms + grace)`, so a legitimately
still-running long task is not reclaimed out from under itself.
