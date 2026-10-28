# Architecture

How `cauli-worker` is built, and the decisions not to undo by accident. Where
this file disagrees with the code or [PROTOCOL.md](../PROTOCOL.md), they win.

## Modules

| File (`worker/src/`) | Role |
|---|---|
| `main.rs`, `lib.rs` | Bootstrap: CLI, logging, Python init, Redis connect and cluster probe, loops, drain, exit |
| `wheel_main.rs` | Entry point of the binary the wheel ships as `cauli-worker-bin` |
| `cli.rs` | Flags and the `-c` derivation |
| `supervisor.rs` | `--procs`: spawn, restart, signal fan out |
| `loops.rs` | Fetch, delayed mover, recovery, trim, stats, wedge watchdog |
| `dispatch.rs` | Per entry: parse, expiry, idempotency, route, finish |
| `exec.rs` | Sync, async and cpu execution and their timeouts |
| `ctx.rs` | Shared context, executor response to outcome |
| `pyrt.rs` | pyo3 glue: interpreter, shim, sync thread pool, async bridge |
| `shim.py` | Embedded Python: app loading, `run_sync`, asyncio loops, soft timeout watchdog, hooks |
| `cpu.rs` | cpu pool: fork server and stdio children |
| `broker.rs` | Key layout, Lua scripts, completion pipeline |
| `envelope.rs` | Envelope, result JSON, expiry deadline, redelivery limit |
| `pyjson.rs` | JSON type gate for io lane results |
| `clock.rs`, `backoff.rs` | Redis anchored clock; retry delay |
| `stats.rs` | Stats line and latency histograms |
| `logsink.rs` | Non blocking log output: a full sink drops and counts lines instead of stalling the runtime |

## Threads and lanes

One worker process holds one embedded CPython and three execution lanes.

- **tokio runtime.** All Redis traffic: one fetch loop (`XREADGROUP BLOCK` over
  every queue, on its own connection so the block never stalls writes), the
  completion flushers, the delayed mover, recovery, trim, clock resampling,
  stats and signals. The GIL is never held on a tokio thread during broker IO.
- **Sync lane.** `--io-threads` OS threads spawned by Rust. Each takes the GIL
  only around `run_sync`, so blocking IO runs in parallel. The job queue holds
  up to `--io-concurrency` jobs. A job whose hard timeout already fired is
  skipped, not run late. A thread abandoned at a hard timeout is replaced.
- **Async lane.** `--io-loops` daemon threads, each running one asyncio loop
  (uvloop when importable). Rust submits through `run_coroutine_threadsafe`;
  the coroutine runs under `asyncio.wait_for` and completes into a tokio
  oneshot. Rust gives up after `timeout_ms + 2000 ms` and drops the slot.
- **CPU lane.** `--cpu-workers` child processes, forked from one preloaded
  fork server parent by default. Each child serves up to
  `--cpu-child-threads` requests over a unix socket, plus `--cpu-prefetch`
  staged ones. A staged request's hard timeout starts only when the request
  ahead of it finishes.

## Data path

1. Fetch: `XREADGROUP` with a per stream `COUNT` of
   `max(1, min(batch, free io slots) / streams)`, only while enough io permits
   are free and the cpu backlog is not full.
2. Dispatch, per entry: size gate, parse, id gate, registry lookup, expiry,
   idempotency claim, route to a lane.
3. Execute on the lane, under its timeouts.
4. Finish: the outcome's writes (result, retry `ZADD`, DLQ `XADD`) join the
   queue's completion buffer, flushed as one pipeline with one `XACK` and one
   `XDEL`. A completion counts as done only once its flush is answered.

Backpressure: the io gate is a semaphore of `--io-concurrency`. The cpu backlog
is a channel of `2 * cpu_workers * cpu_child_threads`; a full channel pauses
fetching for every lane (the lane is unknown before parsing) and logs the pause
and its duration. Children always make progress, because a hard timeout kills
them, so the pause is bounded.

## Process model

- **cpu children.** The fork server parent imports the app once, runs
  `gc.collect()` and `gc.freeze()`, and forks on request, so a respawn costs a
  fork, not an import. Parent and children arm `PR_SET_PDEATHSIG`. The socket
  lives in a private `0700` directory and every connection is checked with
  `SO_PEERCRED`; the kernel reported pid is the one tracked and killed.
- **Supervisor.** `--procs N` runs N independent worker processes, each with
  its own interpreter, pools and stats line, and restarts one after 1 s.
- **Shutdown.** First SIGTERM or SIGINT: stop fetching, keep flushers, mover and
  trim running, drain up to `--drain-timeout`. A second signal at least 1 s
  later kills the cpu pool, removes the socket and exits 130; one sooner is the
  same stop request delivered twice (systemd signals the whole group).
- **Exit codes.** 0 drained, 1 config or startup error (clap usage errors
  included), 87 wedged event loop, 88 drain timeout with tasks left, 101
  panic, 130 forced.

## Known limitations

- A sync task past its hard timeout keeps its thread until the call returns.
  Capacity is restored by a replacement thread, up to 4 times `--io-threads`.
- A blocking call inside `async def` wedges its loop for good; the process
  exits 87 and a supervisor must restart it.
- With `--io-loops` 2 or more, round robin keeps submitting to a wedged loop
  until the process exits.
- A soft timeout lands at an arbitrary bytecode boundary and can leave a half
  finished session in a thread local for the next task on that thread.

## Design decisions

Each entry: the constraint, the choice, what breaks if it is undone.

**Exit through `_exit`.** Daemon loop threads and possibly wedged sync threads
mean `Py_Finalize` can never run, so `exit()` would add only libc atexit
handlers, and `OPENSSL_cleanup` from a database driver's libssl corrupts the
heap racing those threads. The worker releases children and the socket, drains
logs for up to 1 s, then calls `libc::_exit` (`exit_now` in `lib.rs`). No Python
`atexit` handler ever runs: Sentry and OpenTelemetry must flush explicitly.

**One thread state per sync thread for life.** Creating a thread state per task
wiped `threading.local` between tasks, so Django opened a connection per task.
Each pool thread pins one state at spawn (`pyrt.rs`). Consequence: thread local
state, including a dirty one, carries over to the next task.

**Self exit on a wedged loop.** CPython cannot kill a thread. The watchdog
(`wedge_loop` in `loops.rs`) stamps each loop every 5 s and exits 87 only when
the stamp is 15 s unanswered and a second signal agrees (outstanding work with
no completions, or `async_rejected` rising), so GIL starvation under load
cannot trigger it.

**Three lanes, named by task nature.** The public surface is `kind` and
`is_async`, not a pool name, so lanes can change without a protocol change.

**Clock anchored on Redis `TIME`.** Every worker runs the mover, so the fastest
local clock would decide when all delayed work fires, and a worker ahead of a
client would drop valid work as expired. `clock.rs` samples `TIME` at startup
and every 15 to 30 s (spread by pid) and extrapolates on the monotonic clock.
Reading `TIME` per call was rejected: two serial round trips per task. Retry
delays are clamped to 30 days (`MAX_RETRY_DELAY_MS`), since a clock fault
cannot be caught by a horizon check on the same clock.

**Execution timers start at the slot, not at delivery.** Moving the timer to
delivery would charge queueing against `timeout_ms` for every queued task. The
gap is closed at the source instead: fetch never takes more than the free io
slots, and recovery requires `timeout_ms + grace` of idle time.

**Batched completion keeps the per flush `XDEL`.** Leaving removal to the trim
let one slow task pin every completed entry behind it in the stream. The trim
(`XTRIM MINID` below the oldest pending id) only reclaims torn flush orphans.
The `XACK` before `XDEL` order is required: the reverse strands entries in the
PEL.

**Lua scripts publish before they delete or advance.** Lua does not roll back
on error. The mover `XADD`s before `ZREM`, and beat publishes before advancing
the slot, so a failure part way can duplicate, never lose.

**Idempotency is an admission lease.** The claim TTL is
`max(idemp_ttl, timeout_ms + grace)` so it outlives its own execution, the same
task id renews it on retry and redelivery, and a claim is never released, even
after a dead letter, because a failed attempt may have left side effects. The
duplicate result names the claimant so the caller can find the real outcome.
The guard fails open on a Redis error.

**Bounded dead letter streams.** Each DLQ is `MAXLEN ~ 1000` and expires 7 days
after its last write (`DLQ_MAXLEN`, `DLQ_TTL_S` in `broker.rs`). Unbounded, a
long failing queue would run Redis out of memory and stop every queue.

**Expiry has one enforcement point.** It runs at dispatch, after the registry
lookup and before the idempotency claim, because every path (fresh, mover,
retry, reclaim) converges there and an expired task must not burn a key. The
client and the mover never check it.

**No priority levels.** Emulating them on Streams multiplies groups, PELs,
movers and DLQs by N and replaces the single blocking `XREADGROUP` with a poll
or a starvation prone scheduler. Use queue order or separate fleets.

**`Retry` is matched by name.** The shim may run without `cauli` importable,
and the cpu decision in Rust sees only a type name off a pipe, so identity is
impossible. All three lanes match class name `Retry` plus a `countdown`
attribute. The collision is near harmless: every exception retries anyway.

**Error taxonomy.** `error.origin` says who minted the error; the worker's
timeout is `TimeLimitExceeded`, so it never collides with the builtin
`TimeoutError` from `get(timeout=)`. Clients ignore unknown fields, which is
what lets the result document grow. Everything else in the vocabulary is
frozen.

**One stats line, no metrics endpoint.** A logfmt line is a stable contract
with no listener, port or dependency. Latency comes from 24 bucket log2
histograms of atomic counters. Keys may be added in a minor release; renaming
or removing one is a major version change.

**Redis Cluster is refused.** `cauli:q:{queue}` and `cauli:delayed:{queue}`
never share a slot, so the mover fails and a retry can be acked without its
`ZADD` landing: silent loss. The worker sends `INFO cluster` at startup and
exits 1 on `cluster_enabled:1` unless `CAULI_ALLOW_REDIS_CLUSTER` is set. Hash
tags were rejected: beat's claim script spans every queue, which needs one
global tag and removes the reason to run Cluster.

**One worker wheel per CPython minor.** The binary links
`libpython3.X.so.1.0` and has no RUNPATH. The wheel's `cauli-worker` is a
Python launcher that adds the interpreter's libpython directory to
`LD_LIBRARY_PATH`, sets `VIRTUAL_ENV` from `sys.prefix` when unset, and
`execv`s `cauli-worker-bin`, keeping the process tree that `PR_SET_PDEATHSIG`
relies on. Release CI gates on dynamic linkage and runs the integration suite
in a bare `ubuntu:24.04` container.

**Beat is a separate Python process.** Schedules must be Python for an admin
view and for `zoneinfo` DST rules, beat has no hot path, and beat wants about 2
replicas where workers want N. Exactly once per slot comes from a compare and
set on the slot inside the publish script; the lease only saves redundant
polling.
