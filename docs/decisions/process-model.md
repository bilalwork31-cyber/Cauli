# Process, teardown and threading model

Status: Accepted (0.1.0)

## Context

One worker process embeds one CPython interpreter and runs three lanes: a pool
of OS threads for sync io tasks, one or more resident asyncio loops for async
tasks, and child processes for cpu tasks. Four questions about that model had
to be settled before the first release: how the process exits, what happens to
thread state in the sync pool, what happens when an async loop stops
responding, and whether three lanes is the right number.

## Decision

### 1. The process never tears down cleanly

The worker exits through `libc::_exit` (`exit_now` in `worker/src/lib.rs`), not
`std::process::exit`. Python `atexit` handlers and end of process `__del__`
only run inside `Py_Finalize`, and this process can never call `Py_Finalize`
because daemon loop threads and possibly wedged sync threads are still alive.
So the only thing `exit()` would add is libc atexit handlers, and those are the
dangerous ones: `OPENSSL_cleanup`, registered by the libssl a database driver
links, races the threads still running their own OpenSSL teardown and corrupts
the heap after every task has been acked (bench/RESULTS.md, Django ORM section).
Every resource owned outside the process (cpu children, the fork server
socket) is released explicitly first, and the log sink is drained for at most
one second.

### 2. Sync pool threads keep one thread state for life

Each sync pool thread keeps one CPython thread state for its whole life
(PROTOCOL.md section 4.8). A thread abandoned at a hard timeout is replaced, up to
4 times `--io-threads` (`SYNC_POOL_THREAD_CEILING_MULTIPLE` in
`worker/src/pyrt.rs`). `sync_live` and `sync_abandoned` report it.

### 3. A wedged async loop exits the process

A blocking call inside an `async def` starves its loop thread permanently, and
CPython offers no safe way to kill that thread. At the default `--io-loops 1`
that ends all async throughput while the process keeps fetching, fails every
async task at its full timeout, and looks healthy to any orchestrator.

The watchdog (`wedge_loop` in `worker/src/loops.rs`) stamps every loop through
`call_soon_threadsafe` every 5 seconds. A loop is wedged when its stamp has gone
unanswered for 15 seconds and a second signal agrees: work outstanding on that
loop with nothing completed over the same window, or `async_rejected` rising.
Two signals are required so that GIL starvation under load cannot cause an
exit. The process then logs the loop index and lag and exits with code 87. A
supervisor restarts it (`--procs` restarts a child after one second), and in
flight tasks are redelivered under PROTOCOL.md section 4.4. The largest lag is
reported as `loop_lag_ms` in the stats line.

### 4. Three lanes

The lanes match the three execution models Python has: blocking code needs OS
threads that release the GIL during io, coroutines need a resident loop, and
GIL bound code needs another process for both parallelism and killability. The
public surface is `kind` (`io` or `cpu`), `is_async`, and the timeout semantics
of each class. That names a task's nature, not a pool, so the worker can merge,
split or reassign lanes later without a protocol change.

## Consequences

- No Python `atexit` ever runs. Users lose sentry_sdk's exit flush, the
  OpenTelemetry `BatchSpanProcessor` final flush, cProfile and coverage dumps,
  `NamedTemporaryFile` unlinking, and the last buffered `print` output. Sentry
  and OpenTelemetry users must flush explicitly.
- Recovery is the startup path, exercised on every deploy, so there is no
  separate teardown path to go untested.
- Thread locals persist across tasks on the same sync thread. That is why
  `CONN_MAX_AGE`, `requests.Session` and `scoped_session` work. It also means a
  task that dirties thread local state passes it to the next task on that
  thread, and a hard timed out task pins its state, database connection
  included, until restart.
- `PyThreadState_SetAsyncExc` lands at an arbitrary bytecode boundary, so a soft
  timeout can leave a half finished session in a thread local for the next
  task. Django is partly covered because `close_old_connections` runs before and
  after every task; `requests.Session` and SQLAlchemy sessions are not. No soak
  test covers this.
- A CPU heavy task misclassified onto the sync pool starves the async loops of
  the GIL. In bench/RESULTS.md (Claim 4), a 50 ms CPU burst sharing the GIL
  with async tasks raised their p99 18 to 19 times; the cross lane case itself
  is unmeasured. When it happens `async_p99` and `loop_lag_ms` rise and every
  other stats field stays flat.
- At `--io-loops` of 2 or more, the shim's round robin (`_rr % count`) keeps
  submitting to a wedged loop until the process exits.

## Alternatives considered

- Call `exit()` and accept the atexit handlers. Rejected: heap corruption at
  shutdown.
- Let the shim own the sync pool as Python threads so CPython can free their
  thread states. Rejected: GIL churn on every handoff to pay for a teardown that
  never happens.
- Replace a wedged loop in place. Rejected for now: more code, it leaks a loop
  and its coroutines per wedge, and the round robin would need health awareness.
- Merge lanes. Sync through `to_thread` rebuilds the same pool with extra hops,
  cpu on sync threads brings back the GIL convoy, and dropping async drops the
  main use case.
- A `process_shutdown` hook run as an ordinary Python call on a helper thread,
  joined with a cap, before `_exit`. Not built. It is not finalization, so it
  does not bring back the atexit hazard, and it can be added without a protocol
  change.
