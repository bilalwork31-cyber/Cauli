# Decision: process, teardown and threading model at 1.0
> **Historical design note, not current documentation.** This is a record of how one
> pre 1.0 decision was reached and what was known when it was reached. It is kept
> because the reasoning is worth reading, not because it describes today's behaviour.
> Where it disagrees with the code, with [PROTOCOL.md](../../PROTOCOL.md) or with
> [docs/CONFIGURATION.md](../CONFIGURATION.md), those win. The status line below was
> checked against the source, not carried over.
>
> **Status: shipped in 1.0.0.** The wedge watchdog in `worker/src/loops.rs` stamps every
> embedded loop every 5 seconds and exits the process with code 87 once a loop has been
> unresponsive for 15 seconds and a second signal agrees. The other four questions were
> frozen as they were.

**The model is sound. Freeze four of five questions. One thing should not ship as is: a wedged async
loop must trigger self exit, or the flagship lane's failure mode is a permanent brownout that no
supervisor can see.**

## 1. Never a clean teardown: FREEZE, and the reasoning is better than the audit had

The decisive fact, which reframes the whole question: Python level `atexit` and end of process
`__del__` only ever run inside `Py_Finalize`, and this process can never call `Py_Finalize`
regardless, because of live daemon loop threads and unjoinable wedged sync threads. So `_exit` versus
libc `exit()` decides only whether C LIBRARY atexit handlers run, and those are exactly the
corrupting ones. Measured: OPENSSL_cleanup aborted 39 percent of shutdowns at `--io-threads 80`, and
0 percent with `_exit`.

**The unusual choice therefore forecloses nothing that avoiding `Py_Finalize` had not already
foreclosed.** That is a far stronger argument than "we chose safety over cleanliness".

It also corrects a call I made in cycle 2. I rejected a shutdown hook on the grounds that it would
reintroduce the atexit hazard. That conflates two mechanisms: a `process_shutdown` hook run as an
ordinary GIL acquiring Python call on a helper thread, joined with a hard cap, then `_exit`
regardless, is a normal Python call like any task, not finalization. It is additive to the protocol
so it need not block 1.0, but my rejection reasoning was wrong.

What users actually lose, concretely, and this was not previously identified: sentry_sdk's atexit
flush, meaning **the error batch immediately before every deploy restart silently vanishes**; the
OTel BatchSpanProcessor final flush; cProfile and coverage dumps; `NamedTemporaryFile` unlink, so
paths leak on disk though fds close; and the last few KB of buffered task `print` output.
Observability users will hit the first two.

Required before 1.0: one user facing paragraph, because CONFIGURATION.md and the README currently say
nothing at all about it. No Python atexit ever runs, do not rely on exit time cleanup, Sentry and OTel
users must flush explicitly.

Worth keeping in the model's favour: this is textbook crash only software. The recovery path IS the
startup path, exercised on every deploy, validated by 100 of 100 poisoned shutdown runs. That is a
stronger guarantee than a clean teardown that only runs on the happy path.

## 2. The thread state leak: FREEZE. The ceiling changed what it is

With the 4x ceiling added tonight it is no longer a leak, it is a bounded pool of process lifetime
thread states, which is exactly what Celery prefork provides per process. Worst case is fixed at
4 x `--io-threads`, observable as `sync_live`.

The non leaking alternative exists and is worse: let the shim own the pool as real Python threads so
CPython can tear the states down. That costs GIL churn on every handoff to buy a teardown that never
happens under crash only exit.

Promote it to user documentation as a feature with a caveat rather than a confession. Two sentences
operators need: thread locals persist across tasks on the same sync thread, which is why
`CONN_MAX_AGE`, `requests.Session` and `scoped_session` work at all; and therefore thread local state
a task dirties is inherited by the next task on that thread, and a hard timed out task pins its
state, including its database connection, until restart.

## 3. THE CHANGE: a wedged async loop must exit the process

Not acceptable to freeze. At the default `--io-loops 1`, which the configuration docs actively
recommend keeping, one blocking call in one async task ends 100 percent of async throughput for the
process lifetime, while the process keeps fetching, fails every async task at its full timeout, and
burns retry schedules into the dead letter queue. The main selling point becomes a half dead process
that every orchestrator reports as healthy.

Fail stop is unusually cheap here because the binary already ships the answer: `supervisor.rs`
restarts an unexpectedly exited child in about a second, and section 4.4 already redelivers in flight
tasks that die with the process. At least once semantics already promise exactly this.

The line that settles it: **the model already accepts that process death is recoverable; it just
never used death where death is the only recovery.**

Detection: a per loop heartbeat, Rust bumping a timestamp via `call_soon_threadsafe` every few
seconds. Wedged means the heartbeat is stale for about three intervals AND a corroborating signal,
either `async_rejected` rising or backstop timeouts firing. Two signals, so GIL starvation alone
cannot cause a false exit. Export the measured lag in the stats line regardless, because that
instrument is also the answer to question 5.

Replace in place was rejected for 1.0: more code, leaks a whole loop and its coroutines per wedge,
and the round robin in `submit_async` would need health awareness, since today `_rr % count` keeps
feeding a dead loop forever at `--io-loops` of 2 or more. Lanes are internal, so replacement can
arrive post 1.0 without breaking anything.

## 4. Three lanes: FREEZE, and 1.0 freezes less than I assumed

The three lanes map one to one onto the three execution models Python actually has: blocking code
needs OS threads releasing the GIL in IO, coroutine code needs a resident loop, GIL bound code needs
another process for both parallelism and killability. Celery has the same three and makes you pick
one per deployment, which is the ops pain this project removes by colocating them behind one gate.

The important scoping fact: the wire and registry surface is `kind` in {io, cpu} plus `is_async` plus
the per class timeout semantics. That names the task's NATURE, not the pool. The lanes are
implementation behind that surface, so post 1.0 the worker can merge, split or auto promote without a
protocol break. What 1.0 actually freezes is the classification vocabulary and the per class timeout
guarantees, and both are right.

Each proposed merge loses something measured: sync via `to_thread` rebuilds the same pool with extra
hops; cpu on sync threads reintroduces the GIL convoy, and this project's own bench measured the
async lane collapsing to 50 tasks per second under a sync ORM workload sharing its GIL; dropping
async drops the product. Three is the minimum, not an accumulation.

## 5. The incident the audit was most likely to miss: cross lane GIL convoy

One misclassified task, CPU heavy pure Python such as large JSON, ORM serialization or pandas,
running on 64 sync threads starves the async loop's scheduling. Async p99 explodes while every
current stat stays flat: inflight normal, `pending_async` flat, `async_rejected` zero. **Nothing in
the stats line measures GIL wait or loop responsiveness.** The audit hunted hangs and corruption, not
interference. Production hits this the day someone ships one wrong `kind`.

The fix is the same instrument as question 3's detector: one per loop heartbeat whose measured lag
lands in the stats line, plus a mixed workload bench asserting async p99 while the sync lane runs CPU
heavy bodies. One instrument, two problems.

Second candidate: the poisoned pinned thread, which is the direct product of the two decisions frozen
above. `PyThreadState_SetAsyncExc` lands at an arbitrary bytecode boundary, including inside cleanup
code, mid HTTP response read or mid transaction, and that thread's persistent thread locals then
carry the half broken session into the next task. Celery launders this through process recycling;
these threads live forever. Django is partly covered because `close_old_connections` reaps unusable
connections, measured in cycle 2, but `requests.Session` and SQLAlchemy sessions are not. Worth a
chaos soak firing soft timeouts at random offsets and asserting the next task on the same thread sees
clean state.

## Do this

1. Before 1.0: the loop heartbeat, its lag in the stats line, and self exit on a corroborated wedge.
   The single model change.
2. Before 1.0: one process model documentation section covering no atexit ever, thread local
   persistence semantics, the per lane timeout guarantee table, and the wedge and ceiling signals.
3. Freeze questions 1, 2 and 4 exactly as built.
4. Post 1.0, all additive: a bounded `process_shutdown` hook, a graceful path stdout flush, and the
   soft timeout poisoning soak.
